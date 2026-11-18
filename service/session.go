package service

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func (s *Server) run(ss *session) {
	ctx, cancel := context.WithCancel(s.life)
	input := make(chan incoming)
	readDone := make(chan struct{})
	// One reader may own at most one additional bounded frame while dispatch is
	// running. EOF cancels the waiter promptly; the backend still owns accepted
	// command execution and will not undo admission because of this context.
	go func() {
		defer close(readDone)
		defer cancel()
		first := true
		for {
			maximum := s.options.MaxFrameBytes
			if first && maximum > 64<<10 {
				maximum = 64 << 10
			}
			_ = ss.conn.SetReadDeadline(time.Now().Add(s.options.RequestTimeout))
			frame, err := protocol.ReadFrameLimit(ss.conn, maximum)
			first = false
			if err != nil {
				return
			}
			req, err := protocol.DecodeRequest(frame)
			select {
			case input <- incoming{req, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		cancel()
		_ = ss.conn.Close()
		<-readDone
		var cleanupErr error
		for ref := range ss.views {
			cleanupErr = errors.Join(cleanupErr, s.backend.CloseView(context.Background(), api.CloseViewRequest{Ref: ref}))
		}
		s.mu.Lock()
		s.shutdownErr = errors.Join(s.shutdownErr, cleanupErr)
		delete(s.sessions, ss)
		s.notifyLocked()
		s.mu.Unlock()
	}()
	for {
		var next incoming
		select {
		case next = <-input:
		case <-ctx.Done():
			return
		}
		if next.err != nil {
			_ = ss.reply(ss.errResponse("?", next.err))
			return
		}
		req := next.request
		if req.Method == "hello" {
			if ss.principal != "" {
				_ = ss.reply(ss.errResponse(req.CorrelationID, ErrUnauthorized))
				return
			}
			if err := ss.hello(req.Body, req.CorrelationID); err != nil {
				_ = ss.reply(ss.errResponse(req.CorrelationID, err))
				return
			}
			continue
		}
		if ss.principal == "" {
			_ = ss.reply(ss.errResponse(req.CorrelationID, ErrUnauthorized))
			return
		}
		callCtx, finish := context.WithTimeout(ctx, s.options.RequestTimeout)
		if handled, err := ss.serveReadRequest(callCtx, req); handled {
			finish()
			if err != nil {
				return
			}
			continue
		}
		resp := ss.dispatch(callCtx, req)
		finish()
		if ctx.Err() != nil {
			return
		}
		if err := ss.reply(resp); err != nil {
			return
		}
	}
}
func (ss *session) hello(raw []byte, correlation string) error {
	var req protocol.HelloRequest
	if err := protocol.DecodeStrict(raw, &req); err != nil {
		return err
	}
	hash := sha256.Sum256([]byte(req.Token))
	var cred Credential
	valid := false
	for _, entry := range ss.server.credentials {
		if subtle.ConstantTimeCompare(entry.digest[:], hash[:]) == 1 {
			cred = entry.credential
			valid = true
		}
	}
	if !valid {
		return ErrUnauthorized
	}
	ss.principal = cred.Principal
	ss.spaces = make(map[model.SpaceID]bool, len(cred.Spaces))
	for _, space := range cred.Spaces {
		ss.spaces[space] = true
	}
	ss.globalSnapshot = cred.AllowGlobalSnapshot
	result := protocol.HelloResult{
		Principal:    ss.principal,
		Spaces:       cred.Spaces,
		Capabilities: ss.server.Capabilities(),
	}
	return ss.reply(protocol.Response{
		Version:       protocol.Version,
		CorrelationID: correlation,
		Body:          mustJSON(result),
	})
}

// Capabilities returns a fresh list of the capabilities supported by the
// backend's optional interfaces and enabled profiles, without reading store data.
func (s *Server) Capabilities() []string {
	capabilities := []string{"core/storage-v1", "core/state-view-v1", "protocol/local-v1"}
	if _, preparesReads := s.backend.(api.ReadDeliveryPreparer); preparesReads {
		if _, ok := s.backend.(api.BasicNodeReader); ok {
			capabilities = append(capabilities, "core/node-query-v1")
		}
		if _, ok := s.backend.(api.NodeExplorer); ok {
			capabilities = append(capabilities, "core/node-explore-v1")
		}
	}
	if _, ok := s.backend.(api.ContentEmissionReader); ok {
		capabilities = append(capabilities, "core/content-range-v1")
	}
	if _, ok := s.backend.(api.PublicationResolver); ok {
		capabilities = append(capabilities, "core/publication-ref-v1")
	}
	if _, enabled := s.agentBackend(); enabled {
		capabilities = append(capabilities, "agent/v1")
	}
	if b, ok := s.backend.(agentRecordBackend); ok && b.AgentProfileEnabled() {
		capabilities = append(capabilities, "agent/record-read-v1")
	}
	if b, ok := s.backend.(interface{ EventProfileEnabled() bool }); ok && b.EventProfileEnabled() {
		capabilities = append(capabilities, "event/v1")
	}
	if b, ok := s.backend.(taskTraceBackend); ok && b.TaskTraceEnabled() {
		capabilities = append(capabilities, "agent/task-trace-v1")
	}
	if _, ok := s.backend.(api.GraphQueries); ok {
		capabilities = append(capabilities, "graph/query-v1")
	}
	if _, ok := s.backend.(graphBackend); ok {
		capabilities = append(capabilities, "graph/v1")
	}
	if _, ok := s.backend.(memoryBackend); ok {
		capabilities = append(capabilities, "memory/v1")
	}
	if _, ok := s.backend.(knowledgeBackend); ok {
		capabilities = append(capabilities, "knowledge/v1")
	}
	if _, ok := s.backend.(api.AnnotationCommands); ok {
		capabilities = append(capabilities, "annotation/v1")
	}
	if _, ok := s.backend.(investigationBackend); ok {
		capabilities = append(capabilities, "investigation/v1")
	}
	if _, ok := s.backend.(api.TransferCommands); ok {
		capabilities = append(capabilities, "transfer/archive-v1")
	}
	return capabilities
}
func (ss *session) allowed(space model.SpaceID) bool { return ss.spaces[space] }

func dispatchTyped[T any](ss *session, ctx context.Context, req protocol.Request, call func(context.Context, []byte) (T, error)) protocol.Response {
	result, err := call(ctx, req.Body)
	if err != nil {
		return protocol.Response{
			Version: protocol.Version, CorrelationID: req.CorrelationID,
			Body:  mustJSON(result),
			Error: &protocol.Error{Code: ss.classify(err), Message: err.Error()},
		}
	}
	return protocol.Response{Version: protocol.Version, CorrelationID: req.CorrelationID, Body: mustJSON(result)}
}

func (ss *session) dispatch(ctx context.Context, req protocol.Request) protocol.Response {
	fail := func(err error, body any) protocol.Response {
		return protocol.Response{Version: protocol.Version, CorrelationID: req.CorrelationID, Body: mustJSON(body), Error: &protocol.Error{Code: ss.classify(err), Message: err.Error()}}
	}
	ok := func(body any) protocol.Response {
		return protocol.Response{Version: protocol.Version, CorrelationID: req.CorrelationID, Body: mustJSON(body)}
	}
	switch req.Method {
	case "commit", "mutate", "lookup_request", "get_state", "snapshot":
		return ss.dispatchCore(ctx, req, fail, ok)
	case "open_view", "close_view", "get_view_state", "get_view_entries":
		return ss.dispatchView(ctx, req, fail, ok)
	}
	switch req.Method {
	case "resolve_publication":
		return dispatchTyped(ss, ctx, req, ss.resolvePublication)
	case "diff_graph":
		return dispatchTyped(ss, ctx, req, ss.diffGraph)
	case "plan_graph_join":
		return dispatchTyped(ss, ctx, req, ss.planGraphJoin)
	case "trace_graph":
		return dispatchTyped(ss, ctx, req, ss.traceGraph)
	case "get_query_intake":
		return dispatchTyped(ss, ctx, req, ss.getQueryIntake)
	case "get_query_progress":
		return dispatchTyped(ss, ctx, req, ss.getQueryProgress)
	case "export_transfer":
		return dispatchTyped(ss, ctx, req, ss.exportTransfer)
	case "import_transfer":
		return dispatchTyped(ss, ctx, req, ss.importTransfer)
	case "get_transfer":
		return dispatchTyped(ss, ctx, req, ss.getTransfer)
	case "apply_annotation":
		return dispatchTyped(ss, ctx, req, ss.applyAnnotation)
	case "get_annotations":
		return dispatchTyped(ss, ctx, req, ss.getAnnotations)
	case "update_investigation":
		return dispatchTyped(ss, ctx, req, ss.updateInvestigation)
	case "get_investigation":
		return dispatchTyped(ss, ctx, req, ss.getInvestigation)
	case "apply_task_trace":
		return dispatchTyped(ss, ctx, req, ss.applyTaskTrace)
	case "get_task_trace":
		return dispatchTyped(ss, ctx, req, ss.getTaskTrace)
	case "get_task_trace_evidence":
		return dispatchTyped(ss, ctx, req, ss.getTaskTraceEvidence)
	case "apply_summary":
		return dispatchTyped(ss, ctx, req, ss.applySummary)
	case "get_summary":
		return dispatchTyped(ss, ctx, req, ss.getSummary)
	case "apply_emission":
		return dispatchTyped(ss, ctx, req, ss.applyEmission)
	case "get_emission":
		return dispatchTyped(ss, ctx, req, ss.getEmission)
	case "apply_agent":
		return dispatchTyped(ss, ctx, req, ss.applyAgent)
	case "get_agent_state":
		return dispatchTyped(ss, ctx, req, ss.getAgentState)
	case "get_agent_state_records":
		return dispatchTyped(ss, ctx, req, ss.getAgentStateRecords)
	case "apply_graph":
		return dispatchTyped(ss, ctx, req, ss.applyGraph)
	case "get_graph_revision":
		return dispatchTyped(ss, ctx, req, ss.getGraphRevision)
	case "get_graph_branch":
		return dispatchTyped(ss, ctx, req, ss.getGraphBranch)
	case "apply_memory":
		return dispatchTyped(ss, ctx, req, ss.applyMemory)
	case "get_memory":
		return dispatchTyped(ss, ctx, req, ss.getMemory)
	case "apply_knowledge":
		return dispatchTyped(ss, ctx, req, ss.applyKnowledge)
	case "get_knowledge":
		return dispatchTyped(ss, ctx, req, ss.getKnowledge)
	default:
		return fail(fmt.Errorf("%w: method", apiError(api.ErrorUnsupported)), struct{}{})
	}
}
func (ss *session) reply(r protocol.Response) error {
	b, err := encodeResponse(r)
	if errors.Is(err, protocol.ErrFrameLimit) {
		// Reject the entire result before writing even a frame header. This is
		// an application limit, so a subsequent request can reuse the session.
		b, err = encodeResponse(ss.errResponse(r.CorrelationID, protocol.ErrFrameLimit))
	}
	if err != nil {
		return err
	}
	if err := ss.conn.SetWriteDeadline(time.Now().Add(ss.server.options.RequestTimeout)); err != nil {
		return err
	}
	return protocol.WriteFrame(ss.conn, b)
}

// The full JSON envelope (including escaping and correlation ID), not just the
// typed body, must fit the unchanged protocol ceiling.
func encodeResponse(r protocol.Response) ([]byte, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(b) > protocol.MaxFrameBytes {
		return nil, protocol.ErrFrameLimit
	}
	return b, nil
}
func (ss *session) errResponse(id string, err error) protocol.Response {
	return protocol.Response{Version: protocol.Version, CorrelationID: id, Body: json.RawMessage(`{}`), Error: &protocol.Error{Code: ss.classify(err), Message: err.Error()}}
}
func mustJSON(v any) json.RawMessage { b, _ := json.Marshal(v); return b }

type errorClassifier interface{ ClassifyError(error) api.ErrorClass }

func (ss *session) classify(err error) api.ErrorClass {
	if code, ok := classifyLocal(err); ok {
		return code
	}
	if c, ok := ss.server.backend.(errorClassifier); ok {
		code := c.ClassifyError(err)
		if protocol.ValidErrorClass(code) {
			return code
		}
	}
	return api.ErrorInternal
}
func classifyLocal(err error) (api.ErrorClass, bool) {
	var remote *protocol.Error
	if errors.As(err, &remote) {
		return remote.Code, true
	}
	switch {
	case errors.Is(err, ErrUnauthorized), errors.Is(err, ErrSpaceDenied):
		return api.ErrorUnauthorized, true
	case errors.Is(err, protocol.ErrFrameLimit):
		return api.ErrorResourceLimit, true
	case errors.Is(err, protocol.ErrProtocol):
		return api.ErrorInvalidRequest, true
	case err == context.Canceled:
		return api.ErrorCanceled, true
	case err == context.DeadlineExceeded:
		return api.ErrorDeadline, true
	}
	return "", false
}
