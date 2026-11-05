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
	capabilities := []string{"core/storage-v1", "core/state-view-v1", "protocol/local-v1"}
	if _, preparesReads := ss.server.backend.(api.ReadDeliveryPreparer); preparesReads {
		if _, ok := ss.server.backend.(api.BasicNodeReader); ok {
			capabilities = append(capabilities, "core/node-query-v1")
		}
		if _, ok := ss.server.backend.(api.NodeExplorer); ok {
			capabilities = append(capabilities, "core/node-explore-v1")
		}
	}
	if _, ok := ss.server.backend.(api.ContentEmissionReader); ok {
		capabilities = append(capabilities, "core/content-range-v1")
	}
	if _, ok := ss.server.backend.(api.PublicationResolver); ok {
		capabilities = append(capabilities, "core/publication-ref-v1")
	}
	if _, enabled := ss.server.agentBackend(); enabled {
		capabilities = append(capabilities, "agent/v1")
	}
	if b, ok := ss.server.backend.(interface{ EventProfileEnabled() bool }); ok && b.EventProfileEnabled() {
		capabilities = append(capabilities, "event/v1")
	}
	if _, ok := ss.server.backend.(api.GraphQueries); ok {
		capabilities = append(capabilities, "graph/query-v1")
	}
	if _, ok := ss.server.backend.(graphBackend); ok {
		capabilities = append(capabilities, "graph/v1")
	}
	if _, ok := ss.server.backend.(memoryBackend); ok {
		capabilities = append(capabilities, "memory/v1")
	}
	if _, ok := ss.server.backend.(knowledgeBackend); ok {
		capabilities = append(capabilities, "knowledge/v1")
	}
	if _, ok := ss.server.backend.(api.AnnotationCommands); ok {
		capabilities = append(capabilities, "annotation/v1")
	}
	if _, ok := ss.server.backend.(api.TransferCommands); ok {
		capabilities = append(capabilities, "transfer/archive-v1")
	}
	return ss.reply(protocol.Response{Version: protocol.Version, CorrelationID: correlation, Body: mustJSON(protocol.HelloResult{Principal: ss.principal, Spaces: cred.Spaces, Capabilities: capabilities})})
}
func (ss *session) allowed(space model.SpaceID) bool { return ss.spaces[space] }
func (ss *session) dispatch(ctx context.Context, req protocol.Request) protocol.Response {
	fail := func(err error, body any) protocol.Response {
		return protocol.Response{Version: protocol.Version, CorrelationID: req.CorrelationID, Body: mustJSON(body), Error: &protocol.Error{Code: ss.classify(err), Message: err.Error()}}
	}
	ok := func(body any) protocol.Response {
		return protocol.Response{Version: protocol.Version, CorrelationID: req.CorrelationID, Body: mustJSON(body)}
	}
	switch req.Method {
	case "resolve_publication":
		r, err := ss.resolvePublication(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "diff_graph":
		r, err := ss.diffGraph(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "plan_graph_join":
		r, err := ss.planGraphJoin(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "trace_graph":
		r, err := ss.traceGraph(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "get_query_intake":
		r, err := ss.getQueryIntake(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "get_query_progress":
		r, err := ss.getQueryProgress(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "export_transfer":
		r, err := ss.exportTransfer(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "import_transfer":
		r, err := ss.importTransfer(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "get_transfer":
		r, err := ss.getTransfer(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "apply_annotation":
		r, err := ss.applyAnnotation(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "get_annotations":
		r, err := ss.getAnnotations(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "apply_summary":
		r, err := ss.applySummary(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "get_summary":
		r, err := ss.getSummary(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "apply_emission":
		r, err := ss.applyEmission(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "get_emission":
		r, err := ss.getEmission(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "apply_agent":
		r, err := ss.applyAgent(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "get_agent_state":
		r, err := ss.getAgentState(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "apply_graph":
		r, err := ss.applyGraph(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "get_graph_revision":
		r, err := ss.getGraphRevision(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "get_graph_branch":
		r, err := ss.getGraphBranch(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "apply_memory":
		r, err := ss.applyMemory(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "get_memory":
		r, err := ss.getMemory(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "apply_knowledge":
		r, err := ss.applyKnowledge(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "get_knowledge":
		r, err := ss.getKnowledge(ctx, req.Body)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "commit":
		var w protocol.CommitRequest
		if err := protocol.DecodeStrict(req.Body, &w); err != nil {
			return fail(err, api.CommitResult{Code: api.CodeNotSubmitted})
		}
		if !ss.allowed(w.Space) {
			return fail(ErrSpaceDenied, api.CommitResult{Code: api.CodeNotSubmitted, RequestID: w.RequestID})
		}
		r, err := ss.server.backend.Commit(ctx, api.CommitRequest{Space: w.Space, Principal: ss.principal, RequestID: w.RequestID, Method: w.Method, Body: w.Body, ExpectedFence: w.ExpectedFence})
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "mutate":
		var w protocol.MutateRequest
		if err := protocol.DecodeStrict(req.Body, &w); err != nil {
			return fail(err, api.CommitResult{Code: api.CodeNotSubmitted})
		}
		if !ss.allowed(w.Space) {
			return fail(ErrSpaceDenied, api.CommitResult{Code: api.CodeNotSubmitted, RequestID: w.RequestID})
		}
		r, err := ss.server.backend.Mutate(ctx, api.MutateRequest{Key: model.RequestKey{Space: w.Space, Principal: ss.principal, RequestID: w.RequestID}, Batch: w.Batch, ExpectedFence: w.ExpectedFence})
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "lookup_request":
		var w protocol.LookupRequest
		if err := protocol.DecodeStrict(req.Body, &w); err != nil {
			return fail(err, api.LookupRequestResult{Code: api.CodeIndeterminate})
		}
		if !ss.allowed(w.Space) {
			return fail(ErrSpaceDenied, api.LookupRequestResult{Code: api.CodeIndeterminate})
		}
		r, err := ss.server.backend.LookupRequest(ctx, api.LookupRequestRequest{Space: w.Space, Principal: ss.principal, RequestID: w.RequestID})
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "get_state":
		var w protocol.ReadStateRequest
		if err := protocol.DecodeStrict(req.Body, &w); err != nil {
			return fail(err, api.StateValueResult{})
		}
		if !ss.allowed(w.Space) {
			return fail(ErrSpaceDenied, api.StateValueResult{})
		}
		r, err := ss.server.backend.GetState(ctx, api.ReadStateRequest{Space: w.Space, Key: w.Key})
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "snapshot":
		var empty protocol.Empty
		if err := protocol.DecodeStrict(req.Body, &empty); err != nil {
			return fail(err, api.StateSnapshot{})
		}
		if !ss.globalSnapshot {
			return fail(ErrUnauthorized, api.StateSnapshot{})
		}
		r, err := ss.server.backend.Snapshot(ctx)
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "open_view":
		var w protocol.OpenViewRequest
		if err := protocol.DecodeStrict(req.Body, &w); err != nil {
			return fail(err, api.ViewDescriptor{})
		}
		if !ss.allowed(w.Space) {
			return fail(ErrSpaceDenied, api.ViewDescriptor{})
		}
		r, err := ss.server.backend.OpenView(ctx, api.OpenViewRequest{Space: w.Space, Principal: ss.principal, Anchor: w.Anchor, Knowledge: w.Knowledge, Mode: w.Mode})
		if err != nil {
			return fail(err, r)
		}
		ss.views[r.Ref] = struct{}{}
		return ok(r)
	case "close_view":
		var w protocol.CloseViewRequest
		if err := protocol.DecodeStrict(req.Body, &w); err != nil {
			return fail(err, struct{}{})
		}
		if _, owned := ss.views[w.Ref]; !owned {
			return fail(ErrUnauthorized, struct{}{})
		}
		if err := ss.server.backend.CloseView(ctx, api.CloseViewRequest{Ref: w.Ref}); err != nil {
			return fail(err, struct{}{})
		}
		delete(ss.views, w.Ref)
		return ok(struct{}{})
	case "get_view_state":
		var w protocol.ReadViewRequest
		if err := protocol.DecodeStrict(req.Body, &w); err != nil {
			return fail(err, api.StateValueResult{})
		}
		if _, owned := ss.views[w.View]; !owned {
			return fail(ErrUnauthorized, api.StateValueResult{})
		}
		r, err := ss.server.backend.GetViewState(ctx, api.ReadViewStateRequest{View: w.View, Key: w.Key})
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	case "get_view_entries":
		var w protocol.ReadViewEntriesRequest
		if err := protocol.DecodeStrict(req.Body, &w); err != nil {
			return fail(err, api.StateEntriesResult{})
		}
		if _, owned := ss.views[w.View]; !owned {
			return fail(ErrUnauthorized, api.StateEntriesResult{})
		}
		backend, hasEntries := ss.server.backend.(entriesBackend)
		if !hasEntries {
			return fail(fmt.Errorf("%w: view entries", apiError(api.ErrorUnsupported)), api.StateEntriesResult{})
		}
		r, err := backend.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: w.View, Limits: w.Limits})
		if err != nil {
			return fail(err, r)
		}
		return ok(r)
	default:
		return fail(fmt.Errorf("%w: method", apiError(api.ErrorUnsupported)), struct{}{})
	}
}
func (ss *session) reply(r protocol.Response) error {
	if err := ss.conn.SetWriteDeadline(time.Now().Add(ss.server.options.RequestTimeout)); err != nil {
		return err
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return protocol.WriteFrame(ss.conn, b)
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
