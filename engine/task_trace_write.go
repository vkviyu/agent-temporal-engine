package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/investigation"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func taskTraceStateKey(principal model.PrincipalID, subject api.TraceSubject) []byte {
	var raw []byte
	for _, part := range []string{string(principal), subject.Kind, subject.ID} {
		raw, _ = codec.AppendString(raw, part)
	}
	digest := sha256.Sum256(raw)
	return []byte("ate/task-trace/v1/" + hex.EncodeToString(digest[:]))
}

func (e *Engine) ApplyTaskTrace(ctx context.Context, req api.ApplyTaskTraceRequest) (api.CommitResult, error) {
	body, err := api.EncodeTaskTraceEdit(req.Edit)
	if err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	return e.Commit(ctx, api.CommitRequest{Space: req.Key.Space, Principal: req.Key.Principal, RequestID: req.Key.RequestID, Method: api.MethodApplyTaskTrace, Body: body})
}

func (e *Engine) loadTaskTrace(ctx context.Context, roots model.RootSet, req api.TaskTraceRequest) (api.TaskTraceEdit, model.ObjectID, error) {
	raw, id, found, err := domainEventValue(ctx, e.reader(ctx), roots.ExecutionKnowledge, req.Space, taskTraceStateKey(req.Principal, req.Subject))
	if err != nil || !found {
		return api.TaskTraceEdit{}, "", err
	}
	edit, err := api.DecodeTaskTraceEdit(raw)
	if err != nil || edit.Subject != req.Subject || edit.Source.Space != req.Space {
		return api.TaskTraceEdit{}, "", ErrStoredObject
	}
	return edit, id, nil
}

func (e *Engine) prepareTaskTrace(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord) (*commitPlan, error) {
	if !e.TaskTraceEnabled() {
		return nil, ErrUnsupportedMethod
	}
	edit, err := api.DecodeTaskTraceEdit(req.Body)
	if err != nil {
		return nil, err
	}
	if req.ExpectedFence != nil || validAgentScope(req.Space, req.Principal) != nil || edit.Source.Space != req.Space {
		return nil, api.ErrInvalidRequest
	}
	scope := api.TaskTraceRequest{Space: req.Space, Principal: req.Principal, Subject: edit.Subject}
	prior, old, err := e.loadTaskTrace(ctx, e.roots, scope)
	if err != nil {
		return nil, err
	}
	if edit.ExpectedVersion == math.MaxUint64 || (old == "" && edit.ExpectedVersion != 0) || (old != "" && (edit.ExpectedVersion != prior.ExpectedVersion+1 || edit.Source.Fence.LSN < prior.Source.Fence.LSN)) {
		return nil, errCAS
	}
	roots, _, err := e.rootsAtFence(ctx, edit.Source.Fence, true)
	if err != nil {
		return nil, err
	}
	ledger, _, _, err := loadAgentLedger(ctx, e.reader(ctx), roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return nil, err
	}
	snapshot := ledger.Snapshot()
	keys, err := taskTraceScope(scope, ledger, snapshot)
	if err != nil {
		return nil, err
	}
	inScope := make(map[traceEventKey]bool, len(keys))
	for _, key := range keys {
		inScope[key] = true
	}
	var dependencies []model.ObjectID
	provenance := newTraceProvenance(roots, snapshot)
	activities := map[string][]model.ObjectID{}
	for _, milestone := range edit.Milestones {
		event, err := e.visibleEvent(ctx, milestone.Event, edit.Source, req.Principal)
		if err != nil {
			return nil, err
		}
		a := event.Attribution
		if a == nil || !(inScope[traceEventKey{req.Space, req.Principal, "query", a.Query}] ||
			inScope[traceEventKey{req.Space, req.Principal, "task", string(a.Task)}] ||
			inScope[traceEventKey{req.Space, req.Principal, "execution", string(a.Execution)}]) {
			return nil, ErrNodeNotFound
		}
		ids, err := e.traceOccurrenceDependencies(ctx, scope, provenance, activities, event)
		if err != nil {
			return nil, err
		}
		dependencies = append(dependencies, ids...)
	}
	deps, err := e.taskTraceCitations(ctx, req, edit, provenance, activities)
	if err != nil {
		return nil, err
	}
	dependencies = append(dependencies, deps...)
	if err := e.checkDisclosureIDs(ctx, req.Space, dependencies); err != nil {
		return nil, err
	}
	command, err := makeCommand(req)
	if err != nil {
		return nil, err
	}
	commandID, err := codec.ObjectID(command)
	if err != nil {
		return nil, err
	}
	value, err := makeValue(req.Space, api.Value{Kind: api.ValueBytes, Data: req.Body})
	if err != nil {
		return nil, err
	}
	valueID, err := codec.ObjectID(value)
	if err != nil {
		return nil, err
	}
	return e.prepareDerivedValue(ctx, req, digest, txID, lsn, recorded, taskTraceStateKey(req.Principal, edit.Subject), req.Body, map[model.ObjectID][]model.ObjectID{commandID: dependencies, valueID: dependencies})
}

func (e *Engine) taskTraceCitations(ctx context.Context, req api.CommitRequest, edit api.TaskTraceEdit, provenance traceProvenance, activities map[string][]model.ObjectID) ([]model.ObjectID, error) {
	var deps []model.ObjectID
	seen := map[string]bool{}
	scope := api.TaskTraceRequest{Space: req.Space, Principal: req.Principal, Subject: edit.Subject}
	add := func(citations []api.TraceCitation) error {
		for _, c := range citations {
			raw, _ := json.Marshal(c)
			if seen[string(raw)] {
				continue
			}
			seen[string(raw)] = true
			ids, err := e.investigationCitationDependencies(ctx, req.Space, req.Principal, c, "state")
			if err != nil {
				return err
			}
			deps = append(deps, ids...)
			if c.Ref.Kind == model.NodeEvent {
				event, err := e.visibleEvent(ctx, c.Ref.Event, c.Anchor, req.Principal)
				if err != nil {
					return err
				}
				ids, err := e.traceOccurrenceDependencies(ctx, scope, provenance, activities, event)
				if err != nil {
					return err
				}
				deps = append(deps, ids...)
			}
			if len(deps) > maxDerivationEdges {
				return investigation.ErrLimit
			}
		}
		return nil
	}
	for _, milestone := range edit.Milestones {
		if err := add(milestone.Evidence); err != nil {
			return nil, err
		}
	}
	for _, edge := range edit.Assertions {
		if err := add(edge.Evidence); err != nil {
			return nil, err
		}
	}
	return deps, nil
}

func taskTraceRejection(err error) bool {
	return errors.Is(err, api.ErrInvalidRequest) || errors.Is(err, ErrUnsupportedMethod) || investigationCommandRejection(err)
}
