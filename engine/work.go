package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var ErrWorkBoundary = errors.New("work unit boundary rejected")

var _ api.WorkCommands = (*Engine)(nil)

func (e *Engine) ApplyWork(ctx context.Context, r api.ApplyWorkRequest) (api.CommitResult, error) {
	b, err := api.EncodeWorkUnit(r.Unit)
	if err != nil {
		return admissionFailure(r.Key.RequestID, err)
	}
	return e.Commit(ctx, api.CommitRequest{Space: r.Key.Space, Principal: r.Key.Principal, RequestID: r.Key.RequestID, Method: api.MethodApplyWork, Body: b, ExpectedFence: r.ExpectedFence})
}

// workView shares only immutable publication proofs with its parent. No
// intermediate fence, receipt, event or authoritative history entry is created.
func (e *Engine) workView() *Engine {
	return &Engine{privateReduction: true, options: e.options, roots: e.roots, rootSet: e.rootSet,
		replayingEvents: e.replayingEvents, replayEventRegistry: e.replayEventRegistry,
		publication: e.publication, index: newStagingIndex(e.index), journal: e.journal,
		history: e.history, baseHistory: e.baseHistory, envelopes: e.envelopes, baseEnvelopes: e.baseEnvelopes,
		events: e.events, baseEvents: e.baseEvents, eventHeads: e.eventHeads, baseEventHeads: e.baseEventHeads}
}

func absorbWorkPlan(ctx context.Context, v *Engine, b *objectBuilder, p *commitPlan) error {
	for _, object := range p.builder.objects {
		o, err := codec.DecodeObject(object.encoded)
		if err != nil {
			return err
		}
		if _, err = b.add(o, object.kind); err != nil {
			return err
		}
		v.index.staged[object.id] = bytes.Clone(object.encoded)
		v.index.entries[object.id] = storage.RecordLocation{PayloadOffset: -1, PayloadLength: uint32(len(object.encoded))}
	}
	v.roots = p.roots
	return ctx.Err()
}

func (e *Engine) prepareWork(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord) (*commitPlan, error) {
	ctx = context.WithValue(ctx, workAgentImageKey{}, &workAgentImage{})
	unit, view, builder, commandID, err := e.prepareWorkInput(ctx, req)
	if err != nil {
		return nil, err
	}
	if unit.Kind == "query_intake" {
		if err = view.prepareQueryIntake(ctx, req, unit, digest, txID, lsn, builder); err != nil {
			return nil, err
		}
	} else if err = e.prepareWorkSteps(ctx, req, unit, view, builder, digest, txID, lsn); err != nil {
		return nil, err
	}
	return e.finishWorkPlan(ctx, req, unit, view, builder, commandID, digest, txID, lsn, recorded)
}

func workCommandRejection(err error) bool {
	return errors.Is(err, ErrWorkBoundary) || errors.Is(err, api.ErrInvalidRequest) || agentCommandRejection(err) || knowledgeCommandRejection(err) || emissionCommandRejection(err)
}

func intakeKey(space model.SpaceID, owner model.PrincipalID, query string) []byte {
	b, _ := codec.AppendString(nil, "work/intake/v1")
	for _, s := range []string{string(space), string(owner), query} {
		b, _ = codec.AppendString(b, s)
	}
	return b
}
func intakeReservationKey(key model.RequestKey) []byte {
	b, _ := codec.AppendString(nil, "work/intake-request/v1")
	return append(b, receiptKey(key)...)
}

func decisionKey(space model.SpaceID, owner model.PrincipalID, activity string) []byte {
	b, _ := codec.AppendString(nil, "work/decision/v1")
	for _, s := range []string{string(space), string(owner), activity} {
		b, _ = codec.AppendString(b, s)
	}
	return b
}

func (e *Engine) writeWorkRecord(ctx context.Context, space model.SpaceID, key, raw []byte, b *objectBuilder) error {
	o, err := makeValue(space, api.Value{Kind: api.ValueBytes, Data: raw})
	if err != nil {
		return err
	}
	id, err := b.add(o, storage.KindObject)
	if err != nil {
		return err
	}
	delta, err := state.Apply(ctx, b, systemSpace, e.roots.LiveControl, []state.Mutation{{Key: key, Value: id}})
	if err != nil {
		return err
	}
	for _, node := range delta.Objects {
		if _, err = b.add(node, storage.KindObject); err != nil {
			return err
		}
	}
	// These roots are a private candidate, and later records may read them
	// before P1. Make their immutable objects visible to that candidate only.
	for _, object := range b.objects {
		e.index.staged[object.id] = bytes.Clone(object.encoded)
		e.index.entries[object.id] = storage.RecordLocation{PayloadOffset: -1, PayloadLength: uint32(len(object.encoded))}
	}
	e.roots.LiveControl = delta.Root
	return nil
}
func (e *Engine) readIntakeValue(ctx context.Context, key []byte) (api.QueryIntakeState, bool, error) {
	return e.readIntakeValueAt(ctx, e.readRoots(ctx), key)
}

func (e *Engine) readIntakeValueAt(ctx context.Context, roots model.RootSet, key []byte) (api.QueryIntakeState, bool, error) {
	id, found, err := state.Get(ctx, e.reader(ctx), systemSpace, roots.LiveControl, key)
	if err != nil || !found {
		return api.QueryIntakeState{}, found, err
	}
	o, err := readObject(ctx, e.reader(ctx), id)
	if err != nil {
		return api.QueryIntakeState{}, false, err
	}
	value, err := readValue(o)
	if err != nil || value.Kind != api.ValueBytes {
		return api.QueryIntakeState{}, false, ErrInvalidStateData
	}
	var s api.QueryIntakeState
	if err = json.Unmarshal(value.Data, &s); err != nil {
		return s, false, ErrInvalidStateData
	}
	raw, err := json.Marshal(s)
	if err != nil || !bytes.Equal(raw, value.Data) || s.Intake.Query.Space != o.Space {
		return s, false, ErrInvalidStateData
	}
	return s, true, nil
}
func (e *Engine) loadQueryIntake(ctx context.Context, space model.SpaceID, owner model.PrincipalID, query string) (api.QueryIntakeState, bool, error) {
	return e.loadQueryIntakeAt(ctx, e.readRoots(ctx), space, owner, query)
}

func (e *Engine) loadQueryIntakeAt(ctx context.Context, roots model.RootSet, space model.SpaceID, owner model.PrincipalID, query string) (api.QueryIntakeState, bool, error) {
	s, found, err := e.readIntakeValueAt(ctx, roots, intakeKey(space, owner, query))
	if err == nil && found && (s.Intake.Query.Space != space || s.Intake.Query.Owner != owner || s.Intake.Query.ID != query) {
		return s, false, ErrInvalidStateData
	}
	return s, found, err
}
func (e *Engine) writeQueryIntake(ctx context.Context, req api.CommitRequest, s api.QueryIntakeState, b *objectBuilder) error {
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	o, err := makeValue(req.Space, api.Value{Kind: api.ValueBytes, Data: raw})
	if err != nil {
		return err
	}
	id, err := b.add(o, storage.KindObject)
	if err != nil {
		return err
	}
	mutations := []state.Mutation{{Key: intakeKey(req.Space, req.Principal, s.Intake.Query.ID), Value: id}, {Key: intakeReservationKey(s.Intake.Next.Key()), Value: id}}
	delta, err := state.Apply(ctx, b, systemSpace, e.roots.LiveControl, mutations)
	if err != nil {
		return err
	}
	for _, node := range delta.Objects {
		if _, err = b.add(node, storage.KindObject); err != nil {
			return err
		}
	}
	// Make private objects available to subsequent work steps, without publishing.
	for _, object := range b.objects {
		e.index.staged[object.id] = bytes.Clone(object.encoded)
		e.index.entries[object.id] = storage.RecordLocation{PayloadOffset: -1, PayloadLength: uint32(len(object.encoded))}
	}
	e.roots.LiveControl = delta.Root
	return nil
}
func (e *Engine) prepareQueryIntake(ctx context.Context, req api.CommitRequest, u api.WorkUnit, digest model.ObjectID, txID model.TxID, lsn uint64, b *objectBuilder) error {
	q := u.Intake
	if q.Query.Space != req.Space || q.Query.Owner != req.Principal || q.Next.Space != req.Space || q.Next.Principal != req.Principal || q.Next.Key() == req.Key() || !e.knownAgentFence(q.Query.InputFence) {
		return ErrWorkBoundary
	}
	next, err := api.DecodeWorkUnit(q.Next.Body)
	if err != nil || (next.Kind != "query_start" && next.Kind != "query_open") || next.Query != u.Query {
		return ErrWorkBoundary
	}
	if _, found, err := e.loadQueryIntake(ctx, req.Space, req.Principal, u.Query); err != nil {
		return err
	} else if found {
		return agentprofile.ErrUniqueSlot
	}
	if _, found, err := e.readIntakeValue(ctx, intakeReservationKey(q.Next.Key())); err != nil {
		return err
	} else if found {
		return agentprofile.ErrUniqueSlot
	}
	if _, found, err := e.lookup(ctx, q.Next.Key()); err != nil {
		return err
	} else if found {
		return agentprofile.ErrUniqueSlot
	}
	body, err := agentprofile.EncodeCommand(agentprofile.Command{Lifecycle: &agentprofile.LifecycleCommand{PutQuery: &q.Query}})
	if err != nil {
		return err
	}
	sub := api.CommitRequest{Space: req.Space, Principal: req.Principal, RequestID: req.RequestID, Method: api.MethodApplyAgent, Body: body}
	p, err := e.prepareAgent(ctx, sub, digest, txID, lsn, nil)
	if err != nil {
		return err
	}
	if err = absorbWorkPlan(ctx, e, b, p); err != nil {
		return err
	}
	return e.writeQueryIntake(ctx, req, api.QueryIntakeState{Intake: *q}, b)
}

func (e *Engine) checkQueryReservation(ctx context.Context, req api.CommitRequest) error {
	s, found, err := e.readIntakeValue(ctx, intakeReservationKey(req.Key()))
	if err != nil || !found {
		return err
	}
	next := s.Intake.Next
	if next.Key() != req.Key() || next.Method != req.Method || !bytes.Equal(next.Body, req.Body) || !sameExpectedFence(next.ExpectedFence, req.ExpectedFence) {
		return fmt.Errorf("%w: query start request", ErrActionReservation)
	}
	return nil
}

func (e *Engine) GetQueryIntake(ctx context.Context, req api.ReadQueryIntakeRequest) (api.QueryIntakeResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "get_query_intake", req.Principal, req, func(scoped context.Context) (api.QueryIntakeResult, error) {
			return e.GetQueryIntake(scoped, req)
		})
	}
	if err := validAgentScope(req.Space, req.Principal); err != nil {
		return api.QueryIntakeResult{}, err
	}
	if req.Query == "" || len(req.Query) > model.MaxIdentityBytes {
		return api.QueryIntakeResult{}, api.ErrInvalidRequest
	}
	if !e.options.AgentProfile {
		return api.QueryIntakeResult{}, ErrUnsupportedMethod
	}
	if err := e.publicGate(); err != nil {
		return api.QueryIntakeResult{}, err
	}
	if err := e.readReady(ctx); err != nil {
		return api.QueryIntakeResult{}, err
	}
	a := model.SnapshotAnchor{Space: req.Space, Fence: e.readPublication(ctx).Fence()}
	roots := e.readRoots(ctx)
	if req.Anchor != nil {
		a = req.Anchor.Clone()
		if a.Validate() != nil || a.Space != req.Space || a.Knowledge != nil || a.Cursor != nil {
			return api.QueryIntakeResult{}, model.ErrAnchorMismatch
		}
		var err error
		roots, _, err = e.rootsAtFence(ctx, a.Fence, false)
		if err != nil {
			return api.QueryIntakeResult{}, err
		}
	}
	s, found, err := e.loadQueryIntakeAt(ctx, roots, req.Space, req.Principal, req.Query)
	if err != nil {
		return api.QueryIntakeResult{}, e.readFailureAt(ctx, err)
	}
	var ids []model.ObjectID
	if found {
		ids, err = e.checkQueryIntakeDisclosure(ctx, roots, req.Space, req.Principal, s.Intake)
		if err != nil {
			if errors.Is(err, memory.ErrHistoryAccessDenied) {
				return api.QueryIntakeResult{}, err
			}
			return api.QueryIntakeResult{}, e.knowledgeReadFailure(ctx, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return api.QueryIntakeResult{}, err
	}
	if err := e.viewOutputGate(); err != nil {
		return api.QueryIntakeResult{}, err
	}
	out := api.QueryIntakeResult{Found: found, State: s, Anchor: a}
	return finishReadDelivery(ctx, e, "get_query_intake", req.Principal, out, a, "", "", ids)
}
