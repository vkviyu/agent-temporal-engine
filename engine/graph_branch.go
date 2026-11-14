package engine

import (
	"context"
	"fmt"
	"math"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func (e *Engine) branchProducer(ctx context.Context, req api.CommitRequest, branch graph.Branch, auth graph.ExecutionAuthority) (graph.RevisionProducer, error) {
	if branch.Scope == nil || branch.Scope.Principal != req.Principal || branch.Scope.Execution != auth.Execution {
		return graph.RevisionProducer{}, graph.ErrBranchScope
	}
	producer := graph.RevisionProducer{Branch: branch.ID, Principal: req.Principal}
	if auth.Execution == "" {
		return producer, nil
	}
	if !e.options.AgentProfile {
		return graph.RevisionProducer{}, graph.ErrBranchScope
	}
	l, _, _, err := loadAgentLedger(ctx, e.index, e.roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return graph.RevisionProducer{}, err
	}
	binding := agentprofile.AuthorityBinding{Deployment: auth.Deployment, Holder: auth.Holder, OwnerEpoch: auth.OwnerEpoch, Incarnation: auth.Incarnation}
	execution, task, version, err := l.AuthorizeExecution(auth.Execution, binding)
	if err != nil {
		return graph.RevisionProducer{}, fmt.Errorf("%w: %w", graph.ErrBranchScope, err)
	}
	if task.Owner != req.Principal || task.Space != req.Space {
		return graph.RevisionProducer{}, graph.ErrBranchScope
	}
	producer.Execution, producer.Task, producer.Actor = execution.ID, task.ID, execution.Actor
	producer.GoalEpoch, producer.Incarnation, producer.AuthorityVersion = task.GoalEpoch, execution.Incarnation, version
	return producer, nil
}

// A protected branch intent belongs to the same execution and authority that
// reserved its RequestKey. Legacy Graph schema 1 retained opaque host intents.
func checkGraphActionIntent(intent agentprofile.ActionIntent) error {
	if intent.Method != api.MethodApplyGraph || !graph.LocalCommand(intent.ExactCommand) {
		return nil
	}
	space, c, err := graph.DecodeCommand(intent.ExactCommand)
	if err != nil {
		return agentprofile.ErrCommandShape
	}
	var authority graph.ExecutionAuthority
	switch {
	case c.ForkLocal != nil:
		authority = c.ForkLocal.Authority
	case c.WriteBranch != nil:
		authority = c.WriteBranch.Authority
		if _, err := DecodeStateBatch(c.WriteBranch.Batch); err != nil {
			return agentprofile.ErrCommandShape
		}
	case c.JoinLocal != nil:
		authority = c.JoinLocal.Authority
	case c.PublishBranch != nil:
		authority = c.PublishBranch.Authority
	default:
		return nil
	}
	binding := intent.Authority
	if space != intent.Action.Request.Space || authority != (graph.ExecutionAuthority{Execution: intent.Action.Owner, Deployment: binding.Deployment, Holder: binding.Holder, OwnerEpoch: binding.OwnerEpoch, Incarnation: binding.Incarnation}) {
		return agentprofile.ErrCommandScope
	}
	return nil
}

func (e *Engine) localBranchAt(ctx context.Context, req api.CommitRequest, pos graph.BranchPosition) (graph.Branch, error) {
	branch, prune, err := graphBranchAt(ctx, e.index, e.roots, req.Space, pos.Branch)
	if err != nil {
		return graph.Branch{}, err
	}
	if prune != nil {
		return graph.Branch{}, graph.ErrBranchPruned
	}
	if branch.Scope == nil || branch.Scope.Principal != req.Principal {
		return graph.Branch{}, graph.ErrBranchScope
	}
	if branch.Head != pos.Head || branch.Version != pos.Version {
		return graph.Branch{}, errCAS
	}
	return branch, nil
}

func (e *Engine) prepareForkLocal(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord, c graph.ForkLocalCommand) (*commitPlan, error) {
	base, err := e.graphRevision(ctx, req.Space, c.Source)
	if err != nil {
		return nil, err
	}
	if base.Producer != nil && base.Producer.Principal != req.Principal {
		return nil, graph.ErrBranchScope
	}
	if err = e.checkBranchUse(ctx, req.Space, base); err != nil {
		return nil, err
	}
	if _, exists, err := e.graphRecord(ctx, req.Space, graphBranchKey(c.Branch)); err != nil {
		return nil, err
	} else if exists {
		return nil, errCAS
	}
	branch := graph.Branch{ID: c.Branch, Head: c.Source, Scope: &graph.BranchScope{Principal: req.Principal, Execution: c.Authority.Execution, Base: c.Source}}
	if _, err = e.branchProducer(ctx, req, branch, c.Authority); err != nil {
		return nil, err
	}
	raw, err := graph.EncodeBranch(branch)
	if err != nil {
		return nil, err
	}
	return e.prepareTypedValue(ctx, req, digest, txID, lsn, recorded, graphBranchKey(branch.ID), raw)
}

func (e *Engine) checkBranchUse(ctx context.Context, space model.SpaceID, revisions ...graph.Revision) error {
	var ids []model.ObjectID
	for _, revision := range revisions {
		ids = append(ids, model.ObjectID(revision.ID))
		ids = append(ids, graph.ContentReferences(revision)...)
	}
	return e.checkPolicyIDs(ctx, space, ids, memory.AccessReuse)
}

// prepareWriteBranch derives the new revision from the pinned branch root,
// including when the coordinator prepares several writes in one group.

func (e *Engine) prepareWriteBranch(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord, c graph.WriteBranchCommand) (*commitPlan, error) {
	branch, err := e.localBranchAt(ctx, req, c.Target)
	if err != nil {
		return nil, err
	}
	producer, err := e.branchProducer(ctx, req, branch, c.Authority)
	if err != nil {
		return nil, err
	}
	if branch.Version == math.MaxUint64 {
		return nil, graph.ErrLimit
	}
	base, err := e.graphRevision(ctx, req.Space, branch.Head)
	if err != nil {
		return nil, err
	}
	if err = e.checkBranchUse(ctx, req.Space, base); err != nil {
		return nil, err
	}
	batch, err := DecodeStateBatch(c.Batch)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid branch state batch: %s", graph.ErrCommandShape, err)
	}
	b := newBuilder(ctx, e.index, e.options.MaxObjects)
	delta, _, err := applyStateBatch(ctx, b, req.Space, base.StateRoot, batch)
	if err != nil {
		return nil, err
	}
	entries, err := state.Entries(ctx, b, req.Space, delta.Root, state.ScanLimits{})
	if err != nil {
		return nil, err
	}
	values := make([]graph.Entry, len(entries))
	for i, entry := range entries {
		values[i] = graph.Entry{Key: entry.Key, Value: entry.Value}
	}
	producer.BranchVersion = branch.Version + 1
	revision, err := graph.BuildProducedRevision(req.Space, base.ID, nil, delta.Root, nil, values, producer)
	if err != nil {
		return nil, err
	}
	return e.finishLocalRevision(ctx, req, digest, txID, lsn, recorded, b, branch, revision)
}

func (e *Engine) prepareJoinLocal(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord, c graph.JoinLocalCommand) (*commitPlan, error) {
	target, err := e.localBranchAt(ctx, req, c.Target)
	if err != nil {
		return nil, err
	}
	if _, err = e.localBranchAt(ctx, req, c.Source); err != nil {
		return nil, err
	}
	producer, err := e.branchProducer(ctx, req, target, c.Authority)
	if err != nil {
		return nil, err
	}
	if target.Version == math.MaxUint64 {
		return nil, graph.ErrLimit
	}
	reader := &cursorReader{source: e.index}
	g, records, err := loadGraphParents(ctx, reader, e.roots, req.Space, []model.RevisionID{c.Target.Head, c.Source.Head})
	if err != nil {
		return nil, err
	}
	plan, err := g.PlanJoin(c.Target.Head, c.Source.Head)
	if err != nil {
		return nil, err
	}
	base, left, right := records[plan.Base], records[c.Target.Head], records[c.Source.Head]
	if err = e.checkBranchUse(ctx, req.Space, base, left, right); err != nil {
		return nil, err
	}
	allowed := make(map[model.ObjectID]bool)
	for _, revision := range []graph.Revision{base, left, right} {
		if err = verifyGraphEntries(ctx, reader, revision); err != nil {
			return nil, err
		}
		for _, item := range revision.Entries {
			allowed[item.Value] = true
		}
	}
	for _, resolution := range c.Resolutions {
		if resolution.Value != "" && !allowed[resolution.Value] {
			return nil, graph.ErrStateUnverified
		}
		if err = plan.Resolve(resolution.Key, resolution.Value); err != nil {
			return nil, err
		}
	}
	if len(plan.Conflicts) != 0 {
		return nil, graph.ErrJoinConflict
	}
	for _, id := range c.Events {
		if !e.options.EventProfile || !e.eventKnown(id, req.Space, req.Principal) {
			return nil, graph.ErrStateUnverified
		}
	}
	b := newBuilder(ctx, e.index, e.options.MaxObjects)
	root, err := applyGraphChanges(ctx, b, req.Space, left.StateRoot, plan.Changes, false)
	if err != nil {
		return nil, err
	}
	producer.BranchVersion = target.Version + 1
	revision, err := graph.BuildProducedRevision(req.Space, left.ID, []model.RevisionID{right.ID}, root, c.Events, plan.State, producer)
	if err != nil {
		return nil, err
	}
	return e.finishLocalRevision(ctx, req, digest, txID, lsn, recorded, b, target, revision)
}

func (e *Engine) finishLocalRevision(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord, b *objectBuilder, branch graph.Branch, revision graph.Revision) (*commitPlan, error) {
	if err := verifyContentClosure(ctx, b, graph.ContentReferences(revision)); err != nil {
		return nil, err
	}
	branch.Head = revision.ID
	branch.Version++
	rawRevision, err := graph.EncodeRevision(revision)
	if err != nil {
		return nil, err
	}
	rawBranch, err := graph.EncodeBranch(branch)
	if err != nil {
		return nil, err
	}
	return e.prepareGraphValues(ctx, req, digest, txID, lsn, recorded, b, []graphValueWrite{{key: graphRevisionKey(revision.ID), raw: rawRevision}, {key: graphBranchKey(branch.ID), raw: rawBranch}}, nil)
}

func (e *Engine) preparePublishBranch(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord, c graph.PublishBranchCommand) (*commitPlan, error) {
	branch, err := e.localBranchAt(ctx, req, c.Source)
	if err != nil {
		return nil, err
	}
	if _, err = e.branchProducer(ctx, req, branch, c.Authority); err != nil {
		return nil, err
	}
	if branch.Version == math.MaxUint64 {
		return nil, graph.ErrLimit
	}
	current, err := domainRoot(ctx, e.index, e.roots, req.Space)
	if err != nil {
		return nil, err
	}
	if current != c.ExpectedDomainRoot {
		return nil, errCAS
	}
	base := branch.PublishedHead
	if base == "" {
		base = branch.Scope.Base
	}
	reader := &cursorReader{source: e.index}
	g, records, err := loadGraphParents(ctx, reader, e.roots, req.Space, []model.RevisionID{branch.Head})
	if err != nil {
		return nil, err
	}
	if _, exists := records[base]; !exists {
		return nil, ErrInvalidStateData
	}
	if err = e.checkBranchUse(ctx, req.Space, records[base], records[branch.Head]); err != nil {
		return nil, err
	}
	diff, err := g.Diff(base, branch.Head)
	if err != nil {
		return nil, err
	}
	b := newBuilder(ctx, e.index, e.options.MaxObjects)
	root, err := applyGraphChanges(ctx, b, req.Space, current, diff.Changes, true)
	if err != nil {
		return nil, err
	}
	branch.PublishedHead = branch.Head
	branch.Version++
	raw, err := graph.EncodeBranch(branch)
	if err != nil {
		return nil, err
	}
	return e.prepareGraphValues(ctx, req, digest, txID, lsn, recorded, b, []graphValueWrite{{key: graphBranchKey(branch.ID), raw: raw}}, &root)
}

func applyGraphChanges(ctx context.Context, b *objectBuilder, space model.SpaceID, root model.ObjectID, changes []graph.Change, publish bool) (model.ObjectID, error) {
	mutations := make([]state.Mutation, 0, len(changes))
	for _, change := range changes {
		if publish {
			value, _, err := state.Get(ctx, b, space, root, change.Key)
			if err != nil {
				return "", err
			}
			if value == change.After {
				continue
			}
			if value != change.Before {
				return "", fmt.Errorf("%w: key %x", graph.ErrPublishConflict, change.Key)
			}
		}
		mutations = append(mutations, state.Mutation{Key: change.Key, Value: change.After, Delete: change.After == state.EmptyRoot})
	}
	for start := 0; start < len(mutations); start += state.MaxMutations {
		end := min(start+state.MaxMutations, len(mutations))
		delta, err := state.Apply(ctx, b, space, root, mutations[start:end])
		if err != nil {
			return "", err
		}
		for _, obj := range delta.Objects {
			if _, err = b.add(obj, storage.KindObject); err != nil {
				return "", err
			}
		}
		root = delta.Root
	}
	return root, nil
}
