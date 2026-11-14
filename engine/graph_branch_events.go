package engine

import (
	"context"
	"encoding/base64"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func localGraphEventDrafts(ctx context.Context, req api.CommitRequest, reader state.ObjectReader, roots model.RootSet, c graph.Command, linked bool) ([]profileEventDraft, error) {
	var operation, revisionOperation string
	var position graph.BranchPosition
	var authority graph.ExecutionAuthority
	var inputs []model.RevisionID
	switch {
	case c.ForkLocal != nil:
		operation = "Graph.ForkLocal"
		position = graph.BranchPosition{Branch: c.ForkLocal.Branch, Head: c.ForkLocal.Source}
		authority = c.ForkLocal.Authority
	case c.WriteBranch != nil:
		operation, revisionOperation = "Graph.WriteBranch", "Graph.WriteRevision"
		position, authority = c.WriteBranch.Target, c.WriteBranch.Authority
	case c.JoinLocal != nil:
		operation, revisionOperation = "Graph.JoinLocal", "Graph.JoinLocalRevision"
		position, authority = c.JoinLocal.Target, c.JoinLocal.Authority
		inputs = append(inputs, c.JoinLocal.Source.Head)
	case c.PublishBranch != nil:
		operation = "Graph.PublishBranch"
		position, authority = c.PublishBranch.Source, c.PublishBranch.Authority
	default:
		return nil, ErrUnsupportedSchema
	}
	inputs = append(inputs, position.Head)
	branch, _, err := graphBranchAt(ctx, reader, roots, req.Space, position.Branch)
	if err != nil {
		return nil, err
	}
	d := domainDraft(operation, "graph", "Branch", string(branch.ID))
	d.factAliases = []profileEventCause{branchPositionFact(branch.ID, branch.Version)}
	d.actorAuthorized = authority.Execution != ""
	d.attribution.Execution = authority.Execution
	if authority.Execution != "" {
		snapshot, err := eventAgentSnapshot(ctx, reader, roots, req)
		if err != nil {
			return nil, err
		}
		bindProfileOwner(&d, snapshot)
		bindEventTask(&d, snapshot)
		if linked && c.ForkLocal != nil {
			linkBranchAdmission(&d, snapshot)
		}
	}
	if revisionOperation == "" {
		d.causes = append(d.causes, localRevisionCauses(inputs...)...)
		if c.PublishBranch != nil {
			d.causes = append(d.causes, branchPositionFact(position.Branch, position.Version))
		}
		return []profileEventDraft{d}, nil
	}
	r := d
	r.factAliases = nil
	r.attribution.Operation = revisionOperation
	r.attribution.Subject = domainSubject("graph", "Revision", string(branch.Head))
	r.causes = localRevisionCauses(inputs...)
	r.causes = append(r.causes, branchPositionFact(position.Branch, position.Version))
	if c.JoinLocal != nil {
		r.eventParents = c.JoinLocal.Events
		source := c.JoinLocal.Source
		r.causes = append(r.causes, branchPositionFact(source.Branch, source.Version))
	}
	d.causes = []profileEventCause{{revisionOperation, string(branch.Head)}}
	return []profileEventDraft{r, d}, nil
}

func localPruneEventDraft(ctx context.Context, req api.CommitRequest, reader state.ObjectReader, roots model.RootSet, branch graph.Branch, command graph.PruneBranchCommand) ([]profileEventDraft, error) {
	d := domainDraft("Graph.Prune", "graph", "Branch", string(branch.ID))
	d.attribution.Execution = branch.Scope.Execution
	if d.attribution.Execution != "" {
		snapshot, err := eventAgentSnapshot(ctx, reader, roots, req)
		if err != nil {
			return nil, err
		}
		bindProfileOwner(&d, snapshot)
		bindEventTask(&d, snapshot)
		// The principal prunes the branch. Execution is the branch owner.
		d.stream = model.EventStreamKey{}
	}
	d.causes = localRevisionCauses(command.Head)
	d.causes = append(d.causes, branchPositionFact(branch.ID, command.Expected))
	d.factAliases = []profileEventCause{branchPositionFact(branch.ID, branch.Version)}
	return []profileEventDraft{d}, nil
}

func linkBranchAdmission(d *profileEventDraft, snapshot agentprofile.Snapshot) {
	execution := d.attribution.Execution
	if recovery := snapshot.Lifecycle.Recovery; recovery != nil {
		for _, start := range recovery.Starts {
			if start.Execution == execution {
				d.causedBy("Control.StartWork", recoveryNodeID(start.Query, start.Slot))
				return
			}
		}
	}
	d.causedBy("Control.SpawnChild", string(execution))
	d.causedBy("Spawn", string(execution))
}

func branchPositionFact(branch model.BranchID, version uint64) profileEventCause {
	raw, _ := codec.AppendString(nil, string(branch))
	raw = codec.AppendUint(raw, version)
	return profileEventCause{"Graph.BranchPosition", base64.RawURLEncoding.EncodeToString(raw)}
}

func localRevisionCauses(ids ...model.RevisionID) []profileEventCause {
	causes := graphRevisionCauses(ids...)
	for _, id := range ids {
		if id != "" {
			causes = append(causes, profileEventCause{"Graph.WriteRevision", string(id)}, profileEventCause{"Graph.JoinLocalRevision", string(id)})
		}
	}
	return causes
}
