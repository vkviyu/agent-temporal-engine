package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func emissionDigest(data []byte) model.ObjectID {
	s := sha256.Sum256(data)
	return model.ObjectID(hex.EncodeToString(s[:]))
}

func checkEmissionOwner(ledger *agentprofile.Ledger, req api.CommitRequest, s api.EmissionSpec, authority agentprofile.AuthorityBinding) error {
	if s.Query != "" {
		q, ok := queryDefinition(ledger, s.Query)
		if !ok || q.Space != req.Space || q.Owner != req.Principal || q.Status != "open" || q.Version != s.GoalEpoch || authority != (agentprofile.AuthorityBinding{}) || s.AuthorityVersion != 0 {
			return ErrEmissionRejected
		}
		return nil
	}
	x, t, a, err := emissionAuthority(ledger, s.Execution)
	if err != nil {
		return fmt.Errorf("%w: owner", ErrEmissionRejected)
	}
	if t.Status != agentprofile.TaskOpen || x.Status == agentprofile.ExecutionCanceled || x.Status == agentprofile.ExecutionCompleted || x.Task != s.Task || t.Space != req.Space || t.Owner != req.Principal || t.GoalEpoch != s.GoalEpoch || a.Binding != authority || a.Version != s.AuthorityVersion {
		return ErrEmissionRejected
	}
	return nil
}

func checkEmissionActivity(ledger *agentprofile.Ledger, s api.EmissionSpec, proof *api.EmissionProof) error {
	if s.Activity == "" {
		return nil
	}
	spec, err := queryActivitySpec(ledger, s.Activity)
	if err != nil {
		return fmt.Errorf("%w: activity", ErrEmissionRejected)
	}
	if spec.Execution != s.Execution || spec.Query != s.Query || spec.GoalEpoch != s.GoalEpoch || spec.RequestDigest != s.RequestDigest || spec.Recipient != s.Destination {
		return ErrEmissionRejected
	}
	if spec.ContextPacket != "" && (s.ContentKind != "packet" || s.Content != spec.ContextPacket) {
		return ErrEmissionRejected
	}
	recovery := ledger.Snapshot().Lifecycle.Recovery
	if recovery == nil {
		return ErrEmissionRejected
	}
	for _, attempt := range recovery.AttemptControls {
		if attempt.Attempt != s.Attempt {
			continue
		}
		if attempt.Activity != s.Activity || attempt.RequestDigest != s.RequestDigest || attempt.Authority != s.Authority || attempt.Status != "created" {
			return ErrEmissionRejected
		}
		if proof != nil && (proof.ContentDigest != s.RequestDigest || s.Offset != 0 || s.Length != proof.TotalBytes || proof.RangeDigest != s.RequestDigest) {
			return ErrEmissionRejected
		}
		return nil
	}
	return ErrEmissionRejected
}

// resolveTypedEmission returns actual recorded model/output bytes, never a
// JSON rendering of Packet/Summary metadata. Selectors are authenticated at the
// exact published source cut; current policy is checked separately by Issue.
func (e *Engine) resolveTypedEmission(ctx context.Context, req api.CommitRequest, s api.EmissionSpec) ([]byte, []model.ObjectID, error) {
	if s.Anchor.Space != req.Space {
		return nil, nil, agentprofile.ErrCommandScope
	}
	roots, _, err := e.rootsAtFence(ctx, s.Anchor.Fence, true)
	if err != nil {
		return nil, nil, err
	}
	var object model.ObjectID
	var anchor model.SnapshotAnchor
	var ids []model.ObjectID
	switch s.ContentKind {
	case "packet":
		var snap knowledge.Snapshot
		if s.Anchor.Knowledge != nil {
			snap, _, _, _, err = e.selectedKnowledge(ctx, roots, s.Anchor, req.Principal)
		} else if s.Anchor.Cursor != nil {
			return nil, nil, ErrEmissionRejected
		} else {
			snap, _, _, err = e.loadKnowledge(ctx, roots, req.Space, req.Principal)
		}
		if err != nil {
			return nil, nil, err
		}
		found := false
		for _, packet := range snap.Packets {
			if packet.ID != s.Content {
				continue
			}
			if packet.Input == nil || (s.Execution == "" && packet.Key != nil) || (s.Execution != "" && (packet.Key == nil || packet.Key.Execution != s.Execution)) {
				return nil, nil, ErrEmissionRejected
			}
			object, anchor = packet.Input.Object, packet.Input.Anchor
			ids = append(ids, packetPolicyIDs(packet, snap)...)
			selected := map[model.ObjectID]bool{}
			for _, item := range packet.Items {
				selected[item.Observation] = true
			}
			var observations []knowledge.Observation
			for _, o := range snap.Observations {
				if selected[o.ID] {
					observations = append(observations, o)
				}
			}
			dependencies, err := e.summaryDependencyIDs(ctx, roots, req.Space, req.Principal, observationSummaryIDs(observations))
			if err != nil {
				return nil, nil, err
			}
			ids = append(ids, dependencies...)
			found = true
			break
		}
		if !found {
			return nil, nil, ErrEmissionRejected
		}
	case "summary":
		summary, _, found, err := e.loadSummary(ctx, roots, req.Space, req.Principal, s.Content)
		if err != nil {
			return nil, nil, err
		}
		if !found || summary.Validation.Status == "rejected" {
			return nil, nil, ErrEmissionRejected
		}
		if s.Anchor.Knowledge != nil {
			snap, _, _, _, err := e.selectedKnowledge(ctx, roots, s.Anchor, req.Principal)
			if err != nil {
				return nil, nil, err
			}
			adopted := false
			for _, o := range snap.Observations {
				adopted = adopted || (o.PayloadKind == "summary" && o.Payload == s.Content)
			}
			if !adopted {
				return nil, nil, ErrEmissionRejected
			}
		} else if s.Anchor.Cursor != nil {
			return nil, nil, ErrEmissionRejected
		}
		object, anchor = summary.Generation.Output.Object, summary.Generation.Output.Anchor
		ids, err = e.summaryDependencyIDs(ctx, roots, req.Space, req.Principal, []model.ObjectID{s.Content})
		if err != nil {
			return nil, nil, err
		}
	default:
		return nil, nil, ErrEmissionRejected
	}
	if anchor.Space != req.Space || anchor.Fence.Store != s.Anchor.Fence.Store || anchor.Fence.LSN > s.Anchor.Fence.LSN {
		return nil, nil, ErrEmissionRejected
	}
	outputRoots, _, err := e.rootsAtFence(ctx, anchor.Fence, true)
	if err != nil {
		return nil, nil, err
	}
	if err := e.visibleSourceObject(ctx, outputRoots, anchor, req.Principal, object); err != nil {
		return nil, nil, err
	}
	o, err := readObject(ctx, e.index, object)
	if err != nil {
		return nil, nil, err
	}
	data, err := objectContentProjection(o, "value_bytes")
	if err != nil {
		return nil, nil, err
	}
	ids = append(ids, s.Content, object)
	return data, ids, nil
}

// prepareEmissionProof resolves the exact outbound projection and records its
// explicitly declared provenance. No implicit rendering or EOF truncation is
// allowed. Policy is evaluated at the candidate's current cut on every call.
func (e *Engine) prepareEmissionProof(ctx context.Context, req api.CommitRequest, ledger *agentprofile.Ledger, s api.EmissionSpec) (*api.EmissionProof, error) {
	var content []byte
	var ids []model.ObjectID
	var err error
	projection := s.Projection
	if s.ContentKind == "packet" || s.ContentKind == "summary" {
		projection = "value_bytes"
		content, ids, err = e.resolveTypedEmission(ctx, req, s)
	} else {
		if s.Anchor.Cursor != nil || s.Anchor.Knowledge != nil {
			return nil, ErrEmissionRejected
		}
		if err = e.checkEmissionContent(ctx, req, s); err != nil {
			return nil, err
		}
		object, er := readObject(ctx, e.index, s.Content)
		if er != nil {
			return nil, er
		}
		content, err = objectContentProjection(object, projection)
		ids = []model.ObjectID{s.Content}
		if projection == "" {
			projection = "payload"
		}
	}
	if err != nil {
		return nil, err
	}
	if s.Offset > uint64(len(content)) || s.Length > uint64(len(content))-s.Offset {
		return nil, ErrEmissionRejected
	}
	if s.Activity != "" {
		spec, err := queryActivitySpec(ledger, s.Activity)
		if err != nil {
			return nil, ErrEmissionRejected
		}
		if err := e.checkCurrentActivityUse(ctx, req, ledger, spec); err != nil {
			return nil, err
		}
		dependencies, err := e.activityInputDisclosureIDs(ctx, req.Space, req.Principal, spec)
		if err != nil {
			return nil, err
		}
		ids = append(ids, dependencies...)
		if spec.Contract.Definition != "" {
			ids = append(ids, spec.Contract.Definition)
		}
		if spec.Invocation != "" {
			inv, err := ledger.GetInvocation(spec.Invocation)
			if err != nil {
				return nil, ErrEmissionRejected
			}
			ids = append(ids, inv.ToolDefinition)
		}
		if spec.Query != "" {
			p, found, err := e.loadQueryProgress(ctx, req, spec.Query)
			if err != nil {
				return nil, err
			}
			if found {
				if inv, ok := queryInvocation(p, spec.Activity); ok {
					ids = append(ids, inv.Definition)
				}
			}
		}
	}
	current, _, _, err := e.loadMemory(ctx, e.roots, req.Space)
	if err != nil {
		return nil, err
	}
	dependencies, err := current.AccessPolicy().DependencyClosure(ids)
	if err != nil {
		return nil, err
	}
	if err := current.AccessPolicy().Check(dependencies, memory.AccessReuse); err != nil {
		return nil, err
	}
	digest, err := api.EmissionDependencyDigest(dependencies)
	if err != nil {
		return nil, err
	}
	return &api.EmissionProof{Space: req.Space, Principal: req.Principal, PolicyFence: e.publication.Fence(), PolicyRoot: e.roots.ExecutionKnowledge, Dependencies: dependencies, DependencyDigest: digest, Projection: projection, ContentDigest: emissionDigest(content), RangeDigest: emissionDigest(content[s.Offset : s.Offset+s.Length]), TotalBytes: uint64(len(content))}, nil
}

// validateEmissionDispatch is the sole protected publication boundary between
// a single-use grant and a managed external attempt. No arbitrary extra steps
// may be smuggled into this Work and no general checkpoint may dispatch.
func (e *Engine) validateEmissionDispatch(ctx context.Context, req api.CommitRequest, u api.WorkUnit) error {
	if len(u.Steps) != 2 || u.Steps[0].Method != api.MethodApplyEmission || u.Steps[1].Method != api.MethodApplyAgent {
		return ErrWorkBoundary
	}
	claim, err := api.DecodeEmissionCommand(u.Steps[0].Body)
	if err != nil || claim.ClaimID == "" {
		return ErrWorkBoundary
	}
	command, err := agentprofile.DecodeCommand(u.Steps[1].Body)
	if err != nil || command.Control == nil || command.Control.DispatchAttempt == nil {
		return ErrWorkBoundary
	}
	dispatch := command.Control.DispatchAttempt
	grant, found, err := e.loadEmission(ctx, e.roots, req.Space, req.Principal, claim.ClaimID)
	if err != nil {
		return err
	}
	if !found || grant.Status != "issued" || grant.Version != claim.ClaimVersion || grant.Spec.Activity == "" || grant.Spec.Attempt != dispatch.Attempt || grant.Spec.Query != u.Query || grant.Spec.Authority != dispatch.Authority || claim.ClaimAuthority != dispatch.Authority {
		return ErrWorkBoundary
	}
	return nil
}
