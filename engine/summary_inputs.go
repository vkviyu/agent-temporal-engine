package engine

import (
	"bytes"
	"context"
	"reflect"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// checkSummaryInputs authenticates a decoded summary before constructing its
// commit plan. Historical source membership and current authorization are
// separate checks; generation and replacement proofs require published roots.
func (e *Engine) checkSummaryInputs(ctx context.Context, req api.CommitRequest, summary knowledge.Summary) error {
	source := summary.Source
	roots, rootID, err := e.rootsAtFence(ctx, source.Anchor.Fence, true)
	if err != nil {
		return err
	}
	key := *source.Anchor.Knowledge
	for _, scope := range []model.RootSet{roots, e.roots} {
		if err := e.authorizeKnowledge(ctx, scope, req.Space, req.Principal, key); err != nil {
			return err
		}
	}
	if source.ViewDigest != "" && source.ViewDigest != viewDigest(source.Anchor, rootID, req.Principal, "execution_knowledge") {
		return model.ErrAnchorMismatch
	}
	known, _, _, _, err := e.selectedKnowledgeObservations(ctx, roots, source.Anchor, req.Principal)
	if err != nil {
		return err
	}
	if err := knowledge.VerifySummarySources(summary, known); err != nil {
		return err
	}
	input, err := e.summaryContent(ctx, req, summary.Generation.Input)
	if err != nil {
		return err
	}
	output, err := e.summaryContent(ctx, req, summary.Generation.Output)
	if err != nil {
		return err
	}
	for _, evidence := range summary.Validation.Evidence {
		if _, err := e.summaryContent(ctx, req, evidence); err != nil {
			return err
		}
	}
	if err := e.checkSummaryGeneration(ctx, req, summary, input, output); err != nil {
		return err
	}
	if summary.Replaces != "" {
		latest := e.agentPublishedAnchor(req.Space)
		priorRoots, _, err := e.rootsAtFence(ctx, latest.Fence, true)
		if err != nil {
			return err
		}
		prior, _, found, err := e.loadSummary(ctx, priorRoots, req.Space, req.Principal, summary.Replaces)
		if err != nil {
			return err
		}
		if !found || prior.Source.Anchor.Knowledge == nil || *prior.Source.Anchor.Knowledge != key {
			return knowledge.ErrInvalidSummary
		}
	}
	dependencies, err := e.summaryDependencyIDs(ctx, roots, req.Space, req.Principal, summaryParents(summary))
	if err != nil {
		return err
	}
	if err := e.checkPolicyIDs(ctx, req.Space, append(summaryPolicyIDs(summary), dependencies...), memory.AccessReuse); err != nil {
		return err
	}
	return verifyContentClosure(ctx, e.index, physicalReferences(knowledge.SummaryContentReferences(summary)))
}

func (e *Engine) summaryContent(ctx context.Context, req api.CommitRequest, content knowledge.RecordedContent) ([]byte, error) {
	roots, _, err := e.rootsAtFence(ctx, content.Anchor.Fence, true)
	if err != nil {
		return nil, err
	}
	if err := e.visibleSourceObject(ctx, roots, content.Anchor, req.Principal, content.Object); err != nil {
		return nil, err
	}
	object, err := readObject(ctx, e.index, content.Object)
	if err != nil {
		return nil, err
	}
	if object.Type != valueType || object.Space != req.Space {
		return nil, knowledge.ErrInvalidSummary
	}
	value, err := readValue(object)
	if err != nil {
		return nil, err
	}
	if value.Kind != api.ValueBytes {
		return nil, knowledge.ErrInvalidSummary
	}
	return value.Data, nil
}

func (e *Engine) checkSummaryGeneration(ctx context.Context, req api.CommitRequest, summary knowledge.Summary, input, output []byte) error {
	// Generation must already be accepted at a published prefix. A private
	// same-group acceptance is not a publication proof for this source record.
	latest := e.agentPublishedAnchor(req.Space)
	roots, _, err := e.rootsAtFence(ctx, latest.Fence, true)
	if err != nil {
		return err
	}
	ledger, _, _, err := loadAgentLedger(ctx, e.index, roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return err
	}
	snapshot := ledger.Snapshot()
	spec, err := activitySpecInSnapshot(snapshot, summary.Generation.Activity)
	if err != nil {
		return err
	}
	if err := checkSummaryActivity(summary, spec, input); err != nil {
		return err
	}
	result, err := ledger.GetActivityResult(summary.Generation.Result)
	if err != nil {
		return err
	}
	// Bind the successful output to the exact request and claimed attempt
	// before checking whether this result was accepted.
	if result.Activity != spec.Activity || result.Attempt != summary.Generation.Attempt ||
		result.RequestDigest != spec.RequestDigest || result.Outcome != "success" ||
		!bytes.Equal(result.Payload, output) {
		return knowledge.ErrInvalidSummary
	}
	accepted := false
	if snapshot.Lifecycle.Recovery != nil {
		for _, control := range snapshot.Lifecycle.Recovery.ActivityControls {
			if control.Activity == spec.Activity && control.AcceptedResult == result.ID {
				accepted = true
				break
			}
		}
	}
	if !accepted {
		return knowledge.ErrInvalidSummary
	}
	record, err := e.activityPacketAt(ctx, req.Space, req.Principal, spec.InputFence, spec.ContextPacket)
	if err != nil {
		return err
	}
	return checkSummaryPacket(summary, record.packet)
}

// checkSummaryActivity binds generation to a pure execution-scoped summarizer
// and the exact recorded request. summary has already passed DecodeSummary.
func checkSummaryActivity(summary knowledge.Summary, spec agentprofile.ActivitySpec, input []byte) error {
	if spec.Role != "aux_summary" || spec.Execution != summary.Source.Anchor.Knowledge.Execution ||
		spec.Query != "" || spec.Effect != "pure" {
		return knowledge.ErrInvalidSummary
	}
	if spec.Request != summary.Generation.Input.Object ||
		spec.RequestDigest != agentprofile.ExactRequestDigest(input) {
		return knowledge.ErrInvalidSummary
	}
	if spec.Recipient != summary.Generation.Model || spec.ContextPacket == "" {
		return knowledge.ErrInvalidSummary
	}
	return nil
}

// checkSummaryPacket binds the historical source cut and rendered input to the
// packet used by generation. Only observations actually included may be cited.
func checkSummaryPacket(summary knowledge.Summary, packet knowledge.ContextPacket) error {
	if !reflect.DeepEqual(packet.Source, summary.Source) || packet.Input == nil {
		return knowledge.ErrInvalidSummary
	}
	input := summary.Generation.Input
	if packet.Input.Object != input.Object || !reflect.DeepEqual(packet.Input.Anchor, input.Anchor) ||
		packet.Input.Encoding != input.Encoding {
		return knowledge.ErrInvalidSummary
	}
	included := make(map[model.ObjectID]model.ObjectID, len(packet.Items))
	for _, item := range packet.Items {
		included[item.Observation] = item.Content
	}
	for _, source := range summary.Sources {
		if included[source.Observation] != source.Content {
			return knowledge.ErrInvalidSummary
		}
	}
	return nil
}
