package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func summaryStateKey(space model.SpaceID, principal model.PrincipalID, id model.ObjectID) []byte {
	owned, _ := codec.AppendString(nil, string(space))
	owned, _ = codec.AppendString(owned, string(principal))
	digest := sha256.Sum256(owned)
	return []byte("ate/summary/v1/" + hex.EncodeToString(digest[:]) + "/" + string(id))
}

func (e *Engine) loadSummary(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID, id model.ObjectID) (knowledge.Summary, model.ObjectID, bool, error) {
	root, err := slotSpaceRoot(ctx, e.reader(ctx), roots.ExecutionKnowledge, space)
	if err != nil {
		return knowledge.Summary{}, "", false, err
	}
	record, found, err := state.Get(ctx, e.reader(ctx), space, root, summaryStateKey(space, principal, id))
	if err != nil || !found {
		return knowledge.Summary{}, "", found, err
	}
	object, err := readObject(ctx, e.reader(ctx), record)
	if err != nil {
		return knowledge.Summary{}, "", false, err
	}
	value, err := readValue(object)
	if err != nil || object.Space != space || value.Kind != api.ValueBytes {
		return knowledge.Summary{}, "", false, fmt.Errorf("%w: summary record", ErrStoredObject)
	}
	summary, err := knowledge.DecodeSummary(value.Data)
	if err != nil || summary.Space != space || summary.ID != id {
		return knowledge.Summary{}, "", false, fmt.Errorf("%w: summary identity", ErrStoredObject)
	}
	return summary, record, true, nil
}

func (e *Engine) ApplySummary(ctx context.Context, req api.ApplySummaryRequest) (api.CommitResult, error) {
	if err := ctx.Err(); err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	body, err := req.Summary.Canonical()
	if err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	return e.Commit(ctx, api.CommitRequest{Space: req.Key.Space, Principal: req.Key.Principal, RequestID: req.Key.RequestID, Method: api.MethodApplySummary, Body: body, ExpectedFence: req.ExpectedFence})
}

func summaryPolicyIDs(summary knowledge.Summary) []model.ObjectID {
	ids := append([]model.ObjectID{summary.ID, summary.Replaces}, knowledge.SummaryContentReferences(summary)...)
	for _, source := range summary.Sources {
		ids = append(ids, source.Observation, source.Content)
	}
	for _, omitted := range summary.Omissions {
		ids = append(ids, omitted.Observation)
	}
	return ids
}

func summaryParents(summary knowledge.Summary) []model.ObjectID {
	var parents []model.ObjectID
	for _, source := range summary.Sources {
		if source.PayloadKind == "summary" {
			parents = append(parents, source.Content)
		}
	}
	return parents
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
	if spec.Role != "aux_summary" || spec.Execution != summary.Source.Anchor.Knowledge.Execution || spec.Query != "" || spec.Effect != "pure" || spec.Request != summary.Generation.Input.Object || spec.RequestDigest != agentprofile.ExactRequestDigest(input) || spec.Recipient != summary.Generation.Model || spec.ContextPacket == "" {
		return knowledge.ErrInvalidSummary
	}
	result, err := ledger.GetActivityResult(summary.Generation.Result)
	if err != nil {
		return err
	}
	if result.Activity != spec.Activity || result.Attempt != summary.Generation.Attempt || result.RequestDigest != spec.RequestDigest || result.Outcome != "success" || !bytes.Equal(result.Payload, output) {
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
	packet := record.packet
	if !reflect.DeepEqual(packet.Source, summary.Source) || packet.Input == nil || packet.Input.Object != summary.Generation.Input.Object || !reflect.DeepEqual(packet.Input.Anchor, summary.Generation.Input.Anchor) || packet.Input.Encoding != summary.Generation.Input.Encoding {
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

func (e *Engine) prepareSummary(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord) (*commitPlan, error) {
	if !e.options.AgentProfile {
		return nil, ErrUnsupportedMethod
	}
	if err := validAgentScope(req.Space, req.Principal); err != nil {
		return nil, err
	}
	if req.ExpectedFence != nil && *req.ExpectedFence != e.publication.Fence() {
		return nil, errCAS
	}
	summary, err := knowledge.DecodeSummary(req.Body)
	if err != nil {
		return nil, err
	}
	if summary.Space != req.Space {
		return nil, agentprofile.ErrCommandScope
	}
	source := summary.Source
	roots, rootID, err := e.rootsAtFence(ctx, source.Anchor.Fence, true)
	if err != nil {
		return nil, err
	}
	key := *source.Anchor.Knowledge
	for _, scope := range []model.RootSet{roots, e.roots} {
		if err := e.authorizeKnowledge(ctx, scope, req.Space, req.Principal, key); err != nil {
			return nil, err
		}
	}
	if source.ViewDigest != "" && source.ViewDigest != viewDigest(source.Anchor, rootID, req.Principal, "execution_knowledge") {
		return nil, model.ErrAnchorMismatch
	}
	known, _, _, _, err := e.selectedKnowledgeObservations(ctx, roots, source.Anchor, req.Principal)
	if err != nil {
		return nil, err
	}
	if err := knowledge.VerifySummarySources(summary, known); err != nil {
		return nil, err
	}
	input, err := e.summaryContent(ctx, req, summary.Generation.Input)
	if err != nil {
		return nil, err
	}
	output, err := e.summaryContent(ctx, req, summary.Generation.Output)
	if err != nil {
		return nil, err
	}
	for _, evidence := range summary.Validation.Evidence {
		if _, err := e.summaryContent(ctx, req, evidence); err != nil {
			return nil, err
		}
	}
	if err := e.checkSummaryGeneration(ctx, req, summary, input, output); err != nil {
		return nil, err
	}
	if summary.Replaces != "" {
		latest := e.agentPublishedAnchor(req.Space)
		priorRoots, _, err := e.rootsAtFence(ctx, latest.Fence, true)
		if err != nil {
			return nil, err
		}
		prior, _, found, err := e.loadSummary(ctx, priorRoots, req.Space, req.Principal, summary.Replaces)
		if err != nil {
			return nil, err
		}
		if !found || prior.Source.Anchor.Knowledge == nil || *prior.Source.Anchor.Knowledge != key {
			return nil, knowledge.ErrInvalidSummary
		}
	}
	dependencies, err := e.summaryDependencyIDs(ctx, roots, req.Space, req.Principal, summaryParents(summary))
	if err != nil {
		return nil, err
	}
	if err := e.checkPolicyIDs(ctx, req.Space, append(summaryPolicyIDs(summary), dependencies...), memory.AccessReuse); err != nil {
		return nil, err
	}
	if err := verifyContentClosure(ctx, e.index, knowledge.SummaryContentReferences(summary)); err != nil {
		return nil, err
	}
	links := map[model.ObjectID][]model.ObjectID{
		summary.ID:                       summaryPolicyIDs(summary),
		summary.Generation.Output.Object: {summary.ID},
	}
	return e.prepareDerivedValue(ctx, req, digest, txID, lsn, recorded, summaryStateKey(req.Space, req.Principal, summary.ID), req.Body, links)
}

func (e *Engine) GetSummary(ctx context.Context, req api.ReadSummaryRequest) (api.SummaryResult, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "get_summary", req.Principal, req, func(scoped context.Context) (api.SummaryResult, error) {
			return e.GetSummary(scoped, req)
		})
	}
	if err := ctx.Err(); err != nil {
		return api.SummaryResult{}, err
	}
	if !e.options.AgentProfile {
		return api.SummaryResult{}, ErrUnsupportedMethod
	}
	if err := validAgentScope(req.Space, req.Principal); err != nil || !model.ValidDigest(string(req.ID)) {
		return api.SummaryResult{}, api.ErrInvalidRequest
	}
	if err := e.publicGate(); err != nil {
		return api.SummaryResult{}, err
	}
	if err := e.readReady(ctx); err != nil {
		return api.SummaryResult{}, err
	}
	anchor := model.SnapshotAnchor{Space: req.Space, Fence: e.readPublication(ctx).Fence()}
	if req.Anchor != nil {
		anchor = req.Anchor.Clone()
	}
	if anchor.Validate() != nil || anchor.Space != req.Space || anchor.Knowledge != nil || anchor.Cursor != nil {
		return api.SummaryResult{}, model.ErrAnchorMismatch
	}
	roots, rootID, err := e.rootsAtFence(ctx, anchor.Fence, false)
	if err != nil {
		return api.SummaryResult{}, e.knowledgeReadFailure(ctx, err)
	}
	summary, record, found, err := e.loadSummary(ctx, roots, req.Space, req.Principal, req.ID)
	if err != nil {
		return api.SummaryResult{}, e.knowledgeReadFailure(ctx, err)
	}
	var dependencies []model.ObjectID
	if found {
		if err := e.authorizeKnowledge(ctx, e.readRoots(ctx), req.Space, req.Principal, *summary.Source.Anchor.Knowledge); err != nil {
			return api.SummaryResult{}, e.knowledgeReadFailure(ctx, err)
		}
		dependencies, err = e.summaryDependencyIDs(ctx, roots, req.Space, req.Principal, []model.ObjectID{summary.ID})
		if err != nil {
			return api.SummaryResult{}, e.knowledgeReadFailure(ctx, err)
		}
		if err := e.checkDisclosureIDs(ctx, req.Space, dependencies); err != nil {
			return api.SummaryResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return api.SummaryResult{}, err
	}
	if err := e.viewOutputGate(); err != nil {
		return api.SummaryResult{}, err
	}
	out := api.SummaryResult{Found: found, Summary: summary, Record: record, Anchor: anchor, Fence: anchor.Fence, RootSet: rootID}
	return finishReadDelivery(ctx, e, "get_summary", req.Principal, out, anchor, "", "", dependencies)
}

func summaryCommandRejection(err error) bool {
	if errors.Is(err, ErrStoredObject) || errors.Is(err, ErrInvalidStateData) || errors.Is(err, ErrObjectIO) || errors.Is(err, ErrSemanticCorruption) {
		return false
	}
	return errors.Is(err, knowledge.ErrInvalidSummary) || knowledgeCommandRejection(err) || agentCommandRejection(err) || memoryCommandRejection(err)
}

var _ api.SummaryCommands = (*Engine)(nil)
