package engine

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// Import is an explicit provenance-bearing knowledge transition. It validates
// both executions under the authenticated owner, then selects the exact source
// position before creating new target observations. It does not clone current
// global state, share authority, advance the source or erase either history.
func (e *Engine) prepareKnowledgeImport(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord, index knowledgeIndex, legacy bool, command knowledge.ImportCommand) (*commitPlan, error) {
	if command.Source.Anchor.Space != req.Space {
		return nil, knowledge.ErrKnowledgeMismatch
	}
	if err := e.authorizeKnowledge(ctx, e.roots, req.Space, req.Principal, command.Target); err != nil {
		return nil, err
	}
	// The target may have been created after the seed's source cut. Source
	// authority is checked at both cuts; target authority is current only.
	roots, sourceRoot, err := e.rootsAtFence(ctx, command.Source.Anchor.Fence, true)
	if err != nil {
		return nil, err
	}
	sourceKey := *command.Source.Anchor.Knowledge
	if err = e.authorizeKnowledge(ctx, roots, req.Space, req.Principal, sourceKey); err != nil {
		return nil, err
	}
	if err = e.authorizeKnowledge(ctx, e.roots, req.Space, req.Principal, sourceKey); err != nil {
		return nil, err
	}
	selected, _, _, _, err := e.selectedKnowledgeObservations(ctx, roots, command.Source.Anchor, req.Principal)
	if err != nil {
		return nil, err
	}
	if command.Source.Frontier != "" {
		frontier, err := knowledge.Frontier(selected, sourceKey)
		if err != nil {
			return nil, err
		}
		if frontier != command.Source.Frontier {
			return nil, model.ErrAnchorMismatch
		}
	}
	if command.Source.ViewDigest != "" && command.Source.ViewDigest != viewDigest(command.Source.Anchor, sourceRoot, req.Principal, "execution_knowledge") {
		return nil, model.ErrAnchorMismatch
	}
	adopted := make(map[model.ObjectID]knowledge.Observation, len(selected.Observations))
	for _, observation := range selected.Observations {
		adopted[observation.ID] = observation
	}
	for _, id := range command.Observations {
		observation, ok := adopted[id]
		if !ok {
			return nil, knowledge.ErrUnknownObservation
		}
		if observation.PayloadKind != "" {
			if err = e.visibleTypedKnowledgeSource(ctx, roots, req.Space, req.Principal, observation.PayloadKind, observation.Payload); err != nil {
				return nil, err
			}
		}
	}
	if !legacy {
		sequence, err := index.sequence(ctx, command.Target)
		if err != nil {
			return nil, err
		}
		if sequence != command.ExpectedSequence {
			return nil, knowledge.ErrObservationSequence
		}
		imported, err := knowledge.ImportedObservations(command, adopted)
		if err != nil {
			return nil, err
		}
		return e.prepareKnowledgeRecords(ctx, req, digest, txID, lsn, recorded, index, imported, nil, knowledge.Snapshot{})
	}
	next, err := knowledge.ApplyCommand(index.legacy, knowledge.Command{Import: &command})
	if err != nil {
		return nil, err
	}
	if err = verifyContentClosure(ctx, e.index, knowledge.ContentReferences(next)); err != nil {
		return nil, err
	}
	raw, err := knowledge.EncodeSnapshot(next)
	if err != nil {
		return nil, err
	}
	return e.prepareTypedValue(ctx, req, digest, txID, lsn, recorded, knowledgeStateKey(req.Space, req.Principal), raw)
}
