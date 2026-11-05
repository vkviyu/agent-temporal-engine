package engine

import (
	"context"
	"strings"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// derivedDisclosureIDs follows the explicit Packet and Summary relationships
// that can make a raw object an output of withdrawn source material. This is
// intentionally a bounded scan of the published ExecutionKnowledge catalog.
// It lets a domain-state read apply the same disclosure policy as a Knowledge
// View without treating equal bytes as provenance.
func (e *Engine) derivedDisclosureIDs(ctx context.Context, space model.SpaceID, ids []model.ObjectID) ([]model.ObjectID, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	targets := make(map[model.ObjectID]bool, len(ids))
	for _, id := range ids {
		if id != "" {
			targets[id] = true
		}
	}
	root, err := slotSpaceRoot(ctx, e.index, e.roots.ExecutionKnowledge, space)
	if err != nil {
		return nil, err
	}
	entries, err := state.Entries(ctx, e.index, space, root, state.ScanLimits{MaxEntries: 16384, MaxNodes: 16384, MaxBytes: 16 << 20, MaxReadBytes: 64 << 20})
	if err != nil {
		return nil, err
	}
	type packetRecord struct {
		packet   knowledge.ContextPacket
		snapshot knowledge.Snapshot
	}
	packets := make([]packetRecord, 0)
	summaries := make(map[model.ObjectID]knowledge.Summary)
	for _, entry := range entries {
		key := string(entry.Key)
		switch {
		case strings.HasPrefix(key, "ate/knowledge/v1/"):
			obj, readErr := readObject(ctx, e.index, entry.Value)
			if readErr != nil {
				return nil, readErr
			}
			value, readErr := readValue(obj)
			if readErr != nil || value.Kind != api.ValueBytes {
				return nil, ErrInvalidStateData
			}
			snapshot, decodeErr := knowledge.DecodeSnapshot(value.Data)
			if decodeErr != nil {
				return nil, decodeErr
			}
			for _, packet := range snapshot.Packets {
				packets = append(packets, packetRecord{packet: packet, snapshot: snapshot})
			}
		case strings.HasPrefix(key, "ate/summary/v1/"):
			obj, readErr := readObject(ctx, e.index, entry.Value)
			if readErr != nil {
				return nil, readErr
			}
			value, readErr := readValue(obj)
			if readErr != nil || value.Kind != api.ValueBytes {
				return nil, ErrInvalidStateData
			}
			summary, decodeErr := knowledge.DecodeSummary(value.Data)
			if decodeErr != nil {
				return nil, decodeErr
			}
			summaries[summary.ID] = summary
		}
	}
	derived := make([]model.ObjectID, 0)
	seen := make(map[model.ObjectID]bool)
	appendID := func(id model.ObjectID) {
		if id != "" && !seen[id] {
			seen[id] = true
			derived = append(derived, id)
		}
	}
	for _, record := range packets {
		if record.packet.Input == nil || !targets[record.packet.Input.Object] {
			continue
		}
		for _, id := range packetPolicyIDs(record.packet, record.snapshot) {
			appendID(id)
		}
		for _, item := range record.packet.Items {
			for _, observation := range record.snapshot.Observations {
				if observation.ID != item.Observation || observation.PayloadKind != "summary" {
					continue
				}
				queue := []model.ObjectID{observation.Payload}
				visited := make(map[model.ObjectID]bool)
				for len(queue) > 0 {
					id := queue[len(queue)-1]
					queue = queue[:len(queue)-1]
					if visited[id] {
						continue
					}
					visited[id] = true
					if len(visited) > 4096 {
						return nil, storage.ErrLimit
					}
					summary, ok := summaries[id]
					if !ok {
						continue
					}
					for _, dependency := range summaryPolicyIDs(summary) {
						appendID(dependency)
					}
					queue = append(queue, summaryParents(summary)...)
				}
			}
		}
	}
	return derived, nil
}

func (e *Engine) checkDerivedDisclosureIDs(ctx context.Context, space model.SpaceID, ids []model.ObjectID) error {
	derived, err := e.derivedDisclosureIDs(ctx, space, ids)
	if err != nil {
		return err
	}
	return e.checkDisclosureIDs(ctx, space, append(append([]model.ObjectID(nil), ids...), derived...))
}
