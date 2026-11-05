package knowledge

import (
	"sort"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// ContentReferences extracts actual source content held by observations and
// packets. ObservationID, PacketID, knowledge RevisionID, Frontier and ViewDigest
// are embedded record IDs or commitments, not raw codec object addresses.
// SourceAnchors separately identifies the historical roots required to prove
// adoption/provenance. Neither class may be omitted from a retention policy.
func ContentReferences(s Snapshot) []model.ObjectID {
	var out []model.ObjectID
	typed := make(map[model.ObjectID]bool, len(s.Observations))
	typedPayload := make(map[model.ObjectID]bool, len(s.Observations))
	for _, observation := range s.Observations {
		// Memory and experience-path IDs are logical projection identities. The
		// memory root/snapshot is retained through the root set; attempting to
		// read the logical ID as a raw codec object would turn valid provenance
		// into a false storage-corruption report.
		if observation.PayloadKind == "" {
			out = append(out, observation.Payload)
		} else {
			typed[observation.ID] = true
			typedPayload[observation.Payload] = true
		}
	}
	for _, packet := range s.Packets {
		if !typedPayload[packet.GoalRef] {
			out = append(out, packet.GoalRef)
		}
		if packet.Input != nil {
			out = append(out, packet.Input.Object)
		}
		for _, item := range packet.Items {
			if !typed[item.Observation] {
				out = append(out, item.Content)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	unique := out[:0]
	for _, id := range out {
		if id != "" && (len(unique) == 0 || unique[len(unique)-1] != id) {
			unique = append(unique, id)
		}
	}
	return unique
}

// SourceAnchors returns owned provenance records in snapshot order. The
// embedding host resolves them at their exact published fences and enforces
// source Key/Cursor membership; an ID-shaped string alone is not such proof.
func SourceAnchors(s Snapshot) []SourceAnchor {
	out := make([]SourceAnchor, 0, len(s.Observations)+len(s.Packets))
	for _, observation := range s.Observations {
		out = append(out, observation.Source.clone())
	}
	for _, packet := range s.Packets {
		out = append(out, packet.Source.clone())
		if packet.Input != nil {
			out = append(out, SourceAnchor{Anchor: packet.Input.Anchor.Clone()})
		}
	}
	return out
}
