package knowledge

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// Revision is an immutable execution-knowledge position. It is derived from
// the canonical adopted Observation sequence, not from packet rendering or a
// mutable global domain root. Membership must be authenticated by the host's
// published owner snapshot before a caller may use its ID as a Cursor.
// Sequence zero is the deterministic empty position for this Space and Key.
type Revision struct {
	ID          model.RevisionID
	Space       model.SpaceID
	Key         model.KnowledgeKey
	Parent      model.RevisionID
	Sequence    uint64
	Observation model.ObjectID
	StateRoot   model.ObjectID
}

func knowledgeDigest(b []byte) model.ObjectID {
	sum := sha256.Sum256(b)
	return model.ObjectID(hex.EncodeToString(sum[:]))
}

func revisionScope(prefix string, space model.SpaceID, key model.KnowledgeKey) []byte {
	b := codec.AppendUint([]byte(prefix), 1)
	b, _ = codec.AppendString(b, string(space))
	b, _ = codec.AppendString(b, string(key.Actor))
	b, _ = codec.AppendString(b, string(key.Execution))
	return b
}

func makeRevision(space model.SpaceID, key model.KnowledgeKey, parent Revision, observation model.ObjectID, sequence uint64) Revision {
	b := revisionScope("ATE-KNOWLEDGE-STATE\x00", space, key)
	b, _ = codec.AppendString(b, string(parent.StateRoot))
	b = codec.AppendUint(b, sequence)
	b, _ = codec.AppendString(b, string(observation))
	r := Revision{Space: space, Key: key, Parent: parent.ID, Sequence: sequence, Observation: observation, StateRoot: knowledgeDigest(b)}
	b = revisionScope("ATE-KNOWLEDGE-REVISION\x00", space, key)
	b, _ = codec.AppendString(b, string(r.Parent))
	b = codec.AppendUint(b, r.Sequence)
	b, _ = codec.AppendString(b, string(r.Observation))
	b, _ = codec.AppendString(b, string(r.StateRoot))
	r.ID = model.RevisionID(knowledgeDigest(b))
	return r
}

// Revisions reconstructs the exact knowledge version chain in O(n log n)
// time and bounded O(n) space. It never uses Source.Fence as the adoption
// position: source publication and adoption are different events.
func Revisions(s Snapshot, space model.SpaceID, key model.KnowledgeKey) ([]Revision, error) {
	if !validText(string(space)) || !validText(string(key.Actor)) || !validText(string(key.Execution)) {
		return nil, ErrKnowledgeMismatch
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	items := make([]Observation, 0)
	for _, o := range s.Observations {
		if o.Key == key && o.Source.Anchor.Space == space {
			items = append(items, o)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Sequence < items[j].Sequence })
	out := make([]Revision, 1, len(items)+1)
	out[0] = makeRevision(space, key, Revision{}, "", 0)
	for i, o := range items {
		if o.Sequence != uint64(i+1) {
			return nil, ErrObservationSequence
		}
		out = append(out, makeRevision(space, key, out[len(out)-1], o.ID, o.Sequence))
	}
	return out, nil
}

// SelectRevision resolves a knowledge Cursor against the supplied published
// snapshot. An empty ID selects its current head. Future, foreign and forged
// IDs fail explicitly instead of falling back to latest.
// Packets are derived artifacts, not adopted knowledge. A selected historical
// position may expose only packets whose exact observation/goal sources are
// already in that position; a packet never advances the revision chain.
func SelectRevision(s Snapshot, space model.SpaceID, key model.KnowledgeKey, id model.RevisionID) (Snapshot, Revision, error) {
	chain, err := Revisions(s, space, key)
	if err != nil {
		return Snapshot{}, Revision{}, err
	}
	r := chain[len(chain)-1]
	if id != "" {
		found := false
		for _, candidate := range chain {
			if candidate.ID == id {
				r, found = candidate, true
				break
			}
		}
		if !found {
			return Snapshot{}, Revision{}, model.ErrAnchorMismatch
		}
	}
	out := Snapshot{}
	observed := map[model.ObjectID]model.ObjectID{}
	payloads := map[model.ObjectID]bool{}
	for _, o := range s.Observations {
		if o.Key == key && o.Source.Anchor.Space == space && o.Sequence <= r.Sequence {
			out.Observations = append(out.Observations, o.Clone())
			observed[o.ID] = o.Payload
			payloads[o.Payload] = true
		}
	}
	positions := make(map[model.RevisionID]uint64, len(chain))
	for _, version := range chain {
		positions[version.ID] = version.Sequence
	}
	var selectedFrontier model.ObjectID
	if id != "" {
		selectedFrontier, err = Frontier(out, key)
		if err != nil {
			return Snapshot{}, Revision{}, err
		}
	}
	for _, p := range s.Packets {
		if p.Space != space || p.Key == nil || *p.Key != key {
			continue
		}
		known := p.GoalRef == "" || payloads[p.GoalRef]
		if id != "" {
			if source := p.Source.Anchor.Cursor; source != nil {
				sequence, ok := positions[source.Revision]
				known = known && ok && sequence <= r.Sequence
			} else {
				// Legacy packets without a knowledge Cursor carry only a
				// frontier proof. Expose them at that exact cut; do not infer
				// an earlier source position from a subset of packet items.
				known = known && p.Source.Frontier == selectedFrontier
			}
		}
		for _, item := range p.Items {
			value, ok := observed[item.Observation]
			known = known && ok && value == item.Content
		}
		if known {
			out.Packets = append(out.Packets, clonePacket(p))
		}
	}
	return out, r, nil
}
