package engine

import (
	"context"
	"encoding/binary"
	"slices"
	"strings"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// One owner radix gives indexed records their historical publication cut.
// Every index entry points to an immutable canonical record.
type knowledgeIndex struct {
	reader state.ObjectReader
	space  model.SpaceID
	root   model.ObjectID
}

func knowledgeIndexKey(space model.SpaceID, principal model.PrincipalID) []byte {
	return []byte("ate/knowledge/v2/" + knowledgeOwnerDigest(space, principal))
}

func knowledgeScopeKey(key *model.KnowledgeKey) string {
	if key == nil {
		return "query/"
	}
	prefix, _ := codec.AppendString([]byte("execution/"), string(key.Actor))
	prefix, _ = codec.AppendString(prefix, string(key.Execution))
	return string(prefix) + "/"
}

func knowledgeSequenceKey(key model.KnowledgeKey, sequence uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte(knowledgeScopeKey(&key)+"observation/"), sequence)
}

func openKnowledgeIndex(ctx context.Context, reader state.ObjectReader, root model.ObjectID, space model.SpaceID, principal model.PrincipalID) (knowledgeIndex, error) {
	k := knowledgeIndex{reader: reader, space: space}
	// Reject old and mixed stores instead of silently hiding their knowledge.
	if _, found, err := state.Get(ctx, reader, space, root, legacyKnowledgeStateKey(space, principal)); err != nil {
		return k, err
	} else if found {
		return k, knowledge.ErrUnsupportedSchema
	}
	index, found, err := state.Get(ctx, reader, space, root, knowledgeIndexKey(space, principal))
	if err != nil {
		return k, err
	}
	if found {
		k.root = index
	}
	return k, nil
}

func (e *Engine) knowledgeIndexAt(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID) (knowledgeIndex, model.ObjectID, error) {
	reader := &cursorReader{source: e.reader(ctx)}
	root, err := slotSpaceRoot(ctx, reader, roots.ExecutionKnowledge, space)
	if err != nil {
		return knowledgeIndex{}, "", err
	}
	k, err := openKnowledgeIndex(ctx, reader, root, space, principal)
	return k, root, err
}

func (k knowledgeIndex) get(ctx context.Context, key string) ([]byte, bool, error) {
	id, found, err := state.Get(ctx, k.reader, k.space, k.root, []byte(key))
	if err != nil || !found {
		return nil, found, err
	}
	raw, err := k.readValue(ctx, id)
	return raw, true, err
}

func (k knowledgeIndex) readValue(ctx context.Context, id model.ObjectID) ([]byte, error) {
	obj, err := readObject(ctx, k.reader, id)
	if err != nil {
		return nil, err
	}
	v, err := readValue(obj)
	if err != nil || obj.Space != k.space || v.Kind != api.ValueBytes {
		return nil, ErrStoredObject
	}
	return v.Data, nil
}

func (k knowledgeIndex) observation(ctx context.Context, id model.ObjectID) (knowledge.Observation, bool, error) {
	raw, found, err := k.get(ctx, "observation/"+string(id))
	if err != nil {
		return knowledge.Observation{}, false, err
	}
	if found {
		observation, err := knowledge.DecodeObservation(raw)
		if err != nil || observation.ID != id || observation.Source.Anchor.Space != k.space {
			return knowledge.Observation{}, false, ErrStoredObject
		}
		return observation, true, nil
	}
	return knowledge.Observation{}, false, nil
}

func (k knowledgeIndex) packet(ctx context.Context, id model.ObjectID) (knowledge.ContextPacket, bool, error) {
	raw, found, err := k.get(ctx, "packet/"+string(id))
	if err != nil || !found {
		return knowledge.ContextPacket{}, false, err
	}
	packet, err := knowledge.DecodePacket(raw)
	if err != nil || packet.ID != id || packet.Space != k.space {
		return knowledge.ContextPacket{}, false, ErrStoredObject
	}
	return packet, true, nil
}

func (k knowledgeIndex) packetObservations(ctx context.Context, p knowledge.ContextPacket) (knowledge.Snapshot, error) {
	snapshot := knowledge.Snapshot{Observations: make([]knowledge.Observation, 0, len(p.Items))}
	for _, item := range p.Items {
		o, found, err := k.observation(ctx, item.Observation)
		if err != nil {
			return knowledge.Snapshot{}, err
		}
		if !found || p.Key == nil || o.Key != *p.Key || o.Payload != item.Content {
			return knowledge.Snapshot{}, ErrStoredObject
		}
		snapshot.Observations = append(snapshot.Observations, o)
	}
	return snapshot, nil
}

type recordedKnowledgePacket struct {
	packet   knowledge.ContextPacket
	selected knowledge.Snapshot
	roots    model.RootSet
}

// packetAt resolves one packet and only the observations it selects. A caller
// imposing a knowledge Cursor must use selectedKnowledge instead.
func (e *Engine) packetAt(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID, id model.ObjectID) (recordedKnowledgePacket, error) {
	out := recordedKnowledgePacket{roots: roots}
	index, _, err := e.knowledgeIndexAt(ctx, roots, space, principal)
	if err != nil {
		return out, err
	}
	packet, found, err := index.packet(ctx, id)
	if err != nil {
		return out, err
	}
	if !found {
		return out, agentprofile.ErrUnknown
	}
	out.packet = packet
	out.selected, err = index.packetObservations(ctx, packet)
	return out, err
}

func (e *Engine) activityPacketAt(ctx context.Context, space model.SpaceID, principal model.PrincipalID, fence model.CommitFence, id model.ObjectID) (recordedKnowledgePacket, error) {
	roots, _, err := e.rootsAtFence(ctx, fence, true)
	if err != nil {
		return recordedKnowledgePacket{}, err
	}
	return e.packetAt(ctx, roots, space, principal, id)
}

func (e *Engine) packetDependencyIDs(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID, packet knowledge.ContextPacket, selected knowledge.Snapshot) ([]model.ObjectID, error) {
	dependencies, err := e.summaryDependencyIDs(ctx, roots, space, principal, observationSummaryIDs(selected.Observations))
	if err != nil {
		return nil, err
	}
	return append(packetPolicyIDs(packet, selected), dependencies...), nil
}

func (k knowledgeIndex) sequence(ctx context.Context, key model.KnowledgeKey) (uint64, error) {
	raw, found, err := k.get(ctx, knowledgeScopeKey(&key)+"head")
	if err != nil {
		return 0, err
	}
	if found {
		if len(raw) != 8 || binary.BigEndian.Uint64(raw) == 0 {
			return 0, ErrStoredObject
		}
		return binary.BigEndian.Uint64(raw), nil
	}
	return 0, nil
}

func (k knowledgeIndex) snapshot(ctx context.Context, key *model.KnowledgeKey, scoped, packets bool) (knowledge.Snapshot, error) {
	var out knowledge.Snapshot
	prefix := ""
	if scoped {
		prefix = knowledgeScopeKey(key)
	}
	if !scoped || key != nil {
		entries, err := state.EntriesWithPrefix(ctx, k.reader, k.space, k.root, []byte(prefix+"observation/"), state.ScanLimits{})
		if err != nil {
			return knowledge.Snapshot{}, err
		}
		for _, entry := range entries {
			raw, err := k.readValue(ctx, entry.Value)
			if err != nil {
				return knowledge.Snapshot{}, err
			}
			o, err := knowledge.DecodeObservation(raw)
			if err != nil || o.Source.Anchor.Space != k.space {
				return knowledge.Snapshot{}, ErrStoredObject
			}
			want := "observation/" + string(o.ID)
			if scoped {
				want = string(knowledgeSequenceKey(o.Key, o.Sequence))
			}
			if string(entry.Key) != want {
				return knowledge.Snapshot{}, ErrStoredObject
			}
			out.Observations = append(out.Observations, o)
		}
	}
	if packets {
		packetPrefix := prefix + "packet/"
		entries, err := state.EntriesWithPrefix(ctx, k.reader, k.space, k.root, []byte(packetPrefix), state.ScanLimits{})
		if err != nil {
			return knowledge.Snapshot{}, err
		}
		for _, entry := range entries {
			raw, err := k.readValue(ctx, entry.Value)
			if err != nil {
				return knowledge.Snapshot{}, err
			}
			p, err := knowledge.DecodePacket(raw)
			if err != nil || p.Space != k.space || string(entry.Key) != packetPrefix+string(p.ID) || scoped && knowledgeScopeKey(p.Key) != prefix {
				return knowledge.Snapshot{}, ErrStoredObject
			}
			out.Packets = append(out.Packets, p)
		}
	}
	slices.SortFunc(out.Observations, func(a, b knowledge.Observation) int { return strings.Compare(string(a.ID), string(b.ID)) })
	slices.SortFunc(out.Packets, func(a, b knowledge.ContextPacket) int { return strings.Compare(string(a.ID), string(b.ID)) })
	return out, nil
}

func (e *Engine) prepareKnowledgeRecords(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord, index knowledgeIndex, observations []knowledge.Observation, packet *knowledge.ContextPacket, source knowledge.Snapshot) (*commitPlan, error) {
	b := newBuilder(ctx, e.index, e.options.MaxObjects)
	add := func(raw []byte) (model.ObjectID, error) {
		if len(raw) > api.MaxStateValueBytes {
			return "", storage.ErrLimit
		}
		object, err := makeValue(req.Space, api.Value{Kind: api.ValueBytes, Data: raw})
		if err != nil {
			return "", err
		}
		return b.add(object, storage.KindObject)
	}
	var mutations []state.Mutation
	var sources []knowledge.Observation
	var links map[model.ObjectID][]model.ObjectID
	for _, o := range observations {
		if _, exists, err := index.observation(ctx, o.ID); err != nil {
			return nil, err
		} else if exists {
			continue
		}
		raw, err := o.Canonical()
		if err != nil {
			return nil, err
		}
		id, err := add(raw)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, state.Mutation{Key: []byte("observation/" + string(o.ID)), Value: id}, state.Mutation{Key: knowledgeSequenceKey(o.Key, o.Sequence), Value: id})
		sources = append(sources, o)
	}
	if len(sources) > 0 {
		first, last := sources[0], sources[len(sources)-1]
		sequence, err := index.sequence(ctx, first.Key)
		if err != nil {
			return nil, err
		}
		for _, o := range sources {
			if o.Key != first.Key || sequence == ^uint64(0) || o.Sequence != sequence+1 {
				return nil, knowledge.ErrObservationSequence
			}
			sequence++
		}
		head, err := add(binary.BigEndian.AppendUint64(nil, sequence))
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, state.Mutation{Key: []byte(knowledgeScopeKey(&last.Key) + "head"), Value: head})
	}
	if packet != nil {
		_, exists, err := index.packet(ctx, packet.ID)
		if err != nil {
			return nil, err
		}
		if !exists {
			raw, err := packet.Canonical()
			if err != nil {
				return nil, err
			}
			id, err := add(raw)
			if err != nil {
				return nil, err
			}
			mutations = append(mutations, state.Mutation{Key: []byte("packet/" + string(packet.ID)), Value: id}, state.Mutation{Key: []byte(knowledgeScopeKey(packet.Key) + "packet/" + string(packet.ID)), Value: id})
		}
		selected := make(map[model.ObjectID]bool, len(packet.Items))
		for _, item := range packet.Items {
			selected[item.Observation] = true
		}
		for _, observation := range source.Observations {
			if selected[observation.ID] || observation.Payload == packet.GoalRef {
				sources = append(sources, observation)
			}
		}
		links = packetDerivations(*packet, knowledge.Snapshot{Observations: sources})
	}
	dependencies := knowledge.Snapshot{Observations: sources}
	if packet != nil {
		dependencies.Packets = []knowledge.ContextPacket{*packet}
	}
	refs := physicalReferences(knowledge.ContentReferences(dependencies))
	if err := verifyContentClosure(ctx, e.index, refs); err != nil {
		return nil, err
	}
	next := index.root
	for batch := range slices.Chunk(mutations, state.MaxMutations) {
		delta, err := state.Apply(ctx, b, req.Space, next, batch)
		if err != nil {
			return nil, err
		}
		for _, object := range delta.Objects {
			if _, err = b.add(object, storage.KindObject); err != nil {
				return nil, err
			}
		}
		next = delta.Root
	}
	writes := []typedValueWrite{{key: knowledgeIndexKey(req.Space, req.Principal), radixRoot: next}}
	if len(links) > 0 {
		var err error
		writes, err = e.appendDerivations(ctx, req.Space, writes, links)
		if err != nil {
			return nil, err
		}
	}
	return e.prepareTypedValuesWithBuilder(ctx, req, digest, txID, lsn, recorded, b, writes)
}

func packetDerivations(packet knowledge.ContextPacket, snapshot knowledge.Snapshot) map[model.ObjectID][]model.ObjectID {
	links := map[model.ObjectID][]model.ObjectID{packet.ID: packetPolicyIDs(packet, snapshot)}
	if packet.Input != nil {
		links[packet.Input.Object] = []model.ObjectID{packet.ID}
	}
	return links
}
