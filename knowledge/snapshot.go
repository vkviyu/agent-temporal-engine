package knowledge

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const (
	// MaxSnapshotRecords bounds each collection in the in-memory Snapshot DTO
	// and its contiguous sequence validation, not indexed persistence capacity.
	MaxSnapshotRecords = 65536
	MaxCommandBytes    = MaxRecordBytes + 64
	commandPrefix      = "ATE-KNOWLEDGE-COMMAND\x00"
	commandVersion     = 2
)

// Snapshot is an in-memory DTO of immutable, content-addressed observations and
// packets for validation and selection. Persistence uses individual records.
// Sequence is contiguous within each (Space, KnowledgeKey); source publication
// and the observation's adoption cut must additionally be checked by the core.
type Snapshot struct {
	Observations []Observation
	Packets      []ContextPacket
}

// Command is a closed transition union. Its canonical bytes contain exactly
// one canonical Observation, ContextPacket, or ImportCommand, never a replacement
// snapshot. Publication and authentication belong to the outer core command.
type Command struct {
	Observation *Observation
	Packet      *ContextPacket
	Import      *ImportCommand
}

type snapshotScope struct {
	space model.SpaceID
	key   model.KnowledgeKey
}

// Clone owns nested pointers and slices, including Anchor selectors.
func (s Snapshot) Clone() Snapshot {
	out := Snapshot{Observations: make([]Observation, len(s.Observations)), Packets: make([]ContextPacket, len(s.Packets))}
	for i, o := range s.Observations {
		out.Observations[i] = o.Clone()
	}
	for i, p := range s.Packets {
		out.Packets[i] = clonePacket(p)
	}
	return out
}

func (s Snapshot) Validate() error {
	if len(s.Observations) > MaxSnapshotRecords || len(s.Packets) > MaxSnapshotRecords {
		return ErrInvalid
	}
	observations := make(map[model.ObjectID]Observation, len(s.Observations))
	sequences := make(map[snapshotScope]map[uint64]struct{})
	for _, o := range s.Observations {
		raw, err := o.Canonical() // verifies the claimed ID, not only fields
		if err != nil {
			return err
		}
		if len(raw) > MaxRecordBytes {
			return ErrInvalid
		}
		if _, exists := observations[o.ID]; exists {
			return ErrDuplicateItem
		}
		observations[o.ID] = o
		scope := snapshotScope{space: o.Source.Anchor.Space, key: o.Key}
		if sequences[scope] == nil {
			sequences[scope] = make(map[uint64]struct{})
		}
		if _, exists := sequences[scope][o.Sequence]; exists || o.Sequence > MaxSnapshotRecords {
			return ErrObservationSequence
		}
		sequences[scope][o.Sequence] = struct{}{}
	}
	for _, sequence := range sequences {
		for i := uint64(1); i <= uint64(len(sequence)); i++ {
			if _, exists := sequence[i]; !exists {
				return ErrObservationSequence
			}
		}
	}
	packets := make(map[model.ObjectID]struct{}, len(s.Packets))
	for _, p := range s.Packets {
		raw, err := p.Canonical()
		if err != nil {
			return err
		}
		if len(raw) > MaxRecordBytes {
			return ErrInvalid
		}
		if _, exists := packets[p.ID]; exists {
			return ErrDuplicateItem
		}
		packets[p.ID] = struct{}{}
		for _, item := range p.Items {
			o, exists := observations[item.Observation]
			if !exists {
				return ErrUnknownObservation
			}
			if o.Source.Anchor.Space != p.Space || o.Payload != item.Content {
				return ErrInvalid
			}
			if p.Key != nil && o.Key != *p.Key {
				return ErrKnowledgeMismatch
			}
			before, at := o.Source.Anchor.Fence, p.Source.Anchor.Fence
			if before.Store != at.Store || before.LSN > at.LSN || (before.LSN == at.LSN && before.CommitDigest != at.CommitDigest) {
				return ErrFutureSource
			}
		}
		if err := VerifyPacketSelection(p, Snapshot{Observations: s.Observations}); err != nil {
			return err
		}
	}
	return nil
}

func (c Command) Validate() error {
	count := 0
	for _, present := range []bool{c.Observation != nil, c.Packet != nil, c.Import != nil} {
		if present {
			count++
		}
	}
	if count != 1 {
		return ErrInvalid
	}
	var raw []byte
	var err error
	if c.Observation != nil {
		if c.Observation.ImportedFrom != "" {
			return ErrKnowledgeMismatch
		}
		raw, err = c.Observation.Canonical()
	} else if c.Packet != nil {
		raw, err = c.Packet.Canonical()
	} else {
		raw, err = encodeImport(*c.Import)
	}
	if err != nil {
		return err
	}
	if len(raw) > MaxRecordBytes {
		return ErrInvalid
	}
	return nil
}

func EncodeCommand(c Command) ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	operation := uint64(1)
	var raw []byte
	var err error
	if c.Observation != nil {
		raw, err = c.Observation.Canonical()
	} else if c.Packet != nil {
		operation = 2
		raw, err = c.Packet.Canonical()
	} else {
		operation = 3
		raw, err = encodeImport(*c.Import)
	}
	if err != nil {
		return nil, err
	}
	b := codec.AppendUint([]byte(commandPrefix), commandVersion)
	b = codec.AppendUint(b, operation)
	return codec.AppendBytes(b, raw)
}

func DecodeCommand(raw []byte) (Command, error) {
	if len(raw) > MaxCommandBytes || !bytes.HasPrefix(raw, []byte(commandPrefix)) {
		return Command{}, ErrInvalid
	}
	d := codec.NewDecoder(raw[len(commandPrefix):])
	version, err := d.Uint()
	if err != nil {
		return Command{}, ErrInvalid
	}
	if version != commandVersion {
		return Command{}, ErrUnsupportedSchema
	}
	operation, err := d.Uint()
	if err != nil {
		return Command{}, err
	}
	body, err := d.Bytes(MaxRecordBytes)
	if err != nil {
		return Command{}, err
	}
	if err = d.Done(); err != nil {
		return Command{}, err
	}
	c := Command{}
	switch operation {
	case 1:
		value, e := DecodeObservation(body)
		if e != nil {
			return Command{}, e
		}
		c.Observation = &value
	case 2:
		value, e := DecodePacket(body)
		if e != nil {
			return Command{}, e
		}
		c.Packet = &value
	case 3:
		value, e := decodeImport(body)
		if e != nil {
			return Command{}, e
		}
		c.Import = &value
	default:
		return Command{}, ErrInvalid
	}
	canonical, err := EncodeCommand(c)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Command{}, ErrInvalid
	}
	return c, nil
}

// SelectSnapshot returns only the exact execution's observations and packets.
// Query packets without a KnowledgeKey are never implicitly imported.
func SelectSnapshot(s Snapshot, key model.KnowledgeKey) (Snapshot, error) {
	if !validText(string(key.Actor)) || !validText(string(key.Execution)) {
		return Snapshot{}, ErrKnowledgeMismatch
	}
	if err := s.Validate(); err != nil {
		return Snapshot{}, err
	}
	out := Snapshot{}
	for _, o := range s.Observations {
		if o.Key == key {
			out.Observations = append(out.Observations, o.Clone())
		}
	}
	for _, p := range s.Packets {
		if p.Key != nil && *p.Key == key {
			out.Packets = append(out.Packets, clonePacket(p))
		}
	}
	return out, nil
}

// Frontier identifies the exact observation set adopted by a KnowledgeKey.
// It sorts by Space and contiguous Sequence, includes each Observation ID, and
// deliberately excludes packets: rendering a packet does not adopt knowledge.
func Frontier(s Snapshot, key model.KnowledgeKey) (model.ObjectID, error) {
	selected, err := SelectSnapshot(s, key)
	if err != nil {
		return "", err
	}
	sort.Slice(selected.Observations, func(i, j int) bool {
		a, b := selected.Observations[i], selected.Observations[j]
		if a.Source.Anchor.Space != b.Source.Anchor.Space {
			return a.Source.Anchor.Space < b.Source.Anchor.Space
		}
		return a.Sequence < b.Sequence
	})
	b := codec.AppendUint([]byte("ATE-KNOWLEDGE-FRONTIER\x00"), 1)
	b, _ = codec.AppendString(b, string(key.Actor))
	b, _ = codec.AppendString(b, string(key.Execution))
	b = codec.AppendUint(b, uint64(len(selected.Observations)))
	for _, o := range selected.Observations {
		b, _ = codec.AppendString(b, string(o.Source.Anchor.Space))
		b = codec.AppendUint(b, o.Sequence)
		b, _ = codec.AppendString(b, string(o.ID))
	}
	sum := sha256.Sum256(b)
	return model.ObjectID(hex.EncodeToString(sum[:])), nil
}
