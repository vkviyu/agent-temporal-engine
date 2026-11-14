package knowledge

import (
	"bytes"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// ImportCommand explicitly adopts a bounded, ordered selection from another
// execution's published knowledge position. A fresh execution can use this as
// its seed; parent/Actor identity alone never imports anything. ExpectedSequence
// is the target's knowledge CAS, not the source revision sequence.
type ImportCommand struct {
	Target           model.KnowledgeKey
	Source           SourceAnchor
	ExpectedSequence uint64
	Observations     []model.ObjectID
}

func (c ImportCommand) Validate() error {
	if !validText(string(c.Target.Actor)) || !validText(string(c.Target.Execution)) || c.Source.Validate() != nil || c.Source.Anchor.Knowledge == nil || *c.Source.Anchor.Knowledge == c.Target || c.Source.Anchor.Cursor == nil || len(c.Observations) == 0 || len(c.Observations) > MaxItems || uint64(len(c.Observations)) > ^uint64(0)-c.ExpectedSequence {
		return ErrKnowledgeMismatch
	}
	seen := make(map[model.ObjectID]bool, len(c.Observations))
	for _, id := range c.Observations {
		if !model.ValidDigest(string(id)) || seen[id] {
			return ErrDuplicateItem
		}
		seen[id] = true
	}
	return nil
}

func encodeImport(c ImportCommand) ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	b := codec.AppendUint([]byte("ATE-KNOWLEDGE-IMPORT\x00"), 1)
	b, _ = codec.AppendString(b, string(c.Target.Actor))
	b, _ = codec.AppendString(b, string(c.Target.Execution))
	var err error
	b, err = encodeSource(b, c.Source)
	if err != nil {
		return nil, err
	}
	b = codec.AppendUint(b, c.ExpectedSequence)
	b = codec.AppendUint(b, uint64(len(c.Observations)))
	for _, id := range c.Observations {
		b, _ = codec.AppendString(b, string(id))
	}
	return b, nil
}

func decodeImport(raw []byte) (ImportCommand, error) {
	const prefix = "ATE-KNOWLEDGE-IMPORT\x00"
	if len(raw) > MaxRecordBytes || !bytes.HasPrefix(raw, []byte(prefix)) {
		return ImportCommand{}, ErrInvalid
	}
	d := codec.NewDecoder(raw[len(prefix):])
	v, err := d.Uint()
	if err != nil || v != 1 {
		return ImportCommand{}, ErrInvalid
	}
	actor, err := d.String(model.MaxIdentityBytes)
	if err != nil {
		return ImportCommand{}, err
	}
	execution, err := d.String(model.MaxIdentityBytes)
	if err != nil {
		return ImportCommand{}, err
	}
	source, err := decodeSource(d)
	if err != nil {
		return ImportCommand{}, err
	}
	sequence, err := d.Uint()
	if err != nil {
		return ImportCommand{}, err
	}
	n, err := d.Uint()
	if err != nil || n == 0 || n > MaxItems || n > uint64(d.Remaining()/65) {
		return ImportCommand{}, ErrInvalid
	}
	c := ImportCommand{Target: model.KnowledgeKey{Actor: model.ActorID(actor), Execution: model.ExecutionID(execution)}, Source: source, ExpectedSequence: sequence}
	for ; n > 0; n-- {
		id, err := d.String(64)
		if err != nil {
			return ImportCommand{}, err
		}
		c.Observations = append(c.Observations, model.ObjectID(id))
	}
	if err = d.Done(); err != nil {
		return ImportCommand{}, err
	}
	canonical, err := encodeImport(c)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ImportCommand{}, ErrInvalid
	}
	return c, nil
}

func applyImport(s Snapshot, c ImportCommand) (Snapshot, error) {
	source, _, err := SelectRevision(s, c.Source.Anchor.Space, *c.Source.Anchor.Knowledge, c.Source.Anchor.Cursor.Revision)
	if err != nil {
		return Snapshot{}, err
	}
	var sequence uint64
	for _, item := range s.Observations {
		if item.Key == c.Target && item.Source.Anchor.Space == c.Source.Anchor.Space {
			sequence++
		}
	}
	if sequence != c.ExpectedSequence {
		return Snapshot{}, ErrObservationSequence
	}
	origins := make(map[model.ObjectID]Observation, len(source.Observations))
	for _, item := range source.Observations {
		origins[item.ID] = item
	}
	out := s.Clone()
	imported, err := ImportedObservations(c, origins)
	if err != nil {
		return Snapshot{}, err
	}
	out.Observations = append(out.Observations, imported...)
	return out, nil
}

// ImportedObservations derives the ordered target records from origins that
// the caller has authenticated at the selected source position.
func ImportedObservations(c ImportCommand, origins map[model.ObjectID]Observation) ([]Observation, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	out := make([]Observation, 0, len(c.Observations))
	sequence := c.ExpectedSequence
	for _, id := range c.Observations {
		origin, ok := origins[id]
		if !ok || origin.ID != id || origin.Key != *c.Source.Anchor.Knowledge || origin.Source.Anchor.Space != c.Source.Anchor.Space {
			return nil, ErrUnknownObservation
		}
		sequence++
		item := Observation{Key: c.Target, Source: c.Source.clone(), Payload: origin.Payload, PayloadKind: origin.PayloadKind, Sequence: sequence, Label: origin.Label, ImportedFrom: origin.ID}
		item.ID = ObservationID(item)
		out = append(out, item)
	}
	return out, nil
}
