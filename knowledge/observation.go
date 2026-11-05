package knowledge

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type Observation struct {
	ID      model.ObjectID
	Key     model.KnowledgeKey
	Source  SourceAnchor
	Payload model.ObjectID
	// PayloadKind identifies an immutable promoted memory object, experience
	// path, or validated logical Summary. Empty means the ordinary raw form.
	PayloadKind string `json:"payload_kind,omitempty"`
	Sequence    uint64
	Label       string
	// ImportedFrom is an origin ObservationID in Source's selected execution
	// knowledge. Only an explicit Import command creates this schema2/4 record;
	// ordinary observations cannot silently inherit another execution's state.
	ImportedFrom model.ObjectID `json:",omitempty"`
}

func (o Observation) Validate() error {
	if !validText(string(o.Key.Actor)) || !validText(string(o.Key.Execution)) || !validText(o.Label) || !model.ValidDigest(string(o.Payload)) || o.Sequence == 0 {
		return ErrInvalid
	}
	if o.PayloadKind != "" && o.PayloadKind != "memory" && o.PayloadKind != "experience_path" && o.PayloadKind != "summary" {
		return ErrInvalid
	}
	if err := o.Source.Validate(); err != nil {
		return err
	}
	if o.ImportedFrom != "" {
		if !model.ValidDigest(string(o.ImportedFrom)) || o.Source.Anchor.Knowledge == nil || *o.Source.Anchor.Knowledge == o.Key || o.Source.Anchor.Cursor == nil {
			return ErrKnowledgeMismatch
		}
	} else if o.Source.Anchor.Knowledge != nil && *o.Source.Anchor.Knowledge != o.Key {
		return ErrKnowledgeMismatch
	}
	return nil
}
func ObservationID(o Observation) model.ObjectID {
	raw, err := encodeObservation(o, false)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(append([]byte("ATE-OBSERVATION\x00"), raw...))
	return model.ObjectID(fmt.Sprintf("%x", sum))
}
func (o Observation) Canonical() ([]byte, error) { return encodeObservation(o, true) }
func encodeObservation(o Observation, withID bool) ([]byte, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	version := Schema
	if o.ImportedFrom != "" {
		version = 2
	}
	if o.PayloadKind != "" {
		version = 3
		if o.ImportedFrom != "" {
			version = 4
		}
	}
	b := codec.AppendUint(nil, version)
	b, _ = codec.AppendString(b, string(o.Key.Actor))
	b, _ = codec.AppendString(b, string(o.Key.Execution))
	var err error
	b, err = encodeSource(b, o.Source)
	if err != nil {
		return nil, err
	}
	b, _ = codec.AppendString(b, string(o.Payload))
	b = codec.AppendUint(b, o.Sequence)
	b, _ = codec.AppendString(b, o.Label)
	if version == 3 || version == 4 {
		b, _ = codec.AppendString(b, o.PayloadKind)
	}
	if o.ImportedFrom != "" {
		b, _ = codec.AppendString(b, string(o.ImportedFrom))
	}
	if withID {
		if o.ID == "" || ObservationID(o) != o.ID {
			return nil, ErrInvalid
		}
		b, _ = codec.AppendString(b, string(o.ID))
	}
	return b, nil
}
func DecodeObservation(raw []byte) (Observation, error) {
	if len(raw) > MaxRecordBytes {
		return Observation{}, ErrInvalid
	}
	d := codec.NewDecoder(raw)
	v, e := d.Uint()
	if e != nil || (v != Schema && v != 2 && v != 3 && v != 4) {
		return Observation{}, ErrInvalid
	}
	actor, e := d.String(model.MaxIdentityBytes)
	if e != nil {
		return Observation{}, e
	}
	execution, e := d.String(model.MaxIdentityBytes)
	if e != nil {
		return Observation{}, e
	}
	source, e := decodeSource(d)
	if e != nil {
		return Observation{}, e
	}
	payload, e := d.String(64)
	if e != nil || !model.ValidDigest(payload) {
		return Observation{}, ErrInvalid
	}
	seq, e := d.Uint()
	if e != nil {
		return Observation{}, e
	}
	label, e := d.String(model.MaxIdentityBytes)
	if e != nil {
		return Observation{}, e
	}
	var payloadKind string
	if v == 3 || v == 4 {
		payloadKind, e = d.String(model.MaxIdentityBytes)
		if e != nil || (payloadKind != "memory" && payloadKind != "experience_path" && payloadKind != "summary") {
			return Observation{}, ErrInvalid
		}
	}
	var imported model.ObjectID
	if v == 2 || v == 4 {
		origin, err := d.String(64)
		if err != nil || !model.ValidDigest(origin) {
			return Observation{}, ErrInvalid
		}
		imported = model.ObjectID(origin)
	}
	id, e := d.String(64)
	if e != nil || !model.ValidDigest(id) {
		return Observation{}, ErrInvalid
	}
	if e = d.Done(); e != nil {
		return Observation{}, e
	}
	o := Observation{ID: model.ObjectID(id), Key: model.KnowledgeKey{Actor: model.ActorID(actor), Execution: model.ExecutionID(execution)}, Source: source, Payload: model.ObjectID(payload), PayloadKind: payloadKind, Sequence: seq, Label: label, ImportedFrom: imported}
	if err := o.Validate(); err != nil || ObservationID(o) != o.ID {
		return Observation{}, ErrInvalid
	}
	canonical, err := o.Canonical()
	if err != nil || !bytes.Equal(canonical, raw) {
		return Observation{}, ErrInvalid
	}
	return o, nil
}

type ContextItem struct {
	Ordinal     uint64
	Observation model.ObjectID
	Content     model.ObjectID
	Label       string
}

// RecordedInput binds the exact complete framed request supplied to a model.
// Object addresses immutable stored bytes; Anchor proves where those bytes
// were published. It is separate from Source, which selects the knowledge
// used to render them. Rendering must not include this Packet's own final ID.
type RecordedInput struct {
	Object   model.ObjectID
	Anchor   model.SnapshotAnchor
	Encoding string
}

func (o Observation) Clone() Observation { o.Source = o.Source.clone(); return o }
