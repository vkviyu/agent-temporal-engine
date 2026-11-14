package knowledge

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// PacketSchema includes the atomic persisted provenance-index contract.
// Earlier pre-release schemas are deliberately not decoded or replayed.
const PacketSchema uint64 = 5

type ContextPacket struct {
	ID              model.ObjectID
	Space           model.SpaceID
	Key             *model.KnowledgeKey
	Source          SourceAnchor
	GoalRef         model.ObjectID
	Items           []ContextItem
	Renderer        string
	Tokenizer       string
	EstimatedTokens uint64
	Input           *RecordedInput          `json:",omitempty"`
	Selection       *ContextSelectionRecord `json:",omitempty"`
	Requirements    *ContextRequirements    `json:",omitempty"`
}

func (p ContextPacket) Validate() error {
	if !validText(string(p.Space)) || !validText(p.Renderer) || !validText(p.Tokenizer) || p.EstimatedTokens > MaxEstimatedTokens {
		return ErrInvalid
	}
	if p.Key != nil && (!validText(string(p.Key.Actor)) || !validText(string(p.Key.Execution))) {
		return ErrInvalid
	}
	if err := p.Source.Validate(); err != nil {
		return err
	}
	if p.Space != p.Source.Anchor.Space {
		return ErrInvalid
	}
	if p.Key != nil && (p.Source.Anchor.Knowledge == nil || *p.Source.Anchor.Knowledge != *p.Key) {
		return ErrKnowledgeMismatch
	}
	if p.Key != nil && (p.Source.ViewDigest == "" || p.Source.Frontier == "" || p.GoalRef == "") {
		return ErrInvalid
	}
	if p.GoalRef != "" && !model.ValidDigest(string(p.GoalRef)) {
		return ErrInvalid
	}
	if input := p.Input; input != nil {
		if !model.ValidDigest(string(input.Object)) || !validText(input.Encoding) || input.Anchor.Validate() != nil || input.Anchor.Space != p.Space || input.Anchor.Fence.Store != p.Source.Anchor.Fence.Store || input.Anchor.Knowledge != nil {
			return ErrInvalid
		}
	}
	if len(p.Items) > MaxItems {
		return ErrInvalid
	}
	seen := make(map[model.ObjectID]bool, len(p.Items))
	for i, item := range p.Items {
		if !model.ValidDigest(string(item.Observation)) || !model.ValidDigest(string(item.Content)) || item.Ordinal != uint64(i) || !validText(item.Label) {
			return ErrInvalid
		}
		if seen[item.Observation] {
			return ErrDuplicateItem
		}
		seen[item.Observation] = true
	}
	if err := validatePacketSelectionShape(p); err != nil {
		return err
	}
	return validateContextRequirements(p)
}
func (p ContextPacket) ValidateBefore(consumer model.CommitFence) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if err := consumer.Validate(); err != nil {
		return err
	}
	if p.Space != p.Source.Anchor.Space {
		return ErrInvalid
	}
	if err := p.Source.ValidateBefore(consumer); err != nil {
		return err
	}
	if p.Input != nil {
		return (SourceAnchor{Anchor: p.Input.Anchor}).ValidateBefore(consumer)
	}
	return nil
}
func (p ContextPacket) Canonical() ([]byte, error) { return encodePacket(p, true) }
func PacketID(p ContextPacket) model.ObjectID {
	raw, err := encodePacket(p, false)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(append([]byte("ATE-CONTEXT-PACKET\x00"), raw...))
	return model.ObjectID(fmt.Sprintf("%x", sum))
}
func encodePacket(p ContextPacket, withID bool) ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	b := codec.AppendUint(nil, PacketSchema)
	b, _ = codec.AppendString(b, string(p.Space))
	if p.Key == nil {
		b = codec.AppendUint(b, 0)
	} else {
		b = codec.AppendUint(b, 1)
		b, _ = codec.AppendString(b, string(p.Key.Actor))
		b, _ = codec.AppendString(b, string(p.Key.Execution))
	}
	var err error
	b, err = encodeSource(b, p.Source)
	if err != nil {
		return nil, err
	}
	b, _ = codec.AppendString(b, string(p.GoalRef))
	b = codec.AppendUint(b, uint64(len(p.Items)))
	for _, item := range p.Items {
		b = codec.AppendUint(b, item.Ordinal)
		b, _ = codec.AppendString(b, string(item.Observation))
		b, _ = codec.AppendString(b, string(item.Content))
		b, _ = codec.AppendString(b, item.Label)
	}
	b, _ = codec.AppendString(b, p.Renderer)
	b, _ = codec.AppendString(b, p.Tokenizer)
	b = codec.AppendUint(b, p.EstimatedTokens)
	present := uint64(0)
	if p.Input != nil {
		present = 1
	}
	b = codec.AppendUint(b, present)
	if p.Input != nil {
		b, _ = codec.AppendString(b, string(p.Input.Object))
		b, err = encodeSource(b, SourceAnchor{Anchor: p.Input.Anchor})
		if err != nil {
			return nil, err
		}
		b, _ = codec.AppendString(b, p.Input.Encoding)
	}
	present = 0
	if p.Selection != nil {
		present = 1
	}
	b = codec.AppendUint(b, present)
	if p.Selection != nil {
		record, err := encodeContextSelection(*p.Selection)
		if err != nil {
			return nil, err
		}
		b, err = codec.AppendBytes(b, record)
		if err != nil {
			return nil, err
		}
	}
	present = 0
	if p.Requirements != nil {
		present = 1
	}
	b = codec.AppendUint(b, present)
	if p.Requirements != nil {
		b, _ = codec.AppendBytes(b, encodeContextRequirements(*p.Requirements))
	}
	if withID {
		if p.ID == "" || PacketID(p) != p.ID {
			return nil, ErrInvalid
		}
		b, _ = codec.AppendString(b, string(p.ID))
	}
	if len(b) > MaxRecordBytes {
		return nil, ErrInvalid
	}
	return b, nil
}
func DecodePacket(raw []byte) (ContextPacket, error) {
	if len(raw) > MaxRecordBytes {
		return ContextPacket{}, ErrInvalid
	}
	d := codec.NewDecoder(raw)
	v, e := d.Uint()
	if e != nil {
		return ContextPacket{}, ErrInvalid
	}
	if v != PacketSchema {
		return ContextPacket{}, fmt.Errorf("%w: packet %d", ErrUnsupportedSchema, v)
	}
	space, e := d.String(model.MaxIdentityBytes)
	if e != nil {
		return ContextPacket{}, e
	}
	present, e := d.Uint()
	if e != nil {
		return ContextPacket{}, e
	}
	var key *model.KnowledgeKey
	if present == 1 {
		a, e := d.String(model.MaxIdentityBytes)
		if e != nil {
			return ContextPacket{}, e
		}
		x, e := d.String(model.MaxIdentityBytes)
		if e != nil {
			return ContextPacket{}, e
		}
		key = &model.KnowledgeKey{Actor: model.ActorID(a), Execution: model.ExecutionID(x)}
	} else if present != 0 {
		return ContextPacket{}, ErrInvalid
	}
	source, e := decodeSource(d)
	if e != nil {
		return ContextPacket{}, e
	}
	goal, e := d.String(64)
	if e != nil {
		return ContextPacket{}, e
	}
	n, e := d.Uint()
	if e != nil || n > MaxItems || n > uint64(d.Remaining()/133) {
		return ContextPacket{}, ErrInvalid
	}
	items := make([]ContextItem, int(n))
	for i := range items {
		ord, e := d.Uint()
		if e != nil {
			return ContextPacket{}, e
		}
		obs, e := d.String(64)
		if e != nil {
			return ContextPacket{}, e
		}
		content, e := d.String(64)
		if e != nil {
			return ContextPacket{}, e
		}
		label, e := d.String(model.MaxIdentityBytes)
		if e != nil {
			return ContextPacket{}, e
		}
		items[i] = ContextItem{Ordinal: ord, Observation: model.ObjectID(obs), Content: model.ObjectID(content), Label: label}
	}
	renderer, e := d.String(model.MaxIdentityBytes)
	if e != nil {
		return ContextPacket{}, e
	}
	tokenizer, e := d.String(model.MaxIdentityBytes)
	if e != nil {
		return ContextPacket{}, e
	}
	tokens, e := d.Uint()
	if e != nil {
		return ContextPacket{}, e
	}
	var input *RecordedInput
	present, err := d.Uint()
	if err != nil || present > 1 {
		return ContextPacket{}, ErrInvalid
	}
	if present == 1 {
		object, err := d.String(64)
		if err != nil {
			return ContextPacket{}, err
		}
		anchor, err := decodeSource(d)
		if err != nil || anchor.ViewDigest != "" || anchor.Frontier != "" {
			return ContextPacket{}, ErrInvalid
		}
		encoding, err := d.String(model.MaxIdentityBytes)
		if err != nil {
			return ContextPacket{}, err
		}
		input = &RecordedInput{Object: model.ObjectID(object), Anchor: anchor.Anchor, Encoding: encoding}
	}
	var selection *ContextSelectionRecord
	presentSelection, err := d.Uint()
	if err != nil || presentSelection > 1 {
		return ContextPacket{}, ErrInvalid
	}
	if presentSelection == 1 {
		raw, err := d.Bytes(MaxRecordBytes)
		if err != nil {
			return ContextPacket{}, err
		}
		record, err := decodeContextSelection(raw)
		if err != nil {
			return ContextPacket{}, err
		}
		selection = &record
	}
	var requirements *ContextRequirements
	presentRequirements, err := d.Uint()
	if err != nil || presentRequirements > 1 {
		return ContextPacket{}, ErrInvalid
	}
	if presentRequirements == 1 {
		encoded, err := d.Bytes(MaxRecordBytes)
		if err != nil {
			return ContextPacket{}, err
		}
		decoded, err := decodeContextRequirements(encoded)
		if err != nil {
			return ContextPacket{}, err
		}
		requirements = &decoded
	}
	id, e := d.String(64)
	if e != nil || !model.ValidDigest(id) {
		return ContextPacket{}, ErrInvalid
	}
	if e = d.Done(); e != nil {
		return ContextPacket{}, e
	}
	p := ContextPacket{ID: model.ObjectID(id), Space: model.SpaceID(space), Key: key, Source: source, GoalRef: model.ObjectID(goal), Items: items, Renderer: renderer, Tokenizer: tokenizer, EstimatedTokens: tokens, Input: input, Selection: selection, Requirements: requirements}
	if err := p.Validate(); err != nil || PacketID(p) != p.ID {
		return ContextPacket{}, ErrInvalid
	}
	canonical, err := p.Canonical()
	if err != nil || !bytes.Equal(canonical, raw) {
		return ContextPacket{}, ErrInvalid
	}
	return p, nil
}
func (p ContextPacket) Clone() ContextPacket {
	p.Source = p.Source.clone()
	if p.Input != nil {
		input := *p.Input
		input.Anchor = input.Anchor.Clone()
		p.Input = &input
	}
	if p.Key != nil {
		k := *p.Key
		p.Key = &k
	}
	p.Items = append([]ContextItem(nil), p.Items...)
	if p.Selection != nil {
		record := p.Selection.Clone()
		p.Selection = &record
	}
	if p.Requirements != nil {
		r := p.Requirements.Clone()
		p.Requirements = &r
	}
	return p
}
