// Package eventlog defines the statically registered core occurrence schema.
// Events describe published facts; indexes and query projections are derived
// from the journal and never grant external execution authority.
package eventlog

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const Type = "ate.event"
const ProfileType = "ate.agent.event"
const Version uint64 = 1
const KindCommandApplied = "core.command_applied"
const MaxParents = 1024

var ErrInvalid = errors.New("invalid canonical event")

func text(s string) bool { return s != "" && len(s) <= model.MaxIdentityBytes && utf8.ValidString(s) }
func Validate(e model.Event) error {
	if !text(string(e.Space)) || !text(string(e.RecordedBy)) || !text(e.Method) || e.Sequence == 0 || !model.ValidDigest(string(e.Payload)) || !model.ValidDigest(string(e.Command)) || len(e.CausalParents) > MaxParents {
		return ErrInvalid
	}
	if err := validateRegisteredEvent(e); err != nil {
		return err
	}
	if len(e.Transaction) != 32 {
		return ErrInvalid
	}
	b, err := hex.DecodeString(string(e.Transaction))
	if err != nil || hex.EncodeToString(b) != string(e.Transaction) {
		return ErrInvalid
	}
	seen := map[model.EventID]bool{}
	for _, p := range e.CausalParents {
		if !model.ValidDigest(string(p)) || seen[p] {
			return ErrInvalid
		}
		seen[p] = true
	}
	return nil
}

func Object(e model.Event) (codec.Object, error) {
	if err := Validate(e); err != nil {
		return codec.Object{}, err
	}
	parents := append([]model.EventID(nil), e.CausalParents...)
	sort.Slice(parents, func(i, j int) bool { return parents[i] < parents[j] })
	wireVersion, objectType := uint64(1), Type
	if e.Attribution != nil {
		objectType = ProfileType
	} else if e.SchemaVersion == 2 {
		wireVersion = 2
	}
	b := codec.AppendUint([]byte("ATE-EVENT\x00"), wireVersion)
	for _, s := range []string{string(e.Transaction), e.Stream.Kind, e.Stream.Owner} {
		b, _ = codec.AppendString(b, s)
	}
	b = codec.AppendUint(b, e.Stream.Incarnation)
	b = codec.AppendUint(b, e.Sequence)
	for _, s := range []string{string(e.RecordedBy), e.Kind, e.Method} {
		b, _ = codec.AppendString(b, s)
	}
	b = codec.AppendUint(b, e.SchemaVersion)
	b, _ = codec.AppendString(b, string(e.Payload))
	b, _ = codec.AppendString(b, string(e.Command))
	b = codec.AppendUint(b, uint64(len(parents)))
	refs := []model.ObjectID{e.Payload, e.Command}
	for _, p := range parents {
		b, _ = codec.AppendString(b, string(p))
		refs = append(refs, model.ObjectID(p))
	}
	if e.Attribution != nil {
		b, _ = codec.AppendString(b, string(e.PreviousInStream))
		raw, err := json.Marshal(e.Attribution)
		if err != nil {
			return codec.Object{}, err
		}
		b, err = codec.AppendBytes(b, raw)
		if err != nil {
			return codec.Object{}, err
		}
	} else if wireVersion == 2 {
		b, _ = codec.AppendString(b, string(e.PreviousInStream))
		b = codec.AppendUint(b, e.ProfileRegistryVersion)
		b = codec.AppendUint(b, uint64(len(e.ProfileEvents)))
		for _, id := range e.ProfileEvents {
			b, _ = codec.AppendString(b, string(id))
			refs = append(refs, model.ObjectID(id))
		}
	}
	if e.PreviousInStream != "" {
		refs = append(refs, model.ObjectID(e.PreviousInStream))
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i] < refs[j] })
	out := refs[:0]
	for _, r := range refs {
		if len(out) == 0 || out[len(out)-1] != r {
			out = append(out, r)
		}
	}
	o := codec.Object{Type: objectType, Version: wireVersion, Space: e.Space, Refs: out, Payload: b}
	if e.ID != "" {
		id, err := codec.ObjectID(o)
		if err != nil || model.EventID(id) != e.ID {
			return codec.Object{}, ErrInvalid
		}
	}
	return o, nil
}

func Decode(o codec.Object) (model.Event, error) {
	if !((o.Type == Type && (o.Version == 1 || o.Version == 2)) || (o.Type == ProfileType && o.Version == 1)) || !bytes.HasPrefix(o.Payload, []byte("ATE-EVENT\x00")) {
		return model.Event{}, ErrInvalid
	}
	d := codec.NewDecoder(o.Payload[len("ATE-EVENT\x00"):])
	v, err := d.Uint()
	if err != nil || v != o.Version {
		return model.Event{}, ErrInvalid
	}
	e := model.Event{Space: o.Space}
	s, err := d.String(32)
	if err != nil {
		return e, err
	}
	e.Transaction = model.TxID(s)
	e.Stream.Kind, err = d.String(model.MaxIdentityBytes)
	if err != nil {
		return e, err
	}
	e.Stream.Owner, err = d.String(model.MaxIdentityBytes)
	if err != nil {
		return e, err
	}
	e.Stream.Incarnation, err = d.Uint()
	if err != nil {
		return e, err
	}
	e.Sequence, err = d.Uint()
	if err != nil {
		return e, err
	}
	s, err = d.String(model.MaxIdentityBytes)
	if err != nil {
		return e, err
	}
	e.RecordedBy = model.PrincipalID(s)
	e.Kind, err = d.String(model.MaxIdentityBytes)
	if err != nil {
		return e, err
	}
	e.Method, err = d.String(model.MaxIdentityBytes)
	if err != nil {
		return e, err
	}
	e.SchemaVersion, err = d.Uint()
	if err != nil {
		return e, err
	}
	s, err = d.String(64)
	if err != nil {
		return e, err
	}
	e.Payload = model.ObjectID(s)
	s, err = d.String(64)
	if err != nil {
		return e, err
	}
	e.Command = model.ObjectID(s)
	n, err := d.Uint()
	if err != nil || n > MaxParents || n > uint64(d.Remaining()/65) {
		return e, ErrInvalid
	}
	for i := uint64(0); i < n; i++ {
		s, err = d.String(64)
		if err != nil {
			return e, err
		}
		e.CausalParents = append(e.CausalParents, model.EventID(s))
	}
	if o.Type == ProfileType || o.Version == 2 {
		s, err = d.String(64)
		if err != nil {
			return e, err
		}
		e.PreviousInStream = model.EventID(s)
		if o.Type == ProfileType {
			raw, err := d.Bytes(16 << 10)
			if err != nil {
				return e, err
			}
			var a model.EventAttribution
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&a); err != nil {
				return e, err
			}
			canonical, err := json.Marshal(a)
			if err != nil || !bytes.Equal(canonical, raw) {
				return e, ErrInvalid
			}
			e.Attribution = &a
		} else {
			e.ProfileRegistryVersion, err = d.Uint()
			if err != nil {
				return e, err
			}
			n, err = d.Uint()
			if err != nil || n > MaxProfileEvents || n > uint64(d.Remaining()/65) {
				return e, ErrInvalid
			}
			for i := uint64(0); i < n; i++ {
				s, err = d.String(64)
				if err != nil {
					return e, err
				}
				e.ProfileEvents = append(e.ProfileEvents, model.EventID(s))
			}
		}
	}
	if err = d.Done(); err != nil {
		return e, err
	}
	canonical, err := Object(e)
	if err != nil {
		return e, err
	}
	a, err := codec.EncodeObject(canonical)
	if err != nil {
		return e, err
	}
	b, err := codec.EncodeObject(o)
	if err != nil || !bytes.Equal(a, b) {
		return e, ErrInvalid
	}
	id, err := codec.ObjectID(o)
	if err != nil {
		return e, err
	}
	e.ID = model.EventID(id)
	return e, nil
}
