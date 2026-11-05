package knowledge

import (
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type SourceAnchor struct {
	Anchor     model.SnapshotAnchor
	ViewDigest model.ObjectID
	Frontier   model.ObjectID
}

func (s SourceAnchor) Validate() error {
	if err := s.Anchor.Validate(); err != nil {
		return err
	}
	if !validText(string(s.Anchor.Space)) || !validText(string(s.Anchor.Fence.Store)) {
		return ErrInvalid
	}
	if s.Anchor.Knowledge != nil && (!validText(string(s.Anchor.Knowledge.Actor)) || !validText(string(s.Anchor.Knowledge.Execution))) {
		return ErrInvalid
	}
	if s.ViewDigest != "" && !model.ValidDigest(string(s.ViewDigest)) {
		return ErrInvalid
	}
	if s.Frontier != "" && !model.ValidDigest(string(s.Frontier)) {
		return ErrInvalid
	}
	if s.Anchor.Knowledge != nil && s.Anchor.Knowledge.Actor == "" {
		return ErrInvalid
	}
	return nil
}

// ValidateBefore is strict: a ContextPacket/Observation commit cannot source
// its own fence or any later publication. This prevents future leakage and
// hash/self-reference cycles during pending render recovery.
func (s SourceAnchor) ValidateBefore(consumer model.CommitFence) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if err := consumer.Validate(); err != nil {
		return err
	}
	if s.Anchor.Fence.Store != consumer.Store || s.Anchor.Fence.LSN >= consumer.LSN {
		return ErrFutureSource
	}
	return nil
}
func (s SourceAnchor) clone() SourceAnchor {
	return SourceAnchor{Anchor: s.Anchor.Clone(), ViewDigest: s.ViewDigest, Frontier: s.Frontier}
}

func encodeSource(b []byte, s SourceAnchor) ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	a := s.Anchor
	b, _ = codec.AppendString(b, string(a.Fence.Store))
	b = codec.AppendUint(b, a.Fence.LSN)
	b, _ = codec.AppendString(b, string(a.Fence.CommitDigest))
	b, _ = codec.AppendString(b, string(a.Space))
	if a.Cursor == nil {
		b = codec.AppendUint(b, 0)
	} else {
		b = codec.AppendUint(b, 1)
		b, _ = codec.AppendString(b, string(a.Cursor.Store))
		b, _ = codec.AppendString(b, string(a.Cursor.Space))
		b, _ = codec.AppendString(b, string(a.Cursor.Revision))
	}
	if a.Knowledge == nil {
		b = codec.AppendUint(b, 0)
	} else {
		b = codec.AppendUint(b, 1)
		b, _ = codec.AppendString(b, string(a.Knowledge.Actor))
		b, _ = codec.AppendString(b, string(a.Knowledge.Execution))
	}
	if s.ViewDigest == "" {
		b = codec.AppendUint(b, 0)
	} else {
		b = codec.AppendUint(b, 1)
		b, _ = codec.AppendString(b, string(s.ViewDigest))
	}
	if s.Frontier == "" {
		b = codec.AppendUint(b, 0)
	} else {
		b = codec.AppendUint(b, 1)
		b, _ = codec.AppendString(b, string(s.Frontier))
	}
	return b, nil
}
func decodeSource(d *codec.Decoder) (SourceAnchor, error) {
	store, e := d.String(model.MaxIdentityBytes)
	if e != nil {
		return SourceAnchor{}, e
	}
	lsn, e := d.Uint()
	if e != nil {
		return SourceAnchor{}, e
	}
	digest, e := d.String(64)
	if e != nil || !model.ValidDigest(digest) {
		return SourceAnchor{}, ErrInvalid
	}
	space, e := d.String(model.MaxIdentityBytes)
	if e != nil {
		return SourceAnchor{}, e
	}
	var cursor *model.Cursor
	present, e := d.Uint()
	if e != nil {
		return SourceAnchor{}, e
	}
	if present == 1 {
		cs, e := d.String(model.MaxIdentityBytes)
		if e != nil {
			return SourceAnchor{}, e
		}
		cp, e := d.String(model.MaxIdentityBytes)
		if e != nil {
			return SourceAnchor{}, e
		}
		cr, e := d.String(64)
		if e != nil || !model.ValidDigest(cr) {
			return SourceAnchor{}, ErrInvalid
		}
		cursor = &model.Cursor{Store: model.StoreID(cs), Space: model.SpaceID(cp), Revision: model.RevisionID(cr)}
	} else if present != 0 {
		return SourceAnchor{}, ErrInvalid
	}
	var key *model.KnowledgeKey
	present, e = d.Uint()
	if e != nil {
		return SourceAnchor{}, e
	}
	if present == 1 {
		a, e := d.String(model.MaxIdentityBytes)
		if e != nil {
			return SourceAnchor{}, e
		}
		x, e := d.String(model.MaxIdentityBytes)
		if e != nil {
			return SourceAnchor{}, e
		}
		key = &model.KnowledgeKey{Actor: model.ActorID(a), Execution: model.ExecutionID(x)}
	} else if present != 0 {
		return SourceAnchor{}, ErrInvalid
	}
	var view, frontier model.ObjectID
	present, e = d.Uint()
	if e != nil {
		return SourceAnchor{}, e
	}
	if present == 1 {
		v, e := d.String(64)
		if e != nil || !model.ValidDigest(v) {
			return SourceAnchor{}, ErrInvalid
		}
		view = model.ObjectID(v)
	} else if present != 0 {
		return SourceAnchor{}, ErrInvalid
	}
	present, e = d.Uint()
	if e != nil {
		return SourceAnchor{}, e
	}
	if present == 1 {
		v, e := d.String(64)
		if e != nil || !model.ValidDigest(v) {
			return SourceAnchor{}, ErrInvalid
		}
		frontier = model.ObjectID(v)
	} else if present != 0 {
		return SourceAnchor{}, ErrInvalid
	}
	return SourceAnchor{Anchor: model.SnapshotAnchor{Fence: model.CommitFence{Store: model.StoreID(store), LSN: lsn, CommitDigest: model.CommitDigest(digest)}, Space: model.SpaceID(space), Cursor: cursor, Knowledge: key}, ViewDigest: view, Frontier: frontier}, nil
}

func validText(s string) bool {
	return len(s) > 0 && len(s) <= model.MaxIdentityBytes && utf8.ValidString(s)
}
