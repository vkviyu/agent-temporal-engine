package memory

import (
	"bytes"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const MaxSnapshotBytes = 32 << 20
const MaxSnapshotRecords = 65536

type VersionLink struct{ Before, After model.ObjectID }
type SourceBan struct {
	Source     model.ObjectID
	Retraction Retraction
}

// Snapshot preserves immutable audit versions and current withdrawal authority.
// Tokens are a derived index and are intentionally not serialized.
type Snapshot struct {
	Memories       []Memory
	Paths          []ExperiencePath
	Bans           []SourceBan
	Successors     []VersionLink
	PathSuccessors []VersionLink
}

func (s *Store) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := Snapshot{}
	for _, m := range s.memories {
		out.Memories = append(out.Memories, cloneMemory(m))
	}
	for _, p := range s.paths {
		out.Paths = append(out.Paths, clonePath(p))
	}
	for id, r := range s.sourceBan {
		out.Bans = append(out.Bans, SourceBan{id, r})
	}
	for a, b := range s.successors {
		out.Successors = append(out.Successors, VersionLink{a, b})
	}
	for a, b := range s.pathSuccessors {
		out.PathSuccessors = append(out.PathSuccessors, VersionLink{a, b})
	}
	return out
}

func snapshotStore(v Snapshot) (*Store, error) {
	if len(v.Memories) > MaxSnapshotRecords || len(v.Paths) > MaxSnapshotRecords || len(v.Bans) > MaxSnapshotRecords || len(v.Successors) > MaxSnapshotRecords || len(v.PathSuccessors) > MaxSnapshotRecords {
		return nil, ErrInvalidMemory
	}
	s := New()
	for _, m := range v.Memories {
		if m.ID == "" || Validate(m) != nil {
			return nil, ErrInvalidMemory
		}
		if _, ok := s.memories[m.ID]; ok {
			return nil, ErrAlreadyExists
		}
		s.memories[m.ID] = cloneMemory(m)
		s.index(m)
	}
	for _, p := range v.Paths {
		if p.ID == "" {
			return nil, ErrPathInvalid
		}
		if _, err := CanonicalPath(p); err != nil {
			return nil, err
		}
		if _, ok := s.paths[p.ID]; ok {
			return nil, ErrAlreadyExists
		}
		for _, step := range p.Steps {
			m, ok := s.memories[step.Memory]
			if !ok || m.Space != p.Space {
				return nil, ErrPathInvalid
			}
		}
		s.paths[p.ID] = clonePath(p)
	}
	for _, b := range v.Bans {
		if !model.ValidDigest(string(b.Source)) || !validRetraction(&b.Retraction) {
			return nil, ErrInvalidMemory
		}
		if _, ok := s.sourceBan[b.Source]; ok {
			return nil, ErrAlreadyExists
		}
		s.sourceBan[b.Source] = b.Retraction
	}
	checkLinks := func(links []VersionLink, paths bool) error {
		index := s.successors
		if paths {
			index = s.pathSuccessors
		}
		for _, l := range links {
			if l.Before == l.After || !model.ValidDigest(string(l.Before)) || !model.ValidDigest(string(l.After)) {
				return ErrInvalidTransition
			}
			if _, ok := index[l.Before]; ok {
				return ErrInvalidTransition
			}
			valid := false
			if paths {
				a, ok := s.paths[l.Before]
				b, bok := s.paths[l.After]
				if ok && bok && a.Space == b.Space {
					for _, id := range b.DerivedFrom {
						valid = valid || id == l.Before
					}
				}
			} else {
				a, ok := s.memories[l.Before]
				b, bok := s.memories[l.After]
				if ok && bok && a.Space == b.Space && transition(a.Status, b.Status) {
					for _, id := range b.Supersedes {
						valid = valid || id == l.Before
					}
				}
			}
			if !valid {
				return ErrInvalidTransition
			}
			index[l.Before] = l.After
		}
		for id := range index {
			seen := map[model.ObjectID]bool{}
			for id != "" {
				if seen[id] {
					return ErrInvalidTransition
				}
				seen[id] = true
				id = index[id]
			}
		}
		return nil
	}
	if err := checkLinks(v.Successors, false); err != nil {
		return nil, err
	}
	if err := checkLinks(v.PathSuccessors, true); err != nil {
		return nil, err
	}
	return s, nil
}

func EncodeSnapshot(v Snapshot) ([]byte, error) {
	if _, err := snapshotStore(v); err != nil {
		return nil, err
	}
	v.Memories = append([]Memory(nil), v.Memories...)
	v.Paths = append([]ExperiencePath(nil), v.Paths...)
	v.Bans = append([]SourceBan(nil), v.Bans...)
	v.Successors = append([]VersionLink(nil), v.Successors...)
	v.PathSuccessors = append([]VersionLink(nil), v.PathSuccessors...)
	sort.Slice(v.Memories, func(i, j int) bool { return v.Memories[i].ID < v.Memories[j].ID })
	sort.Slice(v.Paths, func(i, j int) bool { return v.Paths[i].ID < v.Paths[j].ID })
	sort.Slice(v.Bans, func(i, j int) bool { return v.Bans[i].Source < v.Bans[j].Source })
	for _, links := range [][]VersionLink{v.Successors, v.PathSuccessors} {
		sort.Slice(links, func(i, j int) bool { return links[i].Before < links[j].Before })
	}
	b := codec.AppendUint([]byte("ATE-MEMORY-SNAPSHOT\x00"), 1)
	add := func(raw []byte) error {
		var err error
		b, err = codec.AppendBytes(b, raw)
		if len(b) > MaxSnapshotBytes {
			return ErrInvalidMemory
		}
		return err
	}
	b = codec.AppendUint(b, uint64(len(v.Memories)))
	for _, m := range v.Memories {
		raw, e := Canonical(m)
		if e != nil {
			return nil, e
		}
		if e = add(raw); e != nil {
			return nil, e
		}
	}
	b = codec.AppendUint(b, uint64(len(v.Paths)))
	for _, p := range v.Paths {
		raw, e := CanonicalPath(p)
		if e != nil {
			return nil, e
		}
		if e = add(raw); e != nil {
			return nil, e
		}
	}
	b = codec.AppendUint(b, uint64(len(v.Bans)))
	for _, ban := range v.Bans {
		b = field(b, string(ban.Source))
		if e := add(encodeRetraction(ban.Retraction)); e != nil {
			return nil, e
		}
	}
	for _, links := range [][]VersionLink{v.Successors, v.PathSuccessors} {
		b = codec.AppendUint(b, uint64(len(links)))
		for _, l := range links {
			b = field(b, string(l.Before))
			b = field(b, string(l.After))
		}
	}
	if len(b) > MaxSnapshotBytes {
		return nil, ErrInvalidMemory
	}
	return b, nil
}

func DecodeSnapshot(raw []byte) (Snapshot, error) {
	const prefix = "ATE-MEMORY-SNAPSHOT\x00"
	if len(raw) > MaxSnapshotBytes || !bytes.HasPrefix(raw, []byte(prefix)) {
		return Snapshot{}, ErrInvalidMemory
	}
	r := &recordReader{d: codec.NewDecoder(raw[len(prefix):])}
	if r.u() != 1 {
		return Snapshot{}, ErrInvalidMemory
	}
	v := Snapshot{}
	for n := r.count(MaxSnapshotRecords, 1); n > 0; n-- {
		m, e := DecodeMemory(r.blob(MaxRecordBytes))
		if e != nil {
			return Snapshot{}, e
		}
		v.Memories = append(v.Memories, m)
	}
	for n := r.count(MaxSnapshotRecords, 1); n > 0; n-- {
		p, e := DecodePath(r.blob(MaxRecordBytes))
		if e != nil {
			return Snapshot{}, e
		}
		v.Paths = append(v.Paths, p)
	}
	for n := r.count(MaxSnapshotRecords, 1); n > 0; n-- {
		id := model.ObjectID(r.text(64))
		ban, e := decodeRetraction(r.blob(MaxRecordBytes))
		if e != nil {
			return Snapshot{}, e
		}
		v.Bans = append(v.Bans, SourceBan{id, ban})
	}
	for i := 0; i < 2; i++ {
		links := []VersionLink{}
		for n := r.count(MaxSnapshotRecords, 2); n > 0; n-- {
			links = append(links, VersionLink{model.ObjectID(r.text(64)), model.ObjectID(r.text(64))})
		}
		if i == 0 {
			v.Successors = links
		} else {
			v.PathSuccessors = links
		}
	}
	if r.err != nil {
		return Snapshot{}, r.err
	}
	if err := r.d.Done(); err != nil {
		return Snapshot{}, err
	}
	b, e := EncodeSnapshot(v)
	if e != nil || !bytes.Equal(b, raw) {
		return Snapshot{}, ErrInvalidMemory
	}
	return v, nil
}

func RestoreSnapshot(raw []byte) (*Store, error) {
	v, err := DecodeSnapshot(raw)
	if err != nil {
		return nil, err
	}
	return snapshotStore(v)
}
