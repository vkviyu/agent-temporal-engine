package graph

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// Schema 2 adds the explicit type/schema domain to RevisionID. Experimental
// schema 1 used a different identity algorithm and is rejected, not silently
// reinterpreted. An adapter must perform an explicit migration if it retains it.
const RevisionSchema uint64 = 2
const ProducedRevisionSchema uint64 = 3
const BranchSchema uint64 = 1
const ScopedBranchSchema uint64 = 2
const MaxBranchBytes = 1024
const revisionDomain = "ATE-REVISION\x00"

func uintSize(n uint64) int   { var b [binary.MaxVarintLen64]byte; return binary.PutUvarint(b[:], n) }
func stringSize(s string) int { return uintSize(uint64(len(s))) + len(s) }
func revisionEncodedSize(r Revision) int {
	n := uintSize(RevisionSchema) + stringSize(string(r.Space)) + stringSize(string(r.StateParent)) + uintSize(uint64(len(r.JoinParents))) + stringSize(string(r.StateRoot)) + uintSize(uint64(len(r.Events))) + uintSize(uint64(len(r.Entries)))
	for _, p := range r.JoinParents {
		n += stringSize(string(p))
	}
	for _, e := range r.Events {
		n += stringSize(string(e))
	}
	for _, e := range r.Entries {
		n += uintSize(uint64(len(e.Key))) + len(e.Key) + stringSize(string(e.Value))
	}
	if p := r.Producer; p != nil {
		n += stringSize(string(p.Branch)) + uintSize(p.BranchVersion) + stringSize(string(p.Principal)) + stringSize(string(p.Execution)) + stringSize(string(p.Task)) + stringSize(string(p.Actor)) + uintSize(p.GoalEpoch) + uintSize(p.Incarnation) + uintSize(p.AuthorityVersion)
	}
	return n
}
func revisionPayload(r Revision) []byte {
	b := make([]byte, 0, revisionEncodedSize(r))
	schema := RevisionSchema
	if r.Producer != nil {
		schema = ProducedRevisionSchema
	}
	b = codec.AppendUint(b, schema)
	// normalizeRevision checked all text, byte and aggregate bounds first.
	b, _ = codec.AppendString(b, string(r.Space))
	b, _ = codec.AppendString(b, string(r.StateParent))
	b = codec.AppendUint(b, uint64(len(r.JoinParents)))
	for _, p := range r.JoinParents {
		b, _ = codec.AppendString(b, string(p))
	}
	b, _ = codec.AppendString(b, string(r.StateRoot))
	b = codec.AppendUint(b, uint64(len(r.Events)))
	for _, e := range r.Events {
		b, _ = codec.AppendString(b, string(e))
	}
	b = codec.AppendUint(b, uint64(len(r.Entries)))
	for _, e := range r.Entries {
		b, _ = codec.AppendBytes(b, e.Key)
		b, _ = codec.AppendString(b, string(e.Value))
	}
	if r.Producer != nil {
		b = appendProducer(b, *r.Producer)
	}
	return b
}
func revisionID(r Revision) model.RevisionID {
	h := sha256.New()
	_, _ = h.Write([]byte(revisionDomain))
	_, _ = h.Write(revisionPayload(r))
	return model.RevisionID(hex.EncodeToString(h.Sum(nil)))
}
func EncodeRevision(r Revision) ([]byte, error) {
	normalized, err := normalizeRevision(r)
	if err != nil {
		return nil, err
	}
	if r.ID != revisionID(normalized) {
		return nil, fmt.Errorf("%w: revision identity", ErrInvalidRevision)
	}
	return revisionPayload(normalized), nil
}
func readDigestString(d *codec.Decoder, optional bool) (string, error) {
	s, err := d.String(64)
	if err != nil {
		return "", err
	}
	if optional && s == "" {
		return s, nil
	}
	if !validDigest(s) {
		return "", ErrInvalidRevision
	}
	return s, nil
}
func checkedCount(d *codec.Decoder, max uint64, minimumBytes uint64) (uint64, error) {
	n, err := d.Uint()
	if err != nil {
		return 0, err
	}
	if n > max {
		return 0, ErrLimit
	}
	if n > uint64(d.Remaining())/minimumBytes {
		return 0, codec.ErrTruncated
	}
	return n, nil
}
func DecodeRevision(raw []byte) (Revision, error) {
	if len(raw) > MaxRevisionBytes {
		return Revision{}, ErrLimit
	}
	d := codec.NewDecoder(raw)
	version, err := d.Uint()
	if err != nil {
		return Revision{}, err
	}
	if version != RevisionSchema && version != ProducedRevisionSchema {
		return Revision{}, ErrUnsupportedSchema
	}
	var r Revision
	space, err := d.String(model.MaxIdentityBytes)
	if err != nil {
		return Revision{}, err
	}
	r.Space = model.SpaceID(space)
	parent, err := readDigestString(d, true)
	if err != nil {
		return Revision{}, err
	}
	r.StateParent = model.RevisionID(parent)
	count, err := checkedCount(d, MaxRevisionParents, 65)
	if err != nil {
		return Revision{}, err
	}
	r.JoinParents = make([]model.RevisionID, int(count))
	for i := range r.JoinParents {
		p, e := readDigestString(d, false)
		if e != nil {
			return Revision{}, e
		}
		r.JoinParents[i] = model.RevisionID(p)
		if i > 0 && r.JoinParents[i-1] >= r.JoinParents[i] {
			return Revision{}, ErrInvalidRevision
		}
	}
	root, err := readDigestString(d, true)
	if err != nil {
		return Revision{}, err
	}
	r.StateRoot = model.ObjectID(root)
	count, err = checkedCount(d, MaxRevisionEvents, 65)
	if err != nil {
		return Revision{}, err
	}
	r.Events = make([]model.EventID, int(count))
	for i := range r.Events {
		e, err := readDigestString(d, false)
		if err != nil {
			return Revision{}, err
		}
		r.Events[i] = model.EventID(e)
	}
	count, err = checkedCount(d, MaxRevisionEntries, 66)
	if err != nil {
		return Revision{}, err
	}
	r.Entries = make([]Entry, int(count))
	for i := range r.Entries {
		key, err := d.Bytes(MaxEntryKeyBytes)
		if err != nil {
			return Revision{}, err
		}
		value, err := readDigestString(d, false)
		if err != nil {
			return Revision{}, err
		}
		r.Entries[i] = Entry{Key: bytes.Clone(key), Value: model.ObjectID(value)}
		if i > 0 && bytes.Compare(r.Entries[i-1].Key, key) >= 0 {
			return Revision{}, ErrInvalidRevision
		}
	}
	if version == ProducedRevisionSchema {
		producer, err := readProducer(d)
		if err != nil {
			return Revision{}, err
		}
		r.Producer = &producer
	}
	if err = d.Done(); err != nil {
		return Revision{}, err
	}
	normalized, err := normalizeRevision(r)
	if err != nil {
		return Revision{}, err
	}
	normalized.ID = revisionID(normalized)
	if !bytes.Equal(revisionPayload(normalized), raw) {
		return Revision{}, ErrInvalidRevision
	}
	return normalized, nil
}
func EncodeBranch(b Branch) ([]byte, error) {
	if !validText(string(b.ID)) || !validDigest(string(b.Head)) || (b.PublishedHead != "" && (b.Scope == nil || !validDigest(string(b.PublishedHead)))) {
		return nil, ErrInvalidRevision
	}
	schema := BranchSchema
	if b.Scope != nil {
		if !b.Scope.valid() {
			return nil, ErrInvalidRevision
		}
		schema = ScopedBranchSchema
	}
	raw := codec.AppendUint(nil, schema)
	raw, _ = codec.AppendString(raw, string(b.ID))
	raw, _ = codec.AppendString(raw, string(b.Head))
	raw = codec.AppendUint(raw, b.Version)
	if p := b.Scope; p != nil {
		raw, _ = codec.AppendString(raw, string(p.Principal))
		raw, _ = codec.AppendString(raw, string(p.Execution))
		raw, _ = codec.AppendString(raw, string(p.Base))
		raw, _ = codec.AppendString(raw, string(b.PublishedHead))
	}
	if len(raw) > MaxBranchBytes {
		return nil, ErrLimit
	}
	return raw, nil
}
func DecodeBranch(raw []byte) (Branch, error) {
	if len(raw) > MaxBranchBytes {
		return Branch{}, ErrLimit
	}
	d := codec.NewDecoder(raw)
	schema, err := d.Uint()
	if err != nil {
		return Branch{}, err
	}
	if schema != BranchSchema && schema != ScopedBranchSchema {
		return Branch{}, ErrUnsupportedSchema
	}
	id, err := d.String(model.MaxIdentityBytes)
	if err != nil {
		return Branch{}, err
	}
	head, err := readDigestString(d, false)
	if err != nil {
		return Branch{}, err
	}
	version, err := d.Uint()
	if err != nil {
		return Branch{}, err
	}
	b := Branch{ID: model.BranchID(id), Head: model.RevisionID(head), Version: version}
	if schema == ScopedBranchSchema {
		principal, err := d.String(model.MaxIdentityBytes)
		if err != nil {
			return Branch{}, err
		}
		execution, err := d.String(model.MaxIdentityBytes)
		if err != nil {
			return Branch{}, err
		}
		base, err := readDigestString(d, false)
		if err != nil {
			return Branch{}, err
		}
		b.Scope = &BranchScope{Principal: model.PrincipalID(principal), Execution: model.ExecutionID(execution), Base: model.RevisionID(base)}
		published, err := readDigestString(d, true)
		if err != nil {
			return Branch{}, err
		}
		b.PublishedHead = model.RevisionID(published)
	}
	if err = d.Done(); err != nil {
		return Branch{}, err
	}
	canonical, err := EncodeBranch(b)
	if err != nil {
		return Branch{}, err
	}
	if !bytes.Equal(canonical, raw) {
		return Branch{}, ErrInvalidRevision
	}
	return b, nil
}
