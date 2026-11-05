package memory

import (
	"bytes"
	"math"
	"time"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type recordReader struct {
	d   *codec.Decoder
	err error
}

func (r *recordReader) u() uint64 {
	if r.err != nil {
		return 0
	}
	v, e := r.d.Uint()
	r.err = e
	return v
}
func (r *recordReader) u32() uint32 {
	v := r.u()
	if v > math.MaxUint32 {
		r.err = ErrInvalidMemory
		return 0
	}
	return uint32(v)
}
func (r *recordReader) text(limit uint64) string {
	if r.err != nil {
		return ""
	}
	v, e := r.d.String(limit)
	r.err = e
	return v
}
func (r *recordReader) blob(limit uint64) []byte {
	if r.err != nil {
		return nil
	}
	v, e := r.d.Bytes(limit)
	r.err = e
	return bytes.Clone(v)
}
func (r *recordReader) flag() bool {
	v := r.u()
	if v > 1 {
		r.err = ErrInvalidMemory
	}
	return v == 1
}
func (r *recordReader) count(maximum, minBytes int) int {
	n := r.u()
	if r.err != nil {
		return 0
	}
	if n > uint64(maximum) || (minBytes > 0 && n > uint64(r.d.Remaining()/minBytes)) {
		r.err = ErrInvalidMemory
		return 0
	}
	return int(n)
}
func (r *recordReader) stamp() time.Time {
	if !r.flag() {
		return time.Time{}
	}
	raw := r.text(64)
	if r.err != nil {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || t.IsZero() || raw != t.UTC().Format(time.RFC3339Nano) {
		r.err = ErrInvalidMemory
		return time.Time{}
	}
	return t.UTC()
}
func (r *recordReader) anchor() model.SnapshotAnchor {
	a := model.SnapshotAnchor{Fence: model.CommitFence{Store: model.StoreID(r.text(model.MaxIdentityBytes)), LSN: r.u(), CommitDigest: model.CommitDigest(r.text(64))}, Space: model.SpaceID(r.text(model.MaxIdentityBytes))}
	if r.flag() {
		a.Cursor = &model.Cursor{Store: model.StoreID(r.text(model.MaxIdentityBytes)), Space: model.SpaceID(r.text(model.MaxIdentityBytes)), Revision: model.RevisionID(r.text(64))}
	}
	if r.flag() {
		a.Knowledge = &model.KnowledgeKey{Actor: model.ActorID(r.text(model.MaxIdentityBytes)), Execution: model.ExecutionID(r.text(model.MaxIdentityBytes))}
	}
	return a
}
func (r *recordReader) source() Source {
	return Source{SourceID: model.ObjectID(r.text(64)), Anchor: r.anchor()}
}
func (r *recordReader) sources() []Source {
	n := r.count(MaxItems, 1)
	out := make([]Source, n)
	for i := range out {
		out[i] = r.source()
	}
	return out
}
func (r *recordReader) applicability() Applicability {
	a := Applicability{Goal: r.text(model.MaxIdentityBytes), Environment: r.text(model.MaxIdentityBytes), SchemaVersion: r.u32()}
	n := r.count(MaxTags, 1)
	a.Tags = make([]string, n)
	for i := range a.Tags {
		a.Tags[i] = r.text(model.MaxIdentityBytes)
	}
	return a
}
func (r *recordReader) validation() Validation {
	return Validation{StrategyVersion: r.u32(), Passed: r.flag(), Evidence: r.sources(), Counterexamples: r.sources(), RecordedAt: r.stamp()}
}
func (r *recordReader) ids() []model.ObjectID {
	n := r.count(MaxItems, 65)
	out := make([]model.ObjectID, n)
	for i := range out {
		out[i] = model.ObjectID(r.text(64))
	}
	return out
}
func (r *recordReader) retraction() *Retraction {
	if !r.flag() {
		return nil
	}
	return &Retraction{ID: model.ObjectID(r.text(64)), Kind: RetractionKind(r.text(model.MaxIdentityBytes)), SourceID: model.ObjectID(r.text(64)), Reason: r.text(model.MaxIdentityBytes), RecordedAt: r.stamp()}
}
func (r *recordReader) node() model.NodeRef {
	n := model.NodeRef{Kind: model.NodeKind(r.text(32))}
	switch n.Kind {
	case model.NodeEntity:
		n.Entity = model.EntityKey{Profile: model.ProfileID(r.text(model.MaxIdentityBytes)), Kind: model.EntityKind(r.text(model.MaxIdentityBytes)), ID: model.EntityID(r.text(model.MaxIdentityBytes))}
	case model.NodeEvent:
		n.Event = model.EventID(r.text(64))
	case model.NodeRevision:
		n.Revision = model.RevisionID(r.text(64))
	case model.NodeObject:
		n.Object = model.ObjectID(r.text(64))
	default:
		r.err = ErrPathInvalid
	}
	return n
}
func newRecordReader(raw []byte, prefix string) (*recordReader, error) {
	if len(raw) > MaxRecordBytes || len(raw) < len(prefix) || string(raw[:len(prefix)]) != prefix {
		return nil, ErrInvalidMemory
	}
	r := &recordReader{d: codec.NewDecoder(raw[len(prefix):])}
	if r.u() != 2 || r.err != nil {
		return nil, ErrInvalidMemory
	}
	return r, nil
}

// DecodeMemory rechecks all fields, the complete semantic identity and exact
// canonical encoding. The returned record owns its nested anchors and slices.
func DecodeMemory(raw []byte) (Memory, error) {
	r, err := newRecordReader(raw, "ATE-MEMORY\x00")
	if err != nil {
		return Memory{}, err
	}
	m := Memory{Space: model.SpaceID(r.text(model.MaxIdentityBytes)), Kind: Kind(r.text(64)), Claim: r.blob(MaxClaimBytes), Sources: r.sources()}
	n := r.count(MaxItems, 1)
	m.Evidence = make([]Evidence, n)
	for i := range m.Evidence {
		m.Evidence[i] = Evidence{Source: r.source(), Counter: r.flag(), Note: r.text(model.MaxIdentityBytes)}
	}
	m.Applicability = r.applicability()
	m.Status = Status(r.text(64))
	m.Confidence = r.u32()
	m.StrategyVersion = r.u32()
	m.RecordedAt = r.stamp()
	m.VisibleAt = r.stamp()
	m.ValidFrom = r.stamp()
	if r.flag() {
		t := r.stamp()
		m.ValidUntil = &t
	}
	m.Supersedes = r.ids()
	m.Retracts = r.ids()
	m.Retraction = r.retraction()
	if r.flag() {
		v := r.validation()
		m.Validation = &v
	}
	if r.err != nil {
		return Memory{}, r.err
	}
	if err = r.d.Done(); err != nil {
		return Memory{}, err
	}
	if err = Validate(m); err != nil {
		return Memory{}, err
	}
	m.ID = hashBytes(raw)
	canonical, err := Canonical(m)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Memory{}, ErrInvalidMemory
	}
	return m, nil
}
func DecodePath(raw []byte) (ExperiencePath, error) {
	r, err := newRecordReader(raw, "ATE-EXPERIENCE-PATH\x00")
	if err != nil {
		return ExperiencePath{}, err
	}
	p := ExperiencePath{Space: model.SpaceID(r.text(model.MaxIdentityBytes)), SourceAnchor: r.anchor()}
	n := r.count(MaxItems, 1)
	p.Steps = make([]Step, n)
	for i := range p.Steps {
		p.Steps[i] = Step{Node: r.node(), Memory: model.ObjectID(r.text(64)), Order: r.u32(), Outcome: r.text(model.MaxIdentityBytes)}
	}
	n = r.count(MaxItems, 1)
	p.Omitted = make([]model.NodeRef, n)
	for i := range p.Omitted {
		p.Omitted[i] = r.node()
	}
	p.Validation = r.validation()
	p.Applicability = r.applicability()
	p.Status = Status(r.text(64))
	p.DerivedFrom = r.ids()
	p.Retraction = r.retraction()
	if r.err != nil {
		return ExperiencePath{}, r.err
	}
	if err = r.d.Done(); err != nil {
		return ExperiencePath{}, err
	}
	if err = validatePathFields(p); err != nil {
		return ExperiencePath{}, err
	}
	p.ID = hashBytes(raw)
	canonical, err := CanonicalPath(p)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ExperiencePath{}, ErrPathInvalid
	}
	return p, nil
}
