package memory

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const MaxItems = 4096
const MaxTags = 256
const MaxRecordBytes = 4 << 20
const MaxClaimBytes = 1 << 20

// Field helpers encode presence explicitly. Time uses UTC calendar text rather
// than UnixNano, which overflows for Go's zero time and dates outside 1678..2262.
func field(b []byte, s string) []byte { b, _ = codec.AppendString(b, s); return b }
func stamp(b []byte, t time.Time) []byte {
	if t.IsZero() {
		return codec.AppendUint(b, 0)
	}
	b = codec.AppendUint(b, 1)
	return field(b, t.UTC().Format(time.RFC3339Nano))
}
func boolean(b []byte, v bool) []byte {
	if v {
		return codec.AppendUint(b, 1)
	}
	return codec.AppendUint(b, 0)
}
func ids(b []byte, in []model.ObjectID) []byte {
	v := append([]model.ObjectID(nil), in...)
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	b = codec.AppendUint(b, uint64(len(v)))
	for _, id := range v {
		b = field(b, string(id))
	}
	return b
}
func anchorBytes(b []byte, a model.SnapshotAnchor) []byte {
	b = field(b, string(a.Fence.Store))
	b = codec.AppendUint(b, a.Fence.LSN)
	b = field(b, string(a.Fence.CommitDigest))
	b = field(b, string(a.Space))
	b = boolean(b, a.Cursor != nil)
	if a.Cursor != nil {
		b = field(b, string(a.Cursor.Store))
		b = field(b, string(a.Cursor.Space))
		b = field(b, string(a.Cursor.Revision))
	}
	b = boolean(b, a.Knowledge != nil)
	if a.Knowledge != nil {
		b = field(b, string(a.Knowledge.Actor))
		b = field(b, string(a.Knowledge.Execution))
	}
	return b
}
func sourceBytes(b []byte, s Source) []byte {
	return anchorBytes(field(b, string(s.SourceID)), s.Anchor)
}
func sourceList(b []byte, sources []Source) []byte {
	b = codec.AppendUint(b, uint64(len(sources)))
	for _, s := range sources {
		b = sourceBytes(b, s)
	}
	return b
}
func applicabilityBytes(b []byte, a Applicability) []byte {
	b = field(b, a.Goal)
	b = field(b, a.Environment)
	b = codec.AppendUint(b, uint64(a.SchemaVersion))
	tags := append([]string(nil), a.Tags...)
	sort.Strings(tags)
	b = codec.AppendUint(b, uint64(len(tags)))
	for _, tag := range tags {
		b = field(b, tag)
	}
	return b
}
func validationBytes(b []byte, v Validation) []byte {
	b = codec.AppendUint(b, uint64(v.StrategyVersion))
	b = boolean(b, v.Passed)
	b = sourceList(b, v.Evidence)
	b = sourceList(b, v.Counterexamples)
	return stamp(b, v.RecordedAt)
}
func retractionBytes(b []byte, r *Retraction) []byte {
	b = boolean(b, r != nil)
	if r != nil {
		b = field(b, string(r.ID))
		b = field(b, string(r.Kind))
		b = field(b, string(r.SourceID))
		b = field(b, r.Reason)
		b = stamp(b, r.RecordedAt)
	}
	return b
}
func nodeBytes(b []byte, n model.NodeRef) []byte {
	b = field(b, string(n.Kind))
	switch n.Kind {
	case model.NodeEntity:
		b = field(b, string(n.Entity.Profile))
		b = field(b, string(n.Entity.Kind))
		b = field(b, string(n.Entity.ID))
	case model.NodeEvent:
		b = field(b, string(n.Event))
	case model.NodeRevision:
		b = field(b, string(n.Revision))
	case model.NodeObject:
		b = field(b, string(n.Object))
	}
	return b
}
func canonicalParts(m Memory) []byte {
	b := []byte("ATE-MEMORY\x00")
	b = codec.AppendUint(b, 2)
	b = field(b, string(m.Space))
	b = field(b, string(m.Kind))
	b, _ = codec.AppendBytes(b, m.Claim)
	b = sourceList(b, m.Sources)
	b = codec.AppendUint(b, uint64(len(m.Evidence)))
	for _, e := range m.Evidence {
		b = sourceBytes(b, e.Source)
		b = boolean(b, e.Counter)
		b = field(b, e.Note)
	}
	b = applicabilityBytes(b, m.Applicability)
	b = field(b, string(m.Status))
	b = codec.AppendUint(b, uint64(m.Confidence))
	b = codec.AppendUint(b, uint64(m.StrategyVersion))
	b = stamp(b, m.RecordedAt)
	b = stamp(b, m.VisibleAt)
	b = stamp(b, m.ValidFrom)
	b = boolean(b, m.ValidUntil != nil)
	if m.ValidUntil != nil {
		b = stamp(b, *m.ValidUntil)
	}
	b = ids(b, m.Supersedes)
	b = ids(b, m.Retracts)
	b = retractionBytes(b, m.Retraction)
	b = boolean(b, m.Validation != nil)
	if m.Validation != nil {
		b = validationBytes(b, *m.Validation)
	}
	return b
}
func pathBytes(p ExperiencePath) []byte {
	b := []byte("ATE-EXPERIENCE-PATH\x00")
	b = codec.AppendUint(b, 2)
	b = field(b, string(p.Space))
	b = anchorBytes(b, p.SourceAnchor)
	b = codec.AppendUint(b, uint64(len(p.Steps)))
	for _, s := range p.Steps {
		b = nodeBytes(b, s.Node)
		b = field(b, string(s.Memory))
		b = codec.AppendUint(b, uint64(s.Order))
		b = field(b, s.Outcome)
	}
	b = codec.AppendUint(b, uint64(len(p.Omitted)))
	for _, n := range p.Omitted {
		b = nodeBytes(b, n)
	}
	b = validationBytes(b, p.Validation)
	b = applicabilityBytes(b, p.Applicability)
	b = field(b, string(p.Status))
	b = ids(b, p.DerivedFrom)
	return retractionBytes(b, p.Retraction)
}
func IDFor(m Memory) model.ObjectID {
	copy := m
	copy.ID = ""
	if validateFields(copy) != nil {
		return ""
	}
	b := canonicalParts(copy)
	if len(b) > MaxRecordBytes {
		return ""
	}
	return hashBytes(b)
}
func PathIDFor(p ExperiencePath) model.ObjectID {
	if validatePathFields(p) != nil {
		return ""
	}
	b := pathBytes(p)
	if len(b) > MaxRecordBytes {
		return ""
	}
	return hashBytes(b)
}
func hashBytes(b []byte) model.ObjectID { return model.ObjectID(fmt.Sprintf("%x", sha256.Sum256(b))) }
func validText(s string, empty bool) bool {
	return (empty || s != "") && len(s) <= model.MaxIdentityBytes && utf8.ValidString(s)
}
func validTime(t time.Time) bool {
	return t.IsZero() || (t.UTC().Year() >= 1 && t.UTC().Year() <= 9999)
}
func validIDs(in []model.ObjectID) bool {
	if len(in) > MaxItems {
		return false
	}
	seen := map[model.ObjectID]bool{}
	for _, id := range in {
		if !model.ValidDigest(string(id)) || seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}
func validAnchor(a model.SnapshotAnchor) bool {
	if a.Validate() != nil || !validText(string(a.Space), false) || !validText(string(a.Fence.Store), false) {
		return false
	}
	if a.Knowledge != nil && (!validText(string(a.Knowledge.Actor), false) || !validText(string(a.Knowledge.Execution), false)) {
		return false
	}
	return true
}
func validApplicability(a Applicability) bool {
	if !validText(a.Goal, true) || !validText(a.Environment, true) || len(a.Tags) > MaxTags {
		return false
	}
	seen := map[string]bool{}
	for _, tag := range a.Tags {
		if !validText(tag, false) || seen[tag] {
			return false
		}
		seen[tag] = true
	}
	return true
}
func validRetraction(r *Retraction) bool {
	return r == nil || (validRetractionKind(r.Kind) && validText(r.Reason, false) && validTime(r.RecordedAt) && (r.ID == "" || model.ValidDigest(string(r.ID))) && (r.SourceID == "" || model.ValidDigest(string(r.SourceID))))
}
func validValidation(v Validation, space model.SpaceID) bool {
	if len(v.Evidence) > MaxItems || len(v.Counterexamples) > MaxItems || !validTime(v.RecordedAt) {
		return false
	}
	for _, list := range [][]Source{v.Evidence, v.Counterexamples} {
		for _, s := range list {
			if validateSource(s) != nil || s.Anchor.Space != space {
				return false
			}
		}
	}
	return true
}
func validateFields(m Memory) error {
	if !validText(string(m.Space), false) || !validKind(m.Kind) || !validStatus(m.Status) || len(m.Claim) == 0 || len(m.Claim) > MaxClaimBytes || m.Confidence > 1000 || len(m.Sources) > MaxItems || len(m.Evidence) > MaxItems || !validApplicability(m.Applicability) || !validIDs(m.Supersedes) || !validIDs(m.Retracts) || !validRetraction(m.Retraction) {
		return ErrInvalidMemory
	}
	if (m.Status == StatusPromoted || m.Status == StatusValidated) && len(m.Sources) == 0 {
		return ErrInvalidMemory
	}
	if !validTime(m.RecordedAt) || !validTime(m.VisibleAt) || !validTime(m.ValidFrom) || (m.ValidUntil != nil && (!validTime(*m.ValidUntil) || m.ValidUntil.Before(m.ValidFrom))) {
		return ErrInvalidMemory
	}
	for _, s := range m.Sources {
		if validateSource(s) != nil || s.Anchor.Space != m.Space {
			return ErrInvalidMemory
		}
	}
	for _, e := range m.Evidence {
		if validateSource(e.Source) != nil || e.Source.Anchor.Space != m.Space || !validText(e.Note, true) {
			return ErrInvalidMemory
		}
	}
	if (m.Status == StatusPromoted || m.Status == StatusValidated) && (m.Validation == nil || !m.Validation.Passed || len(m.Validation.Evidence) == 0) {
		return ErrInvalidMemory
	}
	if m.Validation != nil && !validValidation(*m.Validation, m.Space) {
		return ErrInvalidMemory
	}
	return nil
}
func validatePathFields(p ExperiencePath) error {
	if !validText(string(p.Space), false) || p.SourceAnchor.Space != p.Space || !validAnchor(p.SourceAnchor) || len(p.Steps) == 0 || len(p.Steps) > MaxItems || len(p.Omitted) > MaxItems || !validIDs(p.DerivedFrom) || !validStatus(p.Status) || !validApplicability(p.Applicability) || !validRetraction(p.Retraction) || !validValidation(p.Validation, p.Space) {
		return ErrPathInvalid
	}
	if (p.Status == StatusPromoted || p.Status == StatusValidated) && (!p.Validation.Passed || len(p.Validation.Evidence) == 0) {
		return ErrPathInvalid
	}
	for i, s := range p.Steps {
		if !validNode(s.Node) || !model.ValidDigest(string(s.Memory)) || s.Order != uint32(i) || !validText(s.Outcome, true) {
			return ErrPathInvalid
		}
	}
	for _, n := range p.Omitted {
		if !validNode(n) {
			return ErrPathInvalid
		}
	}
	return nil
}
func Canonical(m Memory) ([]byte, error) {
	if err := Validate(m); err != nil {
		return nil, err
	}
	b := canonicalParts(m)
	if len(b) > MaxRecordBytes {
		return nil, ErrInvalidMemory
	}
	return b, nil
}
func CanonicalPath(p ExperiencePath) ([]byte, error) {
	if err := validatePathFields(p); err != nil {
		return nil, err
	}
	b := pathBytes(p)
	if p.ID != "" && p.ID != hashBytes(b) {
		return nil, ErrPathInvalid
	}
	if len(b) > MaxRecordBytes {
		return nil, ErrPathInvalid
	}
	return b, nil
}

// VerifyRecord checks both the source-bound semantic identity and exact bytes.
func VerifyRecord(m Memory, raw []byte) error {
	b, err := Canonical(m)
	if err != nil || !bytes.Equal(b, raw) {
		return ErrInvalidMemory
	}
	return nil
}

func validNode(n model.NodeRef) bool {
	if n.Validate() != nil {
		return false
	}
	if n.Kind == model.NodeEntity {
		return validText(string(n.Entity.Profile), false) && validText(string(n.Entity.Kind), false) && validText(string(n.Entity.ID), false)
	}
	return true
}
