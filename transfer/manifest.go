// Package transfer defines portable, read-only evidence archives. Foreign
// anchors and records never become local authority or executable continuations.
package transfer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const Schema uint64 = 1
const MaxEntries = 128
const MaxManifestBytes = 512 << 10
const MaxEntryBytes = 256 << 10

var ErrInvalid = errors.New("invalid portable evidence archive")

type Reference struct {
	Kind   string               `json:"kind"`
	ID     model.ObjectID       `json:"id"`
	Anchor model.SnapshotAnchor `json:"anchor"`
}

func (r Reference) Key() string { return r.Kind + "/" + string(r.ID) }

type RedactionPolicy struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}
type Entry struct {
	Source       Reference   `json:"source"`
	Encoding     string      `json:"encoding"`
	Data         []byte      `json:"data"`
	Dependencies []Reference `json:"dependencies,omitempty"`
}
type Gap struct {
	Source Reference `json:"source"`
	Reason string    `json:"reason"`
}
type Manifest struct {
	Schema      uint64               `json:"schema"`
	ID          model.ObjectID       `json:"id"`
	Source      model.SnapshotAnchor `json:"source"`
	Principal   model.PrincipalID    `json:"principal"`
	Policy      RedactionPolicy      `json:"policy"`
	PolicyFence model.CommitFence    `json:"policy_fence"`
	PolicyRoot  model.ObjectID       `json:"policy_root"`
	Roots       []Reference          `json:"roots"`
	Entries     []Entry              `json:"entries"`
	Gaps        []Gap                `json:"gaps,omitempty"`
}
type Mapping struct {
	Source Reference      `json:"source"`
	Target model.ObjectID `json:"target"`
}

// ArchiveEntry stores original canonical bytes in a new security domain. Data
// is foreign evidence, not a destination-domain command or a restored snapshot.
type ArchiveEntry struct {
	Schema         uint64         `json:"schema"`
	Manifest       model.ObjectID `json:"manifest"`
	Source         Entry          `json:"source"`
	Classification string         `json:"classification"`
}
type ImportRecord struct {
	Schema    uint64            `json:"schema"`
	Space     model.SpaceID     `json:"space"`
	Principal model.PrincipalID `json:"principal"`
	Manifest  Manifest          `json:"manifest"`
	Mappings  []Mapping         `json:"mappings"`
	Trust     string            `json:"trust"`
}

func validText(s string) bool { return s != "" && len(s) <= 1024 && utf8.ValidString(s) }
func validRef(r Reference, source model.SnapshotAnchor) bool {
	return validText(r.Kind) && model.ValidDigest(string(r.ID)) && r.Anchor.Validate() == nil && r.Anchor.Space == source.Space && r.Anchor.Fence.Store == source.Fence.Store && r.Anchor.Fence.LSN <= source.Fence.LSN
}
func (m Manifest) Digest() model.ObjectID {
	m.ID = ""
	raw, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(append([]byte("ATE-TRANSFER-MANIFEST\x00\x01"), raw...))
	return model.ObjectID(hex.EncodeToString(sum[:]))
}
func (m Manifest) Canonical() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(m)
}
func DecodeManifest(raw []byte) (Manifest, error) {
	if len(raw) == 0 || len(raw) > MaxManifestBytes {
		return Manifest{}, ErrInvalid
	}
	var m Manifest
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil {
		return Manifest{}, ErrInvalid
	}
	canonical, err := m.Canonical()
	if err != nil || !bytes.Equal(canonical, raw) {
		return Manifest{}, ErrInvalid
	}
	return m, nil
}
func (m Manifest) Validate() error {
	if m.Schema != Schema || m.Source.Validate() != nil || m.Source.Cursor != nil || m.Source.Knowledge != nil || !validText(string(m.Principal)) || !validText(m.Policy.ID) || !validText(m.Policy.Version) || m.PolicyFence.Validate() != nil || m.PolicyFence.Store != m.Source.Fence.Store || m.PolicyFence.LSN < m.Source.Fence.LSN || (m.PolicyRoot != "" && !model.ValidDigest(string(m.PolicyRoot))) || len(m.Roots) == 0 || len(m.Roots) > MaxEntries || len(m.Entries) == 0 || len(m.Entries) > MaxEntries || len(m.Gaps) > MaxEntries*16 || m.ID != m.Digest() {
		return ErrInvalid
	}
	present := map[string]bool{}
	nodes := map[string][]Reference{}
	for i, e := range m.Entries {
		if !validRef(e.Source, m.Source) || len(e.Data) == 0 || len(e.Data) > MaxEntryBytes || (i > 0 && m.Entries[i-1].Source.Key() >= e.Source.Key()) || len(e.Dependencies) > MaxEntries*16 || validateEntry(e) != nil {
			return ErrInvalid
		}
		present[e.Source.Key()] = true
		nodes[e.Source.Key()] = e.Dependencies
		for j, ref := range e.Dependencies {
			if !validRef(ref, m.Source) || (j > 0 && e.Dependencies[j-1].Key() >= ref.Key()) {
				return ErrInvalid
			}
		}
	}
	for i, g := range m.Gaps {
		if !validRef(g.Source, m.Source) || !validText(g.Reason) || present[g.Source.Key()] || (i > 0 && m.Gaps[i-1].Source.Key() >= g.Source.Key()) {
			return ErrInvalid
		}
		present[g.Source.Key()] = true
	}
	queue := append([]Reference(nil), m.Roots...)
	seen := map[string]bool{}
	for i, r := range m.Roots {
		if !validRef(r, m.Source) || (i > 0 && m.Roots[i-1].Key() >= r.Key()) || !present[r.Key()] {
			return ErrInvalid
		}
	}
	for len(queue) > 0 {
		ref := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		key := ref.Key()
		if seen[key] {
			continue
		}
		if !present[key] {
			return ErrInvalid
		}
		seen[key] = true
		queue = append(queue, nodes[key]...)
	}
	if len(seen) != len(present) {
		return ErrInvalid
	}
	raw, err := json.Marshal(m)
	if err != nil || len(raw) > MaxManifestBytes {
		return ErrInvalid
	}
	return nil
}
func validateEntry(e Entry) error {
	if e.Source.Kind != "source" && e.Source.Kind != e.Encoding {
		return ErrInvalid
	}
	switch e.Encoding {
	case "object", "event":
		o, err := codec.DecodeObject(e.Data)
		if err != nil || o.Space != e.Source.Anchor.Space {
			return ErrInvalid
		}
		id, err := codec.ObjectID(o)
		if err != nil || id != e.Source.ID {
			return ErrInvalid
		}
	case "memory":
		m, err := memory.DecodeMemory(e.Data)
		if err != nil || m.ID != e.Source.ID || m.Space != e.Source.Anchor.Space {
			return ErrInvalid
		}
	case "experience_path":
		p, err := memory.DecodePath(e.Data)
		if err != nil || p.ID != e.Source.ID || p.Space != e.Source.Anchor.Space {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	deps, err := Dependencies(e.Source, e.Encoding, e.Data)
	if err != nil || !reflect.DeepEqual(deps, e.Dependencies) {
		return ErrInvalid
	}
	return nil
}

// Dependencies extracts only registered source relations. Raw event/control
// object references can be represented as explicit archive gaps; they never
// authorize import of the source system's live control roots.
func Dependencies(ref Reference, encoding string, data []byte) ([]Reference, error) {
	var deps []Reference
	add := func(id model.ObjectID, anchor model.SnapshotAnchor) {
		if id != "" {
			deps = append(deps, Reference{Kind: "source", ID: id, Anchor: anchor.Clone()})
		}
	}
	addSource := func(s memory.Source) { add(s.SourceID, s.Anchor) }
	switch encoding {
	case "object", "event":
		o, err := codec.DecodeObject(data)
		if err != nil {
			return nil, ErrInvalid
		}
		for _, id := range o.Refs {
			add(id, ref.Anchor)
		}
	case "memory":
		m, err := memory.DecodeMemory(data)
		if err != nil {
			return nil, ErrInvalid
		}
		for _, s := range m.Sources {
			addSource(s)
		}
		for _, e := range m.Evidence {
			addSource(e.Source)
		}
		if m.Validation != nil {
			for _, s := range m.Validation.Evidence {
				addSource(s)
			}
			for _, s := range m.Validation.Counterexamples {
				addSource(s)
			}
		}
		for _, id := range append(append([]model.ObjectID(nil), m.Supersedes...), m.Retracts...) {
			add(id, ref.Anchor)
		}
		if m.Retraction != nil {
			add(m.Retraction.SourceID, ref.Anchor)
		}
	case "experience_path":
		p, err := memory.DecodePath(data)
		if err != nil {
			return nil, ErrInvalid
		}
		for _, s := range p.Validation.Evidence {
			addSource(s)
		}
		for _, s := range p.Validation.Counterexamples {
			addSource(s)
		}
		for _, id := range p.DerivedFrom {
			add(id, p.SourceAnchor)
		}
		nodes := append([]model.NodeRef(nil), p.Omitted...)
		for _, s := range p.Steps {
			add(s.Memory, p.SourceAnchor)
			nodes = append(nodes, s.Node)
		}
		for _, n := range nodes {
			switch n.Kind {
			case model.NodeObject:
				add(n.Object, p.SourceAnchor)
			case model.NodeEvent:
				add(model.ObjectID(n.Event), p.SourceAnchor)
			case model.NodeRevision:
				add(model.ObjectID(n.Revision), p.SourceAnchor)
			default:
				return nil, ErrInvalid
			}
		}
		if p.Retraction != nil {
			add(p.Retraction.SourceID, p.SourceAnchor)
		}
	default:
		return nil, ErrInvalid
	}
	if len(deps) == 0 {
		return nil, nil
	}
	return SortReferences(deps), nil
}
func SortReferences(refs []Reference) []Reference {
	sort.Slice(refs, func(i, j int) bool { return refs[i].Key() < refs[j].Key() })
	out := refs[:0]
	for _, r := range refs {
		if len(out) == 0 || out[len(out)-1].Key() != r.Key() {
			out = append(out, r)
		}
	}
	return out
}
func (r ImportRecord) Canonical() ([]byte, error) {
	if r.Schema != Schema || !validText(string(r.Space)) || !validText(string(r.Principal)) || r.Space == r.Manifest.Source.Space || r.Trust != "caller-approved-foreign-evidence" || r.Manifest.Validate() != nil || len(r.Mappings) != len(r.Manifest.Entries) {
		return nil, ErrInvalid
	}
	seen := map[model.ObjectID]bool{}
	for i, m := range r.Mappings {
		if !reflect.DeepEqual(m.Source, r.Manifest.Entries[i].Source) || !model.ValidDigest(string(m.Target)) || seen[m.Target] {
			return nil, ErrInvalid
		}
		seen[m.Target] = true
	}
	return json.Marshal(r)
}
func DecodeImportRecord(raw []byte) (ImportRecord, error) {
	var r ImportRecord
	if len(raw) > 1<<20 {
		return r, ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return r, ErrInvalid
	}
	b, err := r.Canonical()
	if err != nil || !bytes.Equal(b, raw) {
		return ImportRecord{}, ErrInvalid
	}
	return r, nil
}
func (e ArchiveEntry) Canonical() ([]byte, error) {
	if e.Schema != Schema || !model.ValidDigest(string(e.Manifest)) || e.Classification != "foreign-read-only-evidence" || validateEntry(e.Source) != nil {
		return nil, ErrInvalid
	}
	return json.Marshal(e)
}
func DecodeArchiveEntry(raw []byte) (ArchiveEntry, error) {
	var e ArchiveEntry
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&e); err != nil {
		return e, ErrInvalid
	}
	b, err := e.Canonical()
	if err != nil || !bytes.Equal(b, raw) {
		return ArchiveEntry{}, ErrInvalid
	}
	return e, nil
}
func ContentReferences(r ImportRecord) []model.ObjectID {
	ids := make([]model.ObjectID, 0, len(r.Mappings))
	for _, m := range r.Mappings {
		ids = append(ids, m.Target)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
func (r Reference) String() string { return fmt.Sprintf("%s:%s", r.Kind, r.ID) }
