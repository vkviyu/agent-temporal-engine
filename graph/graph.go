// Package graph implements bounded, deterministic Revision/Branch algorithms.
// It owns metadata and immutable snapshot comparisons, not storage, publication,
// authorization, or verification that an external radix root contains a snapshot.
// A durable adapter must call VerifyState against its authenticated state reader.
package graph

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/model"
)

const (
	MaxRevisionEntries = 65536
	MaxRevisionEvents  = 65536
	MaxRevisionParents = 1024
	MaxEntryKeyBytes   = 1024
	MaxRevisionBytes   = 16 << 20
	MaxRevisions       = 1 << 20
	MaxBranches        = 65536
	MaxWalkNodes       = 65536
)

var (
	ErrUnknownRevision    = errors.New("unknown revision")
	ErrUnknownBranch      = errors.New("unknown branch")
	ErrRevisionCycle      = errors.New("revision parent cycle")
	ErrInvalidRevision    = errors.New("invalid revision")
	ErrAmbiguousMergeBase = errors.New("ambiguous merge base")
	ErrNoMergeBase        = errors.New("no common merge base")
	ErrJoinConflict       = errors.New("join has unresolved conflicts")
	ErrInvalidJoinPlan    = errors.New("join plan differs from its inputs and explicit resolutions")
	ErrBranchCAS          = errors.New("branch compare-and-swap failed")
	ErrSpaceMismatch      = errors.New("graph operation crosses spaces")
	ErrBranchScope        = errors.New("graph operation violates branch scope")
	ErrPublishConflict    = errors.New("branch publication conflicts with current domain state")
	ErrLimit              = errors.New("graph resource limit exceeded")
	ErrUnsupportedSchema  = errors.New("unsupported graph schema")
)

type Entry struct {
	Key   []byte
	Value model.ObjectID
}

// Revision is immutable once accepted by Store. StateRoot is a claimed external
// root: content identity binds it to Entries, but only VerifyState can establish
// that the authenticated state tree actually contains those entries. Events are
// ordered occurrences; JoinParents are a canonical set, excluding StateParent.
type Revision struct {
	ID          model.RevisionID
	Space       model.SpaceID
	StateParent model.RevisionID
	JoinParents []model.RevisionID
	StateRoot   model.ObjectID
	Events      []model.EventID
	Entries     []Entry
	Producer    *RevisionProducer `json:",omitempty"`
}
type Branch struct {
	ID      model.BranchID
	Head    model.RevisionID
	Version uint64
	Scope   *BranchScope `json:",omitempty"`
	// PublishedHead is the last branch revision whose changes were explicitly
	// incorporated into the Space domain. Empty selects the immutable fork base.
	PublishedHead model.RevisionID `json:",omitempty"`
}

// BranchScope binds an exploration branch to its fork base and write owner.
// An empty Execution means this is a host-managed branch.
type BranchScope struct {
	Principal model.PrincipalID
	Execution model.ExecutionID
	Base      model.RevisionID
}

// RevisionProducer records the authority and branch position verified by the
// engine when it produced a branch-local revision.
type RevisionProducer struct {
	Branch           model.BranchID
	BranchVersion    uint64
	Principal        model.PrincipalID
	Execution        model.ExecutionID
	Task             model.EntityID
	Actor            model.ActorID
	GoalEpoch        uint64
	Incarnation      uint64
	AuthorityVersion uint64
}
type ChangeKind string

const (
	ChangeAdd    ChangeKind = "add"
	ChangeRemove ChangeKind = "remove"
	ChangeUpdate ChangeKind = "update"
)

type Change struct {
	Key           []byte
	Kind          ChangeKind
	Before, After model.ObjectID
}
type Diff struct {
	From, To model.RevisionID
	Changes  []Change
}
type Conflict struct {
	Key               []byte
	Base, Left, Right model.ObjectID
}

// Resolution is an explicit decision for one conflicting key. Empty Value means
// delete; nonempty values are immutable content identities, never null/absence.
type Resolution struct {
	Key   []byte
	Value model.ObjectID
}

// JoinPlan holds the unambiguous partial State and original conflicts. Resolve
// moves each resolved conflict into Resolutions and updates State/Changes.
// CommitJoin recomputes the original plan and checks every field, so editing
// public slices cannot turn a proposal into verified merge authority.
type JoinPlan struct {
	Base, Left, Right model.RevisionID
	Changes           []Change
	Conflicts         []Conflict
	State             []Entry
	Resolutions       []Resolution
}

type Store struct {
	mu        sync.RWMutex
	revisions map[model.RevisionID]Revision
	branches  map[model.BranchID]Branch
}

func New() *Store {
	return &Store{revisions: make(map[model.RevisionID]Revision), branches: make(map[model.BranchID]Branch)}
}
func validText(s string) bool {
	return s != "" && len(s) <= model.MaxIdentityBytes && utf8.ValidString(s)
}
func validDigest(s string) bool { return model.ValidDigest(s) && s != strings.Repeat("0", 64) }
func cloneEntries(in []Entry) []Entry {
	out := make([]Entry, len(in))
	for i, e := range in {
		out[i] = Entry{Key: bytes.Clone(e.Key), Value: e.Value}
	}
	return out
}
func cloneRevision(r Revision) Revision {
	r.Entries = cloneEntries(r.Entries)
	r.JoinParents = append([]model.RevisionID(nil), r.JoinParents...)
	r.Events = append([]model.EventID(nil), r.Events...)
	if r.Producer != nil {
		producer := *r.Producer
		r.Producer = &producer
	}
	return r
}
func normalizeEntries(in []Entry) ([]Entry, error) {
	if len(in) > MaxRevisionEntries {
		return nil, ErrLimit
	}
	// Bounds precede copying keys from caller-owned input.
	size := uintSize(uint64(len(in)))
	for _, e := range in {
		if len(e.Key) > MaxEntryKeyBytes {
			return nil, ErrLimit
		}
		if !validDigest(string(e.Value)) {
			return nil, ErrInvalidRevision
		}
		size += uintSize(uint64(len(e.Key))) + len(e.Key) + stringSize(string(e.Value))
		if size > MaxRevisionBytes {
			return nil, ErrLimit
		}
	}
	out := cloneEntries(in)
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Key, out[j].Key) < 0 })
	for i := 1; i < len(out); i++ {
		if bytes.Equal(out[i-1].Key, out[i].Key) {
			return nil, fmt.Errorf("%w: duplicate entry key", ErrInvalidRevision)
		}
	}
	return out, nil
}

// BuildRevision validates and owns all inputs; the returned ID includes a type
// domain, schema, Space, ordered events and canonical metadata/state entries.
// It does not claim that referenced parents, events or StateRoot are published.
func BuildRevision(space string, parent model.RevisionID, joins []model.RevisionID, root model.ObjectID, events []model.EventID, entries []Entry) (Revision, error) {
	return buildRevision(Revision{Space: model.SpaceID(space), StateParent: parent, JoinParents: joins, StateRoot: root, Events: events, Entries: entries})
}

// BuildProducedRevision canonicalizes a local revision. Only the engine's
// authenticated graph reducer can publish the resulting producer assertion.
func BuildProducedRevision(space model.SpaceID, parent model.RevisionID, joins []model.RevisionID, root model.ObjectID, events []model.EventID, entries []Entry, producer RevisionProducer) (Revision, error) {
	return buildRevision(Revision{Space: space, StateParent: parent, JoinParents: joins, StateRoot: root, Events: events, Entries: entries, Producer: &producer})
}

func buildRevision(input Revision) (Revision, error) {
	r, err := normalizeRevision(input)
	if err != nil {
		return Revision{}, err
	}
	r.ID = revisionID(r)
	return r, nil
}

func normalizeRevision(r Revision) (Revision, error) {
	if !validText(string(r.Space)) || (r.StateRoot != "" && !validDigest(string(r.StateRoot))) || (r.StateRoot == "" && len(r.Entries) != 0) || (r.StateParent != "" && !validDigest(string(r.StateParent))) {
		return Revision{}, ErrInvalidRevision
	}
	if len(r.JoinParents) > MaxRevisionParents || len(r.Events) > MaxRevisionEvents {
		return Revision{}, ErrLimit
	}
	if r.Producer != nil {
		producer := *r.Producer
		if r.StateParent == "" || !producer.valid() {
			return Revision{}, ErrInvalidRevision
		}
		r.Producer = &producer
	}
	r.JoinParents = append([]model.RevisionID(nil), r.JoinParents...)
	sort.Slice(r.JoinParents, func(i, j int) bool { return r.JoinParents[i] < r.JoinParents[j] })
	for i, p := range r.JoinParents {
		if p == r.StateParent || (i > 0 && p == r.JoinParents[i-1]) {
			return Revision{}, ErrRevisionCycle
		}
		if !validDigest(string(p)) {
			return Revision{}, ErrInvalidRevision
		}
	}
	seen := make(map[model.EventID]struct{}, len(r.Events))
	for _, e := range r.Events {
		if !validDigest(string(e)) {
			return Revision{}, ErrInvalidRevision
		}
		if _, ok := seen[e]; ok {
			return Revision{}, fmt.Errorf("%w: duplicate event", ErrInvalidRevision)
		}
		seen[e] = struct{}{}
	}
	if revisionEncodedSize(r) > MaxRevisionBytes {
		return Revision{}, ErrLimit
	}
	r.Events = append([]model.EventID(nil), r.Events...)
	var err error
	r.Entries, err = normalizeEntries(r.Entries)
	if err != nil {
		return Revision{}, err
	}
	if revisionEncodedSize(r) > MaxRevisionBytes {
		return Revision{}, ErrLimit
	}
	return r, nil
}

// RevisionIDFor returns the same identity as BuildRevision; invalid inputs have
// no identity and return the empty ID. Use BuildRevision to obtain the error.
func RevisionIDFor(space string, parent model.RevisionID, joins []model.RevisionID, root model.ObjectID, events []model.EventID, entries []Entry) model.RevisionID {
	r, err := BuildRevision(space, parent, joins, root, events, entries)
	if err != nil {
		return ""
	}
	return r.ID
}
func (s *Store) putLocked(r Revision) error {
	canonical, err := buildRevision(r)
	if err != nil {
		return err
	}
	if r.ID != canonical.ID {
		return fmt.Errorf("%w: revision identity", ErrInvalidRevision)
	}
	if s.revisions == nil {
		s.revisions = make(map[model.RevisionID]Revision)
	}
	checkParent := func(id model.RevisionID) error {
		if id == r.ID {
			return ErrRevisionCycle
		}
		p, ok := s.revisions[id]
		if !ok {
			return ErrUnknownRevision
		}
		if p.Space != r.Space {
			return ErrSpaceMismatch
		}
		return nil
	}
	if r.StateParent != "" {
		if err := checkParent(r.StateParent); err != nil {
			return err
		}
	}
	for _, p := range r.JoinParents {
		if err := checkParent(p); err != nil {
			return err
		}
	}
	if _, ok := s.revisions[r.ID]; ok {
		return nil
	}
	if len(s.revisions) >= MaxRevisions {
		return ErrLimit
	}
	s.revisions[r.ID] = canonical
	return nil
}
func (s *Store) PutRevision(r Revision) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putLocked(r)
}
func (s *Store) AddRevision(space string, parent model.RevisionID, joins []model.RevisionID, root model.ObjectID, events []model.EventID, entries []Entry) (Revision, error) {
	r, err := BuildRevision(space, parent, joins, root, events, entries)
	if err != nil {
		return Revision{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.putLocked(r); err != nil {
		return Revision{}, err
	}
	return cloneRevision(r), nil
}
func (s *Store) getLocked(id model.RevisionID) (Revision, error) {
	r, ok := s.revisions[id]
	if !ok {
		return Revision{}, ErrUnknownRevision
	}
	return r, nil
}
func (s *Store) Get(id model.RevisionID) (Revision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, err := s.getLocked(id)
	if err != nil {
		return Revision{}, err
	}
	return cloneRevision(r), nil
}
func (s *Store) Fork(id model.BranchID, source model.RevisionID) (Branch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.previewForkLocked(id, source)
	if err != nil {
		return Branch{}, err
	}
	if s.branches == nil {
		s.branches = make(map[model.BranchID]Branch)
	}
	s.branches[id] = b
	return b, nil
}

// PreviewFork checks all Fork preconditions, including capacity, without mutation.
func (s *Store) PreviewFork(id model.BranchID, source model.RevisionID) (Branch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.previewForkLocked(id, source)
}

func (s *Store) previewForkLocked(id model.BranchID, source model.RevisionID) (Branch, error) {
	if _, err := s.getLocked(source); err != nil {
		return Branch{}, err
	}
	if !validText(string(id)) {
		return Branch{}, ErrInvalidRevision
	}
	if _, ok := s.branches[id]; ok {
		return Branch{}, ErrBranchCAS
	}
	if len(s.branches) >= MaxBranches {
		return Branch{}, ErrLimit
	}
	return Branch{ID: id, Head: source}, nil
}
func (s *Store) Branch(id model.BranchID) (Branch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.branches[id]
	if !ok {
		return Branch{}, ErrUnknownBranch
	}
	return b, nil
}
func (s *Store) AdvanceBranch(id model.BranchID, expected uint64, head model.RevisionID) (Branch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.previewAdvanceBranchLocked(id, expected, head)
	if err != nil {
		return Branch{}, err
	}
	s.branches[id] = b
	return b, nil
}

// PreviewAdvanceBranch validates CAS, Space and version capacity without mutation.
func (s *Store) PreviewAdvanceBranch(id model.BranchID, expected uint64, head model.RevisionID) (Branch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.previewAdvanceBranchLocked(id, expected, head)
}

func (s *Store) previewAdvanceBranchLocked(id model.BranchID, expected uint64, head model.RevisionID) (Branch, error) {
	next, err := s.getLocked(head)
	if err != nil {
		return Branch{}, err
	}
	b, ok := s.branches[id]
	if !ok {
		return Branch{}, ErrUnknownBranch
	}
	if b.Version != expected {
		return Branch{}, ErrBranchCAS
	}
	old, err := s.getLocked(b.Head)
	if err != nil {
		return Branch{}, err
	}
	if old.Space != next.Space {
		return Branch{}, ErrSpaceMismatch
	}
	if b.Version == math.MaxUint64 {
		return Branch{}, ErrLimit
	}
	b.Head = head
	b.Version++
	return b, nil
}
