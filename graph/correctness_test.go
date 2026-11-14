package graph

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func testDigest(s string) model.ObjectID {
	return model.ObjectID(fmt.Sprintf("%x", sha256.Sum256([]byte(s))))
}
func requireRevision(t testing.TB, s *Store, space string, parent model.RevisionID, joins []model.RevisionID, root string, entries []Entry) Revision {
	t.Helper()
	r, err := s.AddRevision(space, parent, joins, testDigest(root), nil, entries)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestRevisionIdentityOwnershipAndSpaceBoundaries(t *testing.T) {
	input := []Entry{{Key: []byte("k"), Value: oid('a')}}
	r, err := BuildRevision("space", "", nil, oid('b'), nil, input)
	if err != nil {
		t.Fatal(err)
	}
	input[0].Key[0] = 'x'
	input[0].Value = oid('c')
	if string(r.Entries[0].Key) != "k" || r.Entries[0].Value != oid('a') {
		t.Fatal("BuildRevision aliases input")
	}
	s := New()
	if err = s.PutRevision(r); err != nil {
		t.Fatal(err)
	}
	originalID := r.ID
	r.Entries[0].Key[0] = 'y'
	stored, err := s.Get(originalID)
	if err != nil || string(stored.Entries[0].Key) != "k" {
		t.Fatal("PutRevision aliases caller")
	}
	stored.Entries[0].Key[0] = 'z'
	again, _ := s.Get(originalID)
	if string(again.Entries[0].Key) != "k" {
		t.Fatal("Get aliases stored snapshot")
	}
	forged := cloneRevision(again)
	forged.ID = model.RevisionID(oid('f'))
	if err = s.PutRevision(forged); !errors.Is(err, ErrInvalidRevision) {
		t.Fatalf("forged ID accepted: %v", err)
	}
	forged.ID = originalID
	forged.Entries[0].Value = oid('f')
	if err = s.PutRevision(forged); !errors.Is(err, ErrInvalidRevision) {
		t.Fatalf("existing ID suppressed payload check: %v", err)
	}
	other := requireRevision(t, s, "other", "", nil, "other", nil)
	if _, err = s.AddRevision("space", other.ID, nil, oid('c'), nil, nil); !errors.Is(err, ErrSpaceMismatch) {
		t.Fatal(err)
	}
	if _, err = s.AddRevision("space", originalID, []model.RevisionID{other.ID}, oid('c'), nil, nil); !errors.Is(err, ErrSpaceMismatch) {
		t.Fatal(err)
	}
	if _, err = s.Diff(originalID, other.ID); !errors.Is(err, ErrSpaceMismatch) {
		t.Fatal(err)
	}
	if _, err = s.PlanJoin(originalID, other.ID); !errors.Is(err, ErrSpaceMismatch) {
		t.Fatal(err)
	}
	if _, err = s.Fork("main", originalID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.AdvanceBranch("main", 0, other.ID); !errors.Is(err, ErrSpaceMismatch) {
		t.Fatal(err)
	}
	branch, _ := s.Branch("main")
	if branch.Version != 0 || branch.Head != originalID {
		t.Fatal("rejected branch update changed state")
	}
	added := requireRevision(t, s, "space", originalID, nil, "owned-result", []Entry{{Key: []byte("value"), Value: oid('e')}})
	added.Entries[0].Key[0] = 'X'
	check, _ := s.Get(added.ID)
	if string(check.Entries[0].Key) != "value" {
		t.Fatal("AddRevision result aliases store")
	}
}
func TestGraphBoundsEmptyKeysAndDuplicateEvents(t *testing.T) {
	r, err := BuildRevision("space", "", nil, oid('a'), nil, []Entry{{Key: nil, Value: oid('b')}, {Key: []byte{0, 255}, Value: oid('c')}})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeRevision(r)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRevision(raw)
	if err != nil || len(decoded.Entries[0].Key) != 0 {
		t.Fatalf("empty key rejected: %v", err)
	}
	invalid := []struct {
		name  string
		build func() error
	}{
		{"duplicate empty key", func() error {
			_, err := BuildRevision("s", "", nil, oid('a'), nil, []Entry{{Key: nil, Value: oid('b')}, {Key: []byte{}, Value: oid('c')}})
			return err
		}},
		{"nonadjacent events", func() error {
			_, err := BuildRevision("s", "", nil, oid('a'), []model.EventID{model.EventID(oid('b')), model.EventID(oid('c')), model.EventID(oid('b'))}, nil)
			return err
		}},
		{"invalid space UTF8", func() error { _, err := BuildRevision(string([]byte{255}), "", nil, oid('a'), nil, nil); return err }},
		{"invalid state root", func() error { _, err := BuildRevision("s", "", nil, "not-a-root", nil, nil); return err }},
		{"nonempty entries at empty root", func() error { _, err := BuildRevision("s", "", nil, "", nil, []Entry{{Value: oid('b')}}); return err }},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.build(); err == nil {
				t.Fatal("invalid revision accepted")
			}
		})
	}
	if _, err = BuildRevision("s", "", nil, oid('a'), nil, []Entry{{Key: make([]byte, MaxEntryKeyBytes+1), Value: oid('b')}}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err = BuildRevision("s", "", make([]model.RevisionID, MaxRevisionParents+1), oid('a'), nil, nil); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if id := RevisionIDFor("s", "", nil, "invalid", nil, nil); id != "" {
		t.Fatal("invalid input acquired content identity")
	}
	raw[0] = byte(ProducedRevisionSchema + 1)
	if _, err = DecodeRevision(raw); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("unknown schema misclassified: %v", err)
	}
	oversized := make([]byte, MaxRevisionBytes+1)
	if allocations := testing.AllocsPerRun(10, func() { _, _ = DecodeRevision(oversized) }); allocations != 0 {
		t.Fatalf("oversized input allocated %v objects", allocations)
	}
	// A tiny declaration of a maximum-sized array cannot force that allocation.
	declared := codec.AppendUint(nil, RevisionSchema)
	declared, _ = codec.AppendString(declared, "s")
	declared, _ = codec.AppendString(declared, "")
	declared = codec.AppendUint(declared, MaxRevisionParents)
	if _, err = DecodeRevision(declared); !errors.Is(err, codec.ErrTruncated) {
		t.Fatalf("declared count not checked before allocation: %v", err)
	}
	if allocations := testing.AllocsPerRun(10, func() { _, _ = DecodeRevision(declared) }); allocations > 8 {
		t.Fatalf("truncated count forced array allocation: %v", allocations)
	}
	empty, err := BuildRevision("s", "", nil, "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = EncodeRevision(empty); err != nil {
		t.Fatal(err)
	}
}
func TestCodecRejectsNoncanonicalContent(t *testing.T) {
	s := New()
	a := requireRevision(t, s, "space", "", nil, "a", nil)
	b := requireRevision(t, s, "space", "", nil, "b", nil)
	r, err := BuildRevision("space", "", []model.RevisionID{a.ID, b.ID}, oid('a'), []model.EventID{model.EventID(oid('b')), model.EventID(oid('c'))}, []Entry{{Key: []byte("a"), Value: oid('b')}, {Key: []byte("b"), Value: oid('c')}})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"joins reversed", "entries reversed", "duplicate events", "bad parent"} {
		t.Run(name, func(t *testing.T) {
			bad := cloneRevision(r)
			switch name {
			case "joins reversed":
				bad.JoinParents[0], bad.JoinParents[1] = bad.JoinParents[1], bad.JoinParents[0]
			case "entries reversed":
				bad.Entries[0], bad.Entries[1] = bad.Entries[1], bad.Entries[0]
			case "duplicate events":
				bad.Events = append(bad.Events, bad.Events[0])
			case "bad parent":
				bad.StateParent = "invalid"
			}
			if _, err := DecodeRevision(revisionPayload(bad)); err == nil {
				t.Fatal("malformed payload accepted")
			}
		})
	}
	valid, _ := EncodeRevision(r)
	got, err := DecodeRevision(valid)
	if err != nil {
		t.Fatal(err)
	}
	for i := range valid {
		valid[i] = 0
	}
	if got.Space != "space" || string(got.Entries[0].Key) != "a" || got.ID != r.ID {
		t.Fatal("decoded record aliases bytes")
	}
	if _, err = EncodeBranch(Branch{ID: model.BranchID(strings.Repeat("x", model.MaxIdentityBytes+1)), Head: a.ID}); err == nil {
		t.Fatal("unbounded branch ID")
	}
	if _, err = DecodeBranch([]byte{byte(ScopedBranchSchema + 1)}); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatal(err)
	}
}

type sourceSnapshot struct {
	entries []Entry
	err     error
}

func (r sourceSnapshot) ReadEntries(model.SpaceID, model.ObjectID) ([]Entry, error) {
	return r.entries, r.err
}
func TestStateRootMustBeAuthenticatedByAdapter(t *testing.T) {
	r, err := BuildRevision("space", "", nil, oid('a'), nil, []Entry{{Key: nil, Value: oid('b')}})
	if err != nil {
		t.Fatal(err)
	}
	if err = VerifyState(r, nil); !errors.Is(err, ErrStateUnverified) {
		t.Fatal(err)
	}
	if err = VerifyState(r, sourceSnapshot{entries: []Entry{{Key: nil, Value: oid('c')}}}); !errors.Is(err, ErrStateMismatch) {
		t.Fatalf("unmatched root snapshot accepted: %v", err)
	}
	unavailable := errors.New("state history unavailable")
	if err = VerifyState(r, sourceSnapshot{err: unavailable}); !errors.Is(err, unavailable) {
		t.Fatal(err)
	}
	if err = VerifyState(r, sourceSnapshot{entries: r.Entries}); err != nil {
		t.Fatal(err)
	}
}
func TestJoinRejectsForgedPlanAndRecordsDeletion(t *testing.T) {
	s := New()
	base := requireRevision(t, s, "s", "", nil, "base", []Entry{{Key: []byte("common"), Value: oid('1')}, {Key: []byte("clash"), Value: oid('2')}})
	left := requireRevision(t, s, "s", base.ID, nil, "left", []Entry{{Key: []byte("common"), Value: oid('1')}, {Key: []byte("clash"), Value: oid('3')}})
	right := requireRevision(t, s, "s", base.ID, nil, "right", []Entry{{Key: []byte("common"), Value: oid('1')}, {Key: []byte("clash"), Value: oid('4')}})
	fresh := func() JoinPlan {
		p, err := s.PlanJoin(left.ID, right.ID)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	count := len(s.revisions)
	cleared := fresh()
	cleared.Conflicts = nil
	if _, err := s.CommitJoin(cleared, oid('a'), nil); !errors.Is(err, ErrJoinConflict) {
		t.Fatalf("clearing conflicts bypassed decisions: %v", err)
	}
	edited := fresh()
	if err := edited.Resolve([]byte("clash"), oid('5')); err != nil {
		t.Fatal(err)
	}
	edited.State[0].Value = oid('6')
	if _, err := s.CommitJoin(edited, oid('a'), nil); !errors.Is(err, ErrInvalidJoinPlan) {
		t.Fatalf("edited state accepted: %v", err)
	}
	alteredBase := fresh()
	alteredBase.Base = left.ID
	if _, err := s.CommitJoin(alteredBase, oid('a'), nil); !errors.Is(err, ErrInvalidJoinPlan) {
		t.Fatal(err)
	}
	unexpected := fresh()
	unexpected.Resolutions = []Resolution{{Key: []byte("common"), Value: oid('6')}}
	unexpected.Conflicts = nil
	if _, err := s.CommitJoin(unexpected, oid('a'), nil); !errors.Is(err, ErrInvalidJoinPlan) {
		t.Fatal(err)
	}
	changedDiff := fresh()
	if err := changedDiff.ResolveDelete([]byte("clash")); err != nil {
		t.Fatal(err)
	}
	changedDiff.Changes[0].Before = oid('7')
	if _, err := s.CommitJoin(changedDiff, oid('a'), nil); !errors.Is(err, ErrInvalidJoinPlan) {
		t.Fatal(err)
	}
	if len(s.revisions) != count {
		t.Fatal("rejected join changed graph")
	}
	deletion := fresh()
	if err := deletion.ResolveDelete([]byte("clash")); err != nil {
		t.Fatal(err)
	}
	joined, err := s.CommitJoin(deletion, oid('a'), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(joined.Entries) != 1 || string(joined.Entries[0].Key) != "common" || len(deletion.Resolutions) != 1 || deletion.Resolutions[0].Value != "" {
		t.Fatalf("deletion lost: %+v %+v", deletion, joined)
	}
}
func TestBothSidesDeleteIsNotConflict(t *testing.T) {
	s := New()
	base := requireRevision(t, s, "s", "", nil, "base", []Entry{{Key: []byte("deleted"), Value: oid('1')}})
	l := requireRevision(t, s, "s", base.ID, nil, "left", nil)
	r := requireRevision(t, s, "s", base.ID, nil, "right", nil)
	p, err := s.PlanJoin(l.ID, r.ID)
	if err != nil || len(p.Conflicts) != 0 || len(p.State) != 0 {
		t.Fatalf("both-delete incorrectly conflicts: %+v %v", p, err)
	}
}
func TestExactMergeBasesCrisscrossAndCycleDetection(t *testing.T) {
	s := New()
	root := requireRevision(t, s, "s", "", nil, "root", nil)
	a := requireRevision(t, s, "s", root.ID, nil, "a", nil)
	b := requireRevision(t, s, "s", root.ID, nil, "b", nil)
	x := requireRevision(t, s, "s", a.ID, []model.RevisionID{b.ID}, "x", nil)
	y := requireRevision(t, s, "s", b.ID, []model.RevisionID{a.ID}, "y", nil)
	if _, err := s.MergeBase(x.ID, y.ID); !errors.Is(err, ErrAmbiguousMergeBase) {
		t.Fatalf("crisscross picked arbitrary base: %v", err)
	}
	if base, err := s.MergeBase(x.ID, a.ID); err != nil || base != a.ID {
		t.Fatalf("dominated common ancestor retained: %s %v", base, err)
	}
	independent := requireRevision(t, s, "s", "", nil, "independent", nil)
	if _, err := s.MergeBase(root.ID, independent.ID); !errors.Is(err, ErrNoMergeBase) {
		t.Fatal(err)
	}
	// Store rejects cycles through its public API. Corrupt the private test
	// fixture to confirm traversal cannot silently accept an invalid import.
	bad := s.revisions[root.ID]
	bad.StateParent = x.ID
	s.revisions[root.ID] = bad
	if _, err := s.MergeBase(x.ID, y.ID); !errors.Is(err, ErrRevisionCycle) {
		t.Fatalf("cycle traversal did not reject: %v", err)
	}
}
func TestDiffAndJoinAgainstIndependentMapOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(7241))
	keys := []string{"", "\x00", "a", "ax", "ay", "bx", "by", "a\x00", "\xff"}
	value := func(n int) model.ObjectID { return testDigest(fmt.Sprint("value-", n)) }
	for iteration := 0; iteration < 350; iteration++ {
		maps := []map[string]model.ObjectID{{}, {}, {}}
		for _, key := range keys {
			for side := range maps {
				n := rng.Intn(5)
				if n != 0 {
					maps[side][key] = value(n)
				}
			}
		}
		s := New()
		b := requireRevision(t, s, "s", "", nil, "base", mapEntries(maps[0]))
		l := requireRevision(t, s, "s", b.ID, nil, "left", mapEntries(maps[1]))
		r := requireRevision(t, s, "s", b.ID, nil, "right", mapEntries(maps[2]))
		d, err := s.Diff(b.ID, l.ID)
		if err != nil {
			t.Fatal(err)
		}
		actual := make(map[string]model.ObjectID)
		for k, v := range maps[0] {
			actual[k] = v
		}
		last := []byte(nil)
		for i, c := range d.Changes {
			if i > 0 && bytes.Compare(last, c.Key) >= 0 {
				t.Fatal("unsorted absolute key changes")
			}
			last = c.Key
			if actual[string(c.Key)] != c.Before {
				t.Fatal("incorrect before value")
			}
			if c.After == "" {
				delete(actual, string(c.Key))
			} else {
				actual[string(c.Key)] = c.After
			}
		}
		if !reflect.DeepEqual(actual, maps[1]) {
			t.Fatalf("map diff mismatch iteration=%d", iteration)
		}
		expected := map[string]model.ObjectID{}
		conflicts := map[string]struct{}{}
		// The oracle uses absent="" (a value cannot be empty), with neither the
		// graph's presence helper nor its tree/map diff implementation.
		for _, key := range keys {
			bv, lv, rv := maps[0][key], maps[1][key], maps[2][key]
			switch {
			case lv == rv:
				if lv != "" {
					expected[key] = lv
				}
			case lv == bv:
				if rv != "" {
					expected[key] = rv
				}
			case rv == bv:
				if lv != "" {
					expected[key] = lv
				}
			default:
				conflicts[key] = struct{}{}
			}
		}
		plan, err := s.PlanJoin(l.ID, r.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.Conflicts) != len(conflicts) {
			t.Fatalf("conflict oracle mismatch iteration=%d: %+v", iteration, plan)
		}
		conflictKeys := make([]string, 0, len(conflicts))
		for k := range conflicts {
			conflictKeys = append(conflictKeys, k)
		}
		sort.Strings(conflictKeys)
		for _, key := range conflictKeys {
			choice := rng.Intn(5)
			selected := model.ObjectID("")
			if choice != 0 {
				selected = value(choice)
				expected[key] = selected
			} else {
				delete(expected, key)
			}
			if err = plan.Resolve([]byte(key), selected); err != nil {
				t.Fatal(err)
			}
		}
		joined, err := s.CommitJoin(plan, testDigest("joined"), nil)
		if err != nil {
			t.Fatalf("iteration=%d %v", iteration, err)
		}
		if !reflect.DeepEqual(entriesMap(joined.Entries), expected) {
			t.Fatalf("joined state differs iteration=%d", iteration)
		}
		if !equalChanges(plan.Changes, diffEntries(l.Entries, joined.Entries)) {
			t.Fatal("application changes are not relative to StateParent")
		}
	}
}
func TestConcurrentSnapshotOwnershipAndBranchCAS(t *testing.T) {
	s := New()
	root := requireRevision(t, s, "s", "", nil, "root", []Entry{{Key: []byte("key"), Value: oid('1')}})
	if _, err := s.Fork("branch", root.ID); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var successes int
	var mu sync.Mutex
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := s.AddRevision("s", root.ID, nil, testDigest(fmt.Sprint(i)), nil, []Entry{{Key: []byte("key"), Value: oid('1')}})
			if err != nil {
				t.Error(err)
				return
			}
			if _, err = s.AdvanceBranch("branch", 0, r.ID); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			} else if !errors.Is(err, ErrBranchCAS) {
				t.Error(err)
			}
			got, err := s.Get(root.ID)
			if err != nil {
				t.Error(err)
				return
			}
			got.Entries[0].Key[0] = 'X'
			_, _ = s.MergeBase(root.ID, r.ID)
		}(i)
	}
	wg.Wait()
	if successes != 1 {
		t.Fatalf("same-version branch updates succeeded %d times", successes)
	}
	got, _ := s.Get(root.ID)
	if string(got.Entries[0].Key) != "key" {
		t.Fatal("concurrent caller mutated stored content")
	}
}
func TestRevisionIndependentGolden(t *testing.T) {
	r, err := BuildRevision("space", "", nil, oid('a'), []model.EventID{model.EventID(oid('e'))}, []Entry{{Key: nil, Value: oid('1')}, {Key: []byte{0, 255}, Value: oid('2')}})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeRevision(r)
	if err != nil {
		t.Fatal(err)
	}
	// Expected bytes/identity were independently generated with Python's
	// unsigned-varint writer and hashlib, not the graph encoder.
	const expectedID = "72f114386477307fe495f4c9c824a9e27e81f4a7e17d3b6c33395689e344696a"
	const expectedBytes = "0205737061636500004061616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161014065656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565020040313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131310200ff4032323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232"
	if string(r.ID) != expectedID || hex.EncodeToString(raw) != expectedBytes {
		t.Fatalf("golden differs id=%s bytes=%x", r.ID, raw)
	}
}

func TestMergeBaseAgainstNaiveAncestorOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(9103))
	s := New()
	ids := make([]model.RevisionID, 0, 70)
	root := requireRevision(t, s, "s", "", nil, "root", nil)
	ids = append(ids, root.ID)
	for i := 1; i < 70; i++ {
		parent := ids[rng.Intn(len(ids))]
		var joins []model.RevisionID
		if i > 2 && rng.Intn(2) == 0 {
			other := ids[rng.Intn(len(ids))]
			if other != parent {
				joins = []model.RevisionID{other}
			}
		}
		r := requireRevision(t, s, "s", parent, joins, fmt.Sprint("node-", i), nil)
		ids = append(ids, r.ID)
	}
	ancestors := func(start model.RevisionID) map[model.RevisionID]bool {
		out := map[model.RevisionID]bool{}
		queue := []model.RevisionID{start}
		for len(queue) > 0 {
			id := queue[0]
			queue = queue[1:]
			if id == "" || out[id] {
				continue
			}
			out[id] = true
			r, _ := s.Get(id)
			queue = append(queue, r.StateParent)
			queue = append(queue, r.JoinParents...)
		}
		return out
	}
	for i := 0; i < 180; i++ {
		left, right := ids[rng.Intn(len(ids))], ids[rng.Intn(len(ids))]
		a, b := ancestors(left), ancestors(right)
		var best []model.RevisionID
		for candidate := range a {
			if !b[candidate] {
				continue
			}
			dominated := false
			for other := range a {
				if other == candidate || !b[other] {
					continue
				}
				if ancestors(other)[candidate] {
					dominated = true
					break
				}
			}
			if !dominated {
				best = append(best, candidate)
			}
		}
		got, err := s.MergeBase(left, right)
		if len(best) == 1 {
			if err != nil || got != best[0] {
				t.Fatalf("oracle disagreement left=%s right=%s expected=%v got=%s err=%v", left, right, best, got, err)
			}
		} else if !errors.Is(err, ErrAmbiguousMergeBase) {
			t.Fatalf("ambiguous oracle result became success: %v %s %v", best, got, err)
		}
	}
}
func TestAncestorWalkStopsAtResourceBound(t *testing.T) {
	s := New()
	var last model.RevisionID
	// Populate a structurally deep private fixture without hiding the traversal
	// under constructor/hash cost. Real accepted revisions have the same topology.
	for i := 0; i < MaxWalkNodes+1; i++ {
		id := model.RevisionID(fmt.Sprintf("%064x", i+1))
		s.revisions[id] = Revision{ID: id, Space: "s", StateParent: last}
		last = id
	}
	if _, err := s.ancestorSet(last); !errors.Is(err, ErrLimit) {
		t.Fatalf("deep traversal ignored bound: %v", err)
	}
}
func FuzzRevisionCodec(f *testing.F) {
	r, _ := BuildRevision("space", "", nil, testDigest("root"), nil, []Entry{{Key: nil, Value: testDigest("value")}})
	raw, _ := EncodeRevision(r)
	f.Add(raw)
	f.Add([]byte{byte(RevisionSchema)})
	f.Add([]byte{1})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > MaxRevisionBytes+1 {
			return
		}
		r, err := DecodeRevision(raw)
		if err != nil {
			return
		}
		canonical, err := EncodeRevision(r)
		if err != nil || !bytes.Equal(canonical, raw) {
			t.Fatalf("decoder accepted noncanonical data: %v", err)
		}
	})
}
