package memory

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func recordFixture() Memory {
	return Memory{Space: "space", Kind: KindFact, Status: StatusCandidate, Claim: []byte("source aware memory"), Sources: []Source{source(1)}, Evidence: []Evidence{{Source: source(1), Note: "original"}}, Applicability: Applicability{SchemaVersion: 1, Tags: []string{"go"}}, RecordedAt: time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)}
}
func TestMemoryIdentityCoversEvidenceAnchorsValidityAndRetraction(t *testing.T) {
	original := recordFixture()
	id := IDFor(original)
	if id == "" {
		t.Fatal("invalid fixture")
	}
	changes := []func(*Memory){
		func(m *Memory) { m.Evidence[0].Counter = true }, func(m *Memory) { m.Evidence[0].Source.Anchor.Fence.LSN++ }, func(m *Memory) { m.Evidence[0].Note = "changed" }, func(m *Memory) { m.RecordedAt = m.RecordedAt.Add(time.Second) }, func(m *Memory) { m.VisibleAt = m.RecordedAt }, func(m *Memory) { m.ValidFrom = m.RecordedAt }, func(m *Memory) { x := m.RecordedAt.Add(time.Hour); m.ValidUntil = &x }, func(m *Memory) { m.Applicability.Tags = []string{"rust"} }, func(m *Memory) { m.Retracts = []model.ObjectID{digest('a')} }, func(m *Memory) {
			m.Sources[0].Anchor.Cursor = &model.Cursor{Store: "s", Space: "space", Revision: model.RevisionID(digest('a'))}
		}, func(m *Memory) { m.Sources[0].Anchor.Knowledge = &model.KnowledgeKey{Actor: "a", Execution: "e"} }, func(m *Memory) {
			m.Retraction = &Retraction{Kind: RetractionUseBan, Reason: "withdraw", RecordedAt: m.RecordedAt}
		}}
	for i, change := range changes {
		m := cloneMemory(original)
		change(&m)
		if got := IDFor(m); got == "" || got == id {
			t.Fatalf("semantic field %d not bound: %s", i, got)
		}
	}
	a := cloneMemory(original)
	a.Applicability.Tags = []string{"a", "b"}
	b := cloneMemory(a)
	b.Applicability.Tags = []string{"b", "a"}
	if IDFor(a) != IDFor(b) {
		t.Fatal("tag set order changed identity")
	}
}
func promoted(t *testing.T, s *Store, m Memory) Memory {
	t.Helper()
	c, err := s.PutCandidate(m)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.Validate(c.ID, Validation{Passed: true, StrategyVersion: 1, Evidence: []Source{source(1)}})
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.Promote(v.ID)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func TestAllSourcesAndValidationEvidenceMustBeAtCut(t *testing.T) {
	s := New()
	m := recordFixture()
	m.Sources = append(m.Sources, Source{SourceID: digest('d'), Anchor: anchor(9)})
	p := promoted(t, s, m)
	got, err := s.Search(Query{Space: "space", At: anchor(2)}, 10)
	if err != nil || len(got) != 0 {
		t.Fatalf("future second source leaked: %v %v", got, err)
	}
	if _, err := s.BuildExperiencePath("space", anchor(2), []Step{{Node: model.NodeRef{Kind: model.NodeObject, Object: digest('a')}, Memory: p.ID}}, nil, Validation{Passed: true, Evidence: []Source{source(1)}}, Applicability{}, nil); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("future path allowed %v", err)
	}
	bad := recordFixture()
	bad.Sources[0].Anchor.Space = "foreign"
	if _, err = s.PutCandidate(bad); !errors.Is(err, ErrInvalidMemory) {
		t.Fatal("foreign source accepted", err)
	}
}
func TestSourceRetractionInvalidatesPathsAndPreservesAudit(t *testing.T) {
	s := New()
	p := promoted(t, s, recordFixture())
	path, err := s.BuildExperiencePath("space", anchor(3), []Step{{Memory: p.ID, Node: model.NodeRef{Kind: model.NodeObject, Object: digest('a')}}}, nil, Validation{Passed: true, StrategyVersion: 1, Evidence: []Source{source(1)}}, Applicability{Tags: []string{"go"}}, []model.ObjectID{p.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReusablePath(path.ID, anchor(3)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.RetractSource(digest('e'), Retraction{Kind: RetractionUseBan, Reason: "revoked"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReusablePath(path.ID, anchor(3)); !errors.Is(err, ErrNotEligible) {
		t.Fatal("path reused withdrawn memory", err)
	}
	original, err := s.GetPath(path.ID)
	if err != nil || original.ID != path.ID || original.Status != StatusPromoted {
		t.Fatal("immutable audit changed", err)
	}
	if s.pathSuccessors[path.ID] == "" {
		t.Fatal("path retraction lineage missing")
	}
	if _, err = s.Validate(p.Supersedes[0], Validation{Passed: true, Evidence: []Source{source(1)}}); err == nil {
		t.Fatal("old version mutated again")
	}
}
func TestPathIdentityCoversValidationAndDerivation(t *testing.T) {
	s := New()
	p := promoted(t, s, recordFixture())
	path, err := s.BuildExperiencePath("space", anchor(3), []Step{{Memory: p.ID, Node: model.NodeRef{Kind: model.NodeObject, Object: digest('a')}}}, nil, Validation{Passed: true, Evidence: []Source{source(1)}, StrategyVersion: 1}, Applicability{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	edits := []func(*ExperiencePath){func(p *ExperiencePath) { p.Validation.Counterexamples = []Source{source(2)} }, func(p *ExperiencePath) { p.Validation.Evidence[0].Anchor.Fence.LSN++ }, func(p *ExperiencePath) { p.DerivedFrom = []model.ObjectID{digest('a')} }, func(p *ExperiencePath) { p.Applicability.SchemaVersion++ }, func(p *ExperiencePath) { p.SourceAnchor.Knowledge = &model.KnowledgeKey{Actor: "a", Execution: "e"} }}
	for i, edit := range edits {
		q := clonePath(path)
		q.ID = ""
		edit(&q)
		if id := PathIDFor(q); id == "" || id == path.ID {
			t.Fatalf("path semantic field %d not bound", i)
		}
	}
	got, _ := s.GetPath(path.ID)
	got.Validation.Evidence[0].Anchor.Space = "changed"
	again, _ := s.GetPath(path.ID)
	if again.Validation.Evidence[0].Anchor.Space != "space" {
		t.Fatal("aliased source")
	}
}
func TestConcurrentMemoryReadsAndPromotionAreBounded(t *testing.T) {
	s := New()
	p := promoted(t, s, recordFixture())
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 20; n++ {
				m, err := s.Get(p.ID)
				if err != nil || !bytes.Equal(m.Claim, p.Claim) {
					t.Error(err)
				}
				_, _ = s.Search(Query{Space: "space", At: anchor(2), Tags: []string{"go"}}, 10)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = s.RetractSource(digest('e'), Retraction{Kind: RetractionUseBan, Reason: "withdraw"})
	}()
	wg.Wait()
}

func TestMemoryAndPathCanonicalDecodersAreOwnedAndStrict(t *testing.T) {
	s := New()
	p := promoted(t, s, recordFixture())
	raw, err := Canonical(p)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeMemory(raw)
	if err != nil || got.ID != p.ID {
		t.Fatalf("memory decode %s %v", got.ID, err)
	}
	again, err := Canonical(got)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatal("memory roundtrip", err)
	}
	for n := 0; n < len(raw); n++ {
		if _, err := DecodeMemory(raw[:n]); err == nil {
			t.Fatalf("accepted truncation %d", n)
		}
	}
	path, err := s.BuildExperiencePath("space", anchor(3), []Step{{Memory: p.ID, Node: model.NodeRef{Kind: model.NodeObject, Object: digest('a')}}}, nil, Validation{Passed: true, Evidence: []Source{source(1)}}, Applicability{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalPath(path)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePath(b)
	if err != nil || decoded.ID != path.ID {
		t.Fatal("path", err)
	}
	decoded.Validation.Evidence[0].Anchor.Space = "modified"
	if path.Validation.Evidence[0].Anchor.Space != "space" {
		t.Fatal("aliased path")
	}
	raw[0] ^= 1
	if _, err = DecodeMemory(raw); err == nil {
		t.Fatal("bad magic accepted")
	}
}
func FuzzMemoryCanonical(f *testing.F) {
	s := New()
	m, err := s.PutCandidate(recordFixture())
	if err != nil {
		f.Fatal(err)
	}
	raw, err := Canonical(m)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		got, err := DecodeMemory(b)
		if err != nil {
			return
		}
		encoded, err := Canonical(got)
		if err != nil || !bytes.Equal(encoded, b) {
			t.Fatal("noncanonical success", err)
		}
	})
}

func TestWithdrawnDependenciesCannotBeReintroducedAfterRetraction(t *testing.T) {
	s := New()
	a := promoted(t, s, recordFixture())
	path, err := s.BuildExperiencePath("space", anchor(3), []Step{{Memory: a.ID, Node: model.NodeRef{Kind: model.NodeObject, Object: digest('a')}}}, nil, Validation{Passed: true, Evidence: []Source{source(1)}}, Applicability{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.RetractPath(path.ID, Retraction{Kind: RetractionUseBan, Reason: "withdraw"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.BuildExperiencePath("space", anchor(3), []Step{{Memory: a.ID, Node: model.NodeRef{Kind: model.NodeObject, Object: digest('a')}}}, nil, Validation{Passed: true, Evidence: []Source{source(1)}}, Applicability{}, []model.ObjectID{path.ID}); !errors.Is(err, ErrNotEligible) {
		t.Fatal("reintroduced path", err)
	}
	if _, err = s.RetractSource(source(1).SourceID, Retraction{Kind: RetractionUseBan, Reason: "withdraw root"}); err != nil {
		t.Fatal(err)
	}
	m := recordFixture()
	m.Claim = []byte("new derived record")
	m.Sources = []Source{{SourceID: a.ID, Anchor: anchor(2)}}
	m.Evidence = nil
	c, err := s.PutCandidate(m)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.Validate(c.ID, Validation{Passed: true, Evidence: []Source{{SourceID: a.ID, Anchor: anchor(2)}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Promote(v.ID); !errors.Is(err, ErrNotEligible) {
		t.Fatal("reintroduced withdrawn memory", err)
	}
}
