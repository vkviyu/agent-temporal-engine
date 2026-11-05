package memory

import (
	"errors"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// These tests come from an independent audit of the immutable record and
// derivation boundaries. They deliberately use only the public Store methods
// to construct dependencies, rather than mutating private indexes.
func auditPromoteFrom(t *testing.T, s *Store, claim string, src Source) Memory {
	t.Helper()
	m := recordFixture()
	m.Claim = []byte(claim)
	m.Sources = []Source{cloneSource(src)}
	m.Evidence = nil
	candidate, err := s.PutCandidate(m)
	if err != nil {
		t.Fatal(err)
	}
	validated, err := s.Validate(candidate.ID, Validation{Passed: true, StrategyVersion: 1, Evidence: []Source{cloneSource(src)}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.Promote(validated.ID)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func auditStep(m Memory) Step {
	return Step{Node: model.NodeRef{Kind: model.NodeObject, Object: digest('a')}, Memory: m.ID, Order: 0, Outcome: "confirmed"}
}
func auditPathValidation() Validation {
	return Validation{Passed: true, StrategyVersion: 1, Evidence: []Source{source(1)}}
}

func TestAuditPathCounterexamplesMustBeWithinSourceCut(t *testing.T) {
	for _, which := range []string{"future", "foreign-store"} {
		t.Run(which, func(t *testing.T) {
			s := New()
			m := auditPromoteFrom(t, s, "path member", source(1))
			validation := auditPathValidation()
			counter := source(9)
			if which == "foreign-store" {
				counter = source(1)
				counter.Anchor.Fence.Store = "foreign"
			}
			validation.Counterexamples = []Source{counter}
			if path, err := s.BuildExperiencePath("space", anchor(3), []Step{auditStep(m)}, nil, validation, Applicability{}, nil); err == nil {
				t.Fatalf("path at cut3 adopted %s counterexample: %+v", which, path)
			}
		})
	}
}

func TestAuditSourceWithdrawalBlocksTransitiveMemory(t *testing.T) {
	s := New()
	a := auditPromoteFrom(t, s, "source fact", source(1))
	dependency := Source{SourceID: a.ID, Anchor: anchor(2)}
	b := auditPromoteFrom(t, s, "derived procedure", dependency)
	if got, err := s.Search(Query{Space: "space", Text: "derived", At: anchor(3)}, 10); err != nil || len(got) != 1 || got[0].Memory.ID != b.ID {
		t.Fatalf("invalid transitive fixture: %+v %v", got, err)
	}
	if _, err := s.RetractSource(source(1).SourceID, Retraction{Kind: RetractionUseBan, Reason: "withdraw root evidence"}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Search(Query{Space: "space", Text: "derived", At: anchor(3)}, 10); err != nil || len(got) != 0 {
		t.Fatalf("derived memory reused withdrawn dependency: %+v %v", got, err)
	}
	// Both immutable versions remain available for audit despite reuse denial.
	if _, err := s.Get(a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(b.ID); err != nil {
		t.Fatal(err)
	}
}

func TestAuditPathWithdrawalBlocksDerivedPath(t *testing.T) {
	s := New()
	m := auditPromoteFrom(t, s, "valid member", source(1))
	first, err := s.BuildExperiencePath("space", anchor(3), []Step{auditStep(m)}, nil, auditPathValidation(), Applicability{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.BuildExperiencePath("space", anchor(3), []Step{auditStep(m)}, nil, auditPathValidation(), Applicability{}, []model.ObjectID{first.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReusablePath(second.ID, anchor(3)); err != nil {
		t.Fatal("invalid derived path fixture", err)
	}
	if _, err = s.RetractPath(first.ID, Retraction{Kind: RetractionUseBan, Reason: "withdraw derived input path"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReusablePath(second.ID, anchor(3)); !errors.Is(err, ErrNotEligible) {
		t.Fatalf("path reused withdrawn DerivedFrom dependency: %v", err)
	}
}

func TestAuditCanonicalTimeRoundTripsAtTimezoneBoundaries(t *testing.T) {
	cases := []time.Time{time.Date(1, time.January, 1, 0, 0, 0, 0, time.FixedZone("east", 3600)), time.Date(9999, time.December, 31, 23, 30, 0, 0, time.FixedZone("west", -3600))}
	for _, stamp := range cases {
		m := recordFixture()
		m.RecordedAt = stamp
		raw, err := Canonical(m)
		if err != nil {
			continue
		} // Rejecting an out-of-range UTC instant is valid.
		decoded, err := DecodeMemory(raw)
		if err != nil {
			t.Fatalf("encoder accepted unrepresentable UTC instant %s: %v", stamp, err)
		}
		if !decoded.RecordedAt.Equal(stamp) {
			t.Fatalf("roundtrip changed instant: %s -> %s", stamp, decoded.RecordedAt)
		}
	}
}
