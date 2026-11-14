package memory

import (
	"errors"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestExplicitMemoryReuseChecksScopeAndTime(t *testing.T) {
	store := New()
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	candidate, err := store.PutCandidate(Memory{
		Space: "space", Kind: KindProcedure, Claim: []byte("rollback database migration"),
		Sources: []Source{source(1)}, ValidFrom: start, ValidUntil: &end,
		Applicability: Applicability{Goal: "repair", Environment: "prod", SchemaVersion: 2, Tags: []string{"sql"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	validation := Validation{StrategyVersion: 1, Passed: true, Evidence: []Source{source(1)}}
	validated, err := store.Validate(candidate.ID, validation)
	if err != nil {
		t.Fatal(err)
	}
	member, err := store.Promote(validated.ID)
	if err != nil {
		t.Fatal(err)
	}
	query := Query{Space: "space", At: anchor(2), AtTime: start, Goal: "repair", Environment: "prod", SchemaVersion: 2, Tags: []string{"sql"}}
	if _, err := store.ReusableMemoryFor(member.ID, query); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Query){
		func(q *Query) { q.AtTime = time.Time{} },
		func(q *Query) { q.AtTime = end },
		func(q *Query) { q.Goal = "inspect" },
		func(q *Query) { q.Environment = "staging" },
		func(q *Query) { q.SchemaVersion = 1 },
		func(q *Query) { q.Tags = []string{"kafka"} },
	} {
		bad := query
		change(&bad)
		if _, err := store.ReusableMemoryFor(member.ID, bad); !errors.Is(err, ErrNotEligible) {
			t.Fatalf("scope or time mismatch reused memory: %+v %v", bad, err)
		}
	}
	path, err := store.BuildExperiencePath("space", anchor(2), []Step{auditStep(member)}, nil, validation, Applicability{
		Goal: "database recovery", Environment: "prod",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	query.Goal, query.Tags = "database recovery", nil
	if _, err := store.ReusablePathFor(path.ID, query); err != nil {
		t.Fatalf("valid path uses a step for a narrower subgoal: %v", err)
	}
	query.Goal = "repair"
	if _, err := store.ReusablePathFor(path.ID, query); !errors.Is(err, ErrNotEligible) {
		t.Fatalf("path scope mismatch: %v", err)
	}
}

func TestExplicitReuseRejectsSupersededTransitiveProvenance(t *testing.T) {
	store := New()
	member := auditPromoteFrom(t, store, "member", source(1))
	other := auditPromoteFrom(t, store, "independent member", source(1))
	basePath, err := store.BuildExperiencePath("space", anchor(3), []Step{auditStep(member)}, nil, auditPathValidation(), Applicability{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	derivedPath, err := store.BuildExperiencePath("space", anchor(3), []Step{auditStep(other)}, nil, auditPathValidation(), Applicability{}, []model.ObjectID{basePath.ID})
	if err != nil {
		t.Fatal(err)
	}
	fromPath := auditPromoteFrom(t, store, "derived from path", Source{SourceID: derivedPath.ID, Anchor: anchor(3)})
	query := Query{Space: "space", At: anchor(4)}
	if _, err := store.ReusablePathFor(derivedPath.ID, query); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReusableMemoryFor(fromPath.ID, query); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transition(member.ID, StatusSuperseded, "choose newer evidence"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReusablePathFor(derivedPath.ID, query); !errors.Is(err, ErrNotEligible) {
		t.Fatalf("derived path reused a superseded ancestor member: %v", err)
	}
	if _, err := store.ReusableMemoryFor(fromPath.ID, query); !errors.Is(err, ErrNotEligible) {
		t.Fatalf("memory reused a superseded ancestor member: %v", err)
	}
	if _, err := store.Get(fromPath.ID); err != nil {
		t.Fatal("historical audit record no longer exists", err)
	}
}
