package memory

import (
	"errors"
	"github.com/vkviyu/agent-temporal-engine/model"
	"strings"
	"testing"
)

func digest(ch byte) model.ObjectID { return model.ObjectID(strings.Repeat(string(ch), 64)) }
func anchor(lsn uint64) model.SnapshotAnchor {
	return model.SnapshotAnchor{Fence: model.CommitFence{Store: "s", LSN: lsn, CommitDigest: model.CommitDigest(digest('f'))}, Space: "space"}
}
func source(lsn uint64) Source { return Source{SourceID: digest('e'), Anchor: anchor(lsn)} }
func TestMemoryLifecycleSearchAndSourceRetraction(t *testing.T) {
	s := New()
	candidate, err := s.PutCandidate(Memory{Space: "space", Kind: KindProcedure, Claim: []byte("restart service safely"), Sources: []Source{source(1)}, Applicability: Applicability{Goal: "repair", Environment: "prod"}, Confidence: 900})
	if err != nil {
		t.Fatal(err)
	}
	validated, err := s.Validate(candidate.ID, Validation{StrategyVersion: 1, Passed: true, Evidence: []Source{source(1)}})
	if err != nil || validated.Status != StatusValidated || validated.ID == candidate.ID {
		t.Fatalf("validated=%+v err=%v", validated, err)
	}
	promoted, err := s.Promote(validated.ID)
	if err != nil || promoted.Status != StatusPromoted || promoted.ID == validated.ID {
		t.Fatalf("promoted=%+v err=%v", promoted, err)
	}
	got, err := s.Search(Query{Space: "space", Text: "restart", Goal: "repair", Environment: "prod", At: anchor(2)}, 10)
	if err != nil || len(got) != 1 || got[0].Memory.ID != promoted.ID {
		t.Fatalf("search=%+v err=%v", got, err)
	}
	changed, err := s.RetractSource(digest('e'), Retraction{Kind: RetractionUseBan, Reason: "source corrected"})
	if err != nil || len(changed) != 1 {
		t.Fatalf("retract=%v %v", changed, err)
	}
	after, err := s.Search(Query{Space: "space", Text: "restart", At: anchor(2)}, 10)
	if err != nil || len(after) != 0 {
		t.Fatalf("banned memory returned: %+v %v", after, err)
	}
	if _, err := s.Promote(promoted.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("old promoted state resurrected: %v", err)
	}
}
func TestExperiencePathRequiresPromotedMembersAndSourceCut(t *testing.T) {
	s := New()
	m, err := s.PutCandidate(Memory{Space: "space", Kind: KindFact, Claim: []byte("stable fact"), Sources: []Source{source(2)}})
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.Validate(m.ID, Validation{StrategyVersion: 1, Passed: true, Evidence: []Source{source(2)}})
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.Promote(v.ID)
	if err != nil {
		t.Fatal(err)
	}
	step := Step{Node: model.NodeRef{Kind: model.NodeObject, Object: digest('a')}, Memory: p.ID, Order: 0, Outcome: "confirmed"}
	path, err := s.BuildExperiencePath("space", anchor(3), []Step{step}, nil, Validation{StrategyVersion: 1, Passed: true, Evidence: []Source{source(2)}}, Applicability{Goal: "g"}, []model.ObjectID{p.ID})
	if err != nil || path.Status != StatusPromoted {
		t.Fatalf("path=%+v err=%v", path, err)
	}
	if _, err := s.BuildExperiencePath("space", anchor(1), []Step{step}, nil, Validation{Passed: true, Evidence: []Source{source(2)}}, Applicability{}, nil); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("future source accepted: %v", err)
	}
	retracted, err := s.RetractPath(path.ID, Retraction{Kind: RetractionFactualCorrection, Reason: "invalid path"})
	if err != nil || retracted.Status != StatusRetracted || retracted.ID == path.ID {
		t.Fatalf("retracted=%+v err=%v", retracted, err)
	}
}
