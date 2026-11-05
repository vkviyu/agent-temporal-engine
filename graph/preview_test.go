package graph

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestPreviewOperationsNeverInstallCandidate(t *testing.T) {
	s := New()
	root := requireRevision(t, s, "space", "", nil, "root", nil)
	left := requireRevision(t, s, "space", root.ID, nil, "left", nil)
	right := requireRevision(t, s, "space", root.ID, nil, "right", nil)
	plan, err := s.PlanJoin(left.ID, right.ID)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := s.PreviewJoin(plan, testDigest("joined"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Get(joined.ID); !errors.Is(err, ErrUnknownRevision) {
		t.Fatalf("PreviewJoin mutated graph: %v", err)
	}
	b, err := s.PreviewFork("branch", root.ID)
	if err != nil || b.Version != 0 {
		t.Fatalf("preview Fork=%+v %v", b, err)
	}
	if _, err = s.Branch("branch"); !errors.Is(err, ErrUnknownBranch) {
		t.Fatalf("PreviewFork mutated graph: %v", err)
	}
	if _, err = s.Fork("branch", root.ID); err != nil {
		t.Fatal(err)
	}
	advanced, err := s.PreviewAdvanceBranch("branch", 0, left.ID)
	if err != nil || advanced.Version != 1 {
		t.Fatalf("preview Advance=%+v %v", advanced, err)
	}
	current, _ := s.Branch("branch")
	if current.Version != 0 || current.Head != root.ID {
		t.Fatalf("PreviewAdvance mutated branch: %+v", current)
	}
}

func TestBranchPreviewsRejectCapacityBeforeInstall(t *testing.T) {
	s := New()
	r := requireRevision(t, s, "space", "", nil, "root", nil)
	for i := 0; i < MaxBranches; i++ {
		id := model.BranchID(fmt.Sprintf("b%d", i))
		s.branches[id] = Branch{ID: id, Head: r.ID}
	}
	if _, err := s.PreviewFork("overflow", r.ID); !errors.Is(err, ErrLimit) {
		t.Fatalf("branch capacity ignored: %v", err)
	}
	s.branches["b0"] = Branch{ID: "b0", Head: r.ID, Version: math.MaxUint64}
	if _, err := s.PreviewAdvanceBranch("b0", math.MaxUint64, r.ID); !errors.Is(err, ErrLimit) {
		t.Fatalf("branch version overflow ignored: %v", err)
	}
}
