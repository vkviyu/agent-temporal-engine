package graphstore

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func joinFixture(t *testing.T, s *Store) graph.JoinPlan {
	t.Helper()
	root, _, err := s.AddRevision(context.Background(), "space", "", nil, gid('a'), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	left, _, err := s.AddRevision(context.Background(), "space", root.ID, nil, gid('b'), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	right, _, err := s.AddRevision(context.Background(), "space", root.ID, nil, gid('c'), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := s.PlanJoin(left.ID, right.ID)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestJoinCandidateAbsentAfterPrepublicationFailure(t *testing.T) {
	for _, boundary := range []string{"state verification", "journal quota", "unknown publication"} {
		t.Run(boundary, func(t *testing.T) {
			path := t.TempDir()
			d := openDir(t, path)
			defer d.Close()
			fault := &publicationFault{DurableIO: d}
			options := storage.JournalOptions{}
			if boundary == "journal quota" {
				options.MaxPublishedTransactions = 3
			}
			oracle := &stateOracle{states: map[model.ObjectID]stateSnapshot{gid('a'): {space: "space"}, gid('b'): {space: "space"}, gid('c'): {space: "space"}}}
			s, err := CreateVerified(fault, options, oracle)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			plan := joinFixture(t, s)
			candidate, err := graph.BuildRevision("space", plan.Left, []model.RevisionID{plan.Right}, gid('d'), nil, plan.State)
			if err != nil {
				t.Fatal(err)
			}
			before := treeBytes(t, path)
			if boundary != "state verification" {
				oracle.states[gid('d')] = stateSnapshot{space: "space"}
			}
			if boundary == "unknown publication" {
				fault.fail = true
			}
			got, proof, err := s.CommitJoin(context.Background(), plan, gid('d'), nil)
			if err == nil || got.ID != "" || !reflect.DeepEqual(proof, storage.PublishedTransaction{}) {
				t.Fatalf("failed Join granted candidate: %+v %+v %v", got, proof, err)
			}
			if _, err := s.graph.Get(candidate.ID); !errors.Is(err, graph.ErrUnknownRevision) {
				t.Fatalf("unpublished join installed in graph: %v", err)
			}
			if _, found := s.committed[candidate.ID]; found {
				t.Fatal("unpublished candidate entered receipts")
			}
			if boundary != "unknown publication" && !reflect.DeepEqual(before, treeBytes(t, path)) {
				t.Fatal("prepublication rejection changed durable bytes")
			}
			fault.fail = false
		})
	}
}

func TestBranchRetriesReturnOriginalTypedResultAfterLaterAdvancesAndReopen(t *testing.T) {
	path := t.TempDir()
	d := openDir(t, path)
	s, err := Create(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	root, _, err := s.AddRevision(context.Background(), "space", "", nil, gid('a'), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	b0, p0, err := s.Fork(context.Background(), "branch", root.ID)
	if err != nil {
		t.Fatal(err)
	}
	next, _, err := s.AddRevision(context.Background(), "space", root.ID, nil, gid('b'), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	b1, p1, err := s.AdvanceBranch(context.Background(), "branch", 0, next.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.AdvanceBranch(context.Background(), "branch", 1, root.ID); err != nil {
		t.Fatal(err)
	}
	assertRetries := func(s *Store) {
		t.Helper()
		got, proof, err := s.Fork(context.Background(), "branch", root.ID)
		if err != nil || got != b0 {
			t.Fatalf("fork retry lost original result: %+v %v", got, err)
		}
		checkOriginalProof(t, proof, p0)
		got, proof, err = s.AdvanceBranch(context.Background(), "branch", 0, next.ID)
		if err != nil || got != b1 {
			t.Fatalf("advance retry returned latest result: %+v %v", got, err)
		}
		checkOriginalProof(t, proof, p1)
		latest, err := s.Branch("branch")
		if err != nil || latest.Version != 2 || latest.Head != root.ID {
			t.Fatalf("retry rewound current branch: %+v %v", latest, err)
		}
	}
	assertRetries(s)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openDir(t, path)
	defer d.Close()
	s, _, err = Open(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	assertRetries(s)
}

func TestBranchReplayRejectsSpaceAndGenerationBeforeRepair(t *testing.T) {
	for _, mismatch := range []string{"space", "generation"} {
		t.Run(mismatch, func(t *testing.T) {
			path := t.TempDir()
			d := openDir(t, path)
			defer d.Close()
			s, err := Create(d, storage.JournalOptions{})
			if err != nil {
				t.Fatal(err)
			}
			root, _, err := s.AddRevision(context.Background(), "space", "", nil, gid('a'), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			j, _, err := storage.OpenJournal(d, storage.JournalOptions{})
			if err != nil {
				t.Fatal(err)
			}
			p, err := j.Published()
			if err != nil {
				t.Fatal(err)
			}
			raw, err := encodeBranchCommand(branchCommand{operation: branchFork, id: "branch", source: root.ID})
			if err != nil {
				t.Fatal(err)
			}
			digest := branchDigest(raw)
			tx := storage.Transaction{TxID: "02020202020202020202020202020202", Request: model.RequestKey{Space: "space", Principal: principal, RequestID: branchRequestID(raw)}, RequestDigest: digest, LSN: 2, PreviousDigest: p.Position.Digest, Records: []storage.Record{{Kind: storage.KindEvent, Payload: raw}}, ControlUpdates: encodePublicationMetadata(2), Receipt: digest, PostRootSet: digest}
			if mismatch == "space" {
				tx.Request.Space = "other"
			} else {
				tx.ControlUpdates = encodePublicationMetadata(1)
			}
			if _, err = j.Publish(context.Background(), tx); err != nil {
				t.Fatal(err)
			}
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			appendTail(t, d, []byte("preserve semantic-invalid branch tail"))
			before := treeBytes(t, path)
			opened, report, err := Open(d, storage.JournalOptions{TailPolicy: storage.TailQuarantine})
			if opened != nil || err == nil || report.Verified {
				t.Fatalf("invalid branch replay accepted: %+v %v", report, err)
			}
			if !reflect.DeepEqual(before, treeBytes(t, path)) {
				t.Fatal("semantic-invalid branch replay repaired tail")
			}
		})
	}
}
