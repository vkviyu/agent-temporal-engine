package graphstore

import (
	"context"
	"errors"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func gid(ch byte) model.ObjectID { return model.ObjectID(strings.Repeat(string(ch), 64)) }
func openDir(t *testing.T, path string) *storage.DirectoryIO {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip()
	}
	d, err := storage.OpenDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func TestDurableRevisionReopenAndUnknownBoundary(t *testing.T) {
	path := t.TempDir()
	d := openDir(t, path)
	s, err := Create(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r, pub, err := s.AddRevision(context.Background(), "graph-space", "", nil, gid('a'), nil, []graph.Entry{{Key: []byte("k"), Value: gid('1')}})
	if err != nil || pub.Publication.Position.LSN != 1 {
		t.Fatalf("add %+v %v", pub, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openDir(t, path)
	reopened, report, err := Open(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close(); _ = d.Close() }()
	if !report.Verified || report.ScannedTransactions != 1 {
		t.Fatalf("report %+v", report)
	}
	got, err := reopened.Get(r.ID)
	if err != nil || got.ID != r.ID || len(got.Entries) != 1 {
		t.Fatalf("got %+v %v", got, err)
	}
	diff, err := reopened.Diff(r.ID, r.ID)
	if err != nil || len(diff.Changes) != 0 {
		t.Fatalf("diff %+v %v", diff, err)
	}
}

func TestDurableBranchForkAdvanceReopenAndCAS(t *testing.T) {
	path := t.TempDir()
	d := openDir(t, path)
	s, err := Create(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	root, _, err := s.AddRevision(context.Background(), "space", "", nil, gid('a'), nil, []graph.Entry{{Key: []byte("k"), Value: gid('1')}})
	if err != nil {
		t.Fatal(err)
	}
	branch, forkProof, err := s.Fork(context.Background(), "main", root.ID)
	if err != nil || branch.Head != root.ID || branch.Version != 0 || forkProof.Publication.Position.LSN != 2 {
		t.Fatalf("fork=%+v proof=%+v err=%v", branch, forkProof, err)
	}
	// The same command returns the original proof instead of publishing a
	// second BranchRef transaction.
	again, againProof, err := s.Fork(context.Background(), "main", root.ID)
	if err != nil || again != branch || !reflect.DeepEqual(againProof, forkProof) {
		t.Fatalf("fork retry=%+v proof=%+v err=%v", again, againProof, err)
	}
	next, _, err := s.AddRevision(context.Background(), "space", root.ID, nil, gid('b'), nil, []graph.Entry{{Key: []byte("k"), Value: gid('2')}})
	if err != nil {
		t.Fatal(err)
	}
	advanced, advanceProof, err := s.AdvanceBranch(context.Background(), "main", 0, next.ID)
	if err != nil || advanced.Head != next.ID || advanced.Version != 1 || advanceProof.Publication.Position.LSN != 4 {
		t.Fatalf("advance=%+v proof=%+v err=%v", advanced, advanceProof, err)
	}
	if again, proof, err := s.AdvanceBranch(context.Background(), "main", 0, next.ID); err != nil || again != advanced || !reflect.DeepEqual(proof, advanceProof) {
		t.Fatalf("advance retry=%+v proof=%+v err=%v", again, proof, err)
	}
	if _, _, err = s.AdvanceBranch(context.Background(), "main", 0, root.ID); !errors.Is(err, graph.ErrBranchCAS) {
		t.Fatalf("stale CAS accepted: %v", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openDir(t, path)
	reopened, report, err := Open(d, storage.JournalOptions{})
	if err != nil || !report.Verified || report.ScannedTransactions != 4 {
		t.Fatalf("reopen report=%+v err=%v", report, err)
	}
	defer func() { _ = reopened.Close(); _ = d.Close() }()
	got, err := reopened.Branch("main")
	if err != nil || got != advanced {
		t.Fatalf("reopened branch=%+v err=%v", got, err)
	}
	_, proofAfter, err := reopened.AdvanceBranch(context.Background(), "main", 0, root.ID)
	if err == nil || proofAfter.Publication.Position.LSN != 0 {
		t.Fatalf("invalid post-reopen branch advance: proof=%+v err=%v", proofAfter, err)
	}
}

func TestDurableJoinCommitReopen(t *testing.T) {
	path := t.TempDir()
	d := openDir(t, path)
	s, err := Create(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	root, _, err := s.AddRevision(context.Background(), "space", "", nil, gid('a'), nil, []graph.Entry{{Key: []byte("base"), Value: gid('1')}})
	if err != nil {
		t.Fatal(err)
	}
	left, _, err := s.AddRevision(context.Background(), "space", root.ID, nil, gid('b'), nil, []graph.Entry{{Key: []byte("base"), Value: gid('1')}, {Key: []byte("left"), Value: gid('2')}})
	if err != nil {
		t.Fatal(err)
	}
	right, _, err := s.AddRevision(context.Background(), "space", root.ID, nil, gid('c'), nil, []graph.Entry{{Key: []byte("base"), Value: gid('1')}, {Key: []byte("right"), Value: gid('3')}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := s.PlanJoin(left.ID, right.ID)
	if err != nil {
		t.Fatal(err)
	}
	joined, proof, err := s.CommitJoin(context.Background(), plan, gid('d'), nil)
	if err != nil || joined.StateParent != left.ID || len(joined.JoinParents) != 1 || proof.Publication.Position.LSN != 4 {
		t.Fatalf("joined=%+v proof=%+v err=%v", joined, proof, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openDir(t, path)
	reopened, report, err := Open(d, storage.JournalOptions{})
	if err != nil || !report.Verified || report.ScannedTransactions != 4 {
		t.Fatalf("reopen report=%+v err=%v", report, err)
	}
	defer func() { _ = reopened.Close(); _ = d.Close() }()
	got, err := reopened.Get(joined.ID)
	if err != nil || got.ID != joined.ID || len(got.Entries) != 3 {
		t.Fatalf("reopened join=%+v err=%v", got, err)
	}
}
