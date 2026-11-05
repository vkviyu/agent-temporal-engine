package memory

import (
	"context"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func durableRequest(id string) model.RequestKey {
	return model.RequestKey{Space: "space", Principal: "agent", RequestID: model.RequestID(id)}
}

func TestDurableMemoryReplaysLifecycleAndIdempotency(t *testing.T) {
	dir := t.TempDir()
	d, err := storage.OpenDirectory(dir)
	if err != nil {
		t.Fatal(err)
	}
	durable, err := CreateDurable(d, storage.JournalOptions{})
	if err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	ctx := context.Background()
	request := durableRequest("put")
	candidate, err := durable.PutCandidate(ctx, request, Memory{Space: "space", Kind: KindFact, Claim: []byte("durable fact"), Sources: []Source{{SourceID: digest('e'), Anchor: anchor(1)}}})
	if err != nil {
		t.Fatal(err)
	}
	if again, err := durable.PutCandidate(ctx, request, Memory{Space: "space", Kind: KindFact, Claim: []byte("durable fact"), Sources: []Source{{SourceID: digest('e'), Anchor: anchor(1)}}}); err != nil || again.ID != candidate.ID {
		t.Fatalf("idempotent retry = %+v, %v", again, err)
	}
	validated, err := durable.Validate(ctx, durableRequest("validate"), candidate.ID, Validation{StrategyVersion: 1, Passed: true, Evidence: []Source{{SourceID: digest('e'), Anchor: anchor(1)}}})
	if err != nil {
		t.Fatal(err)
	}
	promoted, err := durable.Promote(ctx, durableRequest("promote"), validated.ID)
	if err != nil || promoted.Status != StatusPromoted {
		t.Fatalf("promote = %+v, %v", promoted, err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d, err = storage.OpenDirectory(dir)
	if err != nil {
		t.Fatal(err)
	}
	reopened, report, err := OpenDurable(d, storage.JournalOptions{})
	if err != nil || !report.Verified {
		_ = d.Close()
		t.Fatalf("reopen report=%+v err=%v", report, err)
	}
	defer func() {
		_ = reopened.Close()
		_ = d.Close()
	}()
	got, err := reopened.Get(promoted.ID)
	if err != nil || got.Status != StatusPromoted || got.ID != promoted.ID {
		t.Fatalf("replayed memory = %+v, %v", got, err)
	}
	results, err := reopened.Search(Query{Space: "space", Text: "durable", At: anchor(2)}, 10)
	if err != nil || len(results) != 1 || results[0].Memory.ID != promoted.ID {
		t.Fatalf("replayed search = %+v, %v", results, err)
	}
}
