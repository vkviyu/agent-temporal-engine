package benchmarks_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"testing"
	"time"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// These are native-directory microbenchmarks. They do not substitute an
// in-memory IO implementation for the durable publication path. Data sets and
// concurrency are deliberately small and stated in the benchmark names.
func digest(text string) model.ObjectID {
	return model.ObjectID(fmt.Sprintf("%x", sha256.Sum256([]byte(text))))
}

func benchmarkKey(id string) model.RequestKey {
	return model.RequestKey{Space: "bench-space", Principal: "bench-owner", RequestID: model.RequestID(id)}
}

func put(b testing.TB, store *ate.Store, id, name string, raw []byte) api.CommitResult {
	b.Helper()
	ack, err := store.Mutate(context.Background(), api.MutateRequest{Key: benchmarkKey(id), Batch: api.StateBatch{Operations: []api.StateMutation{{Kind: api.MutationPut, Key: []byte(name), Value: api.Value{Kind: api.ValueBytes, Data: raw}}}}})
	if err != nil || ack.Code != api.CodeDurableAck {
		b.Fatalf("native put: %+v %v", ack, err)
	}
	return ack
}

func seedStore(b *testing.B, count, size int) (string, *ate.Store, ate.Options) {
	b.Helper()
	path := b.TempDir()
	options := ate.Options{Batch: ate.BatchOptions{MaxCount: 1}}
	store, err := ate.Create(path, options)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < count; i++ {
		put(b, store, fmt.Sprintf("seed-%03d", i), fmt.Sprintf("key-%03d", i), bytes.Repeat([]byte{byte(i)}, size))
	}
	return path, store, options
}

func reportLatencies(b *testing.B, samples []time.Duration) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	for _, p := range []int{50, 95, 99} {
		index := (len(samples)*p+99)/100 - 1
		b.ReportMetric(float64(samples[index].Nanoseconds()), fmt.Sprintf("p%d-ns/op", p))
	}
}

func allocatedBytes(b testing.TB, root string) int64 {
	b.Helper()
	var size int64
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		size += info.Size()
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	return size
}

func BenchmarkNativeStore(b *testing.B) {
	b.Run("Commit1KiB_32SeedTx", func(b *testing.B) {
		_, store, _ := seedStore(b, 32, 1024)
		defer store.Close()
		raw := bytes.Repeat([]byte{'v'}, 1024)
		samples := make([]time.Duration, 0, b.N)
		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			start := time.Now()
			put(b, store, fmt.Sprintf("commit-%d", i), "updated-key", raw)
			samples = append(samples, time.Since(start))
		}
		b.StopTimer()
		reportLatencies(b, samples)
	})
	b.Run("Read1KiB_32Tx", func(b *testing.B) {
		_, store, _ := seedStore(b, 32, 1024)
		defer store.Close()
		b.ReportAllocs()
		b.SetBytes(1024)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			value, err := store.GetState(context.Background(), api.ReadStateRequest{Space: "bench-space", Key: []byte("key-016")})
			if err != nil || !value.Found || len(value.Value.Data) != 1024 {
				b.Fatalf("native read: %+v %v", value, err)
			}
		}
	})
	b.Run("Reopen32Tx", func(b *testing.B) {
		path, store, options := seedStore(b, 32, 1024)
		defer func() { _ = store.Close() }()
		samples := make([]time.Duration, 0, b.N)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			if err := store.Close(); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			start := time.Now()
			reopened, report, err := ate.OpenExisting(path, options)
			samples = append(samples, time.Since(start))
			if err != nil || !report.Verified || !report.PublicationSynced {
				b.Fatalf("native reopen: %+v %v", report, err)
			}
			store = reopened
		}
		b.StopTimer()
		reportLatencies(b, samples)
	})
	b.Run("AuditFull32Tx", func(b *testing.B) {
		path, store, _ := seedStore(b, 32, 1024)
		defer store.Close()
		samples := make([]time.Duration, 0, b.N)
		var copied uint64
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			start := time.Now()
			report, err := store.CompactAuditFull(context.Background())
			if err != nil || report.Before.Fence() != report.After.Fence() {
				b.Fatalf("native compaction changed the logical cut: %+v %v", report, err)
			}
			copied = report.CopiedBytes
			if _, pinned, err := store.ReclaimRetired(context.Background()); err != nil || pinned != 0 {
				b.Fatalf("native retirement: pins=%d %v", pinned, err)
			}
			samples = append(samples, time.Since(start))
		}
		b.StopTimer()
		b.ReportMetric(float64(copied), "copied-bytes/op")
		b.ReportMetric(float64(allocatedBytes(b, path)), "final-file-bytes")
		reportLatencies(b, samples)
	})
}

func knowledgeCorpus(b *testing.B, count int) (knowledge.Snapshot, model.KnowledgeKey) {
	b.Helper()
	key := model.KnowledgeKey{Actor: "worker", Execution: "long-task"}
	snapshot := knowledge.Snapshot{}
	for i := 0; i < count; i++ {
		o := knowledge.Observation{Key: key, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "bench-space", Fence: model.CommitFence{Store: "bench-store", LSN: uint64(i + 1), CommitDigest: model.CommitDigest(digest(fmt.Sprintf("fence-%d", i)))}}}, Payload: digest(fmt.Sprintf("payload-%d", i)), Sequence: uint64(i + 1), Label: fmt.Sprintf("evidence-%04d", i)}
		o.ID = knowledge.ObservationID(o)
		snapshot.Observations = append(snapshot.Observations, o)
	}
	if err := snapshot.Validate(); err != nil {
		b.Fatal(err)
	}
	return snapshot, key
}

func BenchmarkKnowledge(b *testing.B) {
	for _, count := range []int{32, 256} {
		b.Run(fmt.Sprintf("EncodeSnapshot%d", count), func(b *testing.B) {
			snapshot, _ := knowledgeCorpus(b, count)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := knowledge.EncodeSnapshot(snapshot); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("DecodeSnapshot%d", count), func(b *testing.B) {
			snapshot, _ := knowledgeCorpus(b, count)
			raw, err := knowledge.EncodeSnapshot(snapshot)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := knowledge.DecodeSnapshot(raw); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("SelectRevision%d", count), func(b *testing.B) {
			snapshot, key := knowledgeCorpus(b, count)
			versions, err := knowledge.Revisions(snapshot, "bench-space", key)
			if err != nil {
				b.Fatal(err)
			}
			selected := versions[len(versions)/2].ID
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := knowledge.SelectRevision(snapshot, "bench-space", key, selected); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkContextSelection(b *testing.B) {
	for _, count := range []int{32, 256} {
		b.Run(fmt.Sprintf("Observations%d", count), func(b *testing.B) {
			snapshot, key := knowledgeCorpus(b, count)
			versions, err := knowledge.Revisions(snapshot, "bench-space", key)
			if err != nil {
				b.Fatal(err)
			}
			frontier, err := knowledge.Frontier(snapshot, key)
			if err != nil {
				b.Fatal(err)
			}
			anchor := snapshot.Observations[len(snapshot.Observations)-1].Source.Anchor.Clone()
			anchor.Knowledge = &key
			anchor.Cursor = &model.Cursor{Store: anchor.Fence.Store, Space: anchor.Space, Revision: versions[len(versions)-1].ID}
			request := knowledge.ContextSelectionRequest{Snapshot: snapshot, Source: knowledge.SourceAnchor{Anchor: anchor, Frontier: frontier}, Capacity: knowledge.ContextCapacity{Window: 128, OutputReserve: 32, FramingReserve: 16}, Mandatory: []model.ObjectID{snapshot.Observations[0].ID, snapshot.Observations[1].ID}, Tokenizer: "synthetic-two-token-segments/v1", CountQuality: knowledge.TokenCountEstimated, PolicyVersion: knowledge.ContextSelectionPolicy}
			for _, o := range snapshot.Observations {
				request.Costs = append(request.Costs, knowledge.ContextCost{Observation: o.ID, Tokens: 2})
			}
			for i := 2; i+2 < len(snapshot.Observations); i += 3 {
				bundle := knowledge.ContextBundle{Conclusion: snapshot.Observations[i].ID, Evidence: []model.ObjectID{snapshot.Observations[i+1].ID}, Counterexamples: []model.ObjectID{snapshot.Observations[i+2].ID}, Benefit: 10}
				bundle.ID = knowledge.ContextBundleID(bundle)
				request.Bundles = append(request.Bundles, bundle)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				selection, err := knowledge.SelectContext(request)
				if err != nil || selection.UsedTokens > selection.InputLimit {
					b.Fatalf("selection: %+v %v", selection, err)
				}
			}
		})
	}
}
