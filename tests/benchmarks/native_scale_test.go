package benchmarks_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// nativeMeter only observes actual DirectoryIO calls; it never replaces the
// filesystem, suppresses a Sync, or supplies a synthetic publication result.
type nativeMeter struct {
	storage.DurableIO
	fileSync, directorySync, writes, writtenBytes, currents atomic.Uint64
}
type nativeCounts struct {
	FileSync      uint64 `json:"successful_file_sync"`
	DirectorySync uint64 `json:"successful_directory_sync"`
	Writes        uint64 `json:"write_calls"`
	WrittenBytes  uint64 `json:"written_bytes"`
	Current       uint64 `json:"successful_current_replacement"`
}

func (m *nativeMeter) counts() nativeCounts {
	return nativeCounts{m.fileSync.Load(), m.directorySync.Load(), m.writes.Load(), m.writtenBytes.Load(), m.currents.Load()}
}
func (a nativeCounts) minus(b nativeCounts) nativeCounts {
	return nativeCounts{a.FileSync - b.FileSync, a.DirectorySync - b.DirectorySync, a.Writes - b.Writes, a.WrittenBytes - b.WrittenBytes, a.Current - b.Current}
}

type meteredNativeFile struct {
	storage.DurableFile
	meter *nativeMeter
}

func (f *meteredNativeFile) Write(p []byte) (int, error) {
	n, err := f.DurableFile.Write(p)
	f.meter.writes.Add(1)
	if n > 0 {
		f.meter.writtenBytes.Add(uint64(n))
	}
	return n, err
}
func (f *meteredNativeFile) Sync() error {
	err := f.DurableFile.Sync()
	if err == nil {
		f.meter.fileSync.Add(1)
	}
	return err
}
func (m *nativeMeter) CreateExclusive(name string) (storage.DurableFile, error) {
	f, err := m.DurableIO.CreateExclusive(name)
	if f != nil {
		return &meteredNativeFile{f, m}, err
	}
	return nil, err
}
func (m *nativeMeter) OpenAppend(name string) (storage.DurableFile, error) {
	f, err := m.DurableIO.OpenAppend(name)
	if f != nil {
		return &meteredNativeFile{f, m}, err
	}
	return nil, err
}
func (m *nativeMeter) SyncDir(name string) error {
	err := m.DurableIO.SyncDir(name)
	if err == nil {
		m.directorySync.Add(1)
	}
	return err
}
func (m *nativeMeter) ReplaceAtomic(from, to string) error {
	err := m.DurableIO.ReplaceAtomic(from, to)
	if err == nil && to == "CURRENT" {
		m.currents.Add(1)
	}
	return err
}

func openNativeMeter(b testing.TB) (string, *storage.DirectoryIO, *nativeMeter) {
	b.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		b.Skip("native DirectoryIO requires Darwin/Linux")
	}
	path := b.TempDir()
	d, err := storage.OpenDirectory(path)
	if err != nil {
		b.Fatal(err)
	}
	return path, d, &nativeMeter{DurableIO: d}
}
func nativeQuantile(samples []time.Duration, p int) int64 {
	copySamples := append([]time.Duration(nil), samples...)
	sort.Slice(copySamples, func(i, j int) bool { return copySamples[i] < copySamples[j] })
	return copySamples[(len(copySamples)*p+99)/100-1].Nanoseconds()
}
func nativeLog(b *testing.B, value any) {
	b.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		b.Fatal(err)
	}
	b.Log(string(raw))
}

// Run with -benchtime=1x -count=1 for the stated fixed 64-command sample. The
// standard ns/op is per 64-command trial; commit-ns/op is per acknowledged tx.
func BenchmarkNativeScale(b *testing.B) {
	for _, keys := range []int{128, 1024} {
		for _, batch := range []int{1, 16} {
			b.Run(fmt.Sprintf("EngineKeys%dBatch%dWriters8", keys, batch), func(b *testing.B) { benchmarkNativeEngineScale(b, keys, batch) })
		}
	}
	for _, hold := range []time.Duration{0, 10 * time.Millisecond} {
		b.Run(fmt.Sprintf("PhysicalJournalPinHold%dms", hold.Milliseconds()), func(b *testing.B) { benchmarkNativePin(b, hold) })
	}
}
func benchmarkNativeEngineScale(b *testing.B, keys, batch int) {
	const commands, writers, valueBytes = 64, 8, 1024
	ctx := context.Background()
	var allLatencies []time.Duration
	var elapsedTotal, reopenTotal, compactTotal time.Duration
	var totals nativeCounts
	b.ReportAllocs()
	b.StopTimer()
	for trial := 0; trial < b.N; trial++ {
		path, d, meter := openNativeMeter(b)
		options := engine.Options{Batch: engine.BatchOptions{MaxCount: batch, MaxDelay: time.Millisecond}, Journal: storage.JournalOptions{SegmentTargetBytes: 8 << 20}}
		core, err := engine.Create(meter, options)
		if err != nil {
			b.Fatal(err)
		}
		mutations := make([]api.StateMutation, keys)
		for i := range mutations {
			mutations[i] = api.StateMutation{Kind: api.MutationPut, Key: []byte(fmt.Sprintf("key-%04d", i)), Value: api.Value{Kind: api.ValueBytes, Data: bytes.Repeat([]byte{byte(i % 251)}, valueBytes)}}
		}
		seeded, err := core.Mutate(ctx, api.MutateRequest{Key: benchmarkKey("seed"), Batch: api.StateBatch{Operations: mutations}})
		if err != nil || seeded.Code != api.CodeDurableAck {
			b.Fatal(seeded, err)
		}
		before := meter.counts()
		var sequence atomic.Int64
		startGate := make(chan struct{})
		latencies := make([]time.Duration, commands)
		acks := make([]api.CommitResult, commands)
		errs := make([]error, commands)
		var wg sync.WaitGroup
		b.StartTimer()
		start := time.Now()
		for worker := 0; worker < writers; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-startGate
				for {
					index := int(sequence.Add(1) - 1)
					if index >= commands {
						return
					}
					raw := bytes.Repeat([]byte{byte(index + 1)}, valueBytes)
					began := time.Now()
					acks[index], errs[index] = core.Mutate(ctx, api.MutateRequest{Key: benchmarkKey(fmt.Sprintf("update-%02d", index)), Batch: api.StateBatch{Operations: []api.StateMutation{{Kind: api.MutationPut, Key: []byte(fmt.Sprintf("key-%04d", index%keys)), Value: api.Value{Kind: api.ValueBytes, Data: raw}}}}})
					latencies[index] = time.Since(began)
				}
			}()
		}
		close(startGate)
		wg.Wait()
		elapsed := time.Since(start)
		b.StopTimer()
		for i, err := range errs {
			if err != nil || acks[i].Code != api.CodeDurableAck {
				b.Fatalf("update %d: %+v %v", i, acks[i], err)
			}
		}
		commitCounts := meter.counts().minus(before)
		if commitCounts.Current == 0 || commitCounts.FileSync == 0 || commitCounts.WrittenBytes == 0 {
			b.Fatal("native accounting saw no physical publication")
		}
		snapshot, err := core.Snapshot(ctx)
		if err != nil || snapshot.Fence.LSN != uint64(commands+1) {
			b.Fatal(snapshot, err)
		}
		if err = core.Close(); err != nil {
			b.Fatal(err)
		}
		reopenBefore := meter.counts()
		began := time.Now()
		core, recovery, err := engine.OpenExisting(meter, options)
		reopen := time.Since(began)
		if err != nil || !recovery.Verified || !recovery.PublicationSynced || recovery.ScannedTransactions != uint64(commands+1) {
			b.Fatalf("reopen: %+v %v", recovery, err)
		}
		reopened, err := core.Snapshot(ctx)
		if err != nil || reopened != snapshot {
			b.Fatal("reopen changed roots", err)
		}
		lookup, err := core.LookupRequest(ctx, api.LookupRequestRequest{Space: "bench-space", Principal: "bench-owner", RequestID: "update-00"})
		if err != nil || lookup.Result != acks[0] {
			b.Fatal("reopen changed receipt", err)
		}
		reopenCounts := meter.counts().minus(reopenBefore)
		compactBefore := meter.counts()
		began = time.Now()
		compaction, err := core.CompactAuditFull(ctx)
		compact := time.Since(began)
		if err != nil || compaction.Before.Fence() != snapshot.Fence || compaction.After.Fence() != snapshot.Fence {
			b.Fatalf("compaction %+v %v", compaction, err)
		}
		compactCounts := meter.counts().minus(compactBefore)
		after, err := core.Snapshot(ctx)
		if err != nil || after != snapshot {
			b.Fatal("compaction changed logical roots", err)
		}
		if err = core.Close(); err != nil {
			b.Fatal(err)
		}
		if err = d.Close(); err != nil {
			b.Fatal(err)
		}
		nativeLog(b, struct {
			Kind, OS, Arch, Go                                string
			Trial, Keys, Batch, Writers, Commands, ValueBytes int
			ElapsedNS, P50NS, P95NS, ReopenNS, CompactionNS   int64
			CommitIO, ReopenIO, CompactionIO                  nativeCounts
			CopiedBytes                                       uint64
			FileBytes                                         int64
		}{"native_engine", runtime.GOOS, runtime.GOARCH, runtime.Version(), trial, keys, batch, writers, commands, valueBytes, elapsed.Nanoseconds(), nativeQuantile(latencies, 50), nativeQuantile(latencies, 95), reopen.Nanoseconds(), compact.Nanoseconds(), commitCounts, reopenCounts, compactCounts, compaction.CopiedBytes, allocatedBytes(b, path)})
		elapsedTotal += elapsed
		reopenTotal += reopen
		compactTotal += compact
		allLatencies = append(allLatencies, latencies...)
		totals.FileSync += commitCounts.FileSync
		totals.DirectorySync += commitCounts.DirectorySync
		totals.Writes += commitCounts.Writes
		totals.WrittenBytes += commitCounts.WrittenBytes
		totals.Current += commitCounts.Current
	}
	n := float64(b.N * commands)
	b.ReportMetric(float64(elapsedTotal.Nanoseconds())/n, "commit-ns/op")
	b.ReportMetric(n/elapsedTotal.Seconds(), "ack/s")
	b.ReportMetric(float64(nativeQuantile(allLatencies, 50)), "p50-ns/ack")
	b.ReportMetric(float64(nativeQuantile(allLatencies, 95)), "p95-ns/ack")
	b.ReportMetric(float64(totals.FileSync)/n, "file-sync/ack")
	b.ReportMetric(float64(totals.DirectorySync)/n, "dir-sync/ack")
	b.ReportMetric(float64(totals.WrittenBytes)/n, "written-bytes/ack")
	b.ReportMetric(n/float64(totals.Current), "tx/current")
	b.ReportMetric(float64(reopenTotal.Nanoseconds())/float64(b.N), "reopen-ns/trial")
	b.ReportMetric(float64(compactTotal.Nanoseconds())/float64(b.N), "compact-ns/trial")
}

// The physical pin experiment intentionally uses Journal's public PinReader.
// It verifies retention/readability, not domain replay or engine View leases.
func benchmarkNativePin(b *testing.B, hold time.Duration) {
	ctx := context.Background()
	b.ReportAllocs()
	b.StopTimer()
	var pinTime, compactTime, reclaimTime time.Duration
	for trial := 0; trial < b.N; trial++ {
		_, d, meter := openNativeMeter(b)
		j, err := storage.CreateJournal(meter, storage.JournalOptions{SegmentTargetBytes: 1024})
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < 16; i++ {
			old, err := j.Published()
			if err != nil {
				b.Fatal(err)
			}
			sum := storage.Digest(sha256.Sum256([]byte(fmt.Sprintf("physical-%d", i))))
			tx := storage.Transaction{TxID: model.TxID(fmt.Sprintf("%032x", i+1)), Request: benchmarkKey(fmt.Sprintf("physical-%d", i)), RequestDigest: sum, LSN: old.Position.LSN + 1, PreviousDigest: old.Position.Digest, Records: []storage.Record{{Kind: storage.KindObject, Payload: bytes.Repeat([]byte{byte(i)}, 1024)}}, Receipt: sum, PostRootSet: sum}
			if _, err = j.Publish(ctx, tx); err != nil {
				b.Fatal(err)
			}
		}
		before := meter.counts()
		b.StartTimer()
		began := time.Now()
		pin, err := j.PinReader()
		acquire := time.Since(began)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		raw := make([]byte, 512)
		if _, err = pin.ReadAt(raw, storage.SegmentHeaderSize); err != nil {
			b.Fatal(err)
		}
		began = time.Now()
		report, err := j.CompactAuditFull(ctx)
		compact := time.Since(began)
		if err != nil || report.PinnedSegments == 0 || len(report.ReclaimedSegments) != 0 {
			b.Fatalf("pin did not retain source generation %+v %v", report, err)
		}
		still := make([]byte, len(raw))
		if _, err = pin.ReadAt(still, storage.SegmentHeaderSize); err != nil || !bytes.Equal(still, raw) {
			b.Fatal("pin bytes changed", err)
		}
		if _, pending, err := j.ReclaimRetired(ctx); err != nil || pending == 0 {
			b.Fatalf("premature reclaim pins=%d %v", pending, err)
		}
		time.Sleep(hold)
		began = time.Now()
		if err = pin.Close(); err != nil {
			b.Fatal(err)
		}
		removed, pending, err := j.ReclaimRetired(ctx)
		reclaim := time.Since(began)
		if err != nil || pending != 0 || len(removed) != report.PinnedSegments {
			b.Fatalf("source retirement incomplete removed=%d pinned=%d %v", len(removed), pending, err)
		}
		nativeLog(b, struct {
			Kind, OS, Arch, Go                                            string
			Trial                                                         int
			RequestedHoldNS, PinAcquireNS, CompactionNS, ReleaseReclaimNS int64
			RetiredSegments                                               int
			CopiedBytes                                                   uint64
			IO                                                            nativeCounts
		}{"physical_journal_pin", runtime.GOOS, runtime.GOARCH, runtime.Version(), trial, hold.Nanoseconds(), acquire.Nanoseconds(), compact.Nanoseconds(), reclaim.Nanoseconds(), report.PinnedSegments, report.CopiedBytes, meter.counts().minus(before)})
		if err = j.Close(); err != nil {
			b.Fatal(err)
		}
		if err = d.Close(); err != nil {
			b.Fatal(err)
		}
		pinTime += acquire
		compactTime += compact
		reclaimTime += reclaim
	}
	b.ReportMetric(float64(pinTime.Nanoseconds())/float64(b.N), "pin-acquire-ns/trial")
	b.ReportMetric(float64(compactTime.Nanoseconds())/float64(b.N), "compact-ns/trial")
	b.ReportMetric(float64(reclaimTime.Nanoseconds())/float64(b.N), "release-reclaim-ns/trial")
}
