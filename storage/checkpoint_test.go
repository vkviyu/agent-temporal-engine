package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
)

func certifyTestCheckpoint(t testing.TB, j *Journal) Publication {
	t.Helper()
	var count uint64
	p, err := j.CreateRecoveryCheckpoint(context.Background(), 1,
		func(tx ScannedTransaction, _ io.ReaderAt) error {
			count++
			if tx.LSN != count {
				return ErrCorruption
			}
			return nil
		}, func(p Publication, w io.Writer) error {
			if count != p.Position.LSN {
				return ErrCorruption
			}
			_, err := w.Write([]byte("certified-test-projection"))
			return err
		})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func testCheckpointLoader(t testing.TB, suffix *[]uint64) CheckpointLoader {
	t.Helper()
	return func(ref RecoveryCheckpoint, r io.Reader, _ io.ReaderAt) (ReplayFunc, error) {
		if ref.Schema != 1 {
			return nil, ErrUnsupported
		}
		data, err := io.ReadAll(r)
		if err != nil || string(data) != "certified-test-projection" {
			return nil, ErrCorruption
		}
		return func(tx ScannedTransaction, _ io.ReaderAt) error {
			*suffix = append(*suffix, tx.LSN)
			return nil
		}, nil
	}
}

func TestCheckpointCrashAtEveryMutation(t *testing.T) {
	f, j, _ := compactionSeed(t)
	certifyTestCheckpoint(t, j)
	trace := append([]faultCall(nil), f.calls...)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	for _, call := range trace {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%02d-%s-after-%t", call.Step, call.Op, after), func(t *testing.T) {
				f, j, txs := compactionSeed(t)
				before := faultPublished(t, j)
				f.failAt(call.Step, after)
				_, err := j.CreateRecoveryCheckpoint(context.Background(), 1,
					func(ScannedTransaction, io.ReaderAt) error { return nil },
					func(_ Publication, w io.Writer) error {
						_, err := w.Write([]byte("certified-test-projection"))
						return err
					})
				if err == nil {
					t.Fatal("injected failure hidden")
				}
				crashed := f.crash()
				var suffix []uint64
				var replayed uint64
				opened, report, err := OpenJournalWithCheckpoint(crashed, JournalOptions{TailPolicy: TailQuarantine},
					func(ScannedTransaction, io.ReaderAt) error { replayed++; return nil }, testCheckpointLoader(t, &suffix))
				if err != nil {
					t.Fatal(err)
				}
				defer opened.Close()
				if report.Publication.Fence() != before.Fence() || report.Publication.PostRootSet != before.PostRootSet ||
					report.ScannedTransactions != uint64(len(txs)) || replayed+report.CheckpointTransactions != uint64(len(txs)) || len(suffix) != 0 {
					t.Fatalf("lost history or unselected acceleration: %+v replayed=%d suffix=%v", report, replayed, suffix)
				}
				rotationAssertLogicalTransactions(t, opened, txs)
			})
		}
	}
}

func TestCheckpointSuffixAndCompaction(t *testing.T) {
	f, j, txs := compactionSeed(t)
	p := certifyTestCheckpoint(t, j)
	ref := *rotationManifest(t, f).RecoveryCheckpoint
	next := faultNextTransaction(p, 40)
	if _, err := j.Publish(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	if _, err := j.CompactAuditFull(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := rotationManifest(t, f).RecoveryCheckpoint; got == nil || *got != ref {
		t.Fatal("compaction lost selected checkpoint")
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	var suffix []uint64
	opened, report, err := OpenJournalWithCheckpoint(f, JournalOptions{},
		func(ScannedTransaction, io.ReaderAt) error { return errors.New("unexpected full replay") }, testCheckpointLoader(t, &suffix))
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if report.CheckpointTransactions != p.Position.LSN || !reflect.DeepEqual(suffix, []uint64{next.LSN}) {
		t.Fatal(report, suffix)
	}
	rotationAssertLogicalTransactions(t, opened, append(txs, next))
}

func TestCheckpointFallbackReplaysExactHistoricalTransactions(t *testing.T) {
	for _, mode := range []string{"legacy", "disabled", "missing", "unknown-schema"} {
		t.Run(mode, func(t *testing.T) {
			f, j, txs := compactionSeed(t)
			if mode != "legacy" {
				certifyTestCheckpoint(t, j)
			}
			m := rotationManifest(t, f)
			if mode == "missing" {
				if err := f.Remove(checkpointName(m.RecoveryCheckpoint.ID)); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "unknown-schema" {
				m.RecoveryCheckpoint.Schema++
				checkpointReplaceManifest(t, f, m)
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			var suffix []uint64
			load := testCheckpointLoader(t, &suffix)
			if mode == "disabled" {
				load = nil
			}
			var actual []Transaction
			opened, report, err := OpenJournalWithCheckpoint(f, JournalOptions{},
				func(tx ScannedTransaction, _ io.ReaderAt) error { actual = append(actual, tx.Transaction); return nil }, load)
			if err != nil {
				t.Fatal(err)
			}
			defer opened.Close()
			if report.CheckpointTransactions != 0 || len(suffix) != 0 || !reflect.DeepEqual(actual, txs) {
				t.Fatal("fallback changed historical replay", report, suffix)
			}
		})
	}
}

func checkpointReplaceManifest(t testing.TB, f *faultIO, m Manifest) {
	t.Helper()
	b, err := EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	replayReplaceFile(t, f, manifestName(m.ManifestID), b)
	current, err := DecodeCurrent([]byte(faultReadAll(t, f, "CURRENT")))
	if err != nil {
		t.Fatal(err)
	}
	current.ManifestDigest = Digest(sha256.Sum256(b))
	b, err = EncodeCurrent(current)
	if err != nil {
		t.Fatal(err)
	}
	replayReplaceFile(t, f, "CURRENT", b)
}

func TestCheckpointCorruptionFailsBeforeTailRepair(t *testing.T) {
	for _, mode := range []string{"checkpoint-bytes", "source-manifest", "prefix-binding", "historical-bytes", "physical-header", "fence-binding", "root-binding"} {
		t.Run(mode, func(t *testing.T) {
			f, j, _ := compactionSeed(t)
			certifyTestCheckpoint(t, j)
			m := rotationManifest(t, f)
			switch mode {
			case "checkpoint-bytes":
				name := checkpointName(m.RecoveryCheckpoint.ID)
				b := []byte(faultReadAll(t, f, name))
				b[0] ^= 1
				replayReplaceFile(t, f, name, b)
			case "source-manifest":
				name := manifestName(m.RecoveryCheckpoint.Source.ManifestID)
				b := []byte(faultReadAll(t, f, name))
				b[0] ^= 1
				replayReplaceFile(t, f, name, b)
			case "prefix-binding":
				m.RecoveryCheckpoint.PrefixDigest[0] ^= 1
				checkpointReplaceManifest(t, f, m)
			case "historical-bytes", "physical-header":
				name := segmentName(j.segments[0].descriptor.SegmentID)
				b := []byte(faultReadAll(t, f, name))
				if mode == "physical-header" {
					b[0] ^= 1 // outside the logical checkpoint hash
				} else {
					b[SegmentHeaderSize+HeaderSize] ^= 1
				}
				replayReplaceFile(t, f, name, b)
			case "fence-binding", "root-binding":
				// Add a suffix so changing the source tip remains structurally
				// admissible; its manifest binding must still reject it.
				if _, err := j.Publish(context.Background(), faultNextTransaction(faultPublished(t, j), 50)); err != nil {
					t.Fatal(err)
				}
				m = rotationManifest(t, f)
				if mode == "fence-binding" {
					m.RecoveryCheckpoint.Source.Position.Digest[0] ^= 1
				} else {
					m.RecoveryCheckpoint.Source.PostRootSet[0] ^= 1
				}
				checkpointReplaceManifest(t, f, m)
			}
			if err := writeAll(j.segment, []byte("unpublished diagnostic tail")); err != nil {
				t.Fatal(err)
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			before := faultTreeSnapshot(t, f)
			var suffix []uint64
			opened, _, err := OpenJournalWithCheckpoint(f, JournalOptions{TailPolicy: TailQuarantine},
				func(ScannedTransaction, io.ReaderAt) error { return nil }, testCheckpointLoader(t, &suffix))
			if opened != nil || err == nil {
				t.Fatal("corruption accepted")
			}
			if !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
				t.Fatal("failed authentication repaired the tail")
			}
		})
	}
}

func TestCheckpointCertificationRequiresCompleteVerification(t *testing.T) {
	for _, mode := range []string{"semantic", "physical", "cancel", "authority", "identity"} {
		t.Run(mode, func(t *testing.T) {
			f, j, _ := compactionSeed(t)
			defer j.Close()
			replay := func(ScannedTransaction, io.ReaderAt) error { return nil }
			ctx := context.Background()
			switch mode {
			case "semantic":
				replay = func(ScannedTransaction, io.ReaderAt) error { return ErrCorruption }
			case "physical":
				name := segmentName(j.segments[0].descriptor.SegmentID)
				b := []byte(faultReadAll(t, f, name))
				b[SegmentHeaderSize+HeaderSize] ^= 1
				replayReplaceFile(t, f, name, b)
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "authority", "identity":
				name := "CURRENT"
				if mode == "identity" {
					name = "IDENTITY"
				}
				b := []byte(faultReadAll(t, f, name))
				b[0] ^= 1
				replayReplaceFile(t, f, name, b)
			}
			before := faultTreeSnapshot(t, f)
			encoded := false
			_, err := j.CreateRecoveryCheckpoint(ctx, 1, replay, func(Publication, io.Writer) error {
				encoded = true
				return nil
			})
			after := faultTreeSnapshot(t, f)
			if err == nil || encoded || !reflect.DeepEqual(before, after) {
				t.Fatalf("uncertified projection staged: err=%v encoded=%t unchanged=%t", err, encoded, reflect.DeepEqual(before, after))
			}
		})
	}
}

func TestCheckpointFallbackFailureDoesNotRepairTail(t *testing.T) {
	f, j, _ := compactionSeed(t)
	certifyTestCheckpoint(t, j)
	m := rotationManifest(t, f)
	if err := f.Remove(checkpointName(m.RecoveryCheckpoint.ID)); err != nil {
		t.Fatal(err)
	}
	if err := writeAll(j.segment, []byte("unpublished")); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	before := faultTreeSnapshot(t, f)
	var suffix []uint64
	opened, _, err := OpenJournalWithCheckpoint(f, JournalOptions{TailPolicy: TailQuarantine},
		func(ScannedTransaction, io.ReaderAt) error { return ErrCorruption }, testCheckpointLoader(t, &suffix))
	if opened != nil || !errors.Is(err, ErrCorruption) || !reflect.DeepEqual(before, faultTreeSnapshot(t, f)) {
		t.Fatal("fallback repaired before semantic verification", err)
	}
}

func TestCheckpointManifestPreservesLegacyBytes(t *testing.T) {
	for _, golden := range []string{metadataManifestGolden} {
		b := metadataHex(t, golden)
		m, err := DecodeManifest(b)
		if err != nil {
			t.Fatal(err)
		}
		out, err := EncodeManifest(m)
		if err != nil || !bytes.Equal(b, out) {
			t.Fatal("legacy manifest encoding changed")
		}
	}
	f, j, _ := compactionSeed(t)
	defer j.Close()
	// The existing schema2/3 encodings must round-trip unchanged too.
	for i := 0; i < 2; i++ {
		m := rotationManifest(t, f)
		before := []byte(faultReadAll(t, f, manifestName(m.ManifestID)))
		after, err := EncodeManifest(m)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("old segmented/retirement manifest changed")
		}
		if i == 0 {
			if _, err := j.CompactAuditFull(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestCheckpointShortWritesAndUnselectedFiles(t *testing.T) {
	f, j, txs := compactionSeed(t)
	certifyTestCheckpoint(t, j)
	f.writeLimit = 3
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	m := rotationManifest(t, f)
	// Select the exact original authority again. Even a fully valid orphaned
	// checkpoint must not activate the loader when CURRENT does not select it.
	sourceBytes := []byte(faultReadAll(t, f, manifestName(m.RecoveryCheckpoint.Source.ManifestID)))
	source, err := DecodeManifest(sourceBytes)
	if err != nil {
		t.Fatal(err)
	}
	current, err := DecodeCurrent([]byte(faultReadAll(t, f, "CURRENT")))
	if err != nil {
		t.Fatal(err)
	}
	current.ManifestID, current.Generation = source.ManifestID, source.Generation
	current.ManifestDigest = Digest(sha256.Sum256(sourceBytes))
	b, err := EncodeCurrent(current)
	if err != nil {
		t.Fatal(err)
	}
	replayReplaceFile(t, f, "CURRENT", b)
	var count int
	opened, report, err := OpenJournalWithCheckpoint(f, JournalOptions{},
		func(ScannedTransaction, io.ReaderAt) error { count++; return nil },
		func(RecoveryCheckpoint, io.Reader, io.ReaderAt) (ReplayFunc, error) {
			t.Fatal("unselected checkpoint reached loader")
			return nil, ErrCorruption
		})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if report.CheckpointTransactions != 0 || count != len(txs) {
		t.Fatal(report, count)
	}
}

type checkpointCloseIO struct {
	DurableIO
	armed bool
}

func (d *checkpointCloseIO) OpenRead(name string) (ReadFile, error) {
	f, err := d.DurableIO.OpenRead(name)
	if err != nil || !d.armed || !strings.HasPrefix(name, "checkpoints/") {
		return f, err
	}
	d.armed = false
	// Reuse the existing retryable Close model: Open's deferred cleanup
	// retries once, and the returned CleanupError must still retain ownership.
	return &compactionReadCloseFile{ReadFile: f, failures: 3}, nil
}

func TestCheckpointUnsupportedWithCloseFailureRetainsOwnership(t *testing.T) {
	f, j, _ := compactionSeed(t)
	certifyTestCheckpoint(t, j)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	d := &checkpointCloseIO{DurableIO: f, armed: true}
	var count int
	opened, _, err := OpenJournalWithCheckpoint(d, JournalOptions{},
		func(ScannedTransaction, io.ReaderAt) error { count++; return nil },
		func(RecoveryCheckpoint, io.Reader, io.ReaderAt) (ReplayFunc, error) { return nil, ErrUnsupported })
	if opened != nil || !errors.Is(err, errFaultIO) || count != 0 {
		t.Fatal("cleanup failure hidden by fallback", count, err)
	}
	if lock, lockErr := f.AcquireOwnerLock(); lockErr == nil {
		lock.Close()
		t.Fatal("owner released behind checkpoint file")
	}
	var cleanup *CleanupError
	if !errors.As(err, &cleanup) {
		t.Fatal("cleanup handle missing", err)
	}
	// The third failed close still retains the owner; the fourth retires it.
	if err := cleanup.Close(); !errors.Is(err, errFaultIO) {
		t.Fatal("close retry not retained", err)
	}
	if err := cleanup.Close(); err != nil {
		t.Fatal(err)
	}
	lock, err := f.AcquireOwnerLock()
	if err != nil {
		t.Fatal("owner not released after final cleanup", err)
	}
	lock.Close()
}
