package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func faultNextTransaction(p Publication, n int) Transaction {
	tx := exampleTransaction()
	tx.TxID = model.TxID(fmt.Sprintf("%032x", n))
	tx.Request.RequestID = model.RequestID(fmt.Sprintf("request-%d", n))
	tx.LSN = p.Position.LSN + 1
	tx.PreviousDigest = p.Position.Digest
	tx.PostRootSet = testDigest(fmt.Sprintf("roots-%d", n))
	return tx
}

func faultPublished(t testing.TB, j *Journal) Publication {
	t.Helper()
	p, err := j.Published()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func faultSeedAcknowledged(t testing.TB) (*faultIO, *Journal, Publication, Transaction) {
	t.Helper()
	f := newFaultIO()
	j, err := CreateJournal(f, JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tx := faultNextTransaction(faultPublished(t, j), 1)
	p, err := j.Publish(context.Background(), tx)
	if err != nil {
		t.Fatal(err)
	}
	f.resetFaults()
	return f, j, p, tx
}

// Tree snapshots intentionally include orphan metadata and the entire live
// segment, so a default-open failure cannot silently erase unpublished bytes.
func faultTreeSnapshot(t testing.TB, f *faultIO) map[string]string {
	t.Helper()
	out := make(map[string]string)
	var walk func(string)
	walk = func(dir string) {
		names, err := f.List(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range names {
			full := name
			if dir != "." {
				full = dir + "/" + name
			}
			if _, err := f.List(full); err == nil {
				out[full+"/"] = "directory"
				walk(full)
			} else {
				out[full] = faultReadAll(t, f, full)
			}
		}
	}
	walk(".")
	return out
}

func faultSegmentBytes(t testing.TB, f *faultIO) (string, []byte) {
	t.Helper()
	names, err := f.List("segments")
	if err != nil {
		t.Fatal(err)
	}
	var segment string
	for _, name := range names {
		if strings.HasPrefix(name, "SEGMENT.") {
			if segment != "" {
				t.Fatal("test expects exactly one active segment")
			}
			segment = "segments/" + name
		}
	}
	if segment == "" {
		t.Fatal("no segment")
	}
	return segment, []byte(faultReadAll(t, f, segment))
}

func faultAssertPublishedPrefix(t testing.TB, f *faultIO, p Publication, want []Transaction) {
	t.Helper()
	identity, err := DecodeIdentity([]byte(faultReadAll(t, f, "IDENTITY")))
	if err != nil {
		t.Fatal(err)
	}
	genesis, err := GenesisPosition(identity)
	if err != nil {
		t.Fatal(err)
	}
	_, data := faultSegmentBytes(t, f)
	if p.PublishedEnd > uint64(len(data)) || p.PublishedEnd < SegmentHeaderSize {
		t.Fatalf("published prefix extends outside durable segment: end=%d size=%d", p.PublishedEnd, len(data))
	}
	scanner, err := NewScanner(bytes.NewReader(data[SegmentHeaderSize:p.PublishedEnd]), genesis, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for i, tx := range want {
		got, err := scanner.Next()
		if err != nil || !reflect.DeepEqual(got.Transaction, tx) {
			t.Fatalf("published transaction %d is not whole: got=%+v error=%v", i, got.Transaction, err)
		}
	}
	if _, err := scanner.Next(); err != io.EOF {
		t.Fatalf("published prefix has unexpected transaction/tail: %v", err)
	}
	if scanner.Position() != p.Position {
		t.Fatalf("scanned prefix position=%+v publication=%+v", scanner.Position(), p.Position)
	}
}

func faultSuccessorPublication(t testing.TB, old Publication, tx Transaction) Publication {
	t.Helper()
	encoded, digest, err := EncodeTransaction(tx, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	want := old
	want.Generation++
	want.Position = Position{LSN: tx.LSN, Digest: digest}
	want.PublishedEnd += uint64(len(encoded))
	want.PostRootSet = tx.PostRootSet
	return want
}

func TestJournalInitializationCrashAtEveryMutation(t *testing.T) {
	baseline := newFaultIO()
	j, err := CreateJournal(baseline, JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trace := append([]faultCall(nil), baseline.calls...)
	if len(trace) == 0 {
		t.Fatal("initialization did not exercise fault model")
	}
	_ = j.Close()
	for _, call := range trace {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%02d-%s/after=%t", call.Step, call.Op, after), func(t *testing.T) {
				f := newFaultIO()
				f.failAt(call.Step, after)
				j, err := CreateJournal(f, JournalOptions{})
				if err == nil || j != nil || !errors.Is(err, errFaultIO) {
					t.Fatalf("initialization fault was hidden: journal=%v error=%v", j, err)
				}
				if len(f.calls) < call.Step || f.calls[call.Step-1].Op != call.Op {
					t.Fatalf("fault trace diverged: expected %+v, got %+v", call, f.calls)
				}
				reboot := f.crash()
				beforeOpen := faultTreeSnapshot(t, reboot)
				_, hasCurrent := beforeOpen["CURRENT"]
				recovered, report, openErr := OpenJournal(reboot, JournalOptions{})
				if hasCurrent {
					if openErr != nil {
						t.Fatalf("durable CURRENT failed recovery: %v", openErr)
					}
					p := faultPublished(t, recovered)
					if !report.Verified || !report.PublicationSynced || p.Position.LSN != 0 || p.Generation != 0 {
						t.Fatalf("incomplete genesis recovery: %+v %+v", p, report)
					}
					faultAssertPublishedPrefix(t, reboot, p, nil)
					_ = recovered.Close()
				} else if recovered != nil || !errors.Is(openErr, ErrIncompleteStore) {
					t.Fatalf("missing CURRENT was initialized or guessed: journal=%v error=%v", recovered, openErr)
				}
				if afterOpen := faultTreeSnapshot(t, reboot); !reflect.DeepEqual(beforeOpen, afterOpen) {
					t.Fatal("open mutated an incomplete/genesis store")
				}
			})
		}
	}
}

func TestJournalPublishCrashAtEveryMutation(t *testing.T) {
	baseline, j, p, _ := faultSeedAcknowledged(t)
	if _, err := j.Publish(context.Background(), faultNextTransaction(p, 2)); err != nil {
		t.Fatal(err)
	}
	trace := append([]faultCall(nil), baseline.calls...)
	_ = j.Close()
	if len(trace) == 0 || trace[len(trace)-1].Op != "SyncDir" || trace[len(trace)-1].Name != "." {
		t.Fatalf("publish lacks a final CURRENT directory sync: %+v", trace)
	}
	for _, call := range trace {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%02d-%s/after=%t", call.Step, call.Op, after), func(t *testing.T) {
				f, publisher, old, first := faultSeedAcknowledged(t)
				second := faultNextTransaction(old, 2)
				newPublication := faultSuccessorPublication(t, old, second)
				f.failAt(call.Step, after)
				if got, err := publisher.Publish(context.Background(), second); !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, errFaultIO) || got != (Publication{}) {
					t.Fatalf("fault did not report unknown: publication=%+v error=%v", got, err)
				}
				if len(f.calls) < call.Step || f.calls[call.Step-1].Op != call.Op {
					t.Fatalf("fault trace diverged: expected %+v, got %+v", call, f.calls)
				}
				if status := publisher.Status(); status.State != JournalFenced || status.LastPublication != old {
					t.Fatalf("unknown publication changed diagnostic old ACK: %+v", status)
				}
				callsAtFence := len(f.calls)
				if _, err := publisher.Published(); !errors.Is(err, ErrStoreFenced) {
					t.Fatalf("fenced Published exposed an active fence: %v", err)
				}
				if _, err := publisher.Publish(context.Background(), second); !errors.Is(err, ErrStoreFenced) {
					t.Fatalf("fenced publisher accepted further work: %v", err)
				}
				if len(f.calls) != callsAtFence {
					t.Fatal("fenced publication attempted additional IO")
				}
				reboot := f.crash()
				beforeOpen := faultTreeSnapshot(t, reboot)
				segmentName, segmentBefore := faultSegmentBytes(t, reboot)
				recovered, report, openErr := OpenJournal(reboot, JournalOptions{})
				if errors.Is(openErr, ErrRecoveryRequired) {
					if recovered != nil || !report.Verified || report.PublicationSynced || report.Publication != old || report.TailBytes == 0 {
						t.Fatalf("tail rejection lost verified old ACK: %+v %v", report, openErr)
					}
					if afterOpen := faultTreeSnapshot(t, reboot); !reflect.DeepEqual(beforeOpen, afterOpen) {
						t.Fatal("default recovery modified unpublished tail or metadata")
					}
					var recoverErr error
					recovered, report, recoverErr = OpenJournal(reboot, JournalOptions{TailPolicy: TailQuarantine})
					if recoverErr != nil || recovered == nil {
						t.Fatalf("explicit quarantine failed: %+v %v", report, recoverErr)
					}
					if report.QuarantineFile == "" {
						t.Fatalf("quarantine did not name preserved evidence: %+v", report)
					}
					if got := []byte(faultReadAll(t, reboot, report.QuarantineFile)); !bytes.Equal(got, segmentBefore[old.PublishedEnd:]) {
						t.Fatal("quarantine did not preserve exact unpublished tail")
					}
					if got := []byte(faultReadAll(t, reboot, segmentName)); !bytes.Equal(got, segmentBefore[:old.PublishedEnd]) {
						t.Fatal("quarantine changed the acknowledged prefix")
					}
					// A second reboot proves recovery persisted the truncation.
					_ = recovered.Close()
					reboot = reboot.crash()
					recovered, report, recoverErr = OpenJournal(reboot, JournalOptions{})
					if recoverErr != nil || report.TailBytes != 0 {
						t.Fatalf("quarantine was not durable: %+v %v", report, recoverErr)
					}
				} else if openErr != nil {
					t.Fatalf("valid crash outcome could not recover: %v", openErr)
				}
				if !report.Verified || !report.PublicationSynced {
					t.Fatalf("recovery returned ready before syncing publication: %+v", report)
				}
				got := faultPublished(t, recovered)
				if got != old && got != newPublication {
					t.Fatalf("recovery returned partial/mixed publication: got=%+v old=%+v new=%+v", got, old, newPublication)
				}
				if call.Step == trace[len(trace)-1].Step && after && got != newPublication {
					t.Fatal("error after final directory sync hid a durable new publication")
				}
				want := []Transaction{first}
				if got == newPublication {
					want = append(want, second)
				}
				faultAssertPublishedPrefix(t, reboot, got, want)
				// Every recovered state must accept a new transaction, and its
				// newly acknowledged publication must survive another crash.
				next := faultNextTransaction(got, 3)
				ack, err := recovered.Publish(context.Background(), next)
				if err != nil {
					t.Fatalf("recovered journal cannot continue: %v", err)
				}
				want = append(want, next)
				reboot = reboot.crash()
				final, finalReport, err := OpenJournal(reboot, JournalOptions{})
				if err != nil || finalReport.Publication != ack {
					t.Fatalf("new ACK lost after crash: report=%+v error=%v", finalReport, err)
				}
				faultAssertPublishedPrefix(t, reboot, faultPublished(t, final), want)
				_ = final.Close()
			})
		}
	}
}

func TestJournalShortZeroAndPartialErrorWrites(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit int
		fail  bool
	}{
		{"short-writes-complete", 7, false},
		{"zero-progress", 0, false},
		{"partial-and-error", 7, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, j, old, first := faultSeedAcknowledged(t)
			second := faultNextTransaction(old, 2)
			f.writeLimit = tc.limit
			if tc.fail {
				f.failAt(1, true)
			}
			ack, err := j.Publish(context.Background(), second)
			want := []Transaction{first}
			if tc.limit > 0 && !tc.fail {
				if err != nil || ack != faultSuccessorPublication(t, old, second) {
					t.Fatalf("short writes not completed: %+v %v", ack, err)
				}
				want = append(want, second)
			} else {
				if !errors.Is(err, ErrCommitUnknown) {
					t.Fatalf("invalid write progress did not report unknown: %v", err)
				}
				if tc.limit == 0 && !errors.Is(err, io.ErrNoProgress) {
					t.Fatalf("zero write failure was hidden: %v", err)
				}
				if _, err := j.Published(); !errors.Is(err, ErrStoreFenced) {
					t.Fatalf("failed append did not fence: %v", err)
				}
				ack = old
			}
			reboot := f.crash()
			recovered, report, err := OpenJournal(reboot, JournalOptions{})
			if err != nil || report.Publication != ack {
				t.Fatalf("write outcome not durable/atomic: %+v %v", report, err)
			}
			faultAssertPublishedPrefix(t, reboot, faultPublished(t, recovered), want)
			_ = recovered.Close()
		})
	}
}
