package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"
)

type intentReadFaultIO struct {
	DurableIO
	target string
	stage  string
	cause  error
}

func (d intentReadFaultIO) OpenRead(name string) (ReadFile, error) {
	file, err := d.DurableIO.OpenRead(name)
	if err != nil || name != d.target {
		return file, err
	}
	if d.stage == "open" {
		return file, d.cause
	}
	return intentReadFaultFile{ReadFile: file, stage: d.stage, cause: d.cause}, nil
}

type intentReadFaultFile struct {
	ReadFile
	stage string
	cause error
}

func (f intentReadFaultFile) Size() (int64, error) {
	switch f.stage {
	case "size":
		return 0, f.cause
	case "oversized":
		return maxCompactionIntentBytes + 1, nil
	default:
		return f.ReadFile.Size()
	}
}

func (f intentReadFaultFile) ReadAt(p []byte, off int64) (int, error) {
	if f.stage == "read" {
		return 0, f.cause
	}
	return f.ReadFile.ReadAt(p, off)
}

func TestOpenJournalRetainsCompactionIntentCleanup(t *testing.T) {
	for _, stage := range []string{"open", "size", "read", "oversized", "publication-limit"} {
		for _, failClose := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/close-fails=%t", stage, failClose), func(t *testing.T) {
				f, old, acknowledged, _ := faultSeedAcknowledged(t)
				if err := old.Close(); err != nil {
					t.Fatal(err)
				}
				if err := f.Mkdir("compactions"); err != nil && !errors.Is(err, fs.ErrExist) {
					t.Fatal(err)
				}
				prior := intentName(strings.Repeat("1", 32))
				target := intentName(strings.Repeat("2", 32))
				// Interrupted intent writes are retained as evidence. Two files
				// exercise ownership of both earlier and partially read entries.
				for _, name := range []string{prior, target} {
					file := faultMustCreate(t, f, name, "incomplete intent")
					if err := file.Close(); err != nil {
						t.Fatal(err)
					}
				}
				readErr := errors.New("intent read failed")
				wantErr := readErr
				options := JournalOptions{}
				if stage == "oversized" || stage == "publication-limit" {
					wantErr = ErrLimit
				}
				if stage == "publication-limit" {
					options.MaxPublishedBytes = acknowledged.PublishedEnd - 1
				}
				f.resetFaults()
				closeAttempts := 0
				f.onBoundary = func(b faultBoundary) error {
					if b.Op == "Close" && b.Name == target && !b.After {
						closeAttempts++
						if failClose {
							return errFaultIO
						}
					}
					return nil
				}
				d := intentReadFaultIO{DurableIO: f, target: target, stage: stage, cause: readErr}
				j, report, err := OpenJournal(d, options)
				if j != nil || report.Verified || !errors.Is(err, wantErr) || closeAttempts != 1 {
					t.Fatalf("open lost failure or intent cleanup: journal=%v report=%+v attempts=%d err=%v", j, report, closeAttempts, err)
				}
				var closes []string
				for _, call := range f.calls {
					if call.Op == "Close" && strings.HasPrefix(call.Name, "compactions/") {
						closes = append(closes, call.Name)
					}
				}
				if len(closes) != 2 || closes[0] != prior || closes[1] != target {
					t.Fatalf("partial intent list was not retired in order: %v", closes)
				}
				var cleanup *CleanupError
				if failClose {
					if !errors.As(err, &cleanup) || !errors.Is(err, errFaultIO) || !f.locked {
						t.Fatalf("failed close lost handle, cause or owner: %v locked=%t", err, f.locked)
					}
					cleanupAssertNoOwnerClose(t, f.calls)
					if _, lockErr := f.AcquireOwnerLock(); !errors.Is(lockErr, errFaultLocked) {
						t.Fatalf("owner acquired before cleanup: %v", lockErr)
					}
					f.resetFaults()
					if closeErr := cleanup.Close(); closeErr != nil {
						t.Fatal(closeErr)
					}
					cleanupAssertCloseOrder(t, f.calls, target)
					count := len(f.calls)
					if closeErr := cleanup.Close(); closeErr != nil || len(f.calls) != count {
						t.Fatalf("cleanup retry was not idempotent: %v", closeErr)
					}
				} else if errors.As(err, &cleanup) {
					t.Fatalf("successful cleanup retained pending resources: %v", err)
				}
				if f.locked || !errors.Is(err, wantErr) {
					t.Fatalf("cleanup retained owner or lost original diagnosis: locked=%t err=%v", f.locked, err)
				}
				f.resetFaults()
				reopened, recovered, openErr := OpenJournal(f, JournalOptions{})
				if openErr != nil {
					t.Fatalf("recovery after cleanup failed: %v", openErr)
				}
				defer reopened.Close()
				if !recovered.PublicationSynced || recovered.Publication != acknowledged {
					t.Fatalf("cleanup changed acknowledged publication: %+v", recovered)
				}
			})
		}
	}
}
