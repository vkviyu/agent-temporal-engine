package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"testing"
)

func TestMaintainRequiresOwnerAndOnlyDropsExplicitQuarantine(t *testing.T) {
	path := t.TempDir()
	d, err := OpenDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	j, err := CreateJournal(d, JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Maintain(context.Background(), d, RetentionPolicy{}); !errors.Is(err, ErrMaintenanceBusy) {
		t.Fatalf("maintenance while journal open: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Mkdir("quarantine"); err != nil {
		t.Fatal(err)
	}
	f, err := d.CreateExclusive("quarantine/TAIL.test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("evidence")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.SyncDir("quarantine"); err != nil {
		t.Fatal(err)
	}
	report, err := Maintain(context.Background(), d, RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.QuarantineBefore) != 1 || len(report.RemovedQuarantine) != 0 || report.PublishedSegments != 1 {
		t.Fatalf("inspection report: %+v", report)
	}
	report, err = Maintain(context.Background(), d, RetentionPolicy{DropQuarantine: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.RemovedQuarantine) != 1 {
		t.Fatalf("removal report: %+v", report)
	}
	if _, err := d.OpenRead("quarantine/TAIL.test"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("evidence remains: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

// List is read-only in faultIO, so maintenance injects listing failures here.
type maintenanceListIO struct {
	DurableIO
	list func(string) ([]string, error)
}

func (d maintenanceListIO) List(name string) ([]string, error) { return d.list(name) }

func TestMaintainOwnerCleanup(t *testing.T) {
	businessErr := errors.New("maintenance operation failed")
	for _, scenario := range []string{
		"inspect", "empty", "missing", "removed",
		"list-segments", "list-manifests", "list-quarantine",
		"malformed", "remove", "sync-quarantine", "sync-root",
		"cancel-before-remove", "cancel-after-remove",
	} {
		for _, failClose := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/close-fails=%t", scenario, failClose), func(t *testing.T) {
				f := newFaultIO()
				j, err := CreateJournal(f, JournalOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if err := j.Close(); err != nil {
					t.Fatal(err)
				}
				selected := []string{"CURRENT", "IDENTITY", manifestName(j.manifest.ManifestID)}
				names := []string{"TAIL.a", "TAIL.b"}
				if scenario == "empty" || scenario == "missing" {
					names = nil
				}
				if scenario != "missing" {
					if err := f.Mkdir("quarantine"); err != nil {
						t.Fatal(err)
					}
				}
				for _, name := range names {
					file := faultMustCreate(t, f, "quarantine/"+name, "evidence")
					if err := file.Close(); err != nil {
						t.Fatal(err)
					}
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				d := maintenanceListIO{DurableIO: f}
				d.list = func(name string) ([]string, error) {
					if scenario == "list-"+name {
						return nil, businessErr
					}
					if name == "quarantine" {
						switch scenario {
						case "malformed":
							return []string{".."}, nil
						case "cancel-before-remove":
							cancel()
						}
					}
					return f.List(name)
				}
				f.resetFaults()
				closeAttempts := 0
				f.onBoundary = func(b faultBoundary) error {
					if b.Op == "OwnerLock.Close" && !b.After {
						closeAttempts++
						if failClose {
							return errFaultIO
						}
					}
					if b.Op == "Remove" && b.Name == "quarantine/TAIL.b" && !b.After && scenario == "remove" {
						return businessErr
					}
					if b.Op == "SyncDir" && !b.After &&
						(scenario == "sync-quarantine" && b.Name == "quarantine" || scenario == "sync-root" && b.Name == ".") {
						return businessErr
					}
					if b.Op == "Remove" && b.After && scenario == "cancel-after-remove" {
						cancel()
					}
					return nil
				}
				want := MaintenanceReport{PublishedSegments: 1, PublishedManifests: 1}
				var wantErr error
				switch scenario {
				case "list-segments":
					want = MaintenanceReport{}
					wantErr = businessErr
				case "list-manifests":
					want.PublishedManifests = 0
					wantErr = businessErr
				case "list-quarantine":
					wantErr = businessErr
				case "empty", "missing":
				case "malformed":
					want.QuarantineBefore = []string{".."}
					wantErr = ErrMalformed
				default:
					want.QuarantineBefore = names
					switch scenario {
					case "inspect":
					case "cancel-before-remove":
						wantErr = context.Canceled
					case "cancel-after-remove":
						want.RemovedQuarantine = names[:1]
						wantErr = context.Canceled
					case "remove":
						want.RemovedQuarantine = names[:1]
						wantErr = businessErr
					default:
						want.RemovedQuarantine = names
						if scenario != "removed" {
							wantErr = businessErr
						}
					}
				}
				switch scenario {
				case "list-segments", "list-manifests", "list-quarantine", "malformed", "cancel-before-remove":
					// Inspection did not complete; no authority classification.
				default:
					want.SelectedMetadata = selected
				}
				report, err := Maintain(ctx, d, RetentionPolicy{DropQuarantine: scenario != "inspect"})
				if !reflect.DeepEqual(report, want) {
					t.Fatalf("partial maintenance report: got=%+v want=%+v", report, want)
				}
				if wantErr != nil && !errors.Is(err, wantErr) {
					t.Fatalf("business error lost: got=%v want=%v", err, wantErr)
				}
				if closeAttempts != 1 {
					t.Fatalf("owner close attempts=%d, want 1", closeAttempts)
				}
				var cleanup *CleanupError
				if failClose {
					if !errors.Is(err, errFaultIO) || !errors.As(err, &cleanup) {
						t.Fatalf("owner close failure lost its cause or retry handle: %v", err)
					}
					maintenanceRetryCleanup(t, f, cleanup)
					if !errors.Is(err, errFaultIO) || wantErr != nil && !errors.Is(err, wantErr) {
						t.Fatalf("cleanup erased original diagnostics: %v", err)
					}
				} else {
					if errors.As(err, &cleanup) || wantErr == nil && err != nil || f.locked {
						t.Fatalf("successful close left error or ownership: err=%v locked=%t", err, f.locked)
					}
					f.resetFaults()
				}
				// Retry the pass with a fresh context after cleanup. Previously
				// removed evidence must not be reported as removed a second time.
				remaining := names[len(want.RemovedQuarantine):]
				again, err := Maintain(context.Background(), f, RetentionPolicy{DropQuarantine: true})
				if err != nil || !reflect.DeepEqual(again.RemovedQuarantine, append([]string(nil), remaining...)) {
					t.Fatalf("maintenance retry: report=%+v err=%v", again, err)
				}
				again, err = Maintain(context.Background(), f, RetentionPolicy{DropQuarantine: true})
				if err != nil || len(again.RemovedQuarantine) != 0 || f.locked {
					t.Fatalf("maintenance was not idempotent: report=%+v err=%v locked=%t", again, err, f.locked)
				}
			})
		}
	}
}

// A failed retry must keep ownership. A completed handle must neither repeat
// cleanup IO nor release a lock acquired by a later owner.
func maintenanceRetryCleanup(t *testing.T, f *faultIO, cleanup *CleanupError) {
	t.Helper()
	if !f.locked {
		t.Fatal("failed close unexpectedly released ownership")
	}
	if _, err := f.AcquireOwnerLock(); !errors.Is(err, errFaultLocked) {
		t.Fatalf("acquired owner before cleanup: %v", err)
	}
	if err := cleanup.Close(); !errors.Is(err, errFaultIO) || !f.locked {
		t.Fatalf("failed retry lost ownership: err=%v locked=%t", err, f.locked)
	}
	f.resetFaults()
	if err := cleanup.Close(); err != nil {
		t.Fatalf("cleanup retry failed: %v", err)
	}
	cleanupAssertCloseOrder(t, f.calls)
	owner, err := f.AcquireOwnerLock()
	if err != nil {
		t.Fatalf("cannot reacquire owner after cleanup: %v", err)
	}
	defer owner.Close()
	calls := len(f.calls)
	if err := cleanup.Close(); err != nil || len(f.calls) != calls || !f.locked {
		t.Fatalf("completed cleanup was not idempotent: err=%v calls=%+v locked=%t", err, f.calls, f.locked)
	}
}

func TestMaintainAcquisitionCleanup(t *testing.T) {
	acquireErr := errors.New("owner acquisition failed")
	for _, acquired := range []bool{false, true} {
		for _, failClose := range []bool{false, true} {
			t.Run(fmt.Sprintf("acquired=%t/close-fails=%t", acquired, failClose), func(t *testing.T) {
				f := newFaultIO()
				f.onBoundary = func(b faultBoundary) error {
					if b.Op == "AcquireOwnerLock" && b.After == acquired {
						return acquireErr
					}
					if b.Op == "OwnerLock.Close" && !b.After && failClose {
						return errFaultIO
					}
					return nil
				}
				report, err := Maintain(context.Background(), f, RetentionPolicy{})
				if !reflect.DeepEqual(report, MaintenanceReport{}) || !errors.Is(err, ErrMaintenanceBusy) || !errors.Is(err, acquireErr) {
					t.Errorf("acquisition error lost: report=%+v err=%v", report, err)
				}
				var cleanup *CleanupError
				if acquired && failClose {
					if !errors.Is(err, errFaultIO) || !errors.As(err, &cleanup) {
						t.Fatalf("acquisition discarded owner cleanup: %v", err)
					}
					// The next acquisition should reach the model's busy check.
					f.onBoundary = func(b faultBoundary) error {
						if b.Op == "OwnerLock.Close" && !b.After {
							return errFaultIO
						}
						return nil
					}
					maintenanceRetryCleanup(t, f, cleanup)
				} else if errors.As(err, &cleanup) || f.locked {
					t.Fatalf("acquisition leaked ownership: err=%v locked=%t", err, f.locked)
				}
			})
		}
	}
}
