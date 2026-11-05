package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"
)

func faultMustCreate(t testing.TB, f *faultIO, name, data string) DurableFile {
	t.Helper()
	file, err := f.CreateExclusive(name)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := file.Write([]byte(data)); err != nil || n != len(data) {
		t.Fatalf("write %q: %d %v", name, n, err)
	}
	return file
}

func faultReadAll(t testing.TB, f *faultIO, name string) string {
	t.Helper()
	file, err := f.OpenRead(name)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	size, err := file.Size()
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, size)
	if n, err := file.ReadAt(data, 0); err != nil || n != len(data) {
		t.Fatalf("read %q: %d %v", name, n, err)
	}
	return string(data)
}

func TestFaultIOBytesAndNamesRequireIndependentSync(t *testing.T) {
	for _, syncFile := range []bool{false, true} {
		for _, syncDir := range []bool{false, true} {
			t.Run(fmt.Sprintf("file=%t/dir=%t", syncFile, syncDir), func(t *testing.T) {
				f := newFaultIO()
				file := faultMustCreate(t, f, "value", "durable payload")
				if syncFile {
					if err := file.Sync(); err != nil {
						t.Fatal(err)
					}
				}
				if syncDir {
					if err := f.SyncDir("."); err != nil {
						t.Fatal(err)
					}
				}
				reboot := f.crash()
				if !syncDir {
					if _, err := reboot.OpenRead("value"); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("unsynced name survived: %v", err)
					}
					return
				}
				want := ""
				if syncFile {
					want = "durable payload"
				}
				if got := faultReadAll(t, reboot, "value"); got != want {
					t.Fatalf("recovered %q, want %q", got, want)
				}
			})
		}
	}
}

func TestFaultIOSyncDirPersistsOnlyDirectNames(t *testing.T) {
	for _, rootSync := range []bool{false, true} {
		for _, childSync := range []bool{false, true} {
			t.Run(fmt.Sprintf("root=%t/child=%t", rootSync, childSync), func(t *testing.T) {
				f := newFaultIO()
				if err := f.Mkdir("d"); err != nil {
					t.Fatal(err)
				}
				file := faultMustCreate(t, f, "d/value", "payload")
				if err := file.Sync(); err != nil {
					t.Fatal(err)
				}
				if childSync {
					if err := f.SyncDir("d"); err != nil {
						t.Fatal(err)
					}
				}
				if rootSync {
					if err := f.SyncDir("."); err != nil {
						t.Fatal(err)
					}
				}
				reboot := f.crash()
				if rootSync && childSync {
					if got := faultReadAll(t, reboot, "d/value"); got != "payload" {
						t.Fatal(got)
					}
				} else if _, err := reboot.OpenRead("d/value"); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("recursive/ancestor sync was implied: %v", err)
				}
				if rootSync && !childSync {
					names, err := reboot.List("d")
					if err != nil || len(names) != 0 {
						t.Fatalf("parent sync did not preserve empty directory: %v %v", names, err)
					}
				}
			})
		}
	}
}

func TestFaultIOReplacementAndRemovalNeedDirectorySync(t *testing.T) {
	for _, syncDir := range []bool{false, true} {
		t.Run(fmt.Sprintf("replacement-synced=%t", syncDir), func(t *testing.T) {
			f := newFaultIO()
			old := faultMustCreate(t, f, "CURRENT", "old")
			if err := old.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := f.SyncDir("."); err != nil {
				t.Fatal(err)
			}
			new := faultMustCreate(t, f, "stage", "new")
			if err := new.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := f.ReplaceAtomic("stage", "CURRENT"); err != nil {
				t.Fatal(err)
			}
			if got := faultReadAll(t, f, "CURRENT"); got != "new" {
				t.Fatal(got)
			}
			if syncDir {
				if err := f.SyncDir("."); err != nil {
					t.Fatal(err)
				}
			}
			reboot := f.crash()
			want := "old"
			if syncDir {
				want = "new"
			}
			if got := faultReadAll(t, reboot, "CURRENT"); got != want {
				t.Fatalf("replacement after crash %q, want %q", got, want)
			}
			if err := reboot.Remove("CURRENT"); err != nil {
				t.Fatal(err)
			}
			reboot = reboot.crash()
			if got := faultReadAll(t, reboot, "CURRENT"); got != want {
				t.Fatalf("unsynced removal lost durable name: %q", got)
			}
			if err := reboot.Remove("CURRENT"); err != nil {
				t.Fatal(err)
			}
			if err := reboot.SyncDir("."); err != nil {
				t.Fatal(err)
			}
			if _, err := reboot.crash().OpenRead("CURRENT"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("synced removal did not survive: %v", err)
			}
		})
	}
}

func TestFaultIOInstallNewDoesNotOverwrite(t *testing.T) {
	f := newFaultIO()
	a := faultMustCreate(t, f, "stage", "new")
	b := faultMustCreate(t, f, "immutable", "old")
	for _, file := range []DurableFile{a, b} {
		if err := file.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.InstallNew("stage", "immutable"); !errors.Is(err, os.ErrExist) {
		t.Fatalf("overwrite accepted: %v", err)
	}
	if faultReadAll(t, f, "stage") != "new" || faultReadAll(t, f, "immutable") != "old" {
		t.Fatal("failed installation changed namespace")
	}
	if err := f.InstallNew("stage", "installed"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.OpenRead("stage"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stage not removed: %v", err)
	}
	if err := f.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	reboot := f.crash()
	if got := faultReadAll(t, reboot, "installed"); got != "new" {
		t.Fatal(got)
	}
}

func TestFaultIOCrashDropsHandlesAndLocks(t *testing.T) {
	f := newFaultIO()
	owner, err := f.AcquireOwnerLock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.AcquireOwnerLock(); !errors.Is(err, errFaultLocked) {
		t.Fatalf("multiple owners: %v", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	owner, err = f.AcquireOwnerLock()
	if err != nil {
		t.Fatal(err)
	}
	file := faultMustCreate(t, f, "value", "synced")
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	reboot := f.crash()
	if _, err := file.Write([]byte("late")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("old handle writable after crash: %v", err)
	}
	if err := file.Sync(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("old handle could promote bytes: %v", err)
	}
	if err := owner.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("old lock usable after crash: %v", err)
	}
	newOwner, err := reboot.AcquireOwnerLock()
	if err != nil {
		t.Fatal(err)
	}
	defer newOwner.Close()
	if got := faultReadAll(t, reboot, "value"); got != "synced" {
		t.Fatal(got)
	}
	appender, err := reboot.OpenAppend("value")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := appender.Write([]byte("-volatile")); err != nil {
		t.Fatal(err)
	}
	if got := faultReadAll(t, reboot.crash(), "value"); got != "synced" {
		t.Fatalf("a subsequent crash promoted volatile bytes: %q", got)
	}
}

func TestFaultIOBeforeAndAfterSyncDirFaults(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprintf("after=%t", after), func(t *testing.T) {
			f := newFaultIO()
			file := faultMustCreate(t, f, "CURRENT", "publication")
			if err := file.Sync(); err != nil {
				t.Fatal(err)
			}
			f.resetFaults()
			f.failAt(1, after)
			if err := f.SyncDir("."); !errors.Is(err, errFaultIO) {
				t.Fatalf("missing injected error: %v", err)
			}
			if len(f.calls) != 1 || f.calls[0] != (faultCall{Step: 1, Op: "SyncDir", Name: "."}) {
				t.Fatalf("bad trace: %+v", f.calls)
			}
			reboot := f.crash()
			if after {
				if got := faultReadAll(t, reboot, "CURRENT"); got != "publication" {
					t.Fatal(got)
				}
			} else if _, err := reboot.OpenRead("CURRENT"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("before fault changed durable names: %v", err)
			}
		})
	}
}

func TestFaultIOPartialZeroAndErrorWrites(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit int
		after bool
		wantN int
		want  string
	}{
		{"zero", 0, false, 0, ""},
		{"short", 2, false, 2, "ab"},
		{"short-error", 2, true, 2, "ab"},
		{"complete-error", -1, true, 4, "abcd"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFaultIO()
			file, err := f.CreateExclusive("value")
			if err != nil {
				t.Fatal(err)
			}
			f.resetFaults()
			f.writeLimit = tc.limit
			if tc.after {
				f.failAt(1, true)
			}
			n, err := file.Write([]byte("abcd"))
			if n != tc.wantN || errors.Is(err, errFaultIO) != tc.after || err != nil && !tc.after {
				t.Fatalf("Write returned %d %v, want n=%d error=%t", n, err, tc.wantN, tc.after)
			}
			if got := faultReadAll(t, f, "value"); got != tc.want {
				t.Fatalf("written %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFaultIOBoundaryHookAndConfinedNames(t *testing.T) {
	f := newFaultIO()
	file := faultMustCreate(t, f, "CURRENT", "new")
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	var phases []bool
	f.onBoundary = func(b faultBoundary) error {
		if b.Op == "SyncDir" && b.Name == "." {
			phases = append(phases, b.After)
			if b.After {
				return errFaultIO
			}
		}
		return nil
	}
	if err := f.SyncDir("."); !errors.Is(err, errFaultIO) || !reflect.DeepEqual(phases, []bool{false, true}) {
		t.Fatalf("hook phases=%v err=%v", phases, err)
	}
	reboot := f.crash()
	if got := faultReadAll(t, reboot, "CURRENT"); got != "new" {
		t.Fatalf("successful durable operation hidden by returned error: %q", got)
	}
	for _, name := range []string{"", "/outside", "../outside", "d/../../outside", "bad\\name", "bad\x00name"} {
		if _, err := reboot.CreateExclusive(name); !errors.Is(err, os.ErrInvalid) {
			t.Fatalf("unconfined name %q: %v", name, err)
		}
	}
	if _, err := reboot.OpenRead("CURRENT"); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}
