//go:build darwin || linux

package storage

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestDirectoryRejectsNonregularFile(t *testing.T) {
	d, root := newTestDirectoryIO(t)
	if err := syscall.Mkfifo(filepath.Join(root, "fifo"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Validation must happen before open, because opening a FIFO for reading
	// would otherwise block waiting for a writer.
	if f, err := d.OpenRead("fifo"); err == nil || f != nil {
		t.Fatalf("FIFO read: file=%v err=%v", f, err)
	}
	if f, err := d.OpenAppend("fifo"); err == nil || f != nil {
		t.Fatalf("FIFO append: file=%v err=%v", f, err)
	}
	if err := d.InstallNew("fifo", "installed"); err == nil {
		t.Fatal("FIFO install accepted")
	}
	if err := d.ReplaceAtomic("fifo", "CURRENT"); err == nil {
		t.Fatal("FIFO replacement accepted")
	}
}

func TestDirectoryOwnerLockClosesAfterUnlockError(t *testing.T) {
	// This positive descriptor is beyond Darwin/Linux descriptor limits. An
	// os.File wrapper still tracks its closed state, so the test can verify
	// that Close was attempted despite a real EBADF from the unlock syscall.
	file := os.NewFile(1<<30, "invalid-owner-lock")
	lock := &nativeOwnerLock{file: file}
	unlockErr := lock.Close()
	if unlockErr == nil {
		t.Fatal("invalid descriptor unlock unexpectedly succeeded")
	}
	if _, err := file.Stat(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("descriptor leaked after unlock error: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatalf("subsequent Close must confirm retirement: %v", err)
	}
}

func TestDirectoryFileCloseConfirmsRetirementAfterError(t *testing.T) {
	file := os.NewFile(1<<30, "invalid-directory-file")
	f := &directoryFile{File: file}
	if err := f.Close(); err == nil {
		t.Fatal("invalid descriptor close unexpectedly succeeded")
	}
	if _, err := file.Stat(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("file wrapper was not retired: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("subsequent Close must confirm retirement: %v", err)
	}
}

func TestDirectoryRejectsUntrustedRegularFilePermissions(t *testing.T) {
	for _, mode := range []fs.FileMode{0o620, 0o602, 0o664, 0o666} {
		t.Run(mode.String(), func(t *testing.T) {
			d, root := newTestDirectoryIO(t)
			// A non-writable but searchable directory does not prevent other
			// users from modifying a file that itself grants write permission.
			if err := os.Chmod(root, 0o755); err != nil {
				t.Fatal(err)
			}
			writeDirectoryTestFile(t, d, "CURRENT", "current")
			writeDirectoryTestFile(t, d, ".CURRENT.new", "staged")
			if err := os.Chmod(filepath.Join(root, "CURRENT"), mode); err != nil {
				t.Fatal(err)
			}
			if f, err := d.OpenRead("CURRENT"); f != nil || !errors.Is(err, fs.ErrPermission) {
				t.Fatalf("untrusted file read: file=%v err=%v", f, err)
			}
			if f, err := d.OpenAppend("CURRENT"); f != nil || !errors.Is(err, fs.ErrPermission) {
				t.Fatalf("untrusted file append: file=%v err=%v", f, err)
			}
			for _, op := range []func() error{
				func() error { return d.Remove("CURRENT") },
				func() error { return d.InstallNew("CURRENT", "installed") },
				func() error { return d.InstallNew(".CURRENT.new", "CURRENT") },
				func() error { return d.ReplaceAtomic(".CURRENT.new", "CURRENT") },
			} {
				if err := op(); !errors.Is(err, fs.ErrPermission) {
					t.Fatalf("untrusted transfer/remove: %v", err)
				}
			}
			if err := os.Chmod(filepath.Join(root, "CURRENT"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(filepath.Join(root, ".CURRENT.new"), mode); err != nil {
				t.Fatal(err)
			}
			if err := d.ReplaceAtomic(".CURRENT.new", "CURRENT"); !errors.Is(err, fs.ErrPermission) {
				t.Fatalf("untrusted replace source: %v", err)
			}
			if got := readDirectoryTestFile(t, d, "CURRENT"); got != "current" {
				t.Fatalf("failed replacement changed CURRENT: %q", got)
			}
		})
	}
}

type directoryFileInfoWithOwner struct {
	fs.FileInfo
	stat syscall.Stat_t
}

func (i directoryFileInfoWithOwner) Sys() any { return &i.stat }

func TestDirectoryRegularFileTrustOwnerAndReadPermissions(t *testing.T) {
	d, root := newTestDirectoryIO(t)
	writeDirectoryTestFile(t, d, "readable", "content")
	if err := os.Chmod(filepath.Join(root, "readable"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := readDirectoryTestFile(t, d, "readable"); got != "content" {
		t.Fatal(got)
	}
	f, err := d.OpenAppend("readable")
	if err != nil {
		t.Fatalf("group/other read permissions rejected: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, "readable"))
	if err != nil {
		t.Fatal(err)
	}
	// Verify foreign ownership without requiring privilege to chown a file.
	foreign := directoryFileInfoWithOwner{FileInfo: info, stat: *info.Sys().(*syscall.Stat_t)}
	foreign.stat.Uid ^= 1
	if err := trustedRegularFile(foreign); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("foreign file owner accepted: %v", err)
	}
}

func TestDirectoryRejectsUntrustedOwnerLockFile(t *testing.T) {
	d, root := newTestDirectoryIO(t)
	if err := os.WriteFile(filepath.Join(root, ownerLockName), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(root, ownerLockName), 0o666); err != nil {
		t.Fatal(err)
	}
	if lock, err := d.AcquireOwnerLock(); lock != nil || !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("untrusted owner lock: lock=%v err=%v", lock, err)
	}
}
