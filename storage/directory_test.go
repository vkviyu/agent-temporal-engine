//go:build darwin || linux

package storage

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

func newTestDirectoryIO(t *testing.T) (*DirectoryIO, string) {
	t.Helper()
	name := t.TempDir()
	d, err := OpenDirectory(name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return d, name
}

func writeDirectoryTestFile(t *testing.T, d *DirectoryIO, name, contents string) {
	t.Helper()
	f, err := d.CreateExclusive(name)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := f.Write([]byte(contents))
	if err := errors.Join(writeErr, f.Sync(), f.Close()); err != nil {
		t.Fatal(err)
	}
}

func readDirectoryTestFile(t *testing.T, d *DirectoryIO, name string) string {
	t.Helper()
	f, err := d.OpenRead(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, size)
	if n, err := f.ReadAt(buf, 0); n != len(buf) || err != nil {
		t.Fatalf("read: n=%d err=%v", n, err)
	}
	return string(buf)
}

func TestDirectoryFilesAndSync(t *testing.T) {
	d, root := newTestDirectoryIO(t)
	if d.Profile() != FileSyncExperimental {
		t.Fatal(d.Profile())
	}
	if err := d.Mkdir("segments"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, "segments"))
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("directory permissions: info=%v err=%v", info, err)
	}
	writeDirectoryTestFile(t, d, "segments/a", "first")
	info, err = os.Stat(filepath.Join(root, "segments/a"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("file permissions: info=%v err=%v", info, err)
	}
	if f, err := d.CreateExclusive("segments/a"); !errors.Is(err, fs.ErrExist) || f != nil {
		t.Fatalf("exclusive recreation: file=%v err=%v", f, err)
	}
	f, err := d.OpenAppend("segments/a")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if n, err := f.ReadAt(buf, 0); n != 5 || err != nil || !bytes.Equal(buf, []byte("first")) {
		t.Fatalf("append handle is readable: n=%d data=%q err=%v", n, buf, err)
	}
	if n, err := f.Write([]byte("second")); n != 6 || err != nil {
		t.Fatalf("append: n=%d err=%v", n, err)
	}
	if size, err := f.Size(); size != 11 || err != nil {
		t.Fatalf("size=%d err=%v", size, err)
	}
	if err := errors.Join(f.Sync(), f.Truncate(5), f.Sync(), f.Close()); err != nil {
		t.Fatal(err)
	}
	if got := readDirectoryTestFile(t, d, "segments/a"); got != "first" {
		t.Fatal(got)
	}
	for _, name := range []string{"segments", "."} {
		if err := d.SyncDir(name); err != nil {
			t.Fatalf("sync %s: %v", name, err)
		}
	}
	if names, err := d.List("segments"); err != nil || !reflect.DeepEqual(names, []string{"a"}) {
		t.Fatalf("list=%v err=%v", names, err)
	}
	if err := d.Remove("segments/a"); err != nil {
		t.Fatal(err)
	}
	if f, err := d.OpenRead("segments/a"); !errors.Is(err, fs.ErrNotExist) || f != nil {
		t.Fatalf("removed: file=%v err=%v", f, err)
	}
	if f, err := d.OpenAppend("missing"); !errors.Is(err, fs.ErrNotExist) || f != nil {
		t.Fatalf("append must not create: file=%v err=%v", f, err)
	}
}

func TestDirectoryInstallAndReplace(t *testing.T) {
	d, _ := newTestDirectoryIO(t)
	writeDirectoryTestFile(t, d, "immutable", "old")
	writeDirectoryTestFile(t, d, "staging", "new")
	if err := d.InstallNew("staging", "immutable"); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("overwriting immutable: %v", err)
	}
	if got := readDirectoryTestFile(t, d, "immutable"); got != "old" {
		t.Fatalf("immutable changed: %q", got)
	}
	if got := readDirectoryTestFile(t, d, "staging"); got != "new" {
		t.Fatalf("staging lost: %q", got)
	}
	if err := d.InstallNew("staging", "installed"); err != nil {
		t.Fatal(err)
	}
	if _, err := d.OpenRead("staging"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	if got := readDirectoryTestFile(t, d, "installed"); got != "new" {
		t.Fatal(got)
	}
	writeDirectoryTestFile(t, d, "CURRENT", "before")
	writeDirectoryTestFile(t, d, ".CURRENT.new", "after")
	if err := d.ReplaceAtomic(".CURRENT.new", "CURRENT"); err != nil {
		t.Fatal(err)
	}
	if got := readDirectoryTestFile(t, d, "CURRENT"); got != "after" {
		t.Fatal(got)
	}
	if _, err := d.OpenRead(".CURRENT.new"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	if err := d.ReplaceAtomic("absent", "CURRENT"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	if got := readDirectoryTestFile(t, d, "CURRENT"); got != "after" {
		t.Fatal(got)
	}
	if err := d.ReplaceAtomic("installed", "arbitrary"); err == nil {
		t.Fatal("arbitrary replace target accepted")
	}
	if err := d.InstallNew("installed", "installed"); err == nil {
		t.Fatal("self-install accepted")
	}
	if err := d.Mkdir("other"); err != nil {
		t.Fatal(err)
	}
	if err := d.InstallNew("installed", "other/installed"); err == nil {
		t.Fatal("cross-directory install accepted")
	}
}

func TestDirectoryNamesAndSymlinks(t *testing.T) {
	d, root := newTestDirectoryIO(t)
	writeDirectoryTestFile(t, d, "regular", "content")
	if err := d.Mkdir("dir"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("regular", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("dir", filepath.Join(root, "dirlink")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", ".", "..", "../escape", "/absolute", "dir/../regular", "dir//x", "dir/./x", "regular\x00", "dir\\x", ".owner.lock", "link", "dirlink/x", "escape/x"} {
		t.Run(name, func(t *testing.T) {
			if f, err := d.CreateExclusive(name); err == nil || f != nil {
				t.Fatalf("CreateExclusive(%q): file=%v err=%v", name, f, err)
			}
			if f, err := d.OpenRead(name); err == nil || f != nil {
				t.Fatalf("OpenRead(%q): file=%v err=%v", name, f, err)
			}
			if f, err := d.OpenAppend(name); err == nil || f != nil {
				t.Fatalf("OpenAppend(%q): file=%v err=%v", name, f, err)
			}
			if err := d.Remove(name); err == nil {
				t.Fatalf("Remove(%q) accepted", name)
			}
		})
	}
	for _, name := range []string{"dirlink", "escape", "regular", "../escape"} {
		if _, err := d.List(name); err == nil {
			t.Fatalf("List(%q) accepted", name)
		}
		if err := d.SyncDir(name); err == nil {
			t.Fatalf("SyncDir(%q) accepted", name)
		}
	}
	if err := d.InstallNew("link", "new"); err == nil {
		t.Fatal("symlink source accepted")
	}
	if err := d.InstallNew("regular", "link"); err == nil {
		t.Fatal("symlink destination accepted")
	}
	if err := d.ReplaceAtomic("link", "CURRENT"); err == nil {
		t.Fatal("replace symlink source accepted")
	}
	if err := os.Symlink("regular", filepath.Join(root, "CURRENT")); err != nil {
		t.Fatal(err)
	}
	if err := d.ReplaceAtomic("regular", "CURRENT"); err == nil {
		t.Fatal("replace symlink destination accepted")
	}
	if f, err := d.OpenRead("dir"); err == nil || f != nil {
		t.Fatal("directory opened as file")
	}
}

func TestDirectoryRequiresTrustedDirectory(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "root-link")
	if err := os.Symlink("root", link); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{link, link + "/", link + "/."} {
		if d, err := OpenDirectory(name); err == nil || d != nil {
			t.Fatalf("root symlink accepted: %q", name)
		}
	}
	if err := os.Chmod(root, 0o777); err != nil {
		t.Fatal(err)
	}
	if d, err := OpenDirectory(root); !errors.Is(err, fs.ErrPermission) || d != nil {
		t.Fatalf("untrusted root: d=%v err=%v", d, err)
	}
	d, managed := newTestDirectoryIO(t)
	if err := os.Mkdir(filepath.Join(managed, "untrusted"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(managed, "untrusted"), 0o777); err != nil {
		t.Fatal(err)
	}
	if f, err := d.CreateExclusive("untrusted/file"); !errors.Is(err, fs.ErrPermission) || f != nil {
		t.Fatalf("untrusted parent: file=%v err=%v", f, err)
	}
	if _, err := d.List("untrusted"); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("untrusted list: %v", err)
	}
}

func TestDirectoryOwnerLock(t *testing.T) {
	d, root := newTestDirectoryIO(t)
	other, err := OpenDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	lock, err := d.AcquireOwnerLock()
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	info, err := os.Stat(filepath.Join(root, ownerLockName))
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range []*DirectoryIO{d, other} {
		if got, err := attempt.AcquireOwnerLock(); !errors.Is(err, ErrOwnerBusy) || got != nil {
			t.Fatalf("lock contention: lock=%v err=%v", got, err)
		}
	}
	for _, op := range []func() error{
		func() error { return d.Remove(ownerLockName) },
		func() error { return d.InstallNew(ownerLockName, "other") },
		func() error { return d.InstallNew("other", ownerLockName) },
		func() error { return d.ReplaceAtomic(ownerLockName, "CURRENT") },
	} {
		if err := op(); err == nil {
			t.Fatal("reserved lock name accepted")
		}
	}
	if names, err := d.List("."); err != nil || !reflect.DeepEqual(names, []string{ownerLockName}) {
		t.Fatalf("lock missing from listing: %v err=%v", names, err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lock.Close(); err != nil {
		t.Fatalf("lock close must be idempotent: %v", err)
	}
	lock, err = other.AcquireOwnerLock()
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(filepath.Join(root, ownerLockName))
	if err != nil || !os.SameFile(info, after) {
		t.Fatalf("lock file replaced or removed: %v", err)
	}
}

func TestDirectoryOwnerLockSubprocess(t *testing.T) {
	if root := os.Getenv("ATE_DIRECTORY_LOCK_HELPER"); root != "" {
		d, err := OpenDirectory(root)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		if lock, err := d.AcquireOwnerLock(); !errors.Is(err, ErrOwnerBusy) || lock != nil {
			t.Fatalf("cross-process lock: lock=%v err=%v", lock, err)
		}
		return
	}
	d, root := newTestDirectoryIO(t)
	lock, err := d.AcquireOwnerLock()
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestDirectoryOwnerLockSubprocess$")
	cmd.Env = append(os.Environ(), "ATE_DIRECTORY_LOCK_HELPER="+root)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("helper: %v\n%s", err, output)
	}
}

func TestDirectoryClosedHandles(t *testing.T) {
	root := t.TempDir()
	d, err := OpenDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	f, err := d.CreateExclusive("file")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("file close must be idempotent: %v", err)
	}
	if _, err := f.Size(); !errors.Is(err, fs.ErrClosed) {
		t.Fatal(err)
	}
	if _, err := f.ReadAt(make([]byte, 1), 0); !errors.Is(err, fs.ErrClosed) && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if f, err := d.OpenRead("file"); !errors.Is(err, fs.ErrClosed) || f != nil {
		t.Fatalf("closed directory: file=%v err=%v", f, err)
	}
}
