package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
)

// ErrOwnerBusy means another handle or process owns this store's writer lock.
var ErrOwnerBusy = errors.New("store owner lock is busy")

const ownerLockName = ".owner.lock"

// DirectoryIO implements DurableIO in a pre-existing private directory. The
// caller owns its lifetime and must close all files and owner locks before it.
// A store must not be modified concurrently by other code running as the same
// OS user. The native owner lock coordinates cooperating writers only.
type DirectoryIO struct {
	root *os.Root
}

var _ DurableIO = (*DirectoryIO)(nil)

// OpenDirectory opens an existing directory without creating it. Darwin and
// Linux are supported. The root and managed subdirectories must be owned by the
// effective user and must not be writable by group or other users, as must all
// managed regular files. Group/other read permissions are allowed. The final
// component of path must not be a symbolic link.
func OpenDirectory(path string) (*DirectoryIO, error) {
	return openNativeDirectory(path)
}

func (d *DirectoryIO) Profile() DurabilityProfile { return FileSyncExperimental }

// Close closes the directory handle. Previously opened files and owner locks
// have independent lifetimes and must have been closed by the caller first.
func (d *DirectoryIO) Close() error {
	if d == nil || d.root == nil {
		return os.ErrClosed
	}
	return d.root.Close()
}

// managedName deliberately accepts only canonical relative names. Rejecting
// dot/parent components makes the confinement contract visible at the API edge;
// os.Root independently enforces confinement during actual filesystem access.
func managedName(name string, allowRoot, allowLock bool) error {
	if !fs.ValidPath(name) || strings.ContainsAny(name, "\\\x00") || (!allowRoot && name == ".") {
		return &os.PathError{Op: "validate", Path: name, Err: fs.ErrInvalid}
	}
	if !allowLock {
		for _, component := range strings.Split(name, "/") {
			if component == ownerLockName {
				return &os.PathError{Op: "validate", Path: name, Err: fs.ErrPermission}
			}
		}
	}
	return nil
}

// inspect rejects symbolic links in every managed component. os.Root guarantees
// confinement even if a same-user, noncooperating modifier races these checks;
// rejection of symlinks themselves relies on the private-directory contract.
func (d *DirectoryIO) inspect(name string, allowRoot, allowLock, allowMissing bool) (fs.FileInfo, error) {
	if err := managedName(name, allowRoot, allowLock); err != nil {
		return nil, err
	}
	if d == nil || d.root == nil {
		return nil, os.ErrClosed
	}
	rootInfo, err := d.root.Lstat(".")
	if err != nil {
		return nil, err
	}
	if err := trustedDirectory(rootInfo); err != nil {
		return nil, fmt.Errorf("store root: %w", err)
	}
	if name == "." {
		return rootInfo, nil
	}
	parts := strings.Split(name, "/")
	for i := range parts {
		prefix := strings.Join(parts[:i+1], "/")
		info, err := d.root.Lstat(prefix)
		last := i == len(parts)-1
		if err != nil {
			if last && allowMissing && errors.Is(err, fs.ErrNotExist) {
				return nil, nil
			}
			return nil, err
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			return nil, &os.PathError{Op: "validate", Path: prefix, Err: fs.ErrPermission}
		}
		if !last || info.IsDir() {
			if err := trustedDirectory(info); err != nil {
				return nil, &os.PathError{Op: "validate", Path: prefix, Err: err}
			}
		}
		if last && info.Mode().IsRegular() {
			if err := trustedRegularFile(info); err != nil {
				return nil, &os.PathError{Op: "validate", Path: prefix, Err: err}
			}
		}
		if last {
			return info, nil
		}
	}
	panic("unreachable managed path")
}

type directoryFile struct {
	*os.File
	closeMu sync.Mutex
	retired bool
}

func (f *directoryFile) Close() error {
	f.closeMu.Lock()
	defer f.closeMu.Unlock()
	if f.retired {
		return nil
	}
	// os.File.Close retires the descriptor even if the OS reports a closing
	// error. A retry confirms retirement without closing a reused descriptor.
	f.retired = true
	return f.File.Close()
}

func (f *directoryFile) Size() (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (d *DirectoryIO) openRegular(name string, flag int, allowLock bool) (*directoryFile, error) {
	creating := flag&os.O_CREATE != 0
	info, err := d.inspect(name, false, allowLock, creating)
	if err != nil {
		return nil, err
	}
	if info != nil && !info.Mode().IsRegular() {
		return nil, &os.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	f, err := d.root.OpenFile(name, flag, 0o600)
	if err != nil {
		return nil, err
	}
	openedInfo, err := f.Stat()
	if err == nil {
		if checkErr := trustedRegularFile(openedInfo); checkErr != nil {
			err = &os.PathError{Op: "open", Path: name, Err: checkErr}
		}
	}
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return &directoryFile{File: f}, nil
}

func (d *DirectoryIO) CreateExclusive(name string) (DurableFile, error) {
	f, err := d.openRegular(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, false)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (d *DirectoryIO) OpenRead(name string) (ReadFile, error) {
	f, err := d.openRegular(name, os.O_RDONLY, false)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (d *DirectoryIO) OpenAppend(name string) (DurableFile, error) {
	f, err := d.openRegular(name, os.O_RDWR|os.O_APPEND, false)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (d *DirectoryIO) Mkdir(name string) error {
	if _, err := d.inspect(name, false, false, true); err != nil {
		return err
	}
	return d.root.Mkdir(name, 0o700)
}

func (d *DirectoryIO) openDirectory(name string) (*os.File, error) {
	info, err := d.inspect(name, true, false, false)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, &os.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	f, err := d.root.Open(name)
	if err != nil {
		return nil, err
	}
	info, err = f.Stat()
	if err == nil {
		err = trustedDirectory(info)
	}
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return f, nil
}

func (d *DirectoryIO) List(name string) ([]string, error) {
	f, err := d.openDirectory(name)
	if err != nil {
		return nil, err
	}
	names, readErr := f.Readdirnames(-1)
	if err = errors.Join(readErr, f.Close()); err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func (d *DirectoryIO) checkTransfer(from, to string) error {
	if from == to || path.Dir(from) != path.Dir(to) {
		return &os.LinkError{Op: "validate", Old: from, New: to, Err: fs.ErrInvalid}
	}
	source, err := d.inspect(from, false, false, false)
	if err != nil {
		return err
	}
	if !source.Mode().IsRegular() {
		return &os.PathError{Op: "validate", Path: from, Err: fs.ErrInvalid}
	}
	target, err := d.inspect(to, false, false, true)
	if err != nil {
		return err
	}
	if target != nil && !target.Mode().IsRegular() {
		return &os.PathError{Op: "validate", Path: to, Err: fs.ErrInvalid}
	}
	return nil
}

func (d *DirectoryIO) InstallNew(from, to string) error {
	if err := d.checkTransfer(from, to); err != nil {
		return err
	}
	// Link is atomic and fails when to already exists. A crash before Remove
	// may leave both names, which is safe for an immutable destination.
	if err := d.root.Link(from, to); err != nil {
		return err
	}
	return d.root.Remove(from)
}

func (d *DirectoryIO) ReplaceAtomic(from, to string) error {
	if to != "CURRENT" {
		return &os.PathError{Op: "replace", Path: to, Err: fs.ErrInvalid}
	}
	if err := d.checkTransfer(from, to); err != nil {
		return err
	}
	return d.root.Rename(from, to)
}

func (d *DirectoryIO) SyncDir(name string) error {
	f, err := d.openDirectory(name)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func (d *DirectoryIO) Remove(name string) error {
	if _, err := d.inspect(name, false, false, false); err != nil {
		return err
	}
	return d.root.Remove(name)
}

func (d *DirectoryIO) AcquireOwnerLock() (OwnerLock, error) {
	return acquireNativeOwnerLock(d)
}
