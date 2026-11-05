//go:build darwin || linux

package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

func trustedDirectory(info fs.FileInfo) error {
	if !info.IsDir() || info.Mode()&fs.ModeSymlink != 0 {
		return fmt.Errorf("directory required: %w", fs.ErrInvalid)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("directory must be owned by effective user and not writable by group/others: %w", fs.ErrPermission)
	}
	return nil
}

func trustedRegularFile(info fs.FileInfo) error {
	if !info.Mode().IsRegular() {
		return fmt.Errorf("regular file required: %w", fs.ErrInvalid)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("file must be owned by effective user and not writable by group/others: %w", fs.ErrPermission)
	}
	return nil
}

func openNativeDirectory(path string) (*DirectoryIO, error) {
	// Clean first so a trailing slash or /. cannot cause Lstat to follow a
	// symbolic link in what is logically the final root component.
	path = filepath.Clean(path)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if err := trustedDirectory(info); err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	r, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	openedInfo, err := r.Stat(".")
	if err == nil {
		err = trustedDirectory(openedInfo)
	}
	if err == nil && !os.SameFile(info, openedInfo) {
		err = fmt.Errorf("root changed while opening: %w", fs.ErrPermission)
	}
	if err != nil {
		return nil, errors.Join(err, r.Close())
	}
	return &DirectoryIO{root: r}, nil
}

type nativeOwnerLock struct {
	file *os.File
	once sync.Once
}

func acquireNativeOwnerLock(d *DirectoryIO) (OwnerLock, error) {
	f, err := d.openRegular(ownerLockName, os.O_RDWR|os.O_CREATE, true)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		closeErr := f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			err = ErrOwnerBusy
		}
		return nil, errors.Join(err, closeErr)
	}
	return &nativeOwnerLock{file: f.File}, nil
}

func (l *nativeOwnerLock) Close() (err error) {
	l.once.Do(func() {
		// Always close the descriptor, even if the explicit unlock fails. Close
		// itself also releases flock; keeping a failed-unlock fd would leak it.
		unlockErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
		err = errors.Join(unlockErr, l.file.Close())
	})
	return err
}
