//go:build !darwin && !linux

package storage

import (
	"fmt"
	"io/fs"
	"runtime"
)

func openNativeDirectory(string) (*DirectoryIO, error) {
	return nil, fmt.Errorf("directory durability on %s: %w", runtime.GOOS, ErrUnsupported)
}

func trustedDirectory(fs.FileInfo) error { return ErrUnsupported }

func trustedRegularFile(fs.FileInfo) error { return ErrUnsupported }

func acquireNativeOwnerLock(*DirectoryIO) (OwnerLock, error) { return nil, ErrUnsupported }
