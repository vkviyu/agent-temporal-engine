//go:build !darwin && !linux

package storage

import (
	"errors"
	"testing"
)

func TestDirectoryUnsupportedPlatform(t *testing.T) {
	if d, err := OpenDirectory(t.TempDir()); d != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("unsupported platform: directory=%v err=%v", d, err)
	}
}
