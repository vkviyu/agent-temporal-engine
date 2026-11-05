package storage

import "io"

// DurabilityProfile identifies the synchronization contract, not a certification
// of a particular filesystem/device. FileSync uses OS file and directory sync.
type DurabilityProfile string

const FileSyncExperimental DurabilityProfile = "file-sync-experimental-1"

type ReadFile interface {
	io.ReaderAt
	Size() (int64, error)
	// Close is idempotent after retirement. An error may mean the handle is
	// still live; the caller retains it and retries until Close returns nil.
	Close() error
}

type DurableFile interface {
	ReadFile
	io.Writer
	Truncate(int64) error
	Sync() error
}

// OwnerLock follows the same retry/retirement rule as ReadFile.Close.
type OwnerLock interface{ Close() error }

// DurableIO is rooted at an existing store directory. All names are relative;
// "." means the root. Implementations must confine names and reject symlinks
// for managed files/directories. The caller owns the IO lifetime; Journal owns
// its file handles and exclusive owner lock.
// Acquiring methods may return a non-nil handle with an error; callers must
// close that handle too. An implementation must either retire an acquired
// resource on failure or return its handle, never silently lose ownership.
//
// InstallNew atomically installs a new immutable name without overwriting an
// existing destination, then removes the staging name. ReplaceAtomic replaces
// CURRENT without a missing-name interval. Neither operation implies SyncDir.
type DurableIO interface {
	Profile() DurabilityProfile
	CreateExclusive(name string) (DurableFile, error)
	OpenRead(name string) (ReadFile, error)
	OpenAppend(name string) (DurableFile, error)
	Mkdir(name string) error
	List(name string) ([]string, error)
	InstallNew(from, to string) error
	ReplaceAtomic(from, to string) error
	SyncDir(name string) error
	Remove(name string) error
	AcquireOwnerLock() (OwnerLock, error)
}
