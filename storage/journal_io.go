package storage

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
)

// writeAll handles short writes, including n>0 with an error. Even an error
// accompanying the final byte is retained: the publication outcome is uncertain.
func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if n < 0 || n > len(b) {
			return fmt.Errorf("%w: invalid write count %d", io.ErrShortWrite, n)
		}
		b = b[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

func writeSyncedFile(d DurableIO, name string, b []byte) (err error) {
	f, err := d.CreateExclusive(name)
	if err != nil {
		return cleanupOnError(err, f)
	}
	defer closeInto(&err, f)
	if err = writeAll(f, b); err != nil {
		return err
	}
	return f.Sync()
}

func readMetadata(d DurableIO, name string) (_ []byte, err error) {
	f, err := d.OpenRead(name)
	if err != nil {
		return nil, cleanupOnError(err, f)
	}
	defer closeInto(&err, f)
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	if size <= 0 {
		return nil, fmt.Errorf("%w: empty metadata", ErrMalformed)
	}
	maxBytes := int64(MaxMetadataBytes)
	if strings.HasPrefix(name, "manifests/") {
		maxBytes = MaxManifestBytes
	}
	if size > maxBytes {
		return nil, ErrLimit
	}
	b := make([]byte, int(size))
	n, err := f.ReadAt(b, 0)
	if n == len(b) && err == io.EOF {
		err = nil
	}
	if n < len(b) && errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		return nil, err
	}
	if n != len(b) {
		return nil, io.ErrUnexpectedEOF
	}
	return b, nil
}

// installPublication is shared by Create and Publish. Its sole authority is a
// synced atomic CURRENT replacement, never the existence of a staged manifest.
func installPublication(d DurableIO, m Manifest, identityDigest Digest) error {
	b, err := EncodeManifest(m)
	if err != nil {
		return err
	}
	tempManifest := "manifests/.STAGE." + m.ManifestID
	if err = writeSyncedFile(d, tempManifest, b); err != nil {
		return fmt.Errorf("manifest write/sync: %w", err)
	}
	if err = d.InstallNew(tempManifest, manifestName(m.ManifestID)); err != nil {
		return fmt.Errorf("manifest install: %w", err)
	}
	if err = d.SyncDir("manifests"); err != nil {
		return fmt.Errorf("manifest directory sync: %w", err)
	}
	c := Current{Store: m.Store, Generation: m.Generation, ManifestID: m.ManifestID, ManifestDigest: Digest(sha256.Sum256(b)), IdentityDigest: identityDigest}
	current, err := EncodeCurrent(c)
	if err != nil {
		return err
	}
	tempCurrent := ".CURRENT." + m.ManifestID
	if err = writeSyncedFile(d, tempCurrent, current); err != nil {
		return fmt.Errorf("CURRENT write/sync: %w", err)
	}
	if err = d.ReplaceAtomic(tempCurrent, "CURRENT"); err != nil {
		return fmt.Errorf("CURRENT replace: %w", err)
	}
	if err = d.SyncDir("."); err != nil {
		return fmt.Errorf("CURRENT directory sync: %w", err)
	}
	return nil
}
