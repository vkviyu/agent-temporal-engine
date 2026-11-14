//go:build linux

package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// This opt-in test requires a dedicated, disposable small ext4 mount. The
// size/type checks prevent an accidental attempt to fill a development disk.
func TestNativeExt4FullDiskPreservesAcknowledgedPrefix(t *testing.T) {
	parent := os.Getenv("ATE_NATIVE_FULL_DISK_ROOT")
	if parent == "" {
		t.Skip("set ATE_NATIVE_FULL_DISK_ROOT to a disposable ext4 filesystem of at most 64 MiB")
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(parent, &fs); err != nil {
		t.Fatal(err)
	}
	if fs.Type != 0xef53 || fs.Blocks*uint64(fs.Bsize) > 64<<20 {
		t.Fatal("native full-disk test requires a dedicated ext4 mount of at most 64 MiB")
	}
	path, err := os.MkdirTemp(parent, "ate-enospc-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	d, err := OpenDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	j, err := CreateJournal(d, JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j.Close() }()
	first := faultNextTransaction(faultPublished(t, j), 1)
	ack, err := j.Publish(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	fillerPath := filepath.Join(parent, "ate-filler")
	filler, err := os.OpenFile(fillerPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(fillerPath)
	block := bytes.Repeat([]byte{0x5a}, 64<<10)
	for {
		_, err = filler.Write(block)
		if err != nil {
			break
		}
	}
	syncErr := filler.Sync()
	closeErr := filler.Close()
	if !errors.Is(err, syscall.ENOSPC) && !errors.Is(syncErr, syscall.ENOSPC) {
		t.Fatalf("did not reach native ENOSPC: write=%v sync=%v close=%v", err, syncErr, closeErr)
	}
	second := faultNextTransaction(ack, 2)
	second.Records = append(second.Records, Record{Kind: KindObject, Payload: bytes.Repeat([]byte{0x61}, 256<<10)})
	unknown, err := j.Publish(context.Background(), second)
	if !errors.Is(err, ErrCommitUnknown) || unknown != (Publication{}) {
		t.Fatalf("full disk returned an acknowledgement: publication=%+v err=%v", unknown, err)
	}
	if _, err := j.Publish(context.Background(), second); !errors.Is(err, ErrStoreFenced) {
		t.Fatalf("writer remained usable after partial write: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(fillerPath); err != nil {
		t.Fatal(err)
	}
	j, report, err := OpenJournal(d, JournalOptions{TailPolicy: TailQuarantine})
	if err != nil {
		t.Fatal(report, err)
	}
	if !report.Verified || !report.PublicationSynced || report.Publication != ack || report.ScannedTransactions != 1 {
		t.Fatalf("full-disk recovery lost the acknowledged cut: %+v", report)
	}
	resumed, err := j.Publish(context.Background(), second)
	if err != nil || resumed.Position.LSN != 2 {
		t.Fatalf("could not continue after releasing disk space: %+v %v", resumed, err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, report, err = OpenJournal(d, JournalOptions{})
	if err != nil || report.Publication != resumed || report.ScannedTransactions != 2 {
		t.Fatalf("second reopen changed the resumed prefix: %+v %v", report, err)
	}
}
