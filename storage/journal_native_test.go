//go:build darwin || linux

package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestJournalNativePublishReopenAndTail(t *testing.T) {
	path := t.TempDir()
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
	if _, err = openJournalWithoutReport(d); !errors.Is(err, ErrOwnerBusy) {
		t.Fatalf("second writer: %v", err)
	}
	initial := faultPublished(t, j)
	if err = initial.Fence().Validate(); err != nil {
		t.Fatal(err)
	}
	first := faultNextTransaction(initial, 1)
	ack, err := j.Publish(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	j, report, err := OpenJournal(d, JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Verified || !report.PublicationSynced || report.ScannedTransactions != 1 || faultPublished(t, j) != ack {
		t.Fatalf("reopen: %+v", report)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	// A complete, synced transaction beyond CURRENT is still unpublished.
	tailTx := faultNextTransaction(ack, 2)
	tail, _, err := EncodeTransaction(tailTx, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	segmentPath := filepath.Join(path, filepath.FromSlash(segmentName(j.segmentID)))
	file, err := os.OpenFile(segmentPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeAll(file, tail); err != nil {
		t.Fatal(err)
	}
	if err = file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	opened, report, err := OpenJournal(d, JournalOptions{})
	if !errors.Is(err, ErrRecoveryRequired) || opened != nil || !report.Verified || report.PublicationSynced || report.Publication != ack || report.TailBytes != uint64(len(tail)) {
		t.Fatalf("tail promoted or hidden: %v %+v", err, report)
	}
	info, err := os.Stat(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(ack.PublishedEnd)+int64(len(tail)) {
		t.Fatal("default recovery truncated tail")
	}
	j, report, err = OpenJournal(d, JournalOptions{TailPolicy: TailQuarantine})
	if err != nil {
		t.Fatal(err)
	}
	if report.TailDisposition != "quarantined" || !report.PublicationSynced || faultPublished(t, j) != ack {
		t.Fatalf("bad recovery: %+v", report)
	}
	kept, err := os.ReadFile(filepath.Join(path, filepath.FromSlash(report.QuarantineFile)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(kept, tail) {
		t.Fatal("tail evidence was not retained exactly")
	}
	second, err := j.Publish(context.Background(), tailTx)
	if err != nil {
		t.Fatal(err)
	}
	if second.Position.LSN != 2 {
		t.Fatal(second)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	j, report, err = OpenJournal(d, JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if faultPublished(t, j) != second || report.ScannedTransactions != 2 {
		t.Fatal("resumed prefix changed")
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := CreateJournal(d, JournalOptions{}); !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("existing store overwritten: %v", err)
	}
}

// The helper keeps the owner-competition assertion readable without exposing a
// second persistent handle when the implementation is working correctly.
func openJournalWithoutReport(d DurableIO) (*Journal, error) {
	j, _, err := OpenJournal(d, JournalOptions{})
	if j != nil {
		_ = j.Close()
	}
	return j, err
}

type crashPointIO struct {
	DurableIO
	point    string
	replaced bool
}

func (d *crashPointIO) ReplaceAtomic(from, to string) error {
	if d.point == "before-current" {
		os.Exit(86)
	}
	if err := d.DurableIO.ReplaceAtomic(from, to); err != nil {
		return err
	}
	d.replaced = true
	return nil
}

func (d *crashPointIO) SyncDir(name string) error {
	if err := d.DurableIO.SyncDir(name); err != nil {
		return err
	}
	if name == "." && d.replaced && d.point == "after-current-sync" {
		os.Exit(86)
	}
	return nil
}

func TestJournalCrashChild(t *testing.T) {
	if os.Getenv("ATE_JOURNAL_CRASH_CHILD") != "1" {
		return
	}
	d, err := OpenDirectory(os.Getenv("ATE_JOURNAL_CRASH_DIR"))
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &crashPointIO{DurableIO: d, point: os.Getenv("ATE_JOURNAL_CRASH_POINT")}
	j, _, err := OpenJournal(wrapped, JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = j.Publish(context.Background(), faultNextTransaction(faultPublished(t, j), 2))
	t.Fatalf("child did not exit at its crash point: %v", err)
}

func TestJournalNativeProcessCrash(t *testing.T) {
	for _, point := range []string{"before-current", "after-current-sync"} {
		t.Run(point, func(t *testing.T) {
			path := t.TempDir()
			d, err := OpenDirectory(path)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			j, err := CreateJournal(d, JournalOptions{})
			if err != nil {
				t.Fatal(err)
			}
			first, err := j.Publish(context.Background(), faultNextTransaction(faultPublished(t, j), 1))
			if err != nil {
				t.Fatal(err)
			}
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestJournalCrashChild$", "-test.count=1")
			cmd.Env = append(os.Environ(), "ATE_JOURNAL_CRASH_CHILD=1", "ATE_JOURNAL_CRASH_DIR="+path, "ATE_JOURNAL_CRASH_POINT="+point)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 86 {
				t.Fatalf("child exit: %v %s", err, output)
			}
			opened, report, err := OpenJournal(d, JournalOptions{})
			if point == "before-current" {
				if !errors.Is(err, ErrRecoveryRequired) || opened != nil || report.Publication != first {
					t.Fatalf("unpublished native tail promoted: %v %+v", err, report)
				}
				opened, report, err = OpenJournal(d, JournalOptions{TailPolicy: TailQuarantine})
			} else if report.Publication.Position.LSN != 2 {
				t.Fatalf("synced CURRENT lost: %v %+v", err, report)
			}
			if err != nil || !report.Verified || !report.PublicationSynced {
				t.Fatalf("native recovery: %v %+v", err, report)
			}
			if err = opened.Close(); err != nil {
				t.Fatal(err)
			}
			if report.Publication.Position.LSN < first.Position.LSN {
				t.Fatal(fmt.Sprintf("ack lost at %s", point))
			}
		})
	}
}
