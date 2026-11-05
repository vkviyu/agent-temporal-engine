package storage

import (
	"context"
	"errors"
	"io/fs"
	"testing"
)

func TestMaintainRequiresOwnerAndOnlyDropsExplicitQuarantine(t *testing.T) {
	path := t.TempDir()
	d, err := OpenDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	j, err := CreateJournal(d, JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Maintain(context.Background(), d, RetentionPolicy{}); !errors.Is(err, ErrMaintenanceBusy) {
		t.Fatalf("maintenance while journal open: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Mkdir("quarantine"); err != nil {
		t.Fatal(err)
	}
	f, err := d.CreateExclusive("quarantine/TAIL.test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("evidence")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.SyncDir("quarantine"); err != nil {
		t.Fatal(err)
	}
	report, err := Maintain(context.Background(), d, RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.QuarantineBefore) != 1 || len(report.RemovedQuarantine) != 0 || report.PublishedSegments != 1 {
		t.Fatalf("inspection report: %+v", report)
	}
	report, err = Maintain(context.Background(), d, RetentionPolicy{DropQuarantine: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.RemovedQuarantine) != 1 {
		t.Fatalf("removal report: %+v", report)
	}
	if _, err := d.OpenRead("quarantine/TAIL.test"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("evidence remains: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
