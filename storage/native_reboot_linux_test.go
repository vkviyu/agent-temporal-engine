//go:build linux

package storage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

type nativeRebootWitness struct {
	Schema       int
	BootID       string
	Publication  Publication
	Transactions uint64
}

// Run create and verify in separate Linux boots, keeping both paths on persistent
// ext4 storage. The witness lives outside the store and records a returned ACK.
// The caller controls the restart. This test does not simulate device power loss.
func TestNativeExt4RebootPreservesAcknowledgedPrefix(t *testing.T) {
	phase := os.Getenv("ATE_NATIVE_REBOOT_PHASE")
	if phase == "" {
		t.Skip("set ATE_NATIVE_REBOOT_PHASE=create or verify and persistent root/witness paths")
	}
	if phase != "create" && phase != "verify" {
		t.Fatal("ATE_NATIVE_REBOOT_PHASE must be create or verify")
	}
	root := os.Getenv("ATE_NATIVE_REBOOT_ROOT")
	witnessPath := os.Getenv("ATE_NATIVE_REBOOT_WITNESS")
	if !filepath.IsAbs(root) || !filepath.IsAbs(witnessPath) {
		t.Fatal("ATE_NATIVE_REBOOT_ROOT and ATE_NATIVE_REBOOT_WITNESS must be absolute paths")
	}
	relative, err := filepath.Rel(root, witnessPath)
	if err != nil || (relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator))) {
		t.Fatal("the publication witness must be outside the journal root")
	}
	for _, path := range []string{root, filepath.Dir(witnessPath)} {
		var filesystem syscall.Statfs_t
		if err := syscall.Statfs(path, &filesystem); err != nil {
			t.Fatal(err)
		}
		if filesystem.Type != 0xef53 {
			t.Fatalf("persistent path %q must use ext4", path)
		}
	}
	bootBytes, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	bootID := strings.TrimSpace(string(bootBytes))
	if bootID == "" {
		t.Fatal("empty Linux boot ID")
	}
	d, err := OpenDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if phase == "create" {
		if _, err := os.Stat(witnessPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("create requires an unused witness path: %v", err)
		}
		j, err := CreateJournal(d, JournalOptions{SegmentTargetBytes: MinSegmentTargetBytes})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = j.Close() }()
		ack := faultPublished(t, j)
		const transactions = 8
		for sequence := 1; sequence <= transactions; sequence++ {
			ack, err = j.Publish(context.Background(), faultNextTransaction(ack, sequence))
			if err != nil {
				t.Fatal(err)
			}
		}
		if len(j.segments) < 2 {
			t.Fatal("reboot fixture did not exercise segment rotation")
		}
		witness := nativeRebootWitness{Schema: 1, BootID: bootID, Publication: ack, Transactions: transactions}
		encoded, err := json.Marshal(witness)
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(witnessPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := writeAll(file, encoded); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		if err := errors.Join(file.Sync(), file.Close()); err != nil {
			t.Fatal(err)
		}
		parent, err := os.Open(filepath.Dir(witnessPath))
		if err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
			t.Fatal(err)
		}
		t.Logf("created synced external witness: boot_id=%s lsn=%d segments=%d", bootID, ack.Position.LSN, len(j.segments))
		if err := j.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}
	encoded, err := os.ReadFile(witnessPath)
	if err != nil {
		t.Fatal(err)
	}
	var witness nativeRebootWitness
	if err := json.Unmarshal(encoded, &witness); err != nil {
		t.Fatal(err)
	}
	if witness.Schema != 1 || witness.BootID == "" || witness.Transactions != 8 || witness.Publication.Position.LSN != witness.Transactions {
		t.Fatal("invalid reboot witness")
	}
	if witness.BootID == bootID {
		t.Fatal("verification requires a different Linux boot ID")
	}
	j, report, err := OpenJournal(d, JournalOptions{})
	if err != nil {
		t.Fatal(report, err)
	}
	defer func() { _ = j.Close() }()
	if !report.Verified || !report.PublicationSynced || report.Publication != witness.Publication || report.ScannedTransactions != witness.Transactions {
		t.Fatalf("reboot changed the independently witnessed acknowledged cut: %+v", report)
	}
	if _, err := openJournalWithoutReport(d); !errors.Is(err, ErrOwnerBusy) {
		t.Fatalf("recovered owner lock did not exclude another writer: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("verified witnessed prefix after Linux reboot: old_boot_id=%s new_boot_id=%s lsn=%d transactions=%d", witness.BootID, bootID, witness.Publication.Position.LSN, report.ScannedTransactions)
}
