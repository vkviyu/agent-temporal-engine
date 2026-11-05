package journalstore

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
	"os"
	"runtime"
	"testing"
)

func TestAppendReopenPrivateReplay(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip()
	}
	path := t.TempDir()
	d, err := storage.OpenDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	applied := 0
	key := model.RequestKey{Space: "s", Principal: "p", RequestID: "r"}
	s, err := Create(d, storage.JournalOptions{}, func(r Record) error { applied++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Append(context.Background(), Record{Kind: storage.KindEvent, Payload: []byte("event"), Request: key}); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = storage.OpenDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close(); _ = os.RemoveAll(path) }()
	reopened, report, err := Open(d, storage.JournalOptions{}, func(r Record) error {
		if string(r.Payload) != "event" {
			t.Fatal("payload")
		}
		applied++
		return nil
	})
	if err != nil || !report.Verified || applied != 2 {
		t.Fatalf("report=%+v applied=%d err=%v", report, applied, err)
	}
	if len(reopened.Records()) != 1 {
		t.Fatal("records")
	}
	_ = reopened.Close()
}
