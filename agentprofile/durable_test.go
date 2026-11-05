package agentprofile

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
	"runtime"
	"testing"
)

func TestDurableLedgerReopen(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip()
	}
	path := t.TempDir()
	d, err := storage.OpenDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	s, err := CreateDurable(d, storage.JournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	task, err := s.CreateTask(context.Background(), CreateTask{Task: Task{ID: "task", Space: "space", Owner: "owner", Goal: "goal", GoalEpoch: 1, Status: TaskOpen}, Request: model.RequestKey{Space: "space", Principal: "owner", RequestID: "create"}})
	if err != nil {
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
	reopened, report, err := OpenDurable(d, storage.JournalOptions{})
	if err != nil || !report.Verified {
		t.Fatalf("%+v %v", report, err)
	}
	defer func() { _ = reopened.Close(); _ = d.Close() }()
	got, err := reopened.ledger.GetTask(task.ID)
	if err != nil || got.Goal != task.Goal {
		t.Fatalf("%+v %v", got, err)
	}
}
