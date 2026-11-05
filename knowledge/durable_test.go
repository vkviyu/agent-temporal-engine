package knowledge

import (
	"context"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
	"runtime"
	"strings"
	"testing"
)

func TestDurableKnowledgeReopen(t *testing.T) {
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
	dig := model.ObjectID(strings.Repeat("a", 64))
	source := SourceAnchor{Anchor: model.SnapshotAnchor{Fence: model.CommitFence{Store: "s", LSN: 1, CommitDigest: model.CommitDigest(strings.Repeat("f", 64))}, Space: "space"}}
	consumer := model.CommitFence{Store: "s", LSN: 2, CommitDigest: model.CommitDigest(strings.Repeat("e", 64))}
	o := Observation{Key: model.KnowledgeKey{Actor: "a", Execution: "e"}, Source: source, Payload: dig, Sequence: 1, Label: "x"}
	req := model.RequestKey{Space: "space", Principal: "p", RequestID: "obs"}
	if o, err = s.AppendObservation(context.Background(), req, o, consumer); err != nil {
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
	r, report, err := OpenDurable(d, storage.JournalOptions{})
	if err != nil || !report.Verified {
		t.Fatalf("%+v %v", report, err)
	}
	defer func() { _ = r.Close(); _ = d.Close() }()
	if _, err = r.GetObservation(o.ID); err != nil {
		t.Fatal(err)
	}
}
