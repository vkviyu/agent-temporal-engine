package annotation_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func record(t *testing.T) annotation.Record {
	t.Helper()
	target := model.NodeRef{Kind: model.NodeObject, Object: model.ObjectID(strings.Repeat("a", 64))}
	r, err := annotation.NewRecord(annotation.Record{Target: target, Label: annotation.LabelIrrelevant, Reason: "repeated failed search", Hide: true, Source: model.SnapshotAnchor{Space: "space", Fence: model.CommitFence{Store: "store", LSN: 7, CommitDigest: model.CommitDigest(strings.Repeat("b", 64))}}, Evidence: []model.NodeRef{target}})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// This vector was assembled independently with Python's ordered JSON and SHA256.
func TestAnnotationIndependentCanonicalVector(t *testing.T) {
	r := record(t)
	text, err := os.ReadFile("testdata/record-v1.hex")
	if err != nil {
		t.Fatal(err)
	}
	expected, err := hex.DecodeString(strings.TrimSpace(string(text)))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := annotation.EncodeRecord(r)
	if err != nil || !bytes.Equal(raw, expected) {
		t.Fatalf("canonical mismatch: %x %v", raw, err)
	}
	decoded, err := annotation.DecodeRecord(expected)
	if err != nil || !reflect.DeepEqual(decoded, r) {
		t.Fatalf("decode=%+v %v", decoded, err)
	}
	for _, mutated := range [][]byte{append(bytes.Clone(raw), ' '), bytes.Replace(raw, []byte("irrelevant"), []byte("duplicate"), 1), bytes.Replace(raw, []byte(`"Label":"irrelevant"`), []byte(`"Label":"irrelevant","Label":"irrelevant"`), 1)} {
		if _, err := annotation.DecodeRecord(mutated); err == nil {
			t.Fatal("noncanonical or edited identity accepted")
		}
	}
}

func TestAnnotationRetractionsPreserveIndependentJudgmentsAndOwnedHistory(t *testing.T) {
	first := record(t)
	second, err := annotation.NewRecord(annotation.Record{Target: first.Target, Label: annotation.LabelDuplicate, Reason: "independent host review", Hide: true, Source: first.Source})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := annotation.Apply(annotation.Snapshot{}, first)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = annotation.Apply(snapshot, second)
	if err != nil {
		t.Fatal(err)
	}
	original := snapshot
	retract, err := annotation.NewRecord(annotation.Record{Target: first.Target, Reason: "judgment withdrawn", Source: first.Source, Retracts: first.ID})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = annotation.Apply(snapshot, retract)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Hidden()[first.Target] || len(snapshot.Records) != 3 || len(original.Records) != 2 {
		t.Fatal("retraction erased independent judgment/history")
	}
	retractSecond, err := annotation.NewRecord(annotation.Record{Target: first.Target, Reason: "second judgment withdrawn", Source: first.Source, Retracts: second.ID})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = annotation.Apply(snapshot, retractSecond)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Hidden()[first.Target] {
		t.Fatal("withdrawn judgments still hide target")
	}
	snapshot.Records[0].Evidence[0] = model.NodeRef{}
	if original.Records[0].Evidence[0] != first.Target {
		t.Fatal("apply returned shared evidence storage")
	}
	if _, err := annotation.Apply(original, first); !errors.Is(err, annotation.ErrConflict) {
		t.Fatalf("duplicate record accepted: %v", err)
	}
	if _, err := annotation.Apply(annotation.Snapshot{}, retract); !errors.Is(err, annotation.ErrConflict) {
		t.Fatalf("unknown retraction accepted: %v", err)
	}
	if _, err := annotation.Apply(annotation.Snapshot{Records: []annotation.Record{first, retract}}, retract); !errors.Is(err, annotation.ErrConflict) {
		t.Fatalf("double retraction accepted: %v", err)
	}
}

func TestAnnotationRequiresReasonsAndBounds(t *testing.T) {
	r := record(t)
	for _, change := range []func(*annotation.Record){func(r *annotation.Record) { r.Reason = "" }, func(r *annotation.Record) { r.Label = "unknown" }, func(r *annotation.Record) { r.Evidence = append(r.Evidence, r.Evidence[0]) }, func(r *annotation.Record) { r.Retracts = r.ID }, func(r *annotation.Record) { r.Source.Space = "" }, func(r *annotation.Record) { r.Text = string([]byte{255}) }} {
		candidate := r.Clone()
		change(&candidate)
		if _, err := annotation.NewRecord(candidate); err == nil {
			t.Fatal("malformed judgment accepted")
		}
	}
	r.Text = strings.Repeat("x", 32769)
	if _, err := annotation.NewRecord(r); err == nil {
		t.Fatal("unbounded text accepted")
	}
	if err := (annotation.Snapshot{Records: make([]annotation.Record, annotation.MaxRecords+1)}).Validate(); !errors.Is(err, annotation.ErrLimit) {
		t.Fatal(err)
	}
}
