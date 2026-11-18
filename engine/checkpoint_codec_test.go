package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/formats"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func TestCheckpointGenericProjectionMatchesFullReplay(t *testing.T) {
	d, err := storage.OpenDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	o := Options{EventProfile: true}
	e, err := Create(d, o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if e != nil {
			e.Close()
		}
	}()
	for i := 0; i < 4; i++ {
		_, err := e.Mutate(context.Background(), api.MutateRequest{
			Key:   model.RequestKey{Space: "space", Principal: "owner", RequestID: model.RequestID(string(rune('a' + i)))},
			Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte{byte(i)}, Kind: api.MutationPut, Value: api.Value{Kind: api.ValueNull}}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			if _, err := e.CreateRecoveryCheckpoint(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, report, err := OpenExisting(d, o)
	if err != nil || report.CheckpointTransactions != 2 {
		t.Fatal(report, err)
	}
	var warm bytes.Buffer
	if err := e.encodeRecoveryCheckpoint(&warm); err != nil {
		t.Fatal(err)
	}
	// Compare every recovered generic field, including all query indexes.
	warmEngine := e
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	o.DisableRecoveryCheckpoints = true
	e, report, err = OpenExisting(d, o)
	if err != nil || report.CheckpointTransactions != 0 {
		t.Fatal(report, err)
	}
	var full bytes.Buffer
	if err := e.encodeRecoveryCheckpoint(&full); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(warm.Bytes(), full.Bytes()) ||
		!reflect.DeepEqual(warmEngine.traceEvents, e.traceEvents) ||
		!reflect.DeepEqual(warmEngine.traceEventMeta, e.traceEventMeta) ||
		!reflect.DeepEqual(warmEngine.traceEventKeys, e.traceEventKeys) ||
		!reflect.DeepEqual(warmEngine.traceCausalChildren, e.traceCausalChildren) {
		t.Fatal("warm/suffix and full replay projections differ")
	}
}

func TestCheckpointRecordDecoderBoundsAndCanonicality(t *testing.T) {
	var good bytes.Buffer
	value := checkpointHead{Key: []byte{0, 255, 128, 1}, Head: eventHead{ID: "event", Sequence: 7}}
	if err := writeCheckpointRecord(&good, value); err != nil {
		t.Fatal(err)
	}
	var got checkpointHead
	if err := readCheckpointRecord(bytes.NewReader(good.Bytes()), &got); err != nil || !reflect.DeepEqual(value, got) {
		t.Fatal("binary alias key did not round-trip", err)
	}
	for i := 0; i < good.Len(); i++ {
		var v checkpointHead
		if err := readCheckpointRecord(bytes.NewReader(good.Bytes()[:i]), &v); err == nil {
			t.Fatalf("accepted truncation at %d", i)
		}
	}
	for _, payload := range []string{
		`{"Key":"AP+AAQ==","Head":{"ID":"event","Sequence":7},"Unknown":1}`,
		`{"Key":"AP+AAQ==","Head":{"ID":"event","Sequence":7},"Key":"AP+AAQ=="}`,
		`{ "Key":"AP+AAQ==","Head":{"ID":"event","Sequence":7}}`,
	} {
		var prefix [4]byte
		binary.LittleEndian.PutUint32(prefix[:], uint32(len(payload)))
		input := io.MultiReader(bytes.NewReader(prefix[:]), bytes.NewBufferString(payload))
		if err := readCheckpointRecord(input, &checkpointHead{}); !errors.Is(err, storage.ErrCorruption) {
			t.Fatal("accepted noncanonical record", err)
		}
	}
	var oversized [4]byte
	binary.LittleEndian.PutUint32(oversized[:], checkpointRecordLimit+1)
	if err := readCheckpointRecord(bytes.NewReader(oversized[:]), &got); !errors.Is(err, storage.ErrLimit) {
		t.Fatal("allocated oversized record", err)
	}
}

func TestCheckpointSchemaRegistry(t *testing.T) {
	registry, err := formats.Current()
	if err != nil {
		t.Fatal(err)
	}
	family, found := registry.Family("recovery.checkpoint")
	if !found || len(family.Versions) != 1 || family.Versions[0].Number != recoveryCheckpointSchema {
		t.Fatalf("checkpoint registry differs from decoder epoch: %+v", family)
	}
	for _, purpose := range []formats.Purpose{formats.Decode, formats.Replay, formats.Write} {
		if err := registry.Check(family.ID, recoveryCheckpointSchema, purpose); err != nil {
			t.Fatal(err)
		}
		if err := registry.Check(family.ID, 1, purpose); !errors.Is(err, formats.ErrUnsupported) {
			t.Fatalf("retired checkpoint accepted for %s: %v", purpose, err)
		}
	}
}

func TestCheckpointSchema1RejectionPreservesAuthorityBinding(t *testing.T) {
	p := storage.Publication{
		Store: "store", Generation: 7, PublishedEnd: storage.SegmentHeaderSize,
		Profile: storage.FileSyncExperimental,
	}
	ref := storage.RecoveryCheckpoint{
		Store: p.Store, Schema: 1,
		Source: storage.LayoutSource{
			Generation: p.Generation, Position: p.Position,
			PostRootSet: p.PostRootSet, PublishedEnd: p.PublishedEnd,
		},
	}
	for _, tc := range []struct {
		name   string
		mutate func(*checkpointHeader)
	}{
		{"consistent-schema1", func(h *checkpointHeader) {}},
		{"magic", func(h *checkpointHeader) { h.Magic = "other" }},
		{"schema", func(h *checkpointHeader) { h.Schema = 2 }},
		{"store", func(h *checkpointHeader) { h.Publication.Store = "other" }},
		{"generation", func(h *checkpointHeader) { h.Publication.Generation++ }},
		{"lsn", func(h *checkpointHeader) { h.Publication.Position.LSN++ }},
		{"commit-digest", func(h *checkpointHeader) { h.Publication.Position.Digest[0]++ }},
		{"post-root", func(h *checkpointHeader) { h.Publication.PostRootSet[0]++ }},
		{"published-end", func(h *checkpointHeader) { h.Publication.PublishedEnd++ }},
		{"storage-profile", func(h *checkpointHeader) { h.Publication.Profile = "" }},
		{"history-count", func(h *checkpointHeader) { h.History++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := checkpointHeader{Magic: "ATE-RECOVERY-CHECKPOINT", Schema: 1, Publication: p, History: 1}
			tc.mutate(&h)
			var raw bytes.Buffer
			if err := writeCheckpointRecord(&raw, h); err != nil {
				t.Fatal(err)
			}
			opened, err := decodeRecoveryCheckpoint(Options{}, ref, &raw, nil)
			want := storage.ErrCorruption
			if tc.name == "consistent-schema1" {
				want = storage.ErrUnsupported
			}
			if opened != nil || !errors.Is(err, want) {
				t.Fatalf("schema-1 authority gate: engine=%v error=%v, want %v", opened, err, want)
			}
		})
	}
}
