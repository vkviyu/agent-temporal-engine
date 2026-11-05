package state

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestEntriesPreservesAbsolutePrefixesAndOwnsOutput(t *testing.T) {
	reader := newMemoryReader()
	keys := [][]byte{nil, []byte("ax"), []byte("ay"), []byte("bx"), []byte("by"), {0, 255}, {255, 0}}
	mutations := make([]Mutation, len(keys))
	for i, k := range keys {
		mutations[i] = Mutation{Key: k, Value: valueID("same")}
	}
	delta, err := Apply(context.Background(), reader, testSpace, EmptyRoot, mutations)
	if err != nil {
		t.Fatal(err)
	}
	install(t, reader, delta)
	got, err := Entries(context.Background(), reader, testSpace, delta.Root, ScanLimits{})
	if err != nil {
		t.Fatal(err)
	}
	want := make([][]byte, len(keys))
	for i, k := range keys {
		want[i] = bytes.Clone(k)
	}
	sort.Slice(want, func(i, j int) bool { return bytes.Compare(want[i], want[j]) < 0 })
	if len(got) != len(want) {
		t.Fatalf("suffix shared nodes lost paths: %v", got)
	}
	for i, e := range got {
		if !bytes.Equal(e.Key, want[i]) || e.Value != valueID("same") {
			t.Fatalf("entry %d %v", i, e)
		}
	}
	got[1].Key[0] ^= 255
	again, err := Entries(context.Background(), reader, testSpace, delta.Root, ScanLimits{})
	if err != nil || !bytes.Equal(again[1].Key, want[1]) {
		t.Fatal("owned key", err)
	}
	empty, err := Entries(context.Background(), nil, testSpace, EmptyRoot, ScanLimits{})
	if err != nil || len(empty) != 0 {
		t.Fatal(empty, err)
	}
}
func TestEntriesLimitAndCancellationNeverReturnPartialSnapshot(t *testing.T) {
	reader := newMemoryReader()
	delta, err := Apply(context.Background(), reader, testSpace, EmptyRoot, []Mutation{{Key: []byte("a"), Value: valueID("a")}, {Key: []byte("b"), Value: valueID("b")}})
	if err != nil {
		t.Fatal(err)
	}
	install(t, reader, delta)
	for _, limits := range []ScanLimits{{MaxNodes: 1}, {MaxEntries: 1}, {MaxBytes: 1}, {MaxReadBytes: 1}, {MaxNodes: -1}} {
		got, err := Entries(context.Background(), reader, testSpace, delta.Root, limits)
		if !errors.Is(err, ErrLimit) || got != nil {
			t.Fatalf("partial snapshot %+v %v", got, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := reader.reads
	got, err := Entries(ctx, reader, testSpace, delta.Root, ScanLimits{})
	if !errors.Is(err, context.Canceled) || got != nil || reader.reads != before {
		t.Fatal("canceled scan read data", got, err)
	}
	raw := reader.objects[delta.Root]
	reader.objects[delta.Root] = append([]byte(nil), raw...)
	reader.objects[delta.Root][len(raw)-1] ^= 1
	got, err = Entries(context.Background(), reader, testSpace, delta.Root, ScanLimits{})
	if err == nil || got != nil {
		t.Fatal("bad hash accepted")
	}
}
func TestEntriesMatchesIndependentMapAfterPathSplitAndCollapse(t *testing.T) {
	reader := newMemoryReader()
	root := EmptyRoot
	oracle := map[string]model.ObjectID{}
	for turn := 0; turn < 100; turn++ {
		key := []byte{byte(turn % 13), byte((turn * 19) % 17)}
		remove := turn%3 == 0
		value := valueID(string([]byte{byte(turn)}))
		delta, err := Apply(context.Background(), reader, testSpace, root, []Mutation{{Key: key, Value: value, Delete: remove}})
		if err != nil {
			t.Fatal(err)
		}
		install(t, reader, delta)
		root = delta.Root
		if remove {
			delete(oracle, string(key))
		} else {
			oracle[string(key)] = value
		}
		rows, err := Entries(context.Background(), reader, testSpace, root, ScanLimits{})
		if err != nil {
			t.Fatal(err)
		}
		actual := map[string]model.ObjectID{}
		for i, e := range rows {
			if i > 0 && bytes.Compare(rows[i-1].Key, e.Key) >= 0 {
				t.Fatal("unsorted snapshot")
			}
			actual[string(e.Key)] = e.Value
		}
		if !reflect.DeepEqual(actual, oracle) {
			t.Fatalf("map mismatch at %d", turn)
		}
	}
	if _, err := Entries(context.Background(), reader, "foreign", root, ScanLimits{}); err == nil {
		t.Fatal("cross Space snapshot accepted")
	}
}
