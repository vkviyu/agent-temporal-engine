package state

import (
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/observation"
)

type r13CountingReader struct {
	ObjectReader
	count, bytes uint64
}

func (r *r13CountingReader) ReadObject(ctx context.Context, id model.ObjectID) ([]byte, error) {
	b, err := r.ObjectReader.ReadObject(ctx, id)
	if err == nil {
		r.count++
		r.bytes += uint64(len(b))
	}
	return b, err
}

func TestR13ScanCountsActualReadsAndBudgetFailure(t *testing.T) {
	reader := newMemoryReader()
	delta, err := Apply(t.Context(), reader, testSpace, EmptyRoot, []Mutation{
		{Key: []byte("a"), Value: valueID("a")}, {Key: []byte("b"), Value: valueID("b")},
	})
	if err != nil {
		t.Fatal(err)
	}
	install(t, reader, delta)
	for _, limited := range []bool{false, true} {
		c, _ := observation.New(4)
		counter := &r13CountingReader{ObjectReader: reader}
		limits := ScanLimits{Observation: c}
		if limited {
			limits.MaxReadBytes = 1
		}
		rows, err := Entries(t.Context(), counter, testSpace, delta.Root, limits)
		want := observation.Success
		if limited {
			want = observation.Limit
			if !errors.Is(err, ErrLimit) || rows != nil {
				t.Fatal(rows, err)
			}
		} else if err != nil || len(rows) != 2 {
			t.Fatal(rows, err)
		}
		var events [4]observation.Event
		if c.Drain(events[:]) != 1 {
			t.Fatal("scan must emit once")
		}
		event := events[0]
		if event.Kind != observation.Scan || event.Outcome != want ||
			event.Count != counter.count || event.Bytes != counter.bytes || event.Bytes == 0 {
			t.Fatal(event, counter)
		}
	}
}
