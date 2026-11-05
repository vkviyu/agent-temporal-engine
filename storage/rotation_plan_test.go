package storage

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
)

func TestSegmentTargetOptionsArePureAndBounded(t *testing.T) {
	for _, target := range []uint64{0, MinSegmentTargetBytes, DefaultSegmentTargetBytes, MaxSegmentTargetBytes, MinSegmentTargetBytes - 1, MaxSegmentTargetBytes + 1} {
		input := JournalOptions{SegmentTargetBytes: target}
		got, err := NormalizeJournalOptions(input)
		if target != 0 && (target < MinSegmentTargetBytes || target > MaxSegmentTargetBytes) {
			if !errors.Is(err, ErrLimit) {
				t.Fatalf("target %d: %v", target, err)
			}
			continue
		}
		want := target
		if want == 0 {
			want = DefaultSegmentTargetBytes
		}
		if err != nil || got.SegmentTargetBytes != want || input.SegmentTargetBytes != target {
			t.Fatalf("target %d: got %+v err %v", target, got, err)
		}
	}
}

func TestRotationPlanIsPrivateAndChunksKeepWholeFrames(t *testing.T) {
	f, j, old, _ := faultSeedAcknowledged(t)
	defer j.Close()
	j.options.SegmentTargetBytes = MinSegmentTargetBytes
	_, batch := batchChain(t, old, 2, 3)
	beforeHash := hashDigest(j.prefix)
	beforeDescriptor := j.segments[0].descriptor
	beforeTree := faultTreeSnapshot(t, f)
	f.resetFaults()
	var total uint64
	var canonical []byte
	for _, prepared := range batch {
		total += prepared.Size()
		canonical = append(canonical, prepared.encoded...)
	}
	layout, err := j.prepareLayout(batch, total, "101112131415161718191a1b1c1d1e1f")
	if err != nil {
		t.Fatal(err)
	}
	calls := len(f.calls)
	if hashDigest(j.prefix) != beforeHash || j.segments[0].descriptor != beforeDescriptor || len(j.segments) != 1 || calls != 0 || !reflect.DeepEqual(faultTreeSnapshot(t, f), beforeTree) {
		t.Fatal("layout preparation mutated published prefix/catalog/files")
	}
	if len(layout.segments) < 3 {
		t.Fatalf("no useful rotations: %d", len(layout.segments))
	}
	var rebuilt []byte
	lastComplete := old.Position
	for _, write := range layout.writes {
		for _, chunk := range write.chunks {
			reader := bytes.NewReader(chunk)
			for {
				frame, _, err := ReadFrame(reader, Limits{})
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("frame crosses chunk/segment boundary: %v", err)
				}
				if frame.Kind == KindCommit {
					for _, prepared := range batch {
						if frame.TxID == prepared.txID {
							lastComplete = prepared.position
						}
					}
				}
			}
			rebuilt = append(rebuilt, chunk...)
		}
		if write.segment.descriptor.EndPosition != lastComplete {
			t.Fatalf("segment claims incomplete commit: %+v vs %+v", write.segment.descriptor.EndPosition, lastComplete)
		}
	}
	if !bytes.Equal(rebuilt, canonical) {
		t.Fatal("rotation changed canonical transaction stream")
	}
}

func TestRotationSegmentCountLimitRejectsWithoutIO(t *testing.T) {
	f, j, old, _ := faultSeedAcknowledged(t)
	defer j.Close()
	j.options.SegmentTargetBytes = MinSegmentTargetBytes
	tx := faultNextTransaction(old, 2)
	tx.Records = make([]Record, MaxSegments+1)
	for i := range tx.Records {
		tx.Records[i] = Record{Kind: KindObject, Payload: bytes.Repeat([]byte("x"), 512)}
	}
	prepared, err := PrepareTransaction(tx, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	beforeDescriptor := j.segments[0].descriptor
	beforeHash := hashDigest(j.prefix)
	result, err := j.PublishPreparedBatch(t.Context(), []*PreparedTransaction{prepared})
	if !errors.Is(err, ErrLimit) || !reflect.DeepEqual(result, PublishedBatch{}) || len(f.calls) != 0 || j.Status().State != JournalReady || j.segments[0].descriptor != beforeDescriptor || hashDigest(j.prefix) != beforeHash || j.publication != old {
		t.Fatalf("count limit mutated or published: %+v %v calls=%+v", result, err, f.calls)
	}
}
