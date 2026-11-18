package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// The reader counts immutable object fetches from an in-memory encoded source.
// It does not model filesystem reads, page cache misses, or publication IO.
type agentPrepareReader struct {
	*bytes.Reader
	reads, bytes uint64
}

func (r *agentPrepareReader) ReadAt(dst []byte, offset int64) (int, error) {
	n, err := r.Reader.ReadAt(dst, offset)
	r.reads++
	r.bytes += uint64(n)
	return n, err
}

func agentPrepareMeter(e *Engine) *agentPrepareReader {
	ids := make([]model.ObjectID, 0, len(e.index.staged))
	for id := range e.index.staged {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var encoded []byte
	locations := make(map[model.ObjectID]storage.RecordLocation, len(ids))
	for _, id := range ids {
		raw := e.index.staged[id]
		locations[id] = storage.RecordLocation{PayloadOffset: int64(len(encoded)), PayloadLength: uint32(len(raw))}
		encoded = append(encoded, raw...)
	}
	reader := &agentPrepareReader{Reader: bytes.NewReader(encoded)}
	e.index.source, e.index.entries, e.index.staged = reader, locations, nil
	return reader
}

// Each iteration prepares the same fixed change against the same authenticated
// fixture. The 4000-group fixture starts with 16000 records (97.7% of the record
// count limit), not 97.7% of the payload limit. Warmth exists only within a Work.
// Setup, admission, publication, fsync and transaction framing are not timed.
func BenchmarkR14AgentPrepareGrowth(b *testing.B) {
	for _, groups := range []int{16, 128, 4000} {
		b.Run(fmt.Sprintf("groups=%d", groups), func(b *testing.B) {
			e, req, rootBytes := workImageFixture(b, groups, false, false)
			reader := agentPrepareMeter(e)
			ctx := context.Background()
			ledger, _, found, err := loadAgentLedger(ctx, e.index, e.roots.LiveControl, req.Space, req.Principal)
			if err != nil || !found {
				b.Fatalf("load fixture: found=%t err=%v", found, err)
			}
			image, err := agentprofile.EncodeRecordImage(ledger.Snapshot())
			if err != nil {
				b.Fatal(err)
			}
			keys, err := image.RecordKeys()
			if err != nil {
				b.Fatal(err)
			}
			unit, err := api.DecodeWorkUnit(req.Body)
			if err != nil {
				b.Fatal(err)
			}
			first := unit.Steps[0]
			single := api.CommitRequest{Space: req.Space, Principal: req.Principal, RequestID: req.RequestID, Method: first.Method, Body: first.Body}
			singleDigest, err := CommandDigest(single)
			if err != nil {
				b.Fatal(err)
			}
			unit.Steps = append(unit.Steps, workImageControl(b, agentprofile.ControlCommand{
				CancelExecution: &agentprofile.CancelExecutionCommand{Execution: "exec-0000", ExpectedIncarnation: 99},
			}))
			failed := workImageRequest(b, unit)
			for _, workload := range []struct {
				name string
				call func() (*commitPlan, error)
				fail bool
			}{
				{"single-cold", func() (*commitPlan, error) {
					return e.prepareAgent(ctx, single, singleDigest, workImageTx, 2, nil)
				}, false},
				{"work-uncached", func() (*commitPlan, error) {
					return workImagePrepare(b, ctx, e, req, false)
				}, false},
				{"work-warm", func() (*commitPlan, error) {
					return workImagePrepare(b, ctx, e, req, true)
				}, false},
				{"work-late-failure", func() (*commitPlan, error) {
					return workImagePrepare(b, ctx, e, failed, true)
				}, true},
			} {
				b.Run(workload.name, func(b *testing.B) {
					// Validate and frame one sample outside the timed region.
					sample, err := workload.call()
					if workload.fail {
						if sample != nil || !errors.Is(err, agentprofile.ErrConflict) {
							b.Fatalf("expected private rejection: plan=%t err=%v", sample != nil, err)
						}
					} else if err != nil {
						b.Fatal(err)
					}
					var transactionBytes, objectBytes uint64
					var objects int
					if sample != nil {
						framed, err := storage.PrepareTransaction(sample.transaction, storage.DefaultLimits())
						if err != nil {
							b.Fatal(err)
						}
						transactionBytes = framed.Size()
						objects = len(sample.builder.objects)
						for _, object := range sample.builder.objects {
							objectBytes += uint64(len(object.encoded))
						}
					}
					reader.reads, reader.bytes = 0, 0
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						plan, err := workload.call()
						if workload.fail {
							if plan != nil || !errors.Is(err, agentprofile.ErrConflict) {
								b.Fatalf("expected rejection: plan=%t err=%v", plan != nil, err)
							}
						} else if err != nil {
							b.Fatal(err)
						}
						workImageBenchmarkPlan = plan
					}
					b.StopTimer()
					b.ReportMetric(float64(image.Size()), "state-raw-B")
					b.ReportMetric(float64(len(keys)), "state-records")
					b.ReportMetric(float64(rootBytes), "root-envelope-B")
					b.ReportMetric(float64(reader.reads)/float64(b.N), "object-reads/op")
					b.ReportMetric(float64(reader.bytes)/float64(b.N), "read-B/op")
					b.ReportMetric(float64(objects), "objects/op")
					b.ReportMetric(float64(objectBytes), "object-B/op")
					b.ReportMetric(float64(transactionBytes), "prepared-tx-B/op")
				})
			}
		})
	}
}
