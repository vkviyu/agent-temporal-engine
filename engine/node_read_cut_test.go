package engine

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// Pause the fixed reader at a real physical read in graph traversal or node
// projection. Neither the core lock nor the logical View's active operation
// should turn that disk wait into a global read barrier.
func TestGraphNodeAndViewTraversalReleaseCoreAtPhysicalRead(t *testing.T) {
	for _, method := range []string{"get_graph_revision", "list_nodes", "get_view_entries"} {
		for _, revoke := range []bool{false, true} {
			name := method + "/unchanged"
			if revoke {
				name = method + "/revoked"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				e, disk := coordinatorStore(t, Options{})
				coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, coordinatorRequest(t, "read-cut-content", "document", "protected", api.ValueCondition{}))))
				revision := graphCoordinatorCurrentRevision(t, e, "read-cut-revision", "")
				view, err := e.OpenView(ctx, api.OpenViewRequest{Space: "coordinator-space", Principal: "host"})
				if err != nil {
					t.Fatal(err)
				}
				defer e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
				before, err := e.Snapshot(ctx)
				if err != nil {
					t.Fatal(err)
				}
				read := func(scoped context.Context) (any, error) {
					switch method {
					case "get_graph_revision":
						return e.GetGraphRevision(scoped, api.ReadGraphRevisionRequest{Space: "coordinator-space", ID: revision.ID})
					case "list_nodes":
						return e.ListNodes(scoped, api.ListNodesRequest{View: view.Ref, Layer: "content"})
					default:
						return e.GetViewEntries(scoped, api.ReadViewEntriesRequest{View: view.Ref})
					}
				}
				want, err := read(ctx)
				if err != nil {
					t.Fatal(err)
				}
				wantBody, err := json.Marshal(want)
				if err != nil {
					t.Fatal(err)
				}
				binding := api.ReadDeliveryBinding{
					Destination: api.ReadDestination{Principal: "host", Session: "read-cut-test", Request: "response"},
					Method:      method, RequestDigest: agentprofile.ExactRequestDigest([]byte(method)),
				}
				gate := newCoordinatorGate()
				defer gate.unblock()
				disk.mu.Lock()
				disk.gates = append(disk.gates, gate)
				disk.mu.Unlock()
				type outcome struct {
					grant api.ReadDeliveryGrant
					err   error
				}
				done := make(chan outcome, 1)
				go func() {
					grant, err := e.PrepareReadDelivery(ctx, binding, func(scoped context.Context) error {
						cut := e.readCut(scoped)
						if cut == nil {
							return errors.New("read delivery did not capture a read cut")
						}
						fixed, ok := cut.reader.(*fixedObjectReader)
						if !ok || fixed.source != cut.pin {
							return errors.New("read cut did not use its fixed physical pin")
						}
						physical := &planningObjectGate{ReaderAt: fixed.source, gate: gate}
						physical.armed.Store(true)
						fixed.source = physical
						out, err := read(scoped)
						if err == nil && !reflect.ValueOf(out).IsZero() {
							return errors.New("scoped read released its DTO before grant consumption")
						}
						return err
					})
					done <- outcome{grant, err}
				}()
				coordinatorWait(t, gate.entered)
				bounded, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				if err := e.mu.LockContext(bounded); err != nil {
					t.Fatalf("physical read retained the core lock: %v", err)
				}
				e.mu.Unlock()
				if got, err := e.Snapshot(bounded); err != nil || got != before {
					t.Fatalf("physical read blocked Snapshot or exposed another cut: %+v %v", got, err)
				}
				if revoke {
					ack, err := e.ApplyMemory(bounded, api.ApplyMemoryRequest{
						Key: model.RequestKey{Space: "coordinator-space", Principal: "host", RequestID: "read-cut-revoke"},
						Command: memory.Command{RetractSource: &memory.RetractSourceCommand{
							Source:     revision.Entries[0].Value,
							Retraction: memory.Retraction{Kind: memory.RetractionACLRevocation, Reason: "withdraw during physical traversal"},
						}},
					})
					if err != nil || ack.Code != api.CodeDurableAck {
						t.Fatalf("paused traversal blocked policy publication: %+v %v", ack, err)
					}
				}
				gate.unblock()
				out := coordinatorWait(t, done)
				if revoke {
					if out.grant != nil || !errors.Is(out.err, api.ErrReadAuthorizationChanged) {
						t.Fatalf("obsolete policy produced output: grant=%v err=%v", out.grant, out.err)
					}
					if _, err := read(ctx); !errors.Is(err, memory.ErrHistoryAccessDenied) {
						t.Fatalf("retry bypassed current disclosure revocation: %v", err)
					}
					return
				}
				if out.err != nil || out.grant == nil {
					t.Fatalf("unchanged read failed: %v", out.err)
				}
				body, err := out.grant.Consume(binding)
				if err != nil || string(body) != string(wantBody) {
					t.Fatalf("pinned traversal changed its result: %s %v", body, err)
				}
			})
		}
	}
}
