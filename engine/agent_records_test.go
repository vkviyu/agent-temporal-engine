package engine

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
)

func TestAgentStateRecordsFixedPagesAndBindings(t *testing.T) {
	ctx := context.Background()
	e, _ := coordinatorStore(t, Options{AgentProfile: true})
	for i := 0; i < 4; i++ {
		task := profileCoordinatorTask(model.EntityID(fmt.Sprintf("page-task-%d", i)))
		coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, profileCoordinatorRequest(t, string(task.ID), agentprofile.Command{CreateTask: &task}))))
	}
	req := api.ReadAgentStateRecordsRequest{Space: "coordinator-space", Principal: "host", Limit: 1}
	first, err := e.GetAgentStateRecords(ctx, req)
	if err != nil || !first.Found || first.Catalog == "" || first.RecordCount != 4 || len(first.Records) != 1 || first.Continuation == "" {
		t.Fatalf("first page: %+v %v", first, err)
	}
	want, err := e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: req.Space, Principal: req.Principal, Anchor: &first.Anchor})
	if err != nil {
		t.Fatal(err)
	}
	catalogID, found, err := state.Get(ctx, e.index, systemSpace, first.LiveControlRoot, agentPartitionKey(req.Space, req.Principal))
	if err != nil || !found || catalogID != first.Catalog {
		t.Fatalf("catalog is not partition envelope: %s %v", catalogID, err)
	}
	// An intervening publication must not change any subsequent page.
	task := profileCoordinatorTask("later-task")
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, ctx, profileCoordinatorRequest(t, "later", agentprofile.Command{CreateTask: &task}))))
	latest, err := e.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	req.Anchor, req.Continuation = &first.Anchor, first.Continuation
	for name, change := range map[string]func(*api.ReadAgentStateRecordsRequest){
		"owner": func(r *api.ReadAgentStateRecordsRequest) { r.Principal = "other" },
		"space": func(r *api.ReadAgentStateRecordsRequest) { r.Space = "other" },
		"anchor": func(r *api.ReadAgentStateRecordsRequest) {
			r.Anchor = &model.SnapshotAnchor{Space: r.Space, Fence: latest.Fence}
		},
		"missing-anchor": func(r *api.ReadAgentStateRecordsRequest) { r.Anchor = nil },
		"limit":          func(r *api.ReadAgentStateRecordsRequest) { r.Limit = 2 },
		"negative-limit": func(r *api.ReadAgentStateRecordsRequest) { r.Limit = -1 },
		"large-limit":    func(r *api.ReadAgentStateRecordsRequest) { r.Limit = 257 },
		"malformed":      func(r *api.ReadAgentStateRecordsRequest) { r.Continuation = "invalid" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := req
			change(&bad)
			got, err := e.GetAgentStateRecords(ctx, bad)
			if err == nil || !reflect.DeepEqual(got, api.AgentStateRecordsPage{}) {
				t.Fatalf("rejected page leaked result: %+v %v", got, err)
			}
		})
	}
	records := append([]agentprofile.StateRecord(nil), first.Records...)
	page := first
	for page.Continuation != "" {
		req.Continuation = page.Continuation
		page, err = e.GetAgentStateRecords(ctx, req)
		if err != nil || page.Catalog != first.Catalog || page.Fence != first.Fence || page.RootSet != first.RootSet ||
			page.LiveControlRoot != first.LiveControlRoot || page.RecordCount != first.RecordCount || page.RecordBytes != first.RecordBytes || len(page.Records) != 1 {
			t.Fatalf("mixed page: %+v %v", page, err)
		}
		records = append(records, page.Records...)
	}
	image, err := agentprofile.DecodeRecordImage(records)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := image.Snapshot()
	full := want.Snapshot
	if len(full.Executions) == 0 {
		full.Executions = nil
	}
	if len(full.Steps) == 0 {
		full.Steps = nil
	}
	if len(full.Actions) == 0 {
		full.Actions = nil
	}
	sort.Slice(full.Tasks, func(i, j int) bool { return full.Tasks[i].ID < full.Tasks[j].ID })
	if !reflect.DeepEqual(got, full) {
		t.Fatalf("paged snapshot differs from full read: paged=%+v full=%+v", got, full)
	}
	metadata := first
	metadata.Records, metadata.Continuation = nil, ""
	scope := agentRecordsScope(req, 1, metadata)
	for _, key := range []string{strings.Repeat("0", 80), hex.EncodeToString(records[len(records)-1].Key)} {
		bad := req
		bad.Continuation = writeNodeToken(scope, key)
		got, err := e.GetAgentStateRecords(ctx, bad)
		if !errors.Is(err, api.ErrInvalidRequest) || !reflect.DeepEqual(got, api.AgentStateRecordsPage{}) {
			t.Fatalf("nonexistent/terminal boundary accepted: %+v %v", got, err)
		}
	}
	clear(first.Records[0].Raw)
	clear(first.Records[0].Key)
	req.Continuation = ""
	again, err := e.GetAgentStateRecords(ctx, req)
	if err != nil || len(again.Records[0].Raw) == 0 || again.Records[0].Raw[0] == 0 {
		t.Fatal("returned record buffers alias stored state")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	gotPage, err := e.GetAgentStateRecords(canceled, req)
	if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(gotPage, api.AgentStateRecordsPage{}) {
		t.Fatalf("canceled: %+v %v", gotPage, err)
	}
	after, err := e.Snapshot(ctx)
	if err != nil || !reflect.DeepEqual(after, latest) {
		t.Fatal("reads or rejections changed publication")
	}
	missing, err := e.GetAgentStateRecords(ctx, api.ReadAgentStateRecordsRequest{Space: req.Space, Principal: "missing"})
	if err != nil || missing.Found || missing.Catalog != "" || missing.RecordCount != 0 || missing.RecordBytes != 0 ||
		len(missing.Records) != 0 || missing.Continuation != "" || missing.Fence != latest.Fence || missing.RootSet != latest.RootSet {
		t.Fatalf("missing partition: %+v %v", missing, err)
	}
	disabled, _ := coordinatorStore(t, Options{})
	if out, err := disabled.GetAgentStateRecords(ctx, req); !errors.Is(err, ErrUnsupportedMethod) || !reflect.DeepEqual(out, api.AgentStateRecordsPage{}) {
		t.Fatalf("disabled: %+v %v", out, err)
	}
}

func TestAgentStateRecordsLargeRecordPacking(t *testing.T) {
	ctx := context.Background()
	// Both independent byte fields remain below the allocation preflight.
	q := agentprofile.Query{ID: "a", Space: "s", Owner: "p", Version: 1,
		Input: make([]byte, (1<<20)-2048)}
	second := q
	second.ID = "b"
	image, err := agentprofile.EncodeRecordImage(agentprofile.Snapshot{Lifecycle: agentprofile.LifecycleSnapshot{Queries: []agentprofile.Query{q, second}}})
	if err != nil {
		t.Fatal(err)
	}
	meta := api.AgentStateRecordsPage{Found: true, Catalog: model.ObjectID(strings.Repeat("a", 64)), RecordCount: 2, RecordBytes: image.Size()}
	req := api.ReadAgentStateRecordsRequest{Space: "s", Principal: "p"}
	page, err := packAgentRecords(ctx, req, 256, meta, image)
	if err != nil || len(page.Records) != 2 || page.Continuation != "" || jsonBytes(page) > api.MaxAgentStatePageBytes {
		t.Fatalf("large packing: records=%d err=%v", len(page.Records), err)
	}
	// The format's exact 4 MiB ceiling also fits, even with a next token.
	maxPage := api.AgentStateRecordsPage{Found: true, Catalog: meta.Catalog,
		Records: []agentprofile.StateRecord{{Key: make([]byte, agentRecordKeyBytes), Raw: make([]byte, agentprofile.MaxStateRecordBytes)}}}
	if jsonBytes(maxPage) > api.MaxAgentStatePageBytes {
		t.Fatal("maximum raw record cannot fit its base64 page envelope")
	}
	empty, err := agentprofile.EncodeRecordImage(agentprofile.Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	meta.RecordCount, meta.RecordBytes = 0, 0
	req.Continuation = ""
	zero, err := packAgentRecords(ctx, req, 256, meta, empty)
	if err != nil || !zero.Found || zero.Catalog == "" || len(zero.Records) != 0 || zero.Continuation != "" {
		t.Fatalf("empty catalog: %+v %v", zero, err)
	}
}
