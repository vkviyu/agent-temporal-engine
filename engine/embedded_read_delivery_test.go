package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestReadDeliveryWithoutCaptureDoesNotReleaseData(t *testing.T) {
	e, _ := coordinatorStore(t, Options{})
	value := api.StateValueResult{Found: true, Value: api.Value{Kind: api.ValueBytes, Data: []byte("protected response")}}
	e.mu.Lock()
	out, err := finishReadDelivery(context.Background(), e, "get_state", "embedded-host", value, model.SnapshotAnchor{Space: "coordinator-space", Fence: e.publication.Fence()}, "", "", nil)
	e.mu.Unlock()
	if !errors.Is(err, api.ErrReadEmissionDenied) || !reflect.DeepEqual(out, api.StateValueResult{}) {
		t.Fatalf("an uncaptured read released data without a grant: %+v %v", out, err)
	}
}

// This list links the transport's closed read-method set to public embedded
// entry points. A new Unix read must select its embedded authorization path.
func TestEmbeddedReadDeliveryCoversEveryOrdinaryTransportRead(t *testing.T) {
	methods := map[string]string{
		"open_view":               "OpenView",
		"lookup_request":          "LookupRequest",
		"list_nodes":              "ListNodes",
		"get_node":                "GetNode",
		"get_node_context":        "GetNodeContext",
		"search_nodes":            "SearchNodes",
		"trace_nodes":             "Trace",
		"get_state":               "GetState",
		"get_view_state":          "GetViewState",
		"get_view_entries":        "GetViewEntries",
		"resolve_publication":     "ResolvePublication",
		"snapshot":                "Snapshot",
		"export_transfer":         "ExportTransfer",
		"get_transfer":            "GetTransfer",
		"get_agent_state":         "GetAgentState",
		"get_agent_state_records": "GetAgentStateRecords",
		"get_query_intake":        "GetQueryIntake",
		"get_query_progress":      "GetQueryProgress",
		"get_knowledge":           "GetKnowledgeState",
		"get_summary":             "GetSummary",
		"get_memory":              "GetMemoryState",
		"get_annotations":         "GetAnnotations",
		"get_investigation":       "GetInvestigation",
		"get_task_trace":          "GetTaskTrace",
		"get_task_trace_evidence": "GetTaskTraceEvidence",
		"get_emission":            "GetEmission",
		"get_graph_revision":      "GetGraphRevision",
		"get_graph_branch":        "GetGraphBranch",
		"diff_graph":              "DiffGraph",
		"plan_graph_join":         "PlanGraphJoin",
		"trace_graph":             "TraceGraph",
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate read-method source")
	}
	dir := filepath.Dir(source)
	files := token.NewFileSet()
	transport, err := parser.ParseFile(files, filepath.Join(dir, "..", "service", "nodes.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	for _, declaration := range transport.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "serveReadRequest" {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			clause, ok := node.(*ast.CaseClause)
			if !ok {
				return true
			}
			for _, expression := range clause.List {
				literal, ok := expression.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				method, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatal(err)
				}
				if method == "read_content" {
					continue
				}
				seen[method] = true
				if methods[method] == "" {
					t.Errorf("ordinary transport read %q has no embedded entry-point coverage", method)
				}
			}
			return true
		})
	}
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	getters := make(map[string]*ast.FuncDecl)
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(files, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv == nil || function.Body == nil || len(function.Recv.List) != 1 {
				continue
			}
			receiver, ok := function.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			name, ok := receiver.X.(*ast.Ident)
			if ok && name.Name == "Engine" {
				getters[function.Name.Name] = function
			}
		}
	}
	for method, name := range methods {
		if !seen[method] {
			t.Errorf("covered method %q is missing from the closed transport read set", method)
		}
		function := getters[name]
		if function == nil || len(function.Body.List) == 0 {
			t.Errorf("public embedded getter %s is missing", name)
			continue
		}
		entry, ok := function.Body.List[0].(*ast.IfStmt)
		if !ok {
			t.Errorf("public embedded getter %s has no entry authorization wrapper", name)
			continue
		}
		wrapped := false
		ast.Inspect(entry.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			callee, ok := call.Fun.(*ast.Ident)
			if !ok || (callee.Name != "embeddedReadDelivery" && callee.Name != "embeddedViewReadDelivery") {
				return true
			}
			for _, argument := range call.Args {
				literal, ok := argument.(*ast.BasicLit)
				if ok && literal.Kind == token.STRING {
					value, _ := strconv.Unquote(literal.Value)
					wrapped = wrapped || value == method
				}
			}
			return true
		})
		if !wrapped {
			t.Errorf("public embedded getter %s does not authorize method %q at entry", name, method)
		}
	}
}

type embeddedDeliveryLockContext struct {
	context.Context
	calls   atomic.Int32
	at      int32
	waiting chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (c *embeddedDeliveryLockContext) Done() <-chan struct{} {
	if c.calls.Add(1) == c.at {
		close(c.waiting)
		<-c.resume
	}
	return c.Context.Done()
}

func (c *embeddedDeliveryLockContext) unblock() { c.once.Do(func() { close(c.resume) }) }

func TestEmbeddedGetterCannotReleaseResponseAfterAuthorizationCutChanges(t *testing.T) {
	e, _ := coordinatorStore(t, Options{})
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), coordinatorRequest(t, "embedded-read-source", "document", "original", api.ValueCondition{}))))
	gate := &embeddedDeliveryLockContext{Context: context.Background(), at: 2, waiting: make(chan struct{}), resume: make(chan struct{})}
	defer gate.unblock()
	type result struct {
		value api.StateValueResult
		err   error
	}
	finished := make(chan result, 1)
	go func() {
		value, err := e.GetState(gate, api.ReadStateRequest{Space: "coordinator-space", Key: []byte("document")})
		finished <- result{value, err}
	}()
	coordinatorWait(t, gate.waiting)
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), coordinatorRequest(t, "embedded-read-cut-change", "document", "replacement", api.ValueCondition{}))))
	gate.unblock()
	out := coordinatorWait(t, finished)
	if !errors.Is(out.err, api.ErrReadAuthorizationChanged) || out.value.Found || len(out.value.Value.Data) != 0 {
		t.Fatalf("embedded read released a response from an obsolete authorization cut: %+v %v", out.value, out.err)
	}
	if gate.calls.Load() != 2 {
		t.Fatalf("embedded read retried across an authorization cut: lock attempts=%d", gate.calls.Load())
	}
}

func TestEmbeddedOpenViewAuthorizationFailureRetiresItsHandle(t *testing.T) {
	e, _ := coordinatorStore(t, Options{})
	defer e.stopViews()
	gate := &embeddedDeliveryLockContext{Context: context.Background(), at: 3, waiting: make(chan struct{}), resume: make(chan struct{})}
	defer gate.unblock()
	type result struct {
		view api.ViewDescriptor
		err  error
	}
	finished := make(chan result, 1)
	go func() {
		view, err := e.OpenView(gate, api.OpenViewRequest{Space: "coordinator-space", Principal: "host"})
		finished <- result{view, err}
	}()
	coordinatorWait(t, gate.waiting)
	coordinatorAck(t, coordinatorWait(t, coordinatorCommit(e, context.Background(), coordinatorRequest(t, "embedded-view-cut-change", "key", "new publication", api.ValueCondition{}))))
	gate.unblock()
	out := coordinatorWait(t, finished)
	if !errors.Is(out.err, api.ErrReadAuthorizationChanged) || out.view.Ref != "" {
		t.Fatalf("OpenView released a handle after failed authorization: %+v %v", out.view, out.err)
	}
	e.views.mu.Lock()
	entries, pending := len(e.views.entries), e.views.pending
	e.views.mu.Unlock()
	if entries != 0 || pending != 0 {
		t.Fatalf("failed embedded OpenView retained an unreachable handle: entries=%d pending=%d", entries, pending)
	}
}

type embeddedSerializationProbe struct {
	Data     []byte
	engine   *Engine
	marshals *atomic.Int32
}

func (p embeddedSerializationProbe) MarshalJSON() ([]byte, error) {
	p.engine.mu.init()
	select {
	case <-p.engine.mu.token:
		p.engine.mu.Unlock()
	default:
		return nil, errors.New("embedded request or response serialization held the publication lock")
	}
	p.marshals.Add(1)
	return json.Marshal(struct{ Data []byte }{p.Data})
}

func TestEmbeddedReadSerializesOutsidePublicationLockAndReleasesAuthorizedCopy(t *testing.T) {
	e, _ := coordinatorStore(t, Options{})
	ctx := context.Background()
	var marshals atomic.Int32
	request := embeddedSerializationProbe{Data: []byte("request"), engine: e, marshals: &marshals}
	response := embeddedSerializationProbe{Data: []byte("response"), engine: e, marshals: &marshals}
	digest := sha256.Sum256([]byte(`{"Data":"cmVxdWVzdA=="}`))
	calls := 0
	out, err := embeddedReadDelivery(ctx, e, "serialization_probe", model.PrincipalID("host"), request, func(scoped context.Context) (embeddedSerializationProbe, error) {
		calls++
		capture, _ := scoped.Value(readDeliveryKey{}).(*readDeliveryCapture)
		if capture == nil || capture.binding.Method != "serialization_probe" || capture.binding.Destination.Principal != "host" || capture.binding.Destination.Request == "" || capture.binding.RequestDigest != model.ObjectID(hex.EncodeToString(digest[:])) {
			t.Fatal("embedded delivery did not bind the exact request and destination")
		}
		e.mu.Lock()
		defer e.mu.Unlock()
		return finishReadDelivery(scoped, e, "serialization_probe", "host", response, model.SnapshotAnchor{Space: "coordinator-space", Fence: e.publication.Fence()}, "", "", nil)
	})
	if err != nil || !bytes.Equal(out.Data, response.Data) || calls != 1 || marshals.Load() != 2 {
		t.Fatalf("embedded delivery changed response or bypassed serialization: out=%+v err=%v calls=%d marshals=%d", out, err, calls, marshals.Load())
	}
	response.Data[0] = 'x'
	if string(out.Data) != "response" {
		t.Fatal("embedded delivery returned caller-owned response bytes")
	}
}
