package formats_test

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
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/client"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/formats"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
	"github.com/vkviyu/agent-temporal-engine/service"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func TestRegistryPolicyAndOwnership(t *testing.T) {
	r, err := formats.Current()
	if err != nil {
		t.Fatal(err)
	}
	if formats.Fingerprint() != sha256.Sum256(formats.JSON()) {
		t.Fatal("registry fingerprint does not identify the exact release bytes")
	}
	for _, family := range r.Families {
		for _, v := range family.Versions {
			if err := r.Check(family.ID, v.Number, formats.Replay); err != nil {
				t.Fatal(err)
			}
			err := r.Check(family.ID, v.Number, formats.Write)
			if v.Write && err != nil || !v.Write && !errors.Is(err, formats.ErrReplayOnly) {
				t.Fatal(family.ID, v.Number, err)
			}
			plan, err := r.PlanMigration(family.ID, v.Number, v.Number)
			if err != nil || !plan.PreserveBytes || !plan.PreserveReceipts || !plan.PreserveIdentities {
				t.Fatal(plan, err)
			}
		}
		if err := r.Check(family.ID, ^uint64(0), formats.Decode); !errors.Is(err, formats.ErrUnsupported) {
			t.Fatal(err)
		}
	}
	if _, err := r.PlanMigration("agent.invocation", 1, 2); !errors.Is(err, formats.ErrUnsupported) {
		t.Fatal("legacy invocation version unexpectedly registered", err)
	}
	for family, current := range map[string]uint64{"agent.control": 6, "knowledge.packet": knowledge.PacketSchema, "knowledge.summary": knowledge.SummarySchema} {
		for version := uint64(1); version < current; version++ {
			if err := r.Check(family, version, formats.Decode); !errors.Is(err, formats.ErrUnsupported) {
				t.Fatalf("%s accepted retired schema %d: %v", family, version, err)
			}
		}
	}
	if err := r.Check("future", 1, formats.Decode); !errors.Is(err, formats.ErrUnsupported) {
		t.Fatal(err)
	}
	if err := r.Check("agent.command", 1, "future"); !errors.Is(err, formats.ErrRegistry) {
		t.Fatal(err)
	}
	f, _ := r.Family("agent.control")
	f.Versions[0].Number = 99
	r.Families[0].ID = "caller-mutated"
	raw := formats.JSON()
	raw[0] = 'x'
	again, err := formats.Current()
	if err != nil || again.Families[0].ID == "caller-mutated" {
		t.Fatal("caller mutated embedded registry")
	}
	f, _ = again.Family("agent.control")
	if f.Versions[0].Number != 6 {
		t.Fatal("Family returned shared versions")
	}
}

// The real hello test freezes capability behavior. This additional inventory
// check prevents a production route being added/removed without registry review.
func TestRegistryCoversActualProtocolDispatchMethods(t *testing.T) {
	r, err := formats.Current()
	if err != nil {
		t.Fatal(err)
	}
	registered := map[string]bool{}
	for _, capability := range r.Capabilities {
		for _, method := range capability.Methods {
			registered[method] = true
		}
	}
	actual := map[string]bool{"hello": true}
	for _, path := range []string{"../service/session.go", "../service/nodes.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			s, ok := n.(*ast.SwitchStmt)
			if !ok {
				return true
			}
			tag, ok := s.Tag.(*ast.SelectorExpr)
			if !ok || tag.Sel.Name != "Method" {
				return true
			}
			req, ok := tag.X.(*ast.Ident)
			if !ok || req.Name != "req" {
				return true
			}
			for _, stmt := range s.Body.List {
				for _, expr := range stmt.(*ast.CaseClause).List {
					literal, ok := expr.(*ast.BasicLit)
					if !ok || literal.Kind != token.STRING {
						t.Fatal("wire method route is not a static string")
					}
					method, err := strconv.Unquote(literal.Value)
					if err != nil {
						t.Fatal(err)
					}
					actual[method] = true
				}
			}
			return false
		})
	}
	for method := range actual {
		if !registered[method] {
			t.Errorf("production wire method absent from registry: %s", method)
		}
	}
	for method := range registered {
		if !actual[method] {
			t.Errorf("registry advertises absent wire method: %s", method)
		}
	}
}

func TestRegistryRejectsAmbiguousOrUnsupportedManifest(t *testing.T) {
	raw := formats.JSON()
	for name, bad := range map[string][]byte{
		"duplicate-schema": bytes.Replace(raw, []byte(`"schema": 1,`), []byte(`"schema": 1, "schema": 1,`), 1),
		"future-schema":    bytes.Replace(raw, []byte(`"schema": 1,`), []byte(`"schema": 2,`), 1),
		"unknown-field":    bytes.Replace(raw, []byte(`"schema": 1,`), []byte(`"future": 1, "schema": 1,`), 1),
		"trailing":         append(bytes.Clone(raw), []byte("{}")...),
		"duplicate-family": bytes.Replace(raw, []byte(`"id": "agent.continuation"`), []byte(`"id": "agent.command"`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := formats.Parse(bad); !errors.Is(err, formats.ErrRegistry) {
				t.Fatal(err)
			}
		})
	}
}

func TestRegistryMatchesPublishedCodecConstants(t *testing.T) {
	r, err := formats.Current()
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		id      string
		version uint64
	}{
		{"agent.command", agentprofile.CommandSchema}, {"agent.snapshot", agentprofile.SnapshotVersion},
		{"graph.command", graph.CommandSchema}, {"graph.revision", graph.RevisionSchema}, {"graph.branch", graph.BranchSchema},
		{"graph.command", graph.LocalCommandSchema}, {"graph.revision", graph.ProducedRevisionSchema}, {"graph.branch", graph.ScopedBranchSchema},
		{"knowledge.observation", knowledge.Schema}, {"knowledge.packet", knowledge.PacketSchema}, {"knowledge.summary", knowledge.SummarySchema},
		{"local.protocol", uint64(protocol.Version)}, {"state.radix", state.NodeVersion}, {"storage.frame", uint64(storage.FrameVersion)},
		{"event.profile-registry", eventlog.ProfileRegistryVersion},
	} {
		if err := r.Check(check.id, check.version, formats.Write); err != nil {
			t.Fatal(check, err)
		}
	}
}

func TestCompatibilityCorpusUsesRealCanonicalDecoders(t *testing.T) {
	r, err := formats.Current()
	if err != nil {
		t.Fatal(err)
	}
	var corpus []struct {
		Name, Family string
		Version      uint64
		Hex          string
	}
	b, err := os.ReadFile("testdata/compatibility.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &corpus); err != nil {
		t.Fatal(err)
	}
	for _, vector := range corpus {
		t.Run(vector.Name, func(t *testing.T) {
			if err := r.Check(vector.Family, vector.Version, formats.Decode); err != nil {
				t.Fatal(err)
			}
			raw, err := hex.DecodeString(vector.Hex)
			if err != nil {
				t.Fatal(err)
			}
			encode := func(raw []byte) ([]byte, error) {
				switch vector.Family {
				case "event.core", "event.agent":
					o, e := codec.DecodeObject(raw)
					if e != nil {
						return nil, e
					}
					v, e := eventlog.Decode(o)
					if e != nil {
						return nil, e
					}
					again, e := eventlog.Object(v)
					if e != nil {
						return nil, e
					}
					if again.Version != vector.Version {
						return nil, errors.New("event format version changed")
					}
					return codec.EncodeObject(again)
				case "codec.object":
					v, e := codec.DecodeObject(raw)
					if e != nil {
						return nil, e
					}
					return codec.EncodeObject(v)
				case "agent.tool-definition":
					v, e := agentprofile.DecodeToolDefinition(raw)
					if e != nil {
						return nil, e
					}
					return v.Canonical()
				case "agent.invocation":
					v, e := agentprofile.DecodeInvocation(raw)
					if e != nil {
						return nil, e
					}
					return agentprofile.EncodeInvocation(v)
				case "agent.command":
					v, e := agentprofile.DecodeCommand(raw)
					if e != nil {
						return nil, e
					}
					return agentprofile.EncodeCommand(v)
				case "agent.control", "agent.continuation":
					wrapped := codec.AppendUint([]byte("ATE-AGENT-COMMAND\x00"), agentprofile.CommandSchema)
					op := uint64(13)
					if vector.Family == "agent.continuation" {
						op = 12
					}
					wrapped = codec.AppendUint(wrapped, op)
					wrapped, _ = codec.AppendBytes(wrapped, raw)
					v, e := agentprofile.DecodeCommand(wrapped)
					if e != nil {
						return nil, e
					}
					again, e := agentprofile.EncodeCommand(v)
					if e != nil {
						return nil, e
					}
					if !bytes.Equal(wrapped, again) {
						return nil, errors.New("control replay bytes changed")
					}
					return raw, nil
				case "emission.command":
					v, e := api.DecodeEmissionCommand(raw)
					if e != nil {
						return nil, e
					}
					return api.EncodeEmissionCommand(v)
				case "knowledge.packet":
					v, e := knowledge.DecodePacket(raw)
					if e != nil {
						return nil, e
					}
					return v.Canonical()
				case "graph.branch":
					v, e := graph.DecodeBranch(raw)
					if e != nil {
						return nil, e
					}
					return graph.EncodeBranch(v)
				case "local.protocol":
					v, e := protocol.DecodeRequest(raw)
					if e != nil {
						return nil, e
					}
					return json.Marshal(v)
				default:
					return nil, errors.New("corpus family has no real decoder")
				}
			}
			again, err := encode(raw)
			if err != nil || !bytes.Equal(raw, again) {
				t.Fatalf("canonical corpus changed: %x %v", again, err)
			}
			for end := 0; end < len(raw); end++ {
				if _, err := encode(raw[:end]); err == nil {
					t.Fatalf("accepted truncated vector at %d", end)
				}
			}
			if _, err := encode(append(bytes.Clone(raw), 0)); err == nil {
				t.Fatal("accepted trailing byte")
			}
		})
	}
}

func TestLocalCapabilityGoldenMatchesRealUnixHello(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native store requires Darwin/Linux")
	}
	dir, err := os.MkdirTemp("/tmp", "ate-registry-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	storeDir := filepath.Join(dir, "store")
	if err := os.Mkdir(storeDir, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := ate.Create(storeDir, ate.Options{AgentProfile: true, EventProfile: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	server, err := service.New(store, service.Options{Credentials: map[string]service.Credential{"token": {Principal: "owner", Spaces: []model.SpaceID{"s"}}}})
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "local.sock")
	if err := server.Listen(socket); err != nil {
		if errors.Is(err, syscall.EPERM) || os.IsPermission(err) {
			t.Skipf("native Unix socket unavailable in this test sandbox: %v", err)
		}
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(context.Background()) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, socket, client.Options{Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	actual, err := json.Marshal(c.Identity())
	if err != nil {
		t.Fatal(err)
	}
	golden, err := os.ReadFile("testdata/hello-full.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, bytes.TrimSpace(golden)) {
		t.Fatalf("hello capabilities changed without registry review:\n%s\n%s", actual, golden)
	}
	r, err := formats.Current()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, c := range c.Identity().Capabilities {
		seen[c] = true
	}
	if len(seen) != len(r.Capabilities) {
		t.Fatal("registry capability set diverged from enabled native server")
	}
	for _, capability := range r.Capabilities {
		if !seen[capability.ID] {
			t.Fatal("registry capability not advertised:", capability.ID)
		}
	}
}
