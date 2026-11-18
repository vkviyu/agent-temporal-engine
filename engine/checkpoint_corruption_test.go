package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// These tests deliberately rewrite the entire checkpoint -> manifest ->
// CURRENT digest chain. They test projection validity, not checksum rejection.
func TestCheckpointDigestValidMalformedProjectionNeverRepairs(t *testing.T) {
	cases := []string{
		"empty-events", "missing-core", "missing-head", "extra-head", "head-sequence", "head-predecessor",
		"missing-fact", "extra-alias", "wrong-fact", "history-digest", "history-principal", "genesis",
		"missing-object", "extra-object", "object-location", "missing-receipt", "missing-hint", "extra-hint",
		"schema-mismatch", "magic-profile-mismatch", "source-profile-mismatch", "source-unknown-epoch",
		"hidden-agent-records", "empty-events-profile-mismatch",
		"unknown-epoch-fallback", "schema1-fallback", "source-schema1",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			d, err := storage.OpenDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			options := Options{AgentProfile: true, EventProfile: true, Journal: storage.JournalOptions{TailPolicy: storage.TailQuarantine}}
			e, err := Create(d, options)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			for _, id := range []model.RequestID{"one", "two", "three"} {
				out, err := e.Mutate(context.Background(), api.MutateRequest{
					Key: model.RequestKey{Space: "space", Principal: "owner", RequestID: id},
					Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte("key"), Kind: api.MutationPut,
						Value: api.Value{Kind: api.ValueBytes, Data: []byte(id)}}}},
				})
				if err != nil || out.Code != api.CodeDurableAck {
					t.Fatal(out, err)
				}
			}
			task := agentprofile.Task{ID: "task", Space: "space", Owner: "owner", Goal: "checkpoint test", GoalEpoch: 1, Status: agentprofile.TaskOpen}
			out, err := e.ApplyAgent(context.Background(), api.ApplyAgentRequest{
				Key:     model.RequestKey{Space: "space", Principal: "owner", RequestID: "task"},
				Command: agentprofile.Command{CreateTask: &task},
			})
			if err != nil || out.Code != api.CodeDurableAck {
				t.Fatal(out, err)
			}
			before := e.publication
			if _, err := e.CreateRecoveryCheckpoint(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			e.publication = before // encoder must describe the certified source generation
			tipKey := eventStreamKey("space", "owner")
			factKey := profileFactKey("space", "owner", "CreateTask", "task")
			if _, ok := e.eventHeads[factKey]; !ok {
				t.Fatal("fixture lacks fact head")
			}
			switch name {
			case "empty-events", "empty-events-profile-mismatch":
				clear(e.events)
				clear(e.eventHeads)
				e.eventBlocks = nil
			case "missing-core":
				for block := e.eventBlocks; block != nil; block = block.previous {
					if block.entries[0].entry.LSN == 2 {
						delete(e.events, block.entries[0].id)
						block.entries = nil
					}
				}
			case "missing-head":
				delete(e.eventHeads, tipKey)
			case "extra-head", "extra-alias":
				e.eventHeads[profileFactKey("space", "owner", "unregistered-alias", "task")] = e.eventHeads[factKey]
			case "head-sequence":
				head := e.eventHeads[tipKey]
				head.Sequence = 0
				e.eventHeads[tipKey] = head
			case "head-predecessor":
				head := e.eventHeads[tipKey]
				head.ID = e.eventHeads[factKey].ID
				e.eventHeads[tipKey] = head
			case "missing-fact":
				delete(e.eventHeads, factKey)
			case "wrong-fact":
				e.eventHeads[factKey] = eventHead{ID: e.eventHeads[tipKey].ID}
			case "history-digest", "history-principal":
				v := e.history[1]
				if name == "history-digest" {
					v.fence.CommitDigest = model.CommitDigest(strings.Repeat("a", 64))
				} else {
					v.principal = "other"
				}
				e.history[1] = v
				for tx, original := range e.envelopes {
					if original.fence.LSN == 1 {
						e.envelopes[tx] = v
					}
				}
			case "genesis":
				v := e.history[0]
				v.space = "space"
				e.history[0] = v
			case "missing-object", "object-location":
				// Select a historical value that has been overwritten. Merely
				// checking current roots would not discover its omission.
				e.index.source = checkpointDiskReader{root: root, t: t}
				for id := range e.index.entries {
					obj, err := readObject(context.Background(), e.index, id)
					if err != nil {
						t.Fatal(err)
					}
					if obj.Type == valueType {
						v, err := readValue(obj)
						if err == nil && string(v.Data) == "one" {
							if name == "missing-object" {
								delete(e.index.entries, id)
							} else {
								loc := e.index.entries[id]
								loc.PayloadOffset++
								e.index.entries[id] = loc
							}
							break
						}
					}
				}
			case "missing-receipt":
				e.index.source = checkpointDiskReader{root: root, t: t}
				for id := range e.index.entries {
					obj, err := readObject(context.Background(), e.index, id)
					if err != nil {
						t.Fatal(err)
					}
					if obj.Type == receiptType {
						delete(e.index.entries, id)
						break
					}
				}
			case "extra-object":
				for _, loc := range e.index.entries {
					e.index.entries[model.ObjectID(strings.Repeat("a", 64))] = loc
					break
				}
			case "missing-hint":
				for hint := range e.receiptHints {
					delete(e.receiptHints, hint)
					break
				}
			case "extra-hint":
				// Replace one so it remains inside the count ceiling.
				for hint := range e.receiptHints {
					delete(e.receiptHints, hint)
					break
				}
				e.receiptHints[[32]byte{99}] = struct{}{}
			}
			var raw bytes.Buffer
			if err := e.encodeRecoveryCheckpoint(&raw); err != nil {
				t.Fatal(err)
			}
			input := bytes.NewReader(raw.Bytes())
			var h checkpointHeader
			if err := readCheckpointRecord(input, &h); err != nil {
				t.Fatal(err)
			}
			descriptorSchema := recoveryCheckpointSchema
			switch name {
			case "schema-mismatch":
				h.Schema = 1
			case "magic-profile-mismatch":
				h.Magic = "bad"
				h.AgentProfile = false
			case "source-profile-mismatch":
				h.Publication.Generation++
				h.AgentProfile = false
			case "source-unknown-epoch":
				h.Schema = recoveryCheckpointSchema + 1
				descriptorSchema = h.Schema
				h.Publication.Generation++
			case "unknown-epoch-fallback":
				h.Schema = recoveryCheckpointSchema + 1
				descriptorSchema = h.Schema
			case "schema1-fallback", "source-schema1":
				h.Schema = 1
				descriptorSchema = h.Schema
				if name == "source-schema1" {
					h.Publication.Generation++
				}
			case "hidden-agent-records", "empty-events-profile-mismatch":
				h.AgentProfile = false
			}
			var modified bytes.Buffer
			if err := writeCheckpointRecord(&modified, h); err != nil {
				t.Fatal(err)
			}
			if _, err := io.Copy(&modified, input); err != nil {
				t.Fatal(err)
			}
			checkpointRewriteForTest(t, root, modified.Bytes(), descriptorSchema)
			// There is repairable tail. A malformed projection must fail before
			// quarantine, truncation, CURRENT publication or witness SyncDir.
			current := checkpointCurrentForTest(t, root)
			manifest := checkpointManifestForTest(t, root, current)
			segment := filepath.Join(root, "segments", "SEGMENT."+manifest.SegmentID)
			b, err := os.ReadFile(segment)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(segment, append(b, []byte("unpublished")...), 0600); err != nil {
				t.Fatal(err)
			}
			probe := &checkpointMutationProbe{DurableIO: d}
			opened, report, err := OpenExisting(probe, options)
			if name == "unknown-epoch-fallback" || name == "schema1-fallback" {
				if err != nil || opened == nil {
					t.Fatal("consistent unknown epoch did not replay", err)
				}
				defer opened.Close()
				if report.CheckpointTransactions != 0 || report.ScannedTransactions != before.Position.LSN ||
					report.TailDisposition != "quarantined" {
					t.Fatal("fallback did not complete before repair", report)
				}
				if opened.publication.Fence() != before.Fence() || opened.roots != e.roots || opened.rootSet != e.rootSet ||
					!maps.Equal(opened.history, e.history) || !maps.Equal(opened.envelopes, e.envelopes) ||
					!maps.Equal(opened.receiptHints, e.receiptHints) || !maps.Equal(opened.eventHeads, e.eventHeads) {
					t.Fatal("genesis replay changed the certified state or history")
				}
				return
			}
			if opened != nil {
				opened.Close()
				t.Fatal("malformed projection accepted")
			}
			if err == nil || errors.Is(err, storage.ErrUnsupported) {
				t.Fatalf("malformed projection fell back: %v", err)
			}
			if probe.mutations != 0 {
				t.Fatalf("failed open made %d mutations", probe.mutations)
			}
			after, err := os.ReadFile(segment)
			if err != nil || !bytes.Equal(after, append(b, []byte("unpublished")...)) {
				t.Fatal("tail changed", err)
			}
		})
	}
}

func checkpointCurrentForTest(t *testing.T, root string) storage.Current {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "CURRENT"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := storage.DecodeCurrent(b)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func checkpointManifestForTest(t *testing.T, root string, c storage.Current) storage.Manifest {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "manifests", "MANIFEST."+c.ManifestID))
	if err != nil {
		t.Fatal(err)
	}
	m, err := storage.DecodeManifest(b)
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func checkpointRewriteForTest(t *testing.T, root string, raw []byte, schema uint64) {
	t.Helper()
	c := checkpointCurrentForTest(t, root)
	m := checkpointManifestForTest(t, root, c)
	ref := m.RecoveryCheckpoint
	ref.Schema, ref.Size, ref.Digest = schema, uint64(len(raw)), storage.Digest(sha256.Sum256(raw))
	if err := os.WriteFile(filepath.Join(root, "checkpoints", "CHECKPOINT."+ref.ID), raw, 0600); err != nil {
		t.Fatal(err)
	}
	b, err := storage.EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifests", "MANIFEST."+m.ManifestID), b, 0600); err != nil {
		t.Fatal(err)
	}
	c.ManifestDigest = storage.Digest(sha256.Sum256(b))
	b, err = storage.EncodeCurrent(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "CURRENT"), b, 0600); err != nil {
		t.Fatal(err)
	}
}

// The fixture uses one physical segment; compaction/rotation remain covered by
// the separate public recovery integration tests.
type checkpointDiskReader struct {
	root string
	t    *testing.T
}

func (r checkpointDiskReader) ReadAt(p []byte, off int64) (int, error) {
	c := checkpointCurrentForTest(r.t, r.root)
	m := checkpointManifestForTest(r.t, r.root, c)
	f, err := os.Open(filepath.Join(r.root, "segments", "SEGMENT."+m.SegmentID))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return f.ReadAt(p, off)
}

type checkpointMutationProbe struct {
	storage.DurableIO
	mutations int
}

func (p *checkpointMutationProbe) SyncDir(name string) error {
	p.mutations++
	return p.DurableIO.SyncDir(name)
}
func (p *checkpointMutationProbe) Mkdir(name string) error {
	p.mutations++
	return p.DurableIO.Mkdir(name)
}
func (p *checkpointMutationProbe) Remove(name string) error {
	p.mutations++
	return p.DurableIO.Remove(name)
}
func (p *checkpointMutationProbe) CreateExclusive(name string) (storage.DurableFile, error) {
	p.mutations++
	return p.DurableIO.CreateExclusive(name)
}
func (p *checkpointMutationProbe) ReplaceAtomic(from, to string) error {
	p.mutations++
	return p.DurableIO.ReplaceAtomic(from, to)
}
func (p *checkpointMutationProbe) InstallNew(from, to string) error {
	p.mutations++
	return p.DurableIO.InstallNew(from, to)
}

func TestCheckpointBranchAliasProjectionMustBeComplete(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	d, err := storage.OpenDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	options := Options{AgentProfile: true, EventProfile: true}
	e, err := Create(d, options)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	key := func(id string) model.RequestKey {
		return model.RequestKey{Space: "space", Principal: "owner", RequestID: model.RequestID(id)}
	}
	agent := func(id string, command agentprofile.Command) {
		t.Helper()
		out, err := e.ApplyAgent(ctx, api.ApplyAgentRequest{Key: key(id), Command: command})
		if err != nil || out.Code != api.CodeDurableAck {
			t.Fatal(out, err)
		}
	}
	task := agentprofile.Task{ID: "task", Space: "space", Owner: "owner", Goal: "branch alias", GoalEpoch: 1, Status: agentprofile.TaskOpen}
	agent("task", agentprofile.Command{CreateTask: &task})
	execution := agentprofile.Execution{ID: "execution", Task: task.ID, Actor: "actor", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	agent("spawn", agentprofile.Command{Spawn: &execution})
	binding := agentprofile.AuthorityBinding{Deployment: "local", Holder: "worker", OwnerEpoch: 1, Incarnation: 1}
	agent("grant", agentprofile.Command{Control: &agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{
		Authority: agentprofile.ExecutionAuthority{Execution: execution.ID, Binding: binding, Status: "active", Evidence: model.ObjectID(strings.Repeat("a", 64))},
	}}})
	revision, err := graph.BuildRevision("space", "", nil, "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, command := range []graph.Command{
		{AddRevision: &graph.AddRevisionCommand{}},
		{ForkLocal: &graph.ForkLocalCommand{Branch: "branch", Source: revision.ID, Authority: graph.ExecutionAuthority{
			Execution: execution.ID, Deployment: binding.Deployment, Holder: binding.Holder, OwnerEpoch: 1, Incarnation: 1,
		}}},
	} {
		out, err := e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: key(string(rune('a' + i))), Command: command})
		if err != nil || out.Code != api.CodeDurableAck {
			t.Fatal(out, err)
		}
	}
	var alias string
	for k := range e.eventHeads {
		if strings.Contains(k, "Graph.BranchPosition") {
			alias = k
			break
		}
	}
	if alias == "" {
		t.Fatal("fixture did not create a real branch-position alias")
	}
	before := e.publication
	if _, err := e.CreateRecoveryCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e.publication = before
	heads := maps.Clone(e.eventHeads)
	for _, mode := range []string{"missing", "wrong-event", "extra"} {
		t.Run(mode, func(t *testing.T) {
			e.eventHeads = maps.Clone(heads)
			switch mode {
			case "missing":
				delete(e.eventHeads, alias)
			case "wrong-event":
				e.eventHeads[alias] = eventHead{ID: e.eventHeads[eventStreamKey("space", "owner")].ID}
			case "extra":
				e.eventHeads[alias+"extra"] = e.eventHeads[alias]
			}
			var raw bytes.Buffer
			if err := e.encodeRecoveryCheckpoint(&raw); err != nil {
				t.Fatal(err)
			}
			checkpointRewriteForTest(t, root, raw.Bytes(), recoveryCheckpointSchema)
			probe := &checkpointMutationProbe{DurableIO: d}
			opened, _, err := OpenExisting(probe, options)
			if opened != nil {
				opened.Close()
				t.Fatal("incorrect alias projection accepted")
			}
			if !errors.Is(err, storage.ErrCorruption) || probe.mutations != 0 {
				t.Fatal("alias validation failed after mutation", err)
			}
		})
	}
}
