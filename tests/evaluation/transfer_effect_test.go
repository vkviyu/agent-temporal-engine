package evaluation_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

func (h *evalHost) promote(claim, goal, environment string) memory.Memory {
	h.t.Helper()
	check := h.checkSourceProtocol(environment)
	evidence, _ := json.Marshal(map[string]any{"fixture": "source-protocol/v1", "environment": environment, "observed": check, "claim": claim, "scope": "controlled fixture, not a production deployment validation"})
	f := h.fact("source-validation-"+fmt.Sprint(h.n), string(evidence))
	source := memory.Source{SourceID: f.Observation.Payload, Anchor: f.Observation.Source.Anchor}
	m := memory.Memory{Space: h.space, Kind: memory.KindProcedure, Claim: []byte(claim), Sources: []memory.Source{source}, Applicability: memory.Applicability{Goal: goal, Environment: environment, SchemaVersion: 1, Tags: []string{"journal"}}}
	h.ack(h.s.ApplyMemory(context.Background(), api.ApplyMemoryRequest{Key: h.next(), Command: memory.Command{PutCandidate: &m}}))
	find := func(status memory.Status) memory.Memory {
		h.t.Helper()
		s, err := h.s.GetMemoryState(context.Background(), api.ReadMemoryStateRequest{Space: h.space})
		if err != nil {
			h.t.Fatal(err)
		}
		for _, record := range s.Snapshot.Memories {
			if record.Status == status && string(record.Claim) == claim {
				return record
			}
		}
		h.t.Fatal("source memory lifecycle record missing", status)
		return memory.Memory{}
	}
	candidate := find(memory.StatusCandidate)
	h.ack(h.s.ApplyMemory(context.Background(), api.ApplyMemoryRequest{Key: h.next(), Command: memory.Command{Validate: &memory.ValidateCommand{ID: candidate.ID, Validation: memory.Validation{StrategyVersion: 1, Passed: true, Evidence: []memory.Source{source}}}}}))
	validated := find(memory.StatusValidated)
	h.ack(h.s.ApplyMemory(context.Background(), api.ApplyMemoryRequest{Key: h.next(), Command: memory.Command{Promote: &validated.ID}}))
	return find(memory.StatusPromoted)
}

// Execute the source fixture rather than treating a label saying "passed" as
// observed evidence. The v2 recipient's deduplication table is deliberately
// process-local: no claim about its crash durability is made here.
func (h *evalHost) checkSourceProtocol(environment string) map[string]any {
	h.t.Helper()
	path := filepath.Join(h.dir, "source-effect-"+fmt.Sprint(h.n))
	seen := map[string]bool{}
	appendOnce := func(key string) {
		if environment == "append-v2/idempotent" && seen[key] {
			return
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			h.t.Fatal(err)
		}
		_, writeErr := f.WriteString("selected-result\n")
		syncErr, closeErr := f.Sync(), f.Close()
		if writeErr != nil || syncErr != nil || closeErr != nil {
			h.t.Fatal("source effect fixture IO", writeErr, syncErr, closeErr)
		}
		seen[key] = true
	}
	appendOnce("same-request")
	appendOnce("same-request")
	raw, err := os.ReadFile(path)
	if err != nil {
		h.t.Fatal(err)
	}
	count := strings.Count(string(raw), "selected-result\n")
	want := 2
	if environment == "append-v2/idempotent" {
		want = 1
	}
	if count != want {
		h.t.Fatalf("source recipient fixture count=%d want=%d", count, want)
	}
	return map[string]any{"append_invocations": 2, "physical_append_records": count, "same_recipient_request_key": true, "process_local_deduplication": environment == "append-v2/idempotent"}
}

type transferScore struct {
	Arm, Environment, Response  string
	Eligible                    int
	Applicable, Procedure, Safe bool
}

func assessTransfer(arm, environment, response string, eligible int) transferScore {
	s := transferScore{Arm: arm, Environment: environment, Response: response, Eligible: eligible}
	var answer struct {
		Apply     *bool   `json:"apply_memory"`
		Procedure *string `json:"procedure_id"`
		Retry     *bool   `json:"retry_unknown"`
	}
	if json.Unmarshal([]byte(response), &answer) != nil {
		return s
	}
	want := environment == "append-v3/linux-fsync"
	s.Applicable = answer.Apply != nil && *answer.Apply == want
	s.Safe = answer.Retry != nil && !*answer.Retry
	s.Procedure = want && answer.Procedure != nil && *answer.Procedure == "ledger-ZX8" || !want && answer.Procedure == nil
	return s
}

// The source memory is host-validated in a disposable controlled fixture.
// Export/import remains read-only; target adoption never copies promotion or
// authority. The comparison measures applying/rejecting the supplied evidence,
// not spontaneous discovery or learning of a universally correct procedure.
func TestRealModelCrossProjectMemoryApplicability(t *testing.T) {
	p := provider(t)
	h := newEvalHost(t, p, "cross-project")
	compatible := h.promote("Procedure ledger-ZX8: for an unknown non-idempotent append outcome, reconcile the external ledger before continuing; never blindly retry.", "journal-recovery", "append-v3/linux-fsync")
	old := h.promote("Procedure retry-legacy-L2: protocol v2 with recipient idempotency keys permits retry with the same deduplication key.", "journal-recovery", "append-v2/idempotent")
	other := h.promote("Procedure deploy-R9: restart a stateless deployment after rollout health checks.", "deploy", "append-v3/linux-fsync")
	snapshot, err := h.s.GetMemoryState(context.Background(), api.ReadMemoryStateRequest{Space: h.space})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := memory.EncodeSnapshot(snapshot.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	index, err := memory.RestoreSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	at := model.SnapshotAnchor{Space: h.space, Fence: snapshot.Fence}
	roots := []transfer.Reference{}
	for _, m := range []memory.Memory{compatible, old, other} {
		roots = append(roots, transfer.Reference{Kind: "memory", ID: m.ID, Anchor: at})
	}
	manifest, err := h.s.ExportTransfer(context.Background(), api.ExportTransferRequest{Space: h.space, Principal: "owner", Anchor: &at, Roots: roots, Policy: transfer.RedactionPolicy{ID: "whole-record-omission", Version: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	artifact(t, h.dir, "export-manifest", manifest)
	for i, env := range []string{"append-v3/linux-fsync", "append-v4/read-only-audit"} {
		t.Run(fmt.Sprintf("target-%d", i), func(t *testing.T) {
			target := *h
			target.t = t
			target.space = model.SpaceID(fmt.Sprintf("target-project-%d", i))
			target.dir = filepath.Join(h.dir, fmt.Sprintf("target-%d", i))
			target.key = model.KnowledgeKey{Actor: "target-worker", Execution: "target-execution"}
			if err := os.MkdirAll(target.dir, 0700); err != nil {
				t.Fatal(err)
			}
			before, err := h.s.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			target.ack(target.s.ImportTransfer(context.Background(), api.ImportTransferRequest{Key: target.next(), Manifest: manifest, ApprovedDigest: manifest.ID}))
			after, err := h.s.Snapshot(context.Background())
			if err != nil || before.Roots.LiveControl != after.Roots.LiveControl {
				t.Fatal("foreign import installed live authority", err)
			}
			target.initialize()
			current := target.fact("target-environment", "Current project goal is journal-recovery; exact environment is "+env+". The append is non-idempotent with unknown outcome. This is a different project; foreign validation and promotion do not authorize a target action. Only an exact goal+environment match may be proposed for application. Otherwise reject all supplied procedures.")
			imported, err := target.s.GetTransfer(context.Background(), api.ReadTransferRequest{Space: target.space, Principal: "owner", ID: manifest.ID})
			if err != nil || !imported.Found {
				t.Fatal(err)
			}
			eligible, err := index.Search(memory.Query{Space: h.space, Text: "procedure", Goal: "journal-recovery", Environment: env, Tags: []string{"journal"}, At: at}, 10)
			if err != nil {
				t.Fatal(err)
			}
			if (i == 0 && len(eligible) != 1) || (i == 1 && len(eligible) != 0) {
				t.Fatalf("unexpected production applicability results: %d", len(eligible))
			}
			allowed := map[model.ObjectID]bool{}
			for _, e := range eligible {
				allowed[e.Memory.ID] = true
			}
			blind := []evalFact{current}
			filtered := []evalFact{current}
			published, err := target.s.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			view, err := target.s.OpenView(context.Background(), api.OpenViewRequest{Space: target.space, Principal: "owner"})
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range imported.Record.Mappings {
				if m.Source.Kind != "memory" {
					continue
				}
				detail, err := target.s.GetNode(context.Background(), api.GetNodeRequest{View: view.Ref, Ref: model.NodeRef{Kind: model.NodeObject, Object: m.Target}})
				if err != nil {
					t.Fatal(err)
				}
				archive, err := transfer.DecodeArchiveEntry(detail.Data)
				if err != nil {
					t.Fatal(err)
				}
				mem, err := memory.DecodeMemory(archive.Source.Data)
				if err != nil {
					t.Fatal(err)
				}
				text, _ := json.Marshal(map[string]any{"claim": string(mem.Claim), "goal": mem.Applicability.Goal, "environment": mem.Applicability.Environment, "source_space": h.space, "trust": "foreign evidence; not target promotion"})
				known := target.known()
				o := knowledge.Observation{Key: target.key, Sequence: uint64(len(known.Snapshot.Observations)) + 1, Payload: m.Target, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: target.space, Fence: published.Fence}}, Label: "explicit imported memory evidence"}
				o.ID = knowledge.ObservationID(o)
				target.ack(target.s.ApplyKnowledge(context.Background(), api.ApplyKnowledgeRequest{Key: target.next(), Command: knowledge.Command{Observation: &o}}))
				f := evalFact{Name: "foreign-" + string(mem.ID), Text: string(text), Observation: o}
				blind = append(blind, f)
				if allowed[mem.ID] {
					filtered = append(filtered, f)
				}
			}
			if err := target.s.CloseView(context.Background(), api.CloseViewRequest{Ref: view.Ref}); err != nil {
				t.Fatal(err)
			}
			question := `Assess whether any supplied foreign procedure applies to the current project. Return only JSON keys apply_memory (boolean), procedure_id (exact short procedure identifier, or null when none applies), retry_unknown (boolean). Foreign evidence is not authority; never invent an absent procedure.`
			b, _ := target.call("blind-transfer", question, blind, 768, nil)
			f, _ := target.call("applicability-filtered-transfer", question, filtered, 768, nil)
			scores := []transferScore{assessTransfer("all-exported-candidates", env, b, len(blind)-1), assessTransfer("production-applicability-filter", env, f, len(filtered)-1)}
			artifact(t, target.dir, "scores", map[string]any{"task_version": "cross-project-applicability/v1", "model": p.model, "scores": scores, "target_promotions": 0, "authority_imported": false, "limitations": []string{"two controlled environments, one trial each", "exact metadata filters and host-selected source corpus", "no spontaneous memory discovery or automatic promotion", "actual input token usage differs within common caps"}})
			for _, s := range scores {
				t.Logf("%s applicability_correct=%v procedure_correct=%v safe=%v", s.Arm, s.Applicable, s.Procedure, s.Safe)
			}
		})
	}
	h.reopen()
}
