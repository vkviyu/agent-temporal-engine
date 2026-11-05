package evaluation_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const summaryPrompt = `You are maintaining unreviewed engineering notes for a journal recovery investigation. Compress the supplied notes and new evidence into at most 160 words. Retain applicable constraints, exact evidence labels/identifiers, early facts needed later, the chosen/rejected branch and reasons, and uncertainty. Do not claim a model note is verified evidence. Output only the compressed notes.`

const finalPrompt = `Prepare the final recovery decision from only the supplied evidence. Return one JSON object, no markdown. Use null for an unavailable fact; never guess an opaque identifier. Keys: preserve_file (string), retry_unknown (boolean), reconciliation_key (string), expected_checksum (string), selected_branch (string), rejected_branch (string), rejected_reason (string), evidence_ids (array of strings). The file and reconciliation identifiers and checksum are case-sensitive. The rejected reason must explain the demonstrated failure, not just say "unsafe".`

type effectScore struct {
	Arm              string          `json:"arm"`
	Criteria         map[string]bool `json:"criteria"`
	Earned, Possible int
	Response         string
}

func TestEffectScoreRejectsMissingOrUnparseableEvidence(t *testing.T) {
	for _, raw := range []string{"", "{}", "```json\n{}\n```", `{"retry_unknown":null}`} {
		if got := score("fixture", raw); got.Earned != 0 {
			t.Fatalf("missing evidence earned credit: %+v", got)
		}
	}
	good := `{"preserve_file":"CURRENT.preincident-7Q","retry_unknown":false,"reconciliation_key":"opal-37-Z","expected_checksum":"6d0b8fA2","selected_branch":"evidence-first","rejected_branch":"retry-on-timeout","rejected_reason":"duplicate append writes after timeout","evidence_ids":["C-preserve","C-unknown","E-checksum","B-safe","B-failure"]}`
	if got := score("fixture", good); got.Earned != got.Possible {
		t.Fatalf("complete independently specified answer lost credit: %+v", got)
	}
}

func score(arm, response string) effectScore {
	s := effectScore{Arm: arm, Criteria: map[string]bool{}, Possible: 8, Response: response}
	var out struct {
		Preserve  string   `json:"preserve_file"`
		Retry     *bool    `json:"retry_unknown"`
		Reconcile string   `json:"reconciliation_key"`
		Checksum  string   `json:"expected_checksum"`
		Selected  string   `json:"selected_branch"`
		Rejected  string   `json:"rejected_branch"`
		Reason    string   `json:"rejected_reason"`
		Evidence  []string `json:"evidence_ids"`
	}
	err := json.Unmarshal([]byte(strings.TrimSpace(response)), &out)
	s.Criteria["preserve-exact-file"] = err == nil && out.Preserve == "CURRENT.preincident-7Q"
	s.Criteria["never-retry-unknown"] = err == nil && out.Retry != nil && !*out.Retry
	s.Criteria["recall-reconciliation-key"] = err == nil && out.Reconcile == "opal-37-Z"
	s.Criteria["recall-checksum"] = err == nil && out.Checksum == "6d0b8fA2"
	s.Criteria["select-evidence-first"] = err == nil && out.Selected == "evidence-first"
	s.Criteria["identify-rejected-branch"] = err == nil && out.Rejected == "retry-on-timeout"
	reason := strings.ToLower(out.Reason)
	s.Criteria["use-failure-counterexample"] = err == nil && (strings.Contains(reason, "duplicat") || strings.Contains(reason, "twice")) && (strings.Contains(reason, "write") || strings.Contains(reason, "append"))
	ids := map[string]bool{}
	for _, id := range out.Evidence {
		ids[id] = true
	}
	s.Criteria["explain-source-coverage"] = err == nil && ids["C-preserve"] && ids["C-unknown"] && ids["E-checksum"] && ids["B-safe"] && ids["B-failure"]
	for _, ok := range s.Criteria {
		if ok {
			s.Earned++
		}
	}
	return s
}

// selectEvidence uses the production selector over the actual persisted corpus.
// Relevance and mandatory IDs are explicit host proposals fixed before model
// outputs, not learned semantic retrieval or an oracle provided to the model.
func (h *evalHost) selectEvidence(all []evalFact, mandatory []evalFact, branch []evalFact) ([]evalFact, *knowledge.ContextSelectionRecord) {
	h.t.Helper()
	known := h.known()
	a := known.Anchor.Clone()
	a.Cursor = &model.Cursor{Store: known.Fence.Store, Space: "project", Revision: known.Revision.ID}
	v, err := h.s.OpenView(context.Background(), api.OpenViewRequest{Space: "project", Principal: "owner", Mode: "execution_knowledge", Anchor: &a})
	if err != nil {
		h.t.Fatal(err)
	}
	defer h.s.CloseView(context.Background(), api.CloseViewRequest{Ref: v.Ref})
	source := knowledge.SourceAnchor{Anchor: a, Frontier: known.Frontier, ViewDigest: v.ViewDigest}
	r := knowledge.ContextSelectionRequest{Snapshot: known.Snapshot, Source: source, Capacity: knowledge.ContextCapacity{Window: 4096, OutputReserve: 1024, FramingReserve: 256}, Tokenizer: "utf8-quarter-estimate/v1", CountQuality: knowledge.TokenCountEstimated, PolicyVersion: knowledge.ContextSelectionPolicy}
	byID := map[model.ObjectID]evalFact{}
	for _, f := range all {
		byID[f.Observation.ID] = f
		raw, _ := json.Marshal(map[string]string{"id": f.Name, "text": f.Text})
		r.Costs = append(r.Costs, knowledge.ContextCost{Observation: f.Observation.ID, Tokens: uint64((len(raw) + 3) / 4)})
	}
	for _, f := range mandatory {
		r.Mandatory = append(r.Mandatory, f.Observation.ID)
	}
	b := knowledge.ContextBundle{Conclusion: branch[0].Observation.ID, Evidence: []model.ObjectID{branch[1].Observation.ID}, Counterexamples: []model.ObjectID{branch[2].Observation.ID}, Benefit: 10}
	b.ID = knowledge.ContextBundleID(b)
	r.Bundles = []knowledge.ContextBundle{b}
	record, err := knowledge.RecordContextSelection(r)
	if err != nil {
		h.t.Fatal(err)
	}
	var chosen []evalFact
	for _, id := range record.Result.Selected {
		f, ok := byID[id]
		if !ok {
			h.t.Fatal("selector returned unknown fact")
		}
		chosen = append(chosen, f)
	}
	return chosen, &record
}

// Three paired real-model trials use the same three recursive summary rounds.
// The recent-note arm and evidence-recall arm have equal request/output caps;
// actual usage is retained rather than claimed equal. This is a controlled
// engineering evidence task, not an hours-long autonomous coding benchmark.
func TestRealModelRecursiveSummaryAndEvidenceRecall(t *testing.T) {
	p := provider(t)
	for trial := 1; trial <= 3; trial++ {
		t.Run(fmt.Sprintf("paired-%d", trial), func(t *testing.T) {
			h := newEvalHost(t, p, fmt.Sprintf("summary-paired-%d", trial))
			var all []evalFact
			add := func(name, text string) evalFact { f := h.fact(name, text); all = append(all, f); return f }
			preserve := add("C-preserve", "User invariant: preserve the exact preincident file CURRENT.preincident-7Q; never overwrite or rename it during recovery.")
			unknown := add("C-unknown", "User invariant: the append has unknown outcome and is non-idempotent. Never retry it. Reconcile using the exact external ledger key opal-37-Z before any replacement action.")
			checksum := add("E-checksum", "Early checksum measurement for the accepted historical payload is exactly 6d0b8fA2. It is case-sensitive; later health counters do not replace it.")
			safe := add("B-safe", "Branch decision: select evidence-first; reconcile actual append outcome, preserve original bytes, validate published floor, then restore the verified publication.")
			support := add("B-support", "Verifier accepted the evidence-first branch: after crash and reopen the durable floor, original receipt and preserved file are unchanged.")
			failure := add("B-failure", "Rejected branch retry-on-timeout was experimentally tried in a disposable fixture. It appended the selected result twice because network timeout did not mean the first non-idempotent write had no effect. Keep this counterexample when pruning the branch.")
			initial := []evalFact{preserve, unknown, checksum, safe, support, failure}
			var notes evalFact
			var recent []evalFact
			for round := 1; round <= 3; round++ {
				inputs := []evalFact{}
				if round == 1 {
					inputs = append(inputs, initial...)
				} else {
					inputs = append(inputs, notes)
				}
				recent = nil
				for i := 0; i < 24; i++ {
					f := add(fmt.Sprintf("round-%d-health-%02d", round, i), fmt.Sprintf("Operational inspection %d/%d: cache hit ratio=%d%%; worker queue=%d; read latency=%d ms; these measurements do not establish external append outcome or replace durable recovery evidence.", round, i, 70+i%17, i%5, 2+i%7))
					inputs = append(inputs, f)
					recent = append(recent, f)
				}
				text, record := h.call(fmt.Sprintf("summary-%d", round), summaryPrompt, inputs, 768, nil)
				h.summary(fmt.Sprintf("summary-%d", round), text, record, inputs)
				notes = add(fmt.Sprintf("unreviewed-notes-%d", round), text)
				h.reopen()
			}
			baseline := append([]evalFact{notes}, recent[len(recent)-8:]...)
			plain, _ := h.call("final-recent-note", finalPrompt, baseline, 1024, nil)
			chosen, selection := h.selectEvidence(all, []evalFact{preserve, unknown, checksum, notes}, []evalFact{safe, support, failure})
			recalled, _ := h.call("final-evidence-recall", finalPrompt, chosen, 1024, selection)
			scores := []effectScore{score("recursive-notes-plus-recent-8", plain), score("same-notes-plus-explicit-ate-evidence", recalled)}
			artifact(t, h.dir, "scores", map[string]any{"task_version": "journal-recovery-evidence/v1", "model": p.model, "trial": trial, "summary_rounds": 3, "scores": scores, "request_byte_cap": 20000, "final_max_output_tokens": 1024, "estimated_window": 4096, "token_quality": "estimated; provider actual usage in raw responses", "relevance": "host-declared mandatory IDs and one branch evidence/counterexample bundle", "limitations": []string{"controlled task, not autonomous coding", "unreviewed summaries, no automatic memory promotion", "small sample, no significance or universal quality claim", "same capacity caps, actual input tokens may differ", "proxy upstream framing is not controlled"}})
			for _, s := range scores {
				t.Logf("%s: %d/%d", s.Arm, s.Earned, s.Possible)
			}
			if _, err := h.s.CompactAuditFull(context.Background()); err != nil {
				t.Fatal(err)
			}
			h.reopen()
		})
	}
}

// V2 adds isolated factual review and typed Summary adoption. V1 artifacts and
// score() remain frozen; run this exact test name to avoid rerunning V1.
func TestRealModelTypedSummaryReviewAndEvidenceRecallV2(t *testing.T) {
	p := provider(t)
	for trial := 1; trial <= 3; trial++ {
		t.Run(fmt.Sprintf("paired-%d", trial), func(t *testing.T) {
			h := newEvalHost(t, p, fmt.Sprintf("summary-v2-paired-%d", trial))
			var all []evalFact
			add := func(name, text string) evalFact { f := h.fact(name, text); all = append(all, f); return f }
			preserve := add("C-preserve", "User invariant: preserve the exact preincident file CURRENT.preincident-7Q; never overwrite or rename it during recovery.")
			unknown := add("C-unknown", "User invariant: the append has unknown outcome and is non-idempotent. Never retry it. Reconcile using the exact external ledger key opal-37-Z before any replacement action.")
			checksum := add("E-checksum", "Early checksum measurement for the accepted historical payload is exactly 6d0b8fA2. It is case-sensitive; later health counters do not replace it.")
			safe := add("B-safe", "Branch decision: select evidence-first; reconcile actual append outcome, preserve original bytes, validate published floor, then restore the verified publication.")
			support := add("B-support", "Verifier accepted the evidence-first branch: after crash and reopen the durable floor, original receipt and preserved file are unchanged.")
			failure := add("B-failure", "Rejected branch retry-on-timeout was experimentally tried in a disposable fixture. It appended the selected result twice because network timeout did not mean the first non-idempotent write had no effect. Keep this counterexample when pruning the branch.")
			initial := []evalFact{preserve, unknown, checksum, safe, support, failure}
			var notes evalFact
			var recent []evalFact
			for round := 1; round <= 3; round++ {
				inputs := []evalFact{}
				if round == 1 {
					inputs = append(inputs, initial...)
				} else {
					inputs = append(inputs, notes)
				}
				recent = nil
				for i := 0; i < 24; i++ {
					f := add(fmt.Sprintf("round-%d-health-%02d", round, i), fmt.Sprintf("Operational inspection %d/%d: cache hit ratio=%d%%; worker queue=%d; read latency=%d ms; these measurements do not establish external append outcome or replace durable recovery evidence.", round, i, 70+i%17, i%5, 2+i%7))
					inputs = append(inputs, f)
					recent = append(recent, f)
				}
				text, record := h.call(fmt.Sprintf("summary-%d", round), summaryPrompt, inputs, 768, nil)
				var extras []evalFact
				var summary knowledge.Summary
				var accepted bool
				notes, extras, summary, accepted = h.reviewedSummary(fmt.Sprintf("summary-%d", round), text, record, inputs)
				all = append(all, extras...)
				if !accepted {
					artifact(t, h.dir, "scores", map[string]any{"task_version": "journal-recovery-evidence/v2", "scorer_version": "journal-recovery-evidence/v1", "trial": trial, "trial_status": "summary-rejected", "rejected_round": round, "summary": summary.ID, "review_policy": summaryReviewPolicy, "final_calls_performed": false})
					h.reopen()
					t.Logf("v2 trial stopped after factual review rejected round %d; no rejected summary adopted", round)
					return
				}
				all = append(all, notes)
				h.reopen()
				h.assertTypedSummary(notes, summary)
			}
			baseline := append([]evalFact{notes}, recent[len(recent)-8:]...)
			plain, _ := h.call("final-recent-note", finalPrompt, baseline, 1024, nil)
			chosen, selection := h.selectEvidence(all, []evalFact{preserve, unknown, checksum, notes}, []evalFact{safe, support, failure})
			recalled, _ := h.call("final-evidence-recall", finalPrompt, chosen, 1024, selection)
			scores := []effectScore{score("reviewed-typed-notes-plus-recent-8", plain), score("same-reviewed-typed-notes-plus-explicit-ate-evidence", recalled)}
			artifact(t, h.dir, "scores", map[string]any{"task_version": "journal-recovery-evidence/v2", "scorer_version": "journal-recovery-evidence/v1", "trial_status": "completed", "review_policy": summaryReviewPolicy, "model": p.model, "trial": trial, "summary_rounds": 3, "scores": scores, "request_byte_cap": 20000, "final_max_output_tokens": 1024, "estimated_window": 4096, "token_quality": "estimated; provider actual usage in raw responses", "relevance": "host-declared mandatory IDs and one branch evidence/counterexample bundle", "limitations": []string{"controlled task, not autonomous coding", "independently model-reviewed typed summaries, no automatic memory promotion", "review permits omissions and is not a proof of factual truth", "review adds model calls shared by both final arms", "small sample, no significance or universal quality claim", "same capacity caps, actual input tokens may differ", "proxy upstream framing is not controlled"}})
			for _, s := range scores {
				t.Logf("%s: %d/%d", s.Arm, s.Earned, s.Possible)
			}
			if _, err := h.s.CompactAuditFull(context.Background()); err != nil {
				t.Fatal(err)
			}
			h.reopen()
		})
	}
}

func (h *evalHost) assertTypedSummary(fact evalFact, expected knowledge.Summary) {
	h.t.Helper()
	if fact.Observation.PayloadKind != "summary" || fact.Observation.Payload != expected.ID {
		h.t.Fatal("summary reuse lost typed identity")
	}
	got, err := h.s.GetSummary(context.Background(), api.ReadSummaryRequest{Space: h.space, Principal: "owner", ID: expected.ID, Anchor: &fact.Observation.Source.Anchor})
	gotCanonical, gotErr := got.Summary.Canonical()
	wantCanonical, wantErr := expected.Canonical()
	if err != nil || !got.Found || gotErr != nil || wantErr != nil || !bytes.Equal(gotCanonical, wantCanonical) || got.Summary.Validation.Status != "validated" {
		h.t.Fatalf("reopened summary source differs: found=%v err=%v got=%#v expected=%#v", got.Found, err, got.Summary, expected)
	}
	found := false
	for _, observation := range h.known().Snapshot.Observations {
		if observation.ID == fact.Observation.ID {
			found = reflect.DeepEqual(observation, fact.Observation)
		}
	}
	if !found {
		h.t.Fatal("reopened execution knowledge lost adopted typed summary")
	}
}

func TestSummaryReviewJSONPolicy(t *testing.T) {
	for _, raw := range []string{
		`{}`, `{"supported":null,"contradictions":[],"unsupported_claims":[],"reason":"x"}`,
		`{"supported":true,"supported":false,"contradictions":[],"unsupported_claims":[],"reason":"x"}`,
		`{"supported":true,"contradictions":["x"],"unsupported_claims":[],"reason":"x"}`,
		`{"supported":true,"contradictions":null,"unsupported_claims":[],"reason":"x"}`,
		`{"supported":true,"contradictions":[],"unsupported_claims":[],"reason":"x","extra":1}`,
		`{"supported":true,"contradictions":[],"unsupported_claims":[],"reason":"x"} {}`,
	} {
		if _, err := decodeSummaryReview(raw); err == nil {
			t.Fatalf("invalid review accepted: %s", raw)
		}
	}
	for _, supported := range []string{"true", "false"} {
		if _, err := decodeSummaryReview(`{"supported":` + supported + `,"contradictions":[],"unsupported_claims":[],"reason":"omissions allowed"}`); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTypedSummaryReviewLifecycleWithoutProvider(t *testing.T) {
	for _, accept := range []bool{true, false} {
		t.Run(fmt.Sprintf("accepted-%v", accept), func(t *testing.T) {
			t.Setenv("ATE_EVAL_ARTIFACTS", t.TempDir())
			generation := "Preserve ORIGINAL.fixture. Omitted other details."
			reviewerCalls := 0
			transport := evalRoundTripper(func(r *http.Request) (*http.Response, error) {
				var request struct {
					Input string `json:"input"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					return fixtureResponse(http.StatusBadRequest, err.Error()), nil
				}
				output := generation
				if strings.HasPrefix(request.Input, summaryReviewPrompt) {
					reviewerCalls++
					verdict := map[string]any{"supported": accept, "contradictions": []string{}, "unsupported_claims": []string{}, "reason": "omission is allowed; checked exact candidate against supplied sources"}
					if !accept {
						verdict["unsupported_claims"] = []string{"candidate assertion is not supported"}
					}
					raw, _ := json.Marshal(verdict)
					output = string(raw)
				}
				body, _ := json.Marshal(map[string]any{"id": "offline-fixture", "model": "fixture", "status": "completed", "output": []any{map[string]any{"type": "message", "content": []any{map[string]any{"type": "output_text", "text": output}}}}})
				return fixtureResponse(http.StatusOK, string(body)), nil
			})
			h := newEvalHost(t, responsesProvider{base: "http://fixture.invalid/v1", model: "fixture", key: "fixture-secret", client: &http.Client{Transport: transport}}, "typed-summary")
			input := h.fact("source-invariant", "Preserve ORIGINAL.fixture. Never retry an unknown append.")
			inputs := []evalFact{input}
			rounds := 1
			if accept {
				rounds = 2
			}
			for round := 1; round <= rounds; round++ {
				name := fmt.Sprintf("summary-%d", round)
				text, record := h.call(name, summaryPrompt, inputs, 768, nil)
				notes, extras, summary, adopted := h.reviewedSummary(name, text, record, inputs)
				if adopted != accept || len(extras) != 2 || summary.Validation.Status == "unreviewed" || len(summary.Validation.Evidence) != 2 {
					t.Fatal("review/adoption policy mismatch")
				}
				for i, source := range summary.Sources {
					if source.PayloadKind != inputs[i].Observation.PayloadKind {
						t.Fatal("SummarySource lost payload kind")
					}
				}
				h.reopen()
				if accept {
					h.assertTypedSummary(notes, summary)
					inputs = []evalFact{notes}
				} else {
					if summary.Validation.Status != "rejected" {
						t.Fatal("rejection not persisted")
					}
					for _, observation := range h.known().Snapshot.Observations {
						if observation.PayloadKind == "summary" {
							t.Fatal("rejected summary adopted")
						}
					}
					got, err := h.s.GetSummary(context.Background(), api.ReadSummaryRequest{Space: h.space, Principal: "owner", ID: summary.ID})
					if err != nil || !got.Found || got.Summary.Validation.Status != "rejected" {
						t.Fatal("rejected record lost on reopen", err)
					}
				}
			}
			if reviewerCalls != rounds {
				t.Fatal("review calls were bypassed", reviewerCalls)
			}
		})
	}
}
