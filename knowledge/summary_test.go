package knowledge

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func summaryTestID(text string) model.ObjectID {
	sum := sha256.Sum256([]byte(text))
	return model.ObjectID(hex.EncodeToString(sum[:]))
}

func summaryTestAnchor(lsn uint64) model.SnapshotAnchor {
	return model.SnapshotAnchor{Space: "summary-space", Fence: model.CommitFence{Store: "summary-store", LSN: lsn, CommitDigest: model.CommitDigest(summaryTestID("published-cut"))}}
}

func summaryFixture(t testing.TB) (Summary, Snapshot) {
	t.Helper()
	key := model.KnowledgeKey{Actor: "worker", Execution: "long-task"}
	snapshot := Snapshot{}
	for i, label := range []string{"early mandatory constraint", "promoted decision memory", "omitted unrelated evidence", "future failure constraint"} {
		o := Observation{Key: key, Source: SourceAnchor{Anchor: summaryTestAnchor(uint64(i + 1))}, Payload: summaryTestID(label), Sequence: uint64(i + 1), Label: label}
		if i == 1 {
			o.PayloadKind = "memory"
		}
		o.ID = ObservationID(o)
		snapshot.Observations = append(snapshot.Observations, o)
	}
	versions, err := Revisions(snapshot, "summary-space", key)
	if err != nil {
		t.Fatal(err)
	}
	anchor := summaryTestAnchor(5)
	anchor.Knowledge = &key
	anchor.Cursor = &model.Cursor{Store: anchor.Fence.Store, Space: anchor.Space, Revision: versions[3].ID}
	selected, _, err := SelectRevision(snapshot, "summary-space", key, versions[3].ID)
	if err != nil {
		t.Fatal(err)
	}
	frontier, err := Frontier(selected, key)
	if err != nil {
		t.Fatal(err)
	}
	s := Summary{Space: "summary-space", Source: SourceAnchor{Anchor: anchor, Frontier: frontier, ViewDigest: summaryTestID("authenticated-view")}, Topics: []SummaryTopic{{Name: "goal", Observations: []model.ObjectID{snapshot.Observations[0].ID}}, {Name: "decision", Observations: []model.ObjectID{snapshot.Observations[1].ID}}}, Omissions: []SummaryOmission{{Observation: snapshot.Observations[2].ID, Reason: "not relevant to the declared goal"}}, Applicability: SummaryApplicability{Goal: "repair", Environment: "go1.27", SchemaVersion: 1, Tags: []string{"go", "local"}}, Confidence: 700, GeneratedAt: time.Date(2026, 11, 2, 10, 0, 0, 123456789, time.UTC), VisibleAt: time.Date(2026, 11, 2, 10, 1, 0, 0, time.UTC), Generation: SummaryGeneration{Activity: "aux-summary-1", Attempt: "aux-summary-1-attempt-1", Result: "aux-summary-1-result", Model: "deterministic-provider/model-v1", PromptVersion: "summary-prompt/v1", PolicyVersion: "retain-constraints/v1", Input: RecordedContent{Object: summaryTestID("exact-generation-input"), Anchor: summaryTestAnchor(6), Encoding: "application/json"}, Output: RecordedContent{Object: summaryTestID("exact-summary-output"), Anchor: summaryTestAnchor(8), Encoding: "text/plain;charset=utf-8"}}, Validation: SummaryValidation{Status: "validated", Reviewer: "trusted-reviewer", PolicyVersion: "review-policy/v1", Evidence: []RecordedContent{{Object: summaryTestID("review-evidence"), Anchor: summaryTestAnchor(10), Encoding: "application/json"}}}}
	for _, observation := range snapshot.Observations[:2] {
		s.Sources = append(s.Sources, SummarySource{Observation: observation.ID, Content: observation.Payload, PayloadKind: observation.PayloadKind})
	}
	s.SourceSetDigest = SummarySourceSetDigest(s.Source, s.Sources)
	s.ID = SummaryID(s)
	if s.ID == "" {
		t.Fatal("invalid summary fixture")
	}
	return s, snapshot
}

func TestSummaryCanonicalAndExactSourceClosure(t *testing.T) {
	s, snapshot := summaryFixture(t)
	if err := VerifySummarySources(s, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := s.ValidateBefore(summaryTestAnchor(12).Fence); err != nil {
		t.Fatal(err)
	}
	raw, err := s.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeSummary(raw)
	if err != nil || decoded.ID != s.ID || decoded.SourceSetDigest != s.SourceSetDigest || !decoded.GeneratedAt.Equal(s.GeneratedAt) || !decoded.VisibleAt.Equal(s.VisibleAt) || !reflect.DeepEqual(decoded.Generation, s.Generation) {
		t.Fatalf("summary round trip changed provenance: %+v %v", decoded, err)
	}
	if err := VerifySummarySources(decoded, snapshot); err != nil {
		t.Fatal(err)
	}
	again, err := decoded.Canonical()
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatal("decoded summary changed canonical bytes", err)
	}
	// Set presentation order is not semantic identity. Input/output bytes and
	// generated/visible instants are, and remain distinct below.
	permuted := s.Clone()
	slices.Reverse(permuted.Sources)
	slices.Reverse(permuted.Topics)
	slices.Reverse(permuted.Applicability.Tags)
	if permuted.SourceSetDigest != SummarySourceSetDigest(permuted.Source, permuted.Sources) || SummaryID(permuted) != s.ID {
		t.Fatal("set order changed source or summary identity")
	}
	for _, ref := range SummaryContentReferences(s) {
		if ref == s.ID || ref == snapshot.Observations[0].ID || ref == snapshot.Observations[1].Payload || ref == s.SourceSetDigest || ref == model.ObjectID(s.Source.Anchor.Cursor.Revision) {
			t.Fatalf("logical identity was treated as a raw codec object: %s", ref)
		}
	}
	for _, content := range []model.ObjectID{snapshot.Observations[0].Payload, s.Generation.Input.Object, s.Generation.Output.Object, s.Validation.Evidence[0].Object} {
		if !slices.Contains(SummaryContentReferences(s), content) {
			t.Fatal("lost actual content dependency", content)
		}
	}
	anchors := SummarySourceAnchors(s)
	anchors[0].Anchor.Knowledge.Execution = "mutated"
	if s.Source.Anchor.Knowledge.Execution != "long-task" {
		t.Fatal("source anchor extractor leaked aliases")
	}
}

func TestSummaryIdentityBindsDerivationAndReviewFields(t *testing.T) {
	original, _ := summaryFixture(t)
	for _, test := range []struct {
		name string
		edit func(*Summary)
	}{
		{"topic", func(s *Summary) { s.Topics[0].Name = "different topic" }},
		{"omission reason", func(s *Summary) { s.Omissions[0].Reason = "different omission rule" }},
		{"goal", func(s *Summary) { s.Applicability.Goal = "deploy" }},
		{"environment", func(s *Summary) { s.Applicability.Environment = "go1.28" }},
		{"confidence", func(s *Summary) { s.Confidence++ }},
		{"activity", func(s *Summary) { s.Generation.Activity = "another-activity" }},
		{"attempt", func(s *Summary) { s.Generation.Attempt = "another-attempt" }},
		{"accepted result", func(s *Summary) { s.Generation.Result = "another-result" }},
		{"model", func(s *Summary) { s.Generation.Model = "another-model" }},
		{"prompt", func(s *Summary) { s.Generation.PromptVersion = "summary-prompt/v2" }},
		{"policy", func(s *Summary) { s.Generation.PolicyVersion = "retain-constraints/v2" }},
		{"exact input", func(s *Summary) { s.Generation.Input.Object = summaryTestID("other framed input") }},
		{"exact output", func(s *Summary) { s.Generation.Output.Object = summaryTestID("other actual output") }},
		{"input anchor", func(s *Summary) { s.Generation.Input.Anchor = summaryTestAnchor(7) }},
		{"generated instant", func(s *Summary) { s.GeneratedAt = s.GeneratedAt.Add(time.Second) }},
		{"visible instant", func(s *Summary) { s.VisibleAt = s.VisibleAt.Add(time.Second) }},
		{"reviewer", func(s *Summary) { s.Validation.Reviewer = "another-reviewer" }},
		{"review policy", func(s *Summary) { s.Validation.PolicyVersion = "review-policy/v2" }},
		{"review evidence", func(s *Summary) { s.Validation.Evidence[0].Object = summaryTestID("different-review") }},
		{"prior version", func(s *Summary) { s.Replaces = summaryTestID("prior-summary") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := original.Clone()
			test.edit(&changed)
			if id := SummaryID(changed); id == "" || id == original.ID {
				t.Fatalf("semantic edit did not change summary identity: %s", id)
			}
			if changed.SourceSetDigest != original.SourceSetDigest {
				t.Fatal("metadata edit rewrote source identity")
			}
		})
	}
	changed := original.Clone()
	changed.Source.Frontier = summaryTestID("different-frontier")
	changed.SourceSetDigest = SummarySourceSetDigest(changed.Source, changed.Sources)
	if changed.SourceSetDigest == original.SourceSetDigest || SummaryID(changed) == original.ID {
		t.Fatal("source cut did not change both source-set and summary identity")
	}
}

func TestSummaryRejectsFutureAndUnadoptedEvidence(t *testing.T) {
	original, snapshot := summaryFixture(t)
	for _, test := range []struct {
		name string
		edit func(*Summary)
		want error
	}{
		{"future source member", func(s *Summary) {
			o := snapshot.Observations[3]
			s.Sources[0] = SummarySource{Observation: o.ID, Content: o.Payload}
			s.Topics[0].Observations = []model.ObjectID{o.ID}
		}, ErrUnknownObservation},
		{"future omitted member", func(s *Summary) { s.Omissions[0].Observation = snapshot.Observations[3].ID }, ErrUnknownObservation},
		{"forged content", func(s *Summary) { s.Sources[0].Content = summaryTestID("not adopted payload") }, ErrInvalidSummary},
		{"wrong content kind", func(s *Summary) { s.Sources[0].PayloadKind = "experience_path" }, ErrInvalidSummary},
		{"wrong frontier", func(s *Summary) { s.Source.Frontier = summaryTestID("unknown frontier") }, model.ErrAnchorMismatch},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := original.Clone()
			test.edit(&changed)
			changed.SourceSetDigest = SummarySourceSetDigest(changed.Source, changed.Sources)
			changed.ID = SummaryID(changed)
			if err := VerifySummarySources(changed, snapshot); !errors.Is(err, test.want) {
				t.Fatalf("got %v, want %v", err, test.want)
			}
		})
	}
	for _, anchor := range []model.CommitFence{summaryTestAnchor(5).Fence, summaryTestAnchor(8).Fence, summaryTestAnchor(10).Fence} {
		if err := original.ValidateBefore(anchor); !errors.Is(err, ErrFutureSource) {
			t.Fatalf("source/input/output/review at or after consumer was allowed: %v", err)
		}
	}
	// A later summary of old observations remains a later derived record. Its
	// generation output and review anchor forbid insertion into the old cut.
	if err := original.ValidateBefore(summaryTestAnchor(6).Fence); !errors.Is(err, ErrFutureSource) {
		t.Fatalf("later generation was backdated into a strict past: %v", err)
	}
}

func TestSummaryRejectsMalformedProvenance(t *testing.T) {
	original, _ := summaryFixture(t)
	for _, edit := range []func(*Summary){
		func(s *Summary) { s.Source.Anchor.Knowledge = nil },
		func(s *Summary) { s.Source.Anchor.Cursor = nil },
		func(s *Summary) { s.Source.Frontier = "" },
		func(s *Summary) { s.SourceSetDigest = summaryTestID("forged sources") },
		func(s *Summary) { s.Sources = append(s.Sources, s.Sources[0]) },
		func(s *Summary) { s.Topics[0].Observations = []model.ObjectID{summaryTestID("uncovered topic")} },
		func(s *Summary) { s.Omissions[0].Observation = s.Sources[0].Observation },
		func(s *Summary) { s.Omissions[0].Reason = "" },
		func(s *Summary) { s.Confidence = 1001 },
		func(s *Summary) { s.Generation.Activity = "" },
		func(s *Summary) { s.Generation.Input.Anchor.Space = "foreign-space" },
		func(s *Summary) { s.Generation.Output.Anchor = summaryTestAnchor(1) },
		func(s *Summary) { s.Validation.Evidence = nil },
		func(s *Summary) { s.Validation.Status = "silently-trusted" },
		func(s *Summary) { s.GeneratedAt = time.Time{} },
		func(s *Summary) { s.VisibleAt = s.GeneratedAt.Add(-time.Second) },
		func(s *Summary) { s.Replaces = s.ID },
	} {
		changed := original.Clone()
		edit(&changed)
		if _, err := changed.Canonical(); err == nil {
			t.Fatal("malformed provenance was encoded")
		}
	}
}

func TestSummaryCodecRejectsTruncationMutationAndUnboundedDeclarations(t *testing.T) {
	s, _ := summaryFixture(t)
	raw, err := s.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < len(raw); n++ {
		if _, err := DecodeSummary(raw[:n]); err == nil {
			t.Fatalf("accepted truncation at %d", n)
		}
	}
	for i := range raw {
		changed := bytes.Clone(raw)
		changed[i] ^= 1
		if _, err := DecodeSummary(changed); err == nil {
			t.Fatalf("accepted changed canonical byte at %d", i)
		}
	}
	if _, err := DecodeSummary(append(bytes.Clone(raw), 0)); err == nil {
		t.Fatal("accepted trailing data")
	}
	prefix := codec.AppendUint([]byte(summaryDomain), SummarySchema)
	prefix, _ = codec.AppendString(prefix, string(s.Space))
	prefix, _ = encodeSource(prefix, s.Source)
	prefix = codec.AppendUint(prefix, uint64(MaxItems+1))
	if _, err := DecodeSummary(prefix); err == nil {
		t.Fatal("accepted oversized source count before allocation")
	}
	if _, err := DecodeSummary(bytes.Repeat([]byte{'x'}, MaxRecordBytes+1)); err == nil {
		t.Fatal("accepted oversized summary record")
	}
}

func TestSummaryCloneAndDecodeOwnAllNestedAnchors(t *testing.T) {
	original, _ := summaryFixture(t)
	raw, err := original.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeSummary(raw)
	if err != nil {
		t.Fatal(err)
	}
	cloned := decoded.Clone()
	cloned.Source.Anchor.Knowledge.Execution = "changed"
	cloned.Topics[0].Observations[0] = summaryTestID("changed topic")
	cloned.Applicability.Tags[0] = "changed tag"
	cloned.Validation.Evidence[0].Anchor.Space = "changed space"
	if same, err := decoded.Canonical(); err != nil || !bytes.Equal(raw, same) {
		t.Fatal("clone mutated its source summary", err)
	}
	for i := range raw {
		raw[i] = 0
	}
	if _, err := decoded.Canonical(); err != nil {
		t.Fatal("decoder retained aliases into caller bytes", err)
	}
}

func FuzzSummaryCanonical(f *testing.F) {
	s, _ := summaryFixture(f)
	raw, err := s.Canonical()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, raw []byte) {
		decoded, err := DecodeSummary(raw)
		if err != nil {
			return
		}
		canonical, err := decoded.Canonical()
		if err != nil || !bytes.Equal(canonical, raw) {
			t.Fatal("successful decode was not exactly canonical", err)
		}
	})
}
