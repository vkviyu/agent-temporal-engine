package knowledge

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// Independently generated using Python hashlib + unsigned LEB128 + UTF-8
// length-prefixed strings, without importing this package or invoking Go.
//
//go:embed testdata/summary-schema2.hex
var summarySchema2Golden string

// These pre-release bytes are retained only as a rejection fixture.
//
//go:embed testdata/summary-schema1.hex
var summarySchema1Retired string

//go:embed testdata/packet-schema4.hex
var packetSchema4Retired string

func TestSummarySchema2IndependentGolden(t *testing.T) {
	anchor := func(lsn uint64) model.SnapshotAnchor {
		return model.SnapshotAnchor{Space: "s", Fence: model.CommitFence{Store: "r", LSN: lsn, CommitDigest: model.CommitDigest(strings.Repeat("a", 64))}}
	}
	source := SourceAnchor{Anchor: anchor(1), ViewDigest: model.ObjectID(strings.Repeat("d", 64)), Frontier: model.ObjectID(strings.Repeat("c", 64))}
	source.Anchor.Knowledge = &model.KnowledgeKey{Actor: "w", Execution: "x"}
	source.Anchor.Cursor = &model.Cursor{Store: "r", Space: "s", Revision: model.RevisionID(strings.Repeat("b", 64))}
	observation := model.ObjectID(strings.Repeat("e", 64))
	stamp := time.Date(2026, time.November, 2, 0, 0, 0, 0, time.UTC)
	summary := Summary{Space: "s", Source: source, Sources: []SummarySource{{Observation: observation, Content: model.ObjectID(strings.Repeat("f", 64))}}, Topics: []SummaryTopic{{Name: "topic", Observations: []model.ObjectID{observation}}}, Applicability: SummaryApplicability{Goal: "goal", Environment: "env", SchemaVersion: 1, Tags: []string{"tag"}}, Confidence: 700, Generation: SummaryGeneration{Activity: "act", Attempt: "attempt", Result: "result", Model: "model", PromptVersion: "prompt/v1", PolicyVersion: "policy/v1", Input: RecordedContent{Object: model.ObjectID(strings.Repeat("1", 64)), Anchor: anchor(2), Encoding: "application/json"}, Output: RecordedContent{Object: model.ObjectID(strings.Repeat("2", 64)), Anchor: anchor(3), Encoding: "text/plain"}}, GeneratedAt: stamp, VisibleAt: stamp, Validation: SummaryValidation{Status: "unreviewed"}}
	summary.SourceSetDigest = SummarySourceSetDigest(summary.Source, summary.Sources)
	summary.ID = SummaryID(summary)
	golden, err := hex.DecodeString(strings.TrimSpace(summarySchema2Golden))
	if err != nil {
		t.Fatal(err)
	}
	got, err := summary.Canonical()
	if err != nil || !bytes.Equal(got, golden) {
		t.Fatalf("independent canonical bytes differ: got %x err=%v", got, err)
	}
	decoded, err := DecodeSummary(golden)
	if err != nil || decoded.ID != summary.ID || decoded.SourceSetDigest != summary.SourceSetDigest {
		t.Fatal("independent Summary identity mismatch", decoded.ID, err)
	}
}

func TestDerivationCleanCutRejectsPreIndexKnowledgeFormats(t *testing.T) {
	oldSummary, err := hex.DecodeString(strings.TrimSpace(summarySchema1Retired))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeSummary(oldSummary); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("retired Summary schema 1 was not explicitly unsupported: %v", err)
	}
	oldPacket, err := hex.DecodeString(strings.TrimSpace(packetSchema4Retired))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodePacket(oldPacket); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("retired Packet schema 4 was not explicitly unsupported: %v", err)
	}
}
