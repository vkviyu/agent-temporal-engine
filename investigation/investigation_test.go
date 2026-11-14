package investigation

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func citedNode(lsn uint64) Citation {
	digest := strings.Repeat("1", 64)
	return Citation{
		Ref: model.NodeRef{Kind: model.NodeEvent, Event: model.EventID(digest)},
		Anchor: model.SnapshotAnchor{Space: "project", Fence: model.CommitFence{
			Store: "store", LSN: lsn, CommitDigest: model.CommitDigest(digest),
		}},
	}
}

func TestApplyCheckpoint(t *testing.T) {
	subject, source := citedNode(1), citedNode(3)
	source.Anchor.Cursor = &model.Cursor{Store: "store", Space: "project", Revision: model.RevisionID(strings.Repeat("2", 64))}
	questions := []string{"What authorized the action?"}
	position := &Position{Focus: source, Mode: "retrospective", Sections: []string{"upstream", "downstream"}}
	checkpoint, err := Apply(nil, Update{
		ID: "inspect", Subject: &subject, OpenQuestions: &questions, Position: position, Sources: []Citation{source},
		Findings: []Finding{{ID: "original", Text: "The action was retried.", Evidence: []Citation{source}}},
	})
	if err != nil || checkpoint.Version != 1 || len(checkpoint.Citations()) != 2 {
		t.Fatalf("initial checkpoint: %+v, %v", checkpoint, err)
	}
	position.Sections[0] = "invalid"
	source.Anchor.Cursor.Revision = "invalid"
	questions[0] = "mutated"
	if err := checkpoint.Validate(); err != nil || checkpoint.OpenQuestions[0] == questions[0] {
		t.Fatalf("checkpoint aliases caller input: %+v, %v", checkpoint, err)
	}
	var clear []string
	note := "Read the earlier authorization decision."
	edit := Update{ID: "inspect", ExpectedVersion: 1, RemoveFindings: []string{"original"}, OpenQuestions: &clear, NextFocus: &note}
	next, err := Apply(&checkpoint, edit)
	if err != nil || next.Version != 2 || len(next.Findings)+len(next.OpenQuestions) != 0 || next.NextFocus != note || len(next.Sources) != 1 {
		t.Fatalf("second checkpoint: %+v, %v", next, err)
	}
	if _, err := Apply(&next, edit); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale version: %v", err)
	}
	if _, err := Apply(nil, edit); !errors.Is(err, ErrConflict) {
		t.Fatalf("missing version: %v", err)
	}
	raw, err := EncodeCheckpoint(next)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := DecodeCheckpoint(raw)
	if err != nil || reopened.Position == nil || reopened.Position.ViewMode() != "audit_snapshot" ||
		!slices.Equal(reopened.Position.Sections, []string{"upstream", "downstream"}) || reopened.Position.Focus.Anchor.Fence.LSN != 3 {
		t.Fatalf("reopened checkpoint: %+v, %v", reopened, err)
	}
}

func TestUpdateCanonicalAndBounded(t *testing.T) {
	var clear []string
	u := Update{ID: "inspect", ExpectedVersion: 1, OpenQuestions: &clear}
	raw, err := EncodeUpdate(u)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeUpdate(raw)
	if err != nil || decoded.OpenQuestions == nil || len(*decoded.OpenQuestions) != 0 {
		t.Fatalf("clearing update: %+v, %v", decoded, err)
	}
	withDuplicate := bytes.Replace(raw, []byte(`"ID":"inspect"`), []byte(`"ID":"other","ID":"inspect"`), 1)
	if _, err := DecodeUpdate(withDuplicate); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate field: %v", err)
	}
	withFutureSchema := bytes.Replace(raw, []byte(updatePrefix), []byte("ATE-INVESTIGATION-UPDATE\x00v2\x00"), 1)
	if _, err := DecodeUpdate(withFutureSchema); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("future format: %v", err)
	}
	for i := range MaxCitations + 1 {
		u.Sources = append(u.Sources, citedNode(uint64(i)))
	}
	if _, err := EncodeUpdate(u); !errors.Is(err, ErrLimit) {
		t.Fatalf("citation limit: %v", err)
	}
}
