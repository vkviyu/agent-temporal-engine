package engine_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestExperiencePathChecksAnchoredStepAndOmissionEvidence(t *testing.T) {
	ctx := context.Background()
	s, src := memoryFixture(t)
	base := graphRevisionFromCurrent(t, s, "experience-base", "")
	unrelatedAck := integrationMutate(t, s.e, integrationRequest("unrelated", integrationPut([]byte("unrelated"), integrationBytes('a'))))
	unrelatedSource := memory.Source{SourceID: pathTestStateObject(t, s.e, "unrelated"), Anchor: sourceAnchor(unrelatedAck.Fence)}
	// Capture a later revision with only the unrelated object. Its predecessor
	// contains the positive source, but that does not support this revision.
	integrationMutate(t, s.e, integrationRequest("remove-positive", api.StateMutation{Key: []byte("memory-evidence"), Kind: api.MutationDelete}))
	unrelatedRevision := graphRevisionFromCurrent(t, s, "experience-unrelated-revision", base.ID)
	validation := memory.Validation{StrategyVersion: 1, Passed: true, Evidence: []memory.Source{src}, Counterexamples: []memory.Source{unrelatedSource}}
	candidate := memory.Memory{
		Space: src.Anchor.Space, Kind: memory.KindProcedure, Claim: []byte("use the positively validated result"),
		Sources: []memory.Source{src}, Evidence: []memory.Evidence{{Source: unrelatedSource, Counter: true}},
	}
	applyMemory(t, s.e, "path-memory", memory.Command{PutCandidate: &candidate})
	candidate = memoryWithStatus(t, s.e, memory.StatusCandidate)
	applyMemory(t, s.e, "path-validate", memory.Command{Validate: &memory.ValidateCommand{ID: candidate.ID, Validation: validation}})
	validated := memoryWithStatus(t, s.e, memory.StatusValidated)
	applyMemory(t, s.e, "path-promote", memory.Command{Promote: &validated.ID})
	promoted := memoryWithStatus(t, s.e, memory.StatusPromoted)
	before := integrationSnapshot(t, s.e)
	cut := sourceAnchor(before.Fence)
	good := model.NodeRef{Kind: model.NodeRevision, Revision: base.ID}
	bad := model.NodeRef{Kind: model.NodeObject, Object: model.ObjectID(integrationID('f'))}
	unrelatedNode := model.NodeRef{Kind: model.NodeObject, Object: unrelatedSource.SourceID}
	inheritedOnly := model.NodeRef{Kind: model.NodeRevision, Revision: unrelatedRevision.ID}
	pathRequest := func(id string, step model.NodeRef, omitted ...model.NodeRef) api.ApplyMemoryRequest {
		return api.ApplyMemoryRequest{Key: agentKey(id), Command: memory.Command{BuildPath: &memory.BuildPathCommand{
			Space: cut.Space, Anchor: cut, Steps: []memory.Step{{Memory: promoted.ID, Node: step, Outcome: "accepted"}},
			Validation: validation, Omitted: omitted,
		}}}
	}
	for _, tt := range []struct {
		name string
		req  api.ApplyMemoryRequest
		want error
	}{
		{"unpublished step", pathRequest("unpublished-step", bad), memory.ErrSourceUnavailable},
		{"unpublished omission", pathRequest("unpublished-omission", good, bad), memory.ErrSourceUnavailable},
		{"counterexample as support", pathRequest("counterexample-step", unrelatedNode), memory.ErrStepEvidenceMismatch},
		{"ancestor content as support", pathRequest("ancestor-step", inheritedOnly), memory.ErrStepEvidenceMismatch},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ack, err := s.e.ApplyMemory(ctx, tt.req)
			if ack.Code != api.CodeRejected || !errors.Is(err, tt.want) {
				t.Fatalf("expected %v, got %+v %v", tt.want, ack, err)
			}
			if got := integrationSnapshot(t, s.e); got != before {
				t.Fatalf("rejected path advanced the publication: %+v", got)
			}
		})
	}
	request := pathRequest("verified-path", good, unrelatedNode)
	ack, err := s.e.ApplyMemory(ctx, request)
	if ack.Code != api.CodeDurableAck || err != nil {
		t.Fatalf("valid path rejected: %+v %v", ack, err)
	}
	got, err := s.e.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: cut.Space})
	if err != nil || len(got.Snapshot.Paths) != 1 {
		t.Fatalf("path missing: %+v %v", got, err)
	}
	path := got.Snapshot.Paths[0]
	if path.Steps[0].Node != good || !reflect.DeepEqual(path.Omitted, []model.NodeRef{unrelatedNode}) {
		t.Fatalf("path provenance changed: %+v", path)
	}
	s.reopen()
	if retry, err := s.e.ApplyMemory(ctx, request); err != nil || retry != ack {
		t.Fatalf("exact retry changed after replay: %+v %v", retry, err)
	}
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = s.e.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: cut.Space})
	if err != nil || !reflect.DeepEqual(got.Snapshot.Paths, []memory.ExperiencePath{path}) {
		t.Fatalf("compaction lost path: %+v %v", got, err)
	}
}

func TestExperiencePathOmissionWithdrawalProtectsPathAndEvent(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{EventProfile: true})
	ack := integrationMutate(t, s.e, integrationRequest("path-source", integrationPut([]byte("path-source"), integrationBytes('a'))))
	src := memory.Source{SourceID: pathTestStateObject(t, s.e, "path-source"), Anchor: sourceAnchor(ack.Fence)}
	validation := memory.Validation{StrategyVersion: 1, Passed: true, Evidence: []memory.Source{src}}
	applyMemory(t, s.e, "path-candidate", memory.Command{PutCandidate: &memory.Memory{Space: src.Anchor.Space, Kind: memory.KindFact, Claim: []byte("path claim"), Sources: []memory.Source{src}}})
	candidate := memoryWithStatus(t, s.e, memory.StatusCandidate)
	applyMemory(t, s.e, "validate-path-member", memory.Command{Validate: &memory.ValidateCommand{ID: candidate.ID, Validation: validation}})
	validated := memoryWithStatus(t, s.e, memory.StatusValidated)
	applyMemory(t, s.e, "promote-path-member", memory.Command{Promote: &validated.ID})
	member := memoryWithStatus(t, s.e, memory.StatusPromoted)
	integrationMutate(t, s.e, integrationRequest("publish-omitted", integrationPut([]byte("omitted"), integrationBytes('b'))))
	omitted := pathTestStateObject(t, s.e, "omitted")
	pathAnchor := sourceAnchor(integrationSnapshot(t, s.e).Fence)
	pathAck := applyMemory(t, s.e, "build-omitted-path", memory.Command{BuildPath: &memory.BuildPathCommand{
		Space: pathAnchor.Space, Anchor: pathAnchor, Validation: validation,
		Steps:   []memory.Step{{Memory: member.ID, Node: model.NodeRef{Kind: model.NodeObject, Object: src.SourceID}}},
		Omitted: []model.NodeRef{{Kind: model.NodeObject, Object: omitted}},
	}})
	state, err := s.e.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: src.Anchor.Space})
	if err != nil || len(state.Snapshot.Paths) != 1 {
		t.Fatalf("read experience path: %+v %v", state, err)
	}
	pathID := state.Snapshot.Paths[0].ID
	event := domainFact(t, s, pathAck, "Memory.BuildPath", "memory", "ExperiencePath", string(pathID))
	integrationMutate(t, s.e, integrationRequest("remove-omitted", api.StateMutation{Key: []byte("omitted"), Kind: api.MutationDelete}))
	applyMemory(t, s.e, "withdraw-omitted", memory.Command{RetractSource: &memory.RetractSourceCommand{
		Source: omitted, Retraction: memory.Retraction{Kind: memory.RetractionPrivacyErasure, Reason: "omit private evidence"},
	}})
	for _, reopen := range []bool{false, true} {
		if reopen {
			s.reopen()
		}
		got, err := s.e.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: src.Anchor.Space})
		if !errors.Is(err, memory.ErrHistoryAccessDenied) || len(got.Snapshot.Paths) > 0 {
			t.Fatalf("withdrawn path disclosed after reopen=%v: %+v %v", reopen, got, err)
		}
		view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: src.Anchor.Space, Principal: "host-principal"})
		if err != nil {
			t.Fatal(err)
		}
		ref := model.NodeRef{Kind: model.NodeEvent, Event: event.ID}
		detail, err := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: ref})
		if closeErr := s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); closeErr != nil {
			t.Fatal(closeErr)
		}
		if !errors.Is(err, memory.ErrHistoryAccessDenied) || len(detail.Data) != 0 {
			t.Fatalf("withdrawn path event disclosed after reopen=%v: %+v %v", reopen, detail, err)
		}
	}
}

func pathTestStateObject(t *testing.T, e *engine.Engine, key string) model.ObjectID {
	t.Helper()
	ctx := context.Background()
	view, err := e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil {
		t.Fatal(err)
	}
	defer e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	entries, err := e.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries.Items {
		if string(entry.Key) == key {
			return entry.Value
		}
	}
	t.Fatalf("state key %s not found", key)
	return ""
}
