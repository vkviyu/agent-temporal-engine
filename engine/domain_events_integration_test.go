package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/eventlog"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

func domainFact(t *testing.T, s *integrationStore, ack api.CommitResult, operation, profile, kind, id string) model.Event {
	t.Helper()
	var fact model.Event
	var core model.Event
	for _, e := range readProfileOccurrences(t, s) {
		if e.Transaction != ack.TxID {
			continue
		}
		if e.Attribution == nil {
			core = e
		} else if e.Attribution.Operation == operation {
			if fact.ID != "" {
				t.Fatalf("duplicate semantic fact %s", operation)
			}
			fact = e
		}
	}
	if fact.ID == "" || fact.Attribution.Subject != (model.EntityKey{Profile: model.ProfileID(profile), Kind: model.EntityKind(kind), ID: model.EntityID(id)}) || fact.RecordedBy != "host-principal" {
		t.Fatalf("wrong subject for %s: %+v", operation, fact)
	}
	if core.ID == "" || core.SchemaVersion != 2 {
		t.Fatalf("semantic fact replaced the core occurrence: %+v", core)
	}
	listed := false
	for _, eventID := range core.ProfileEvents {
		listed = listed || eventID == fact.ID
	}
	if !listed {
		t.Fatal("core manifest omitted subject fact")
	}
	ctx := context.Background()
	v, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: v.Ref})
	subject, err := eventlog.SubjectRef(*fact.Attribution)
	if err != nil {
		t.Fatal(err)
	}
	detail, err := s.e.GetNode(ctx, api.GetNodeRequest{View: v.Ref, Ref: subject})
	if err != nil || detail.Ref != subject || len(detail.Data) == 0 || detail.Anchor.Fence != ack.Fence {
		t.Fatalf("subject cannot be inspected at its exact publication: %+v %v", detail, err)
	}
	expanded, err := s.e.GetNodeContext(ctx, api.NodeContextRequest{View: v.Ref, Ref: subject, Mode: "evidence_neighborhood", Sections: []string{"node", "upstream"}})
	if err != nil || expanded.Ref != subject || len(expanded.Sections["node"]) != 1 {
		t.Fatalf("subject cannot be expanded at its fixed View: %+v %v", expanded, err)
	}
	event, err := s.e.GetNode(ctx, api.GetNodeRequest{View: v.Ref, Ref: model.NodeRef{Kind: model.NodeEvent, Event: fact.ID}})
	if err != nil {
		t.Fatal(err)
	}
	linked := false
	for _, src := range event.Sources {
		linked = linked || src == subject
	}
	if !linked {
		t.Fatal("event detail omitted navigable subject relation")
	}
	expandedEvent, err := s.e.GetNodeContext(ctx, api.NodeContextRequest{View: v.Ref, Ref: event.Ref, Mode: "evidence_neighborhood", Sections: []string{"upstream"}})
	if err != nil {
		t.Fatal(err)
	}
	linked = false
	for _, node := range expandedEvent.Sections["upstream"] {
		linked = linked || node.Ref == subject
	}
	if !linked {
		t.Fatal("event expansion omitted its actual subject")
	}
	return fact
}

func TestDomainEventsUseSubjectsAndSurviveRetryReopenCompaction(t *testing.T) {
	ctx := context.Background()
	s, payload, source := knowledgeFixture(t, engine.Options{AgentProfile: true, EventProfile: true})
	root, err := graph.BuildRevision("host-space", "", nil, "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	add := api.ApplyGraphRequest{Key: agentKey("domain-revision"), Command: graph.Command{AddRevision: &graph.AddRevisionCommand{}}}
	added, err := s.e.ApplyGraph(ctx, add)
	if err != nil || added.Code != api.CodeDurableAck {
		t.Fatal(added, err)
	}
	domainFact(t, s, added, "Graph.AddRevision", "graph", "Revision", string(root.ID))
	forked := graphCoreApply(t, s.e, "domain-fork", graph.Command{Fork: &graph.ForkCommand{Branch: "main", Source: root.ID}})
	domainFact(t, s, forked, "Graph.Fork", "graph", "Branch", "main")
	advanced := graphCoreApply(t, s.e, "domain-advance", graph.Command{Advance: &graph.AdvanceBranchCommand{Branch: "main", Expected: 0, Head: root.ID}})
	domainFact(t, s, advanced, "Graph.Advance", "graph", "Branch", "main")
	pruned := graphCoreApply(t, s.e, "domain-prune", graph.Command{Prune: &graph.PruneBranchCommand{Branch: "main", Expected: 1, Head: root.ID, Reason: "finished exploration"}})
	domainFact(t, s, pruned, "Graph.Prune", "graph", "Branch", "main")

	o := knowledgeObservation(payload, source, "one", 1)
	_, observed := applyObservation(t, s.e, "domain-observe", o)
	domainFact(t, s, observed, "Knowledge.Observation", "knowledge", "Observation", string(o.ID))
	known, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Knowledge: &o.Key})
	if err != nil {
		t.Fatal(err)
	}
	anchor := known.Anchor.Clone()
	anchor.Cursor = &model.Cursor{Store: anchor.Fence.Store, Space: anchor.Space, Revision: known.Revision.ID}
	view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor, Mode: "execution_knowledge"})
	if err != nil {
		t.Fatal(err)
	}
	packet := knowledge.ContextPacket{Space: "host-space", Key: &o.Key, Source: knowledge.SourceAnchor{Anchor: anchor, Frontier: known.Frontier, ViewDigest: view.ViewDigest}, GoalRef: payload, Items: []knowledge.ContextItem{{Observation: o.ID, Content: payload, Label: "evidence"}}, Renderer: "event-test/v1", Tokenizer: "test/v1"}
	packet.ID = knowledge.PacketID(packet)
	s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	packetACK, err := s.e.ApplyKnowledge(ctx, api.ApplyKnowledgeRequest{Key: agentKey("domain-packet"), Command: knowledge.Command{Packet: &packet}})
	if err != nil || packetACK.Code != api.CodeDurableAck {
		t.Fatal(packetACK, err)
	}
	domainFact(t, s, packetACK, "Knowledge.Packet", "knowledge", "ContextPacket", string(packet.ID))

	src := memory.Source{SourceID: payload, Anchor: model.SnapshotAnchor{Space: "host-space", Fence: source}}
	m := memory.Memory{Space: "host-space", Kind: memory.KindFact, Claim: []byte("a preserved observation"), Sources: []memory.Source{src}}
	putRequest := api.ApplyMemoryRequest{Key: agentKey("domain-memory"), Command: memory.Command{PutCandidate: &m}}
	put, err := s.e.ApplyMemory(ctx, putRequest)
	if err != nil || put.Code != api.CodeDurableAck {
		t.Fatal(put, err)
	}
	candidate := memoryWithStatus(t, s.e, memory.StatusCandidate)
	domainFact(t, s, put, "Memory.PutCandidate", "memory", "Memory", string(candidate.ID))
	validatedACK := applyMemory(t, s.e, "domain-validate", memory.Command{Validate: &memory.ValidateCommand{ID: candidate.ID, Validation: memory.Validation{StrategyVersion: 1, Passed: true, Evidence: []memory.Source{src}}}})
	validated := memoryWithStatus(t, s.e, memory.StatusValidated)
	validationEvent := domainFact(t, s, validatedACK, "Memory.Validate", "memory", "Memory", string(validated.ID))
	if len(validationEvent.CausalParents) != 1 {
		t.Fatal("new immutable memory version lost its predecessor fact")
	}
	promotedACK := applyMemory(t, s.e, "domain-promote", memory.Command{Promote: &validated.ID})
	promoted := memoryWithStatus(t, s.e, memory.StatusPromoted)
	domainFact(t, s, promotedACK, "Memory.Promote", "memory", "Memory", string(promoted.ID))
	pathSource := model.SnapshotAnchor{Space: "host-space", Fence: promotedACK.Fence}
	pathACK := applyMemory(t, s.e, "domain-path", memory.Command{BuildPath: &memory.BuildPathCommand{Space: "host-space", Anchor: pathSource, Steps: []memory.Step{{Node: model.NodeRef{Kind: model.NodeObject, Object: payload}, Memory: promoted.ID, Outcome: "verified"}}, Validation: memory.Validation{Passed: true, Evidence: []memory.Source{src}}}})
	memState, err := s.e.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "host-space"})
	if err != nil || len(memState.Snapshot.Paths) != 1 {
		t.Fatal(memState, err)
	}
	path := memState.Snapshot.Paths[0]
	domainFact(t, s, pathACK, "Memory.BuildPath", "memory", "ExperiencePath", string(path.ID))
	retractPath := applyMemory(t, s.e, "domain-retract-path", memory.Command{RetractPath: &memory.RetractPathCommand{ID: path.ID, Retraction: memory.Retraction{Kind: memory.RetractionFactualCorrection, Reason: "revisit path"}}})
	memState, err = s.e.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "host-space"})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range memState.Snapshot.Paths {
		if p.Status == memory.StatusRetracted {
			domainFact(t, s, retractPath, "Memory.RetractPath", "memory", "ExperiencePath", string(p.ID))
		}
	}
	transitioned := applyMemory(t, s.e, "domain-transition", memory.Command{Transition: &memory.TransitionCommand{ID: promoted.ID, To: memory.StatusSuperseded, Why: "reviewed a newer observation"}})
	superseded := memoryWithStatus(t, s.e, memory.StatusSuperseded)
	domainFact(t, s, transitioned, "Memory.Transition", "memory", "Memory", string(superseded.ID))

	authority := agentprofile.AuthorityBinding{Deployment: "local", Holder: "runner", OwnerEpoch: 1, Incarnation: 1}
	recoveryCoreControl(t, s, "domain-emitter", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: "one", Status: "active", Binding: authority, Evidence: model.ObjectID(integrationID('a'))}}})
	grant := api.EmissionSpec{ID: "domain-grant", Execution: "one", Task: "kt", GoalEpoch: 1, Authority: authority, AuthorityVersion: 1, Content: payload, Anchor: src.Anchor, Length: 1, Destination: "audit"}
	issue, err := s.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey("domain-issue"), Command: api.EmissionCommand{Issue: &grant}})
	if err != nil || issue.Code != api.CodeDurableAck {
		t.Fatal(issue, err)
	}
	issued := domainFact(t, s, issue, "Emission.Issue", "agent", "Emission", grant.ID)
	if issued.Attribution.InitiatingActor != "same-actor" || issued.Attribution.Execution != "one" {
		t.Fatalf("emission authority did not bind actual actor: %+v", issued)
	}
	claim, err := s.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey("domain-claim"), Command: api.EmissionCommand{ClaimID: grant.ID, ClaimVersion: 1, ClaimAuthority: authority}})
	if err != nil || claim.Code != api.CodeDurableAck {
		t.Fatal(claim, err)
	}
	domainFact(t, s, claim, "Emission.Claim", "agent", "Emission", grant.ID)
	revoked, err := s.e.ApplyEmission(ctx, api.ApplyEmissionRequest{Key: agentKey("domain-revoke"), Command: api.EmissionCommand{RevokeID: grant.ID, RevokeVersion: 2}})
	if err != nil || revoked.Code != api.CodeDurableAck {
		t.Fatal(revoked, err)
	}
	if e := domainFact(t, s, revoked, "Emission.Revoke", "agent", "Emission", grant.ID); e.Attribution.InitiatingActor != "" {
		t.Fatal("host revocation invented an initiating actor")
	}
	withdrawn := applyMemory(t, s.e, "domain-withdraw", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: payload, Retraction: memory.Retraction{Kind: memory.RetractionUseBan, Reason: "no future reuse"}}})
	domainFact(t, s, withdrawn, "Memory.RetractSource", "memory", "Source", string(payload))

	all := readProfileOccurrences(t, s)
	assertOccurrenceStreams(t, all)
	check := func() {
		t.Helper()
		if retry, err := s.e.ApplyGraph(ctx, add); err != nil || retry != added {
			t.Fatal("graph receipt retry changed", retry, err)
		}
		if retry, err := s.e.ApplyMemory(ctx, putRequest); err != nil || retry != put {
			t.Fatal("memory receipt retry changed", retry, err)
		}
		if got := readProfileOccurrences(t, s); !reflect.DeepEqual(got, all) {
			t.Fatal("retry/reopen/compaction changed event identity, stream, subject or count")
		}
		assertOccurrenceStreams(t, readProfileOccurrences(t, s))
		domainFact(t, s, validatedACK, "Memory.Validate", "memory", "Memory", string(validated.ID))
	}
	check()
	s.reopen()
	check()
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	check()
}

func assertOccurrenceStreams(t *testing.T, events []model.Event) {
	t.Helper()
	type streamKey struct {
		space     model.SpaceID
		principal model.PrincipalID
		stream    model.EventStreamKey
	}
	streams := map[streamKey][]model.Event{}
	for _, event := range events {
		key := streamKey{event.Space, event.RecordedBy, event.Stream}
		streams[key] = append(streams[key], event)
	}
	for key, chain := range streams {
		sort.Slice(chain, func(i, j int) bool { return chain[i].Sequence < chain[j].Sequence })
		var previous model.EventID
		for i, event := range chain {
			if event.Sequence != uint64(i+1) {
				t.Fatalf("stream %+v duplicated/skipped occurrence sequence: %+v", key, chain)
			}
			if event.Attribution == nil && event.SchemaVersion == 1 {
				if (previous == "" && len(event.CausalParents) != 0) || (previous != "" && (len(event.CausalParents) != 1 || event.CausalParents[0] != previous)) {
					t.Fatalf("core schema-1 stream predecessor disconnected: %+v", event)
				}
			} else if event.PreviousInStream != previous {
				t.Fatalf("stream %+v PreviousInStream=%s want=%s event=%+v", key, event.PreviousInStream, previous, event)
			}
			previous = event.ID
		}
	}
}

func TestPrincipalOccurrenceStreamSharesOneCounterAndKeepsOwnersIsolated(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{EventProfile: true})
	root, err := graph.BuildRevision("host-space", "", nil, "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	requests := map[model.PrincipalID]api.ApplyGraphRequest{}
	acks := map[model.PrincipalID]api.CommitResult{}
	before := map[model.PrincipalID][]model.Event{}
	for _, principal := range []model.PrincipalID{"alice", "bob"} {
		key := model.RequestKey{Space: "host-space", Principal: principal, RequestID: "same-request"}
		if ack, err := s.e.ApplyGraph(ctx, api.ApplyGraphRequest{Key: key, Command: graph.Command{AddRevision: &graph.AddRevisionCommand{}}}); err != nil || ack.Code != api.CodeDurableAck {
			t.Fatal(ack, err)
		}
		key.RequestID = "same-fork-request"
		request := api.ApplyGraphRequest{Key: key, Command: graph.Command{Fork: &graph.ForkCommand{Branch: model.BranchID(principal), Source: root.ID}}}
		ack, err := s.e.ApplyGraph(ctx, request)
		if err != nil || ack.Code != api.CodeDurableAck {
			t.Fatal(ack, err)
		}
		requests[principal], acks[principal] = request, ack
		before[principal] = readProfileOccurrences(t, s, principal)
		assertOccurrenceStreams(t, before[principal])
		if len(before[principal]) != 4 {
			t.Fatal("expected two facts and two core occurrences")
		}
		for _, event := range before[principal] {
			if event.RecordedBy != principal || event.Stream.Kind != "principal" || event.Stream.Owner != string(principal) {
				t.Fatalf("principal stream crossed its authenticated owner: %+v", event)
			}
		}
	}
	check := func() {
		t.Helper()
		seen := map[model.EventID]bool{}
		for principal, request := range requests {
			if ack, err := s.e.ApplyGraph(ctx, request); err != nil || ack != acks[principal] {
				t.Fatal("principal retry changed receipt", ack, err)
			}
			got := readProfileOccurrences(t, s, principal)
			if !reflect.DeepEqual(got, before[principal]) {
				t.Fatal("principal event history changed")
			}
			assertOccurrenceStreams(t, got)
			for _, event := range got {
				if seen[event.ID] {
					t.Fatal("two principals observed the same occurrence")
				}
				seen[event.ID] = true
			}
		}
	}
	check()
	s.reopen()
	check()
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	check()
}

func TestDomainGraphJoinEventsIdentifyGeneratedRevisionAndBranch(t *testing.T) {
	ctx := context.Background()
	s := newIntegrationStore(t, engine.Options{EventProfile: true})
	base := graphRevisionFromCurrent(t, s, "domain-join-base", "")
	graphCoreApply(t, s.e, "domain-join-fork", graph.Command{Fork: &graph.ForkCommand{Branch: "main", Source: base.ID}})
	integrationMutate(t, s.e, integrationRequest("domain-left-state", integrationPut([]byte("a"), integrationBytes('l'))))
	left := graphRevisionFromCurrent(t, s, "domain-left", base.ID)
	graphCoreApply(t, s.e, "domain-left-head", graph.Command{Advance: &graph.AdvanceBranchCommand{Branch: "main", Head: left.ID}})
	integrationMutate(t, s.e, integrationRequest("domain-right-state", api.StateMutation{Key: []byte("a"), Kind: api.MutationDelete}, integrationPut([]byte("b"), integrationBytes('r'))))
	right := graphRevisionFromCurrent(t, s, "domain-right", base.ID)
	request := api.ApplyGraphRequest{Key: agentKey("domain-join"), Command: graph.Command{Join: &graph.JoinCommand{Branch: "main", Expected: 1, Left: left.ID, Right: right.ID, ExpectedDomainRoot: right.StateRoot}}}
	ack, err := s.e.ApplyGraph(ctx, request)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatal(ack, err)
	}
	branch, err := s.e.GetGraphBranch(ctx, api.ReadGraphBranchRequest{Space: "host-space", ID: "main"})
	if err != nil {
		t.Fatal(err)
	}
	revisionFact := domainFact(t, s, ack, "Graph.JoinRevision", "graph", "Revision", string(branch.Value.Head))
	branchFact := domainFact(t, s, ack, "Graph.Join", "graph", "Branch", "main")
	if len(revisionFact.CausalParents) != 2 || len(branchFact.CausalParents) != 1 || branchFact.CausalParents[0] != revisionFact.ID {
		t.Fatalf("merge subjects lost exact result and parent causality: revision=%+v branch=%+v", revisionFact, branchFact)
	}
	all := readProfileOccurrences(t, s)
	for i := range 3 {
		if i == 1 {
			s.reopen()
		} else if i == 2 {
			if _, err := s.e.CompactAuditFull(ctx); err != nil {
				t.Fatal(err)
			}
			s.reopen()
		}
		if retry, err := s.e.ApplyGraph(ctx, request); err != nil || retry != ack || !reflect.DeepEqual(readProfileOccurrences(t, s), all) {
			t.Fatal("merge retry/reopen/compaction changed event facts", retry, err)
		}
	}
}

func TestSummaryAndTransferEventsResolveExactOwnedSubjects(t *testing.T) {
	ctx := context.Background()
	s, summary := summaryCoreFixture(t, engine.Options{AgentProfile: true, EventProfile: true})
	past := integrationSnapshot(t, s.e).Fence
	summaryACK, err := s.e.ApplySummary(ctx, api.ApplySummaryRequest{Key: agentKey("domain-summary"), Summary: summary})
	if err != nil || summaryACK.Code != api.CodeDurableAck {
		t.Fatal(summaryACK, err)
	}
	fact := domainFact(t, s, summaryACK, "Summary.Apply", "knowledge", "Summary", string(summary.ID))
	if fact.Attribution.Execution != "one" || fact.Attribution.InitiatingActor != "" || len(fact.CausalParents) == 0 {
		t.Fatalf("summary provenance attribution=%+v", fact)
	}
	summaryRef, err := eventlog.SubjectRef(*fact.Attribution)
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []api.OpenViewRequest{
		{Space: "host-space", Principal: "other"},
		{Space: "host-space", Principal: "host-principal", Anchor: &model.SnapshotAnchor{Space: "host-space", Fence: past}},
		{Space: "host-space", Principal: "host-principal", Mode: "execution_knowledge", Anchor: &summary.Source.Anchor},
	} {
		view, err := s.e.OpenView(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: summaryRef}); !errors.Is(err, engine.ErrNodeNotFound) {
			t.Fatalf("subject navigation widened principal, historical or adopted-knowledge scope: %v", err)
		}
		s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	}
	src := memory.Source{SourceID: summary.Generation.Output.Object, Anchor: summary.Generation.Output.Anchor}
	m := memory.Memory{Space: "host-space", Kind: memory.KindFact, Claim: []byte("a portable result"), Sources: []memory.Source{src}}
	applyMemory(t, s.e, "domain-portable", memory.Command{PutCandidate: &m})
	candidate := memoryWithStatus(t, s.e, memory.StatusCandidate)
	applyMemory(t, s.e, "domain-portable-validation", memory.Command{Validate: &memory.ValidateCommand{ID: candidate.ID, Validation: memory.Validation{Passed: true, Evidence: []memory.Source{src}}}})
	validated := memoryWithStatus(t, s.e, memory.StatusValidated)
	promote := applyMemory(t, s.e, "domain-portable-promote", memory.Command{Promote: &validated.ID})
	promoted := memoryWithStatus(t, s.e, memory.StatusPromoted)
	anchor := model.SnapshotAnchor{Space: "host-space", Fence: promote.Fence}
	manifest, err := s.e.ExportTransfer(ctx, api.ExportTransferRequest{Space: "host-space", Principal: "host-principal", Anchor: &anchor, Roots: []transfer.Reference{{Kind: "memory", ID: promoted.ID, Anchor: anchor}}, Policy: transfer.RedactionPolicy{ID: "whole-record", Version: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	targetKey := agentKey("domain-import")
	targetKey.Space = "destination"
	request := api.ImportTransferRequest{Key: targetKey, Manifest: manifest, ApprovedDigest: manifest.ID}
	importACK, err := s.e.ImportTransfer(ctx, request)
	if err != nil || importACK.Code != api.CodeDurableAck {
		t.Fatal(importACK, err)
	}
	check := func() {
		t.Helper()
		view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "destination", Principal: "host-principal"})
		if err != nil {
			t.Fatal(err)
		}
		defer s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
		matches, err := s.e.SearchNodes(ctx, api.SearchNodesRequest{View: view.Ref, Layer: "events", EntityKind: "Event", EventOperation: "Transfer.Import"})
		if err != nil || len(matches.Items) != 1 {
			t.Fatal(matches, err)
		}
		detail, err := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: matches.Items[0].Node.Ref})
		if err != nil {
			t.Fatal(err)
		}
		var event model.Event
		if json.Unmarshal(detail.Data, &event) != nil || event.Attribution == nil || string(event.Attribution.Subject.ID) != string(manifest.ID) || event.Attribution.Subject.Kind != "ImportRecord" || event.Attribution.Execution != "" {
			t.Fatal("foreign archive invented local execution or lost import identity")
		}
		ref, err := eventlog.SubjectRef(*event.Attribution)
		if err != nil {
			t.Fatal(err)
		}
		if record, err := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: ref}); err != nil || record.Encoding != "transfer/ImportRecord/v1+json" {
			t.Fatal("import subject not inspectable", err)
		}
		if retry, err := s.e.ImportTransfer(ctx, request); err != nil || retry != importACK {
			t.Fatal("import retry duplicated event", retry, err)
		}
	}
	check()
	s.reopen()
	check()
	if _, err := s.e.CompactAuditFull(ctx); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	check()
}
