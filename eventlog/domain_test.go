package eventlog

import (
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestDomainRegistryIsClosedAndBindsOperationToSubject(t *testing.T) {
	digest := strings.Repeat("a", 64)
	for _, tc := range []struct{ operation, profile, kind, id string }{
		{"Graph.AddRevision", "graph", "Revision", digest},
		{"Graph.JoinRevision", "graph", "Revision", digest},
		{"Graph.Fork", "graph", "Branch", "main"},
		{"Graph.Advance", "graph", "Branch", "main"},
		{"Graph.Join", "graph", "Branch", "main"},
		{"Graph.Prune", "graph", "Branch", "main"},
		{"Memory.PutCandidate", "memory", "Memory", digest},
		{"Memory.Validate", "memory", "Memory", digest},
		{"Memory.Promote", "memory", "Memory", digest},
		{"Memory.Transition", "memory", "Memory", digest},
		{"Memory.RetractSource", "memory", "Source", digest},
		{"Memory.BuildPath", "memory", "ExperiencePath", digest},
		{"Memory.RetractPath", "memory", "ExperiencePath", digest},
		{"Summary.Apply", "knowledge", "Summary", digest},
		{"Emission.Issue", "agent", "Emission", "grant"},
		{"Emission.Claim", "agent", "Emission", "grant"},
		{"Emission.Revoke", "agent", "Emission", "grant"},
		{"Transfer.Import", "transfer", "ImportRecord", digest},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			e := sampleEvent()
			e.Kind = KindAgentTransition
			e.Attribution = &model.EventAttribution{Subject: model.EntityKey{Profile: model.ProfileID(tc.profile), Kind: model.EntityKind(tc.kind), ID: model.EntityID(tc.id)}, Operation: tc.operation, Ordinal: 1}
			if _, err := Object(e); err != nil {
				t.Fatal(err)
			}
			if _, err := SubjectRef(*e.Attribution); err != nil {
				t.Fatal(err)
			}
			e.Stream.Owner = "another-principal"
			if _, err := Object(e); err == nil {
				t.Fatal("principal stream accepted an owner different from RecordedBy")
			}
			e.Stream.Owner = string(e.RecordedBy)
			e.Attribution.Subject = model.EntityKey{Profile: "agent", Kind: "Task", ID: "forged"}
			if _, err := Object(e); err == nil {
				t.Fatal("domain operation accepted unrelated subject namespace")
			}
		})
	}
	for _, operation := range []string{"Core.anything", "Core.graph.apply/v2", "Core.future.apply/v1", "Graph.Callback", "Memory.Unknown"} {
		if RegisteredAgentOperation(operation) {
			t.Fatalf("unknown operation admitted by closed registry: %s", operation)
		}
	}
	for _, operation := range []string{"Core.graph.apply/v1", "Core.memory.apply/v1", "Core.knowledge.apply/v1", "Core.knowledge.summary.apply/v1", "Core.agent.emission.apply/v1", "Core.transfer.import/v1"} {
		if !RegisteredAgentOperation(operation) {
			t.Fatalf("known method occurrence lost its explicit registration: %s", operation)
		}
	}
}

func TestDomainSubjectRefsUseExistingNavigationIdentities(t *testing.T) {
	id := model.EntityID(strings.Repeat("b", 64))
	for _, tc := range []struct {
		subject model.EntityKey
		want    model.NodeRef
	}{
		{model.EntityKey{Profile: "graph", Kind: "Revision", ID: id}, model.NodeRef{Kind: model.NodeRevision, Revision: model.RevisionID(id)}},
		{model.EntityKey{Profile: "memory", Kind: "Memory", ID: id}, model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "memory", Kind: "memory", ID: id}}},
		{model.EntityKey{Profile: "memory", Kind: "ExperiencePath", ID: id}, model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "memory", Kind: "experience_path", ID: id}}},
		{model.EntityKey{Profile: "knowledge", Kind: "Summary", ID: id}, model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{Profile: "knowledge", Kind: "summary", ID: id}}},
	} {
		ref, err := SubjectRef(model.EventAttribution{Subject: tc.subject})
		if err != nil || ref != tc.want {
			t.Fatalf("subject=%+v ref=%+v want=%+v err=%v", tc.subject, ref, tc.want, err)
		}
	}
}
