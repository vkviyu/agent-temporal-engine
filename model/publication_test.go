package model_test

import (
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func TestSourcePublicationReferenceValidatesScopeAndOwnsSelectors(t *testing.T) {
	base := model.SourcePublicationRef{Store: "store", Space: "s", ProducerTx: model.TxID(strings.Repeat("a", 32)), Cursor: &model.Cursor{Store: "store", Space: "s", Revision: model.RevisionID(strings.Repeat("b", 64))}, Knowledge: &model.KnowledgeKey{Actor: "actor", Execution: "execution"}}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*model.SourcePublicationRef){"missing-store": func(r *model.SourcePublicationRef) { r.Store = "" }, "missing-space": func(r *model.SourcePublicationRef) { r.Space = "" }, "bad-producer": func(r *model.SourcePublicationRef) { r.ProducerTx = "short" }, "noncanonical-producer": func(r *model.SourcePublicationRef) { r.ProducerTx = model.TxID(strings.Repeat("A", 32)) }, "zero-producer": func(r *model.SourcePublicationRef) { r.ProducerTx = model.TxID(strings.Repeat("0", 32)) }, "foreign-cursor-store": func(r *model.SourcePublicationRef) { r.Cursor.Store = "other" }, "foreign-cursor-space": func(r *model.SourcePublicationRef) { r.Cursor.Space = "other" }, "missing-revision": func(r *model.SourcePublicationRef) { r.Cursor.Revision = "" }, "missing-actor": func(r *model.SourcePublicationRef) { r.Knowledge.Actor = "" }, "invalid-execution": func(r *model.SourcePublicationRef) { r.Knowledge.Execution = model.ExecutionID(string([]byte{255})) }} {
		t.Run(name, func(t *testing.T) {
			bad := base.Clone()
			change(&bad)
			if bad.Validate() == nil {
				t.Fatalf("invalid source accepted: %+v", bad)
			}
		})
	}
	copy := base.Clone()
	copy.Cursor.Revision = model.RevisionID(strings.Repeat("f", 64))
	copy.Knowledge.Execution = "other"
	if base.Cursor.Revision == copy.Cursor.Revision || base.Knowledge.Execution == copy.Knowledge.Execution {
		t.Fatal("clone aliased publication selectors")
	}
}
