package agentprofile

import (
	"bytes"
	"encoding/hex"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func sourceContinuationFixture() Continuation {
	return Continuation{Execution: "e", Version: 1, Incarnation: 1, PendingKey: "next", PendingGeneration: 1, InputFence: model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest(digest('f'))}, Phase: "pending-render", Position: &ContinuationPosition{Space: "s", Principal: "p", AdapterVersion: "host/v1", NextActions: []PendingActionRef{{Key: "next", Generation: 1}}, KnowledgeSource: &model.SourcePublicationRef{Store: "store", Space: "s", ProducerTx: model.TxID(strings.Repeat("a", 32)), Cursor: &model.Cursor{Store: "store", Space: "s", Revision: model.RevisionID(digest('b'))}, Knowledge: &model.KnowledgeKey{Actor: "actor", Execution: "e"}}}}
}

func TestContinuationSourcePublicationIndependentGoldenAndOwnedClone(t *testing.T) {
	c := sourceContinuationFixture()
	raw, err := EncodeCommand(Command{Lifecycle: &LifecycleCommand{PutContinuation: &c}})
	if err != nil {
		t.Fatal(err)
	}
	goldenHex, err := os.ReadFile("testdata/continuation-source-v1.hex")
	if err != nil {
		t.Fatal(err)
	}
	golden, err := hex.DecodeString(strings.TrimSpace(string(goldenHex)))
	if err != nil || !bytes.Equal(golden, raw) {
		t.Fatalf("source continuation bytes changed: %x / %x %v", raw, golden, err)
	}
	decoded, err := DecodeCommand(golden)
	if err != nil || !reflect.DeepEqual(*decoded.Lifecycle.PutContinuation, c) {
		t.Fatalf("lost producer cut: %+v %v", decoded, err)
	}
	refs := PublicationReferences(Snapshot{Lifecycle: LifecycleSnapshot{Continuations: []Continuation{c, cloneContinuation(c)}}})
	if len(refs) != 1 || !reflect.DeepEqual(refs[0], *c.Position.KnowledgeSource) {
		t.Fatalf("lost exact producer dependency: %+v", refs)
	}
	refs[0].Knowledge.Actor = "mutated-extractor-output"
	if c.Position.KnowledgeSource.Knowledge.Actor != "actor" {
		t.Fatal("publication extractor shared selectors")
	}
	owned := cloneContinuation(c)
	owned.Position.KnowledgeSource.Cursor.Revision = model.RevisionID(digest('c'))
	owned.Position.KnowledgeSource.Knowledge.Actor = "caller-mutated"
	if c.Position.KnowledgeSource.Cursor.Revision != model.RevisionID(digest('b')) || c.Position.KnowledgeSource.Knowledge.Actor != "actor" {
		t.Fatal("clone shared producer selectors")
	}
	for end := 0; end < len(golden); end++ {
		if _, err := DecodeCommand(golden[:end]); err == nil {
			t.Fatalf("accepted truncated SourcePublication at %d", end)
		}
	}
	for _, change := range []func(*Continuation){
		func(c *Continuation) { c.Position.KnowledgeFence = c.InputFence },
		func(c *Continuation) { c.Position.KnowledgeSource.ProducerTx = "" },
		func(c *Continuation) { c.Position.KnowledgeSource.Knowledge = nil },
		func(c *Continuation) { c.Position.KnowledgeSource.Knowledge.Execution = "other" },
		func(c *Continuation) { c.Position.KnowledgeSource.Cursor = nil },
	} {
		bad := cloneContinuation(c)
		change(&bad)
		if _, err := EncodeCommand(Command{Lifecycle: &LifecycleCommand{PutContinuation: &bad}}); err == nil {
			t.Fatal("accepted partial or conflicting producer knowledge cut")
		}
	}
}
