package engine_test

import (
	"context"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// A checkpoint fixture chooses a published cut explicitly. It may be older
// than latest when recovering a pending renderer's original input.
func bindTestContinuationKnowledge(t *testing.T, s *integrationStore, c *agentprofile.Continuation, key model.KnowledgeKey, cut model.CommitFence) {
	t.Helper()
	a := model.SnapshotAnchor{Space: "host-space", Fence: cut, Knowledge: &key}
	known, err := s.e.GetKnowledgeState(context.Background(), api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &a})
	if err != nil {
		t.Fatal(err)
	}
	if known.Revision == nil || known.Revision.Sequence == 0 {
		return
	}
	c.Position.KnowledgeFence = known.Fence
	c.Position.KnowledgeCursor = model.Cursor{Store: cut.Store, Space: a.Space, Revision: known.Revision.ID}
	c.Position.KnowledgeKey = key
}
