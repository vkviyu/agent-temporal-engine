package client

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/investigation"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func investigationResponse() api.InvestigationResult {
	source := model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest(strings.Repeat("a", 64))}
	fence := model.CommitFence{Store: "store", LSN: 2, CommitDigest: model.CommitDigest(strings.Repeat("b", 64))}
	subject := investigation.Citation{
		Ref:    model.NodeRef{Kind: model.NodeObject, Object: model.ObjectID(strings.Repeat("c", 64))},
		Anchor: model.SnapshotAnchor{Space: "s", Fence: source},
	}
	return api.InvestigationResult{
		Found: true, Checkpoint: investigation.Checkpoint{ID: "inspect-result", Version: 1, Subject: subject},
		Anchor: model.SnapshotAnchor{Space: "s", Fence: fence}, Fence: fence,
		RootSet: model.ObjectID(strings.Repeat("d", 64)),
	}
}

func TestInvestigationClientRejectsMalformedRemoteState(t *testing.T) {
	for name, mutate := range map[string]func(*api.InvestigationResult){
		"wrong-investigation": func(r *api.InvestigationResult) { r.Checkpoint.ID = "other" },
		"wrong-evidence-cut":  func(r *api.InvestigationResult) { r.Checkpoint.Subject.Anchor = r.Anchor },
		"foreign-evidence":    func(r *api.InvestigationResult) { r.Checkpoint.Subject.Anchor.Space = "foreign" },
		"mismatched-fence":    func(r *api.InvestigationResult) { r.Fence.LSN++ },
		"data-without-match":  func(r *api.InvestigationResult) { r.Found = false },
	} {
		t.Run(name, func(t *testing.T) {
			response := investigationResponse()
			anchor := response.Anchor.Clone()
			request := api.GetInvestigationRequest{Space: "s", ID: response.Checkpoint.ID, Anchor: &anchor}
			mutate(&response)
			c := nodeResponseClient(t, response)
			if _, err := c.GetInvestigation(context.Background(), request); !errors.Is(err, protocol.ErrProtocol) || !c.closed.Load() {
				t.Fatalf("malformed investigation response accepted: %v", err)
			}
		})
	}
}

func TestInvestigationClientRejectsAckAtSourcePublication(t *testing.T) {
	checkpoint := investigationResponse().Checkpoint
	ack := api.CommitResult{
		Code: api.CodeDurableAck, RequestID: "update", TxID: model.TxID(strings.Repeat("a", 32)),
		Fence: checkpoint.Subject.Anchor.Fence, RequestDigest: model.ObjectID(strings.Repeat("c", 64)),
		ResultDigest: model.ObjectID(strings.Repeat("d", 64)), RootSet: model.ObjectID(strings.Repeat("e", 64)),
	}
	c := nodeResponseClient(t, ack)
	_, err := c.UpdateInvestigation(context.Background(), api.UpdateInvestigationRequest{
		Key:     model.RequestKey{Space: "s", RequestID: "update"},
		Command: investigation.Update{ID: checkpoint.ID, Subject: &checkpoint.Subject},
	})
	var unknown *OutcomeUnknownError
	if !errors.As(err, &unknown) || !errors.Is(err, protocol.ErrProtocol) || !c.closed.Load() {
		t.Fatalf("invalid publication ACK accepted: %v", err)
	}
}
