package agentprofile

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// Independently assembled with Python SHA-256 and unsigned-varint framing;
// fields are explicitly ordered, without calling either Go codec.
const controlPutActionV6Golden = "4154452d4147454e542d434f4d4d414e4400010dd4024154452d4147454e542d434f4e54524f4c000613016b0101730170017240303464633562323133363332386130646362313839646639373733346337633732653565313232376661306330333436396136636536303866333266316236360573746f726500406464646464646464646464646464646464646464646464646464646464646464646464646464646464646464646464646464646464646464646464646464646401650770656e64696e670007686f73742f763103636d64016401010168014065656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565656565010573746f7265074064646464646464646464646464646464646464646464646464646464646464646464646464646464646464646464646464646464646464646464646464646464016401010168"

func TestControlV6ExpectedFenceGolden(t *testing.T) {
	binding := AuthorityBinding{Deployment: "d", OwnerEpoch: 1, Incarnation: 1, Holder: "h"}
	input := model.CommitFence{Store: "store", CommitDigest: model.CommitDigest(digest('d'))}
	expected := input
	expected.LSN = 7
	intent := ActionIntent{Action: PendingAction{Key: "k", Generation: 1, Request: model.RequestKey{Space: "s", Principal: "p", RequestID: "r"}, CommandDigest: ExactRequestDigest([]byte("cmd")), InputFence: input, Owner: "e", Status: ActionPending}, Method: "host/v1", ExactCommand: []byte("cmd"), Authority: binding, InputObjects: []model.ObjectID{digest('e')}, ExpectedFence: &expected}
	continuation := Continuation{Execution: "e", Version: 1, Incarnation: 1, PendingKey: "k", PendingGeneration: 1, InputFence: input, Phase: "dispatch", Position: &ContinuationPosition{Space: "s", Principal: "p", Step: 1, KnowledgeFence: input, KnowledgeCursor: model.Cursor{Store: "store", Space: "s", Revision: model.RevisionID(digest('b'))}, KnowledgeKey: model.KnowledgeKey{Actor: "a", Execution: "e"}, AdapterVersion: "host/v1", NextActions: []PendingActionRef{{Key: "k", Generation: 1}}}}
	commands := []ControlCommand{
		{PutAction: &PutActionCommand{Intent: intent, Authority: binding}},
		{Checkpoint: &CheckpointCommand{Intent: intent, Continuation: continuation, Authority: binding}},
		{RebindAction: &RebindActionCommand{Key: "k", ExpectedGeneration: 1, NewRequest: intent.Action.Request, Method: intent.Method, ExactCommand: intent.ExactCommand, CommandDigest: intent.Action.CommandDigest, Authority: binding, FenceEvidence: digest('f'), ExpectedFence: &expected}},
	}
	want, err := hex.DecodeString(controlPutActionV6Golden)
	if err != nil {
		t.Fatal(err)
	}
	for index, c := range commands {
		raw, err := EncodeCommand(Command{Control: &c})
		if err != nil || c.SchemaVersion() != 6 {
			t.Fatalf("schema6 encode=%v version=%d", err, c.SchemaVersion())
		}
		if index == 0 && !bytes.Equal(raw, want) {
			t.Fatalf("schema6 golden changed: got=%x want=%x", raw, want)
		}
		decoded, err := DecodeCommand(raw)
		if err != nil {
			t.Fatal(err)
		}
		var actual *model.CommitFence
		switch index {
		case 0:
			actual = decoded.Control.PutAction.Intent.ExpectedFence
		case 1:
			actual = decoded.Control.Checkpoint.Intent.ExpectedFence
		case 2:
			actual = decoded.Control.RebindAction.ExpectedFence
		}
		if actual == nil || *actual != expected {
			t.Fatalf("expected fence not preserved: %+v", actual)
		}
		for n := 0; n < len(raw); n++ {
			if _, err := DecodeCommand(raw[:n]); err == nil {
				t.Fatalf("accepted truncated operation %d at %d", index, n)
			}
		}
	}
}

func TestControlV3ExpectedFenceSurvivesRebindAndOwnedSnapshots(t *testing.T) {
	l, binding := recoveryFixture(t)
	expected := recoveryFence()
	intent := reservationIntent(binding, "next", "original")
	intent.ExpectedFence = &expected
	recoveryControl(t, l, "reserve", ControlCommand{PutAction: &PutActionCommand{Intent: intent, Authority: binding}})
	snapshot := l.Snapshot()
	snapshot.Lifecycle.Recovery.Actions[0].ExpectedFence.LSN++
	if *l.Snapshot().Lifecycle.Recovery.Actions[0].ExpectedFence != expected {
		t.Fatal("snapshot pointer mutated the authoritative reservation")
	}
	next := recoveryTakeover(t, l, binding)
	recoveryControl(t, l, "absent", ControlCommand{ResolveAction: &ResolveActionCommand{Resolution: ActionResolution{Key: "next", Generation: 1, Outcome: "absent", ObservedFence: recoveryFence()}}})
	reboundFence := recoveryFence()
	reboundFence.LSN = 9
	recoveryControl(t, l, "rebind", ControlCommand{RebindAction: &RebindActionCommand{Key: "next", ExpectedGeneration: 1, NewRequest: commandKey("rebound"), Method: intent.Method, ExactCommand: intent.ExactCommand, CommandDigest: intent.Action.CommandDigest, Authority: next, FenceEvidence: digest('a'), ExpectedFence: &reboundFence}})
	restored := restoreRecord(t, l.Snapshot())
	for _, record := range restored.Snapshot().Lifecycle.Recovery.Actions {
		want := &expected
		if record.Action.Generation == 2 {
			want = &reboundFence
		}
		if !reflect.DeepEqual(record.ExpectedFence, want) {
			t.Fatalf("restore replaced historical outer precondition: %+v", record)
		}
	}
}
