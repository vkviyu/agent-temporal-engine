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

func TestOutputChunksKeepOneResponseAndRequireExactTerminalManifest(t *testing.T) {
	l, auth := recoveryFixture(t)
	spec := recoveryActivity(t, l, auth, "stream", "pure")
	recoveryControl(t, l, "claim", ControlCommand{ClaimAttempt: &ClaimAttemptCommand{Activity: spec.Activity, Attempt: "attempt", Sequence: 1, Authority: auth}})
	recoveryControl(t, l, "dispatch", ControlCommand{DispatchAttempt: &DispatchAttemptCommand{Attempt: "attempt", Authority: auth, Evidence: digest('d')}})
	chunk := OutputChunk{Activity: spec.Activity, Attempt: "attempt", RequestDigest: spec.RequestDigest, Incarnation: 1, Response: "response-1", Sequence: 1, Payload: []byte("first")}
	chunk.ID = OutputChunkID(chunk)
	recoveryControl(t, l, "chunk-1", ControlCommand{CaptureOutputChunk: &CaptureOutputChunkCommand{Chunk: chunk}})
	recoveryControl(t, l, "chunk-1-again", ControlCommand{CaptureOutputChunk: &CaptureOutputChunkCommand{Chunk: chunk}})
	for name, mutate := range map[string]func(*OutputChunk){
		"gap":         func(c *OutputChunk) { c.Sequence = 3 },
		"response":    func(c *OutputChunk) { c.Response = "response-2" },
		"request":     func(c *OutputChunk) { c.RequestDigest = digest('e') },
		"incarnation": func(c *OutputChunk) { c.Incarnation = 2 },
		"replacement": func(c *OutputChunk) { c.Sequence = 1 },
	} {
		bad := chunk
		bad.Sequence = 2
		bad.Payload = []byte("second")
		mutate(&bad)
		bad.ID = OutputChunkID(bad)
		t.Run(name, func(t *testing.T) {
			assertRecoveryRejected(t, l, Command{Control: &ControlCommand{CaptureOutputChunk: &CaptureOutputChunkCommand{Chunk: bad}}}, ErrConflict)
		})
	}
	second := chunk
	second.Sequence = 2
	second.Payload = []byte("second")
	second.ID = OutputChunkID(second)
	recoveryControl(t, l, "chunk-2", ControlCommand{CaptureOutputChunk: &CaptureOutputChunkCommand{Chunk: second}})
	result := ActivityResult{ID: "result", Activity: spec.Activity, Attempt: chunk.Attempt, RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("firstsecond"), Capture: "full", Response: chunk.Response, OutputChunks: []model.ObjectID{chunk.ID, second.ID}}
	for name, mutate := range map[string]func(*ActivityResult){
		"omission": func(r *ActivityResult) { r.OutputChunks = r.OutputChunks[:1] },
		"reorder":  func(r *ActivityResult) { r.OutputChunks = []model.ObjectID{second.ID, chunk.ID} },
		"payload":  func(r *ActivityResult) { r.Payload = []byte("different") },
		"response": func(r *ActivityResult) { r.Response = "other" },
		"unframed": func(r *ActivityResult) { r.Response = ""; r.OutputChunks = nil },
	} {
		bad := result
		mutate(&bad)
		t.Run(name, func(t *testing.T) {
			assertRecoveryRejected(t, l, Command{Control: &ControlCommand{CaptureResult: &CaptureResultCommand{Result: bad}}}, ErrConflict)
		})
	}
	recoveryControl(t, l, "finalize", ControlCommand{CaptureResult: &CaptureResultCommand{Result: result}})
	third := second
	third.Sequence = 3
	third.Payload = []byte("late")
	third.ID = OutputChunkID(third)
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{CaptureOutputChunk: &CaptureOutputChunkCommand{Chunk: third}}}, ErrUniqueSlot)
	recoveryControl(t, l, "accept", ControlCommand{AcceptActivity: &AcceptActivityCommand{Activity: spec.Activity, Result: result.ID, ExpectedVersion: 1, Authority: auth, Evidence: digest('f')}})
	raw, err := EncodeSnapshot(l.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(l.Snapshot(), restored.Snapshot()) {
		t.Fatal("restore changed the output manifest")
	}
	copy := restored.Snapshot()
	copy.Lifecycle.Recovery.OutputChunks[0].Payload[0] = 'x'
	copy.Lifecycle.Results[0].OutputChunks[0] = digest('a')
	if !bytes.Equal(restored.Snapshot().Lifecycle.Recovery.OutputChunks[0].Payload, []byte("first")) && !bytes.Equal(restored.Snapshot().Lifecycle.Recovery.OutputChunks[0].Payload, []byte("second")) {
		t.Fatal("snapshot shared chunk storage")
	}
	if restored.Snapshot().Lifecycle.Results[0].OutputChunks[0] != chunk.ID {
		t.Fatal("snapshot shared manifest storage")
	}
}

func TestOutputCaptureCompletenessIsOrthogonalAndEnforcedAtAcceptance(t *testing.T) {
	for _, role := range []string{"resource_prepare", "aux_summary"} {
		for _, capture := range []string{"full", "truncated", "partial", "unknown"} {
			t.Run(role+"/"+capture, func(t *testing.T) {
				l, auth := recoveryFixture(t)
				spec := ActivitySpec{Activity: "a", Execution: "exec", Role: role, GoalEpoch: 1, InputFence: recoveryFence(), ContextPacket: digest('a'), Request: digest('b'), RequestDigest: digest('c'), Recipient: "executor", Effect: "pure", ResourceScope: "workspace"}
				recoveryControl(t, l, "create", ControlCommand{CreateActivity: &CreateActivityCommand{Spec: spec, Authority: auth}})
				recoveryControl(t, l, "claim", ControlCommand{ClaimAttempt: &ClaimAttemptCommand{Activity: "a", Attempt: "at", Sequence: 1, Authority: auth}})
				recoveryControl(t, l, "dispatch", ControlCommand{DispatchAttempt: &DispatchAttemptCommand{Attempt: "at", Authority: auth, Evidence: digest('d')}})
				result := ActivityResult{ID: "r", Activity: "a", Attempt: "at", RequestDigest: spec.RequestDigest, Incarnation: 1, Outcome: "success", Payload: []byte("recorded bytes"), Capture: capture}
				recoveryControl(t, l, "capture", ControlCommand{CaptureResult: &CaptureResultCommand{Result: result}})
				accept := Command{Control: &ControlCommand{AcceptActivity: &AcceptActivityCommand{Activity: "a", Result: "r", ExpectedVersion: 1, Authority: auth, Evidence: digest('e')}}}
				allowed := capture == "full" || capture == "truncated" && role == "resource_prepare"
				if allowed {
					recoveryApply(t, l, "accept", accept)
				} else {
					assertRecoveryRejected(t, l, accept, ErrConflict)
				}
				if got, _ := l.GetActivityResult("r"); got.Outcome != "success" || got.Capture != capture {
					t.Fatal("capture changed the recorded outcome")
				}
			})
		}
	}
}

func TestOutputChunkTotalBytesAreBoundedPerAttempt(t *testing.T) {
	l, auth := recoveryFixture(t)
	spec := recoveryActivity(t, l, auth, "bounded", "pure")
	recoveryControl(t, l, "claim", ControlCommand{ClaimAttempt: &ClaimAttemptCommand{Activity: spec.Activity, Attempt: "at", Sequence: 1, Authority: auth}})
	recoveryControl(t, l, "dispatch", ControlCommand{DispatchAttempt: &DispatchAttemptCommand{Attempt: "at", Authority: auth, Evidence: digest('d')}})
	chunk := OutputChunk{Activity: spec.Activity, Attempt: "at", RequestDigest: spec.RequestDigest, Incarnation: 1, Response: "response", Payload: make([]byte, MaxCommandBytes/2)}
	for i := uint64(1); i <= 2; i++ {
		chunk.Sequence = i
		chunk.ID = OutputChunkID(chunk)
		recoveryControl(t, l, "chunk-"+string(rune('0'+i)), ControlCommand{CaptureOutputChunk: &CaptureOutputChunkCommand{Chunk: chunk}})
	}
	chunk.Sequence = 3
	chunk.Payload = []byte("over limit")
	chunk.ID = OutputChunkID(chunk)
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{CaptureOutputChunk: &CaptureOutputChunkCommand{Chunk: chunk}}}, ErrConflict)
}

func TestOutputChunkIndependentGoldenAndOldControlRejection(t *testing.T) {
	chunk := OutputChunk{Activity: "a", Attempt: "at", RequestDigest: digest('c'), Incarnation: 1, Response: "response", Sequence: 1, Payload: []byte("hello")}
	chunk.ID = OutputChunkID(chunk)
	raw, err := EncodeCommand(Command{Control: &ControlCommand{CaptureOutputChunk: &CaptureOutputChunkCommand{Chunk: chunk}}})
	if err != nil {
		t.Fatal(err)
	}
	goldenHex, err := os.ReadFile("testdata/output-chunk-v1.hex")
	if err != nil {
		t.Fatal(err)
	}
	golden, err := hex.DecodeString(strings.TrimSpace(string(goldenHex)))
	if err != nil || !bytes.Equal(raw, golden) {
		t.Fatalf("output golden changed: %x / %x %v", raw, golden, err)
	}
	decoded, err := DecodeCommand(golden)
	if err != nil || !reflect.DeepEqual(decoded.Control.CaptureOutputChunk.Chunk, chunk) {
		t.Fatal("output chunk codec lost fields", err)
	}
	old := bytes.Clone(raw)
	offset := bytes.Index(old, []byte(controlDomain)) + len(controlDomain)
	old[offset] = 5
	if _, err = DecodeCommand(old); err != ErrCommandVersion {
		t.Fatal("old control schema accepted", err)
	}
}
