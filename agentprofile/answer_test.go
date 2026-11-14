package agentprofile

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestFinalAnswerInputCanonicalWitness(t *testing.T) {
	input := FinalAnswerInput{SchemaVersion: 1, Query: "query", Outcomes: []TaskOutcome{{Task: "task", Execution: "exec", GoalEpoch: 1, Result: digest('a'), Validation: digest('b'), Sequence: 1}}, ContextPacket: digest('c'), Request: digest('d'), RequestDigest: digest('e'), HostPolicyVersion: "renderer/v1"}
	want := `{"SchemaVersion":1,"Query":"query","Outcomes":[{"Task":"task","Execution":"exec","GoalEpoch":1,"Result":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","Validation":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","Sequence":1}],"ContextPacket":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","Request":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","RequestDigest":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","HostPolicyVersion":"renderer/v1"}`
	raw, err := EncodeFinalAnswerInput(input)
	if err != nil || !bytes.Equal(raw, []byte(want)) {
		t.Fatalf("witness changed canonical bytes: %s %v", raw, err)
	}
	got, err := DecodeFinalAnswerInput([]byte(want))
	if err != nil || !reflect.DeepEqual(got, input) {
		t.Fatalf("witness changed on decode: %+v %v", got, err)
	}
	for _, malformed := range []string{
		strings.Replace(want, `"SchemaVersion":1`, `"SchemaVersion":2`, 1),
		strings.Replace(want, `"GoalEpoch":1`, `"GoalEpoch":0`, 1),
		strings.Replace(want, `"Sequence":1`, `"Sequence":0`, 1),
		strings.Replace(want, `"Task":"task"`, `"Task":""`, 1),
		strings.Replace(want, `"HostPolicyVersion":"renderer/v1"`, `"HostPolicyVersion":""`, 1),
		strings.Replace(want, `"Query":"query"`, `"Query":"query","Unrecognized":true`, 1),
		want + "\n",
	} {
		if _, err := DecodeFinalAnswerInput([]byte(malformed)); err == nil {
			t.Fatalf("accepted malformed witness: %s", malformed)
		}
	}
	duplicate := input
	duplicate.Outcomes = []TaskOutcome{input.Outcomes[0], input.Outcomes[0]}
	if _, err := EncodeFinalAnswerInput(duplicate); err == nil {
		t.Fatal("accepted duplicate task outcomes")
	}
	unordered := input
	first := input.Outcomes[0]
	first.Task = "z"
	unordered.Outcomes = []TaskOutcome{first, input.Outcomes[0]}
	if _, err := EncodeFinalAnswerInput(unordered); err == nil {
		t.Fatal("accepted unordered task outcomes")
	}
}
