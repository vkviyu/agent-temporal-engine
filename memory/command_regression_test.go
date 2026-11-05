package memory

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func memoryCommandKey(id string) model.RequestKey {
	return model.RequestKey{Space: "space", Principal: "owner", RequestID: model.RequestID(id)}
}
func candidateCommand() Memory {
	return Memory{Space: "space", Kind: KindFact, Claim: []byte("canonical source fact"), Sources: []Source{source(1)}}
}

func TestMemoryCommandCodecClosedAndCanonical(t *testing.T) {
	candidate := candidateCommand()
	id := digest('a')
	commands := []Command{
		{PutCandidate: &candidate},
		{Validate: &ValidateCommand{ID: id, Validation: Validation{StrategyVersion: 1, Passed: true, Evidence: []Source{source(1)}}}},
		{Promote: &id},
		{Transition: &TransitionCommand{ID: id, To: StatusRejected, Why: "invalid claim"}},
		{RetractSource: &RetractSourceCommand{Source: id, Retraction: Retraction{Kind: RetractionUseBan, Reason: "revoked source"}}},
		{BuildPath: &BuildPathCommand{Space: "space", Anchor: anchor(2), Steps: []Step{{Node: model.NodeRef{Kind: model.NodeObject, Object: id}, Memory: id, Order: 0}}, Validation: Validation{Passed: true, Evidence: []Source{source(1)}}}},
		{RetractPath: &RetractPathCommand{ID: id, Retraction: Retraction{Kind: RetractionPrivacyErasure, Reason: "remove path"}}},
	}
	for _, command := range commands {
		raw, err := EncodeCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeCommand(raw)
		if err != nil {
			t.Fatal(err)
		}
		canonical, err := EncodeCommand(decoded)
		if err != nil || !bytes.Equal(raw, canonical) {
			t.Fatalf("roundtrip: %v", err)
		}
		for n := 0; n < len(raw); n++ {
			if _, err = DecodeCommand(raw[:n]); err == nil {
				t.Fatalf("truncation accepted at %d", n)
			}
		}
		if _, err = DecodeCommand(append(raw, 0)); err == nil {
			t.Fatal("trailing command bytes accepted")
		}
	}
	for _, c := range []Command{{}, {PutCandidate: &candidate, Promote: &id}} {
		if _, err := EncodeCommand(c); err == nil {
			t.Fatal("invalid union accepted")
		}
	}
	if _, err := DecodeCommand([]byte(`{"PutCandidate":{}}`)); err == nil {
		t.Fatal("wire JSON treated as identity")
	}
}

func TestMemoryCommandCannotBypassValidationOrCrossSpace(t *testing.T) {
	store := New()
	candidate := candidateCommand()
	created, err := store.ApplyCommand(memoryCommandKey("candidate"), Command{PutCandidate: &candidate})
	if err != nil {
		t.Fatal(err)
	}
	id := created.Memory.ID
	for _, to := range []Status{StatusValidated, StatusPromoted} {
		before, _ := EncodeSnapshot(store.Snapshot())
		_, err = store.ApplyCommand(memoryCommandKey("bypass"), Command{Transition: &TransitionCommand{ID: id, To: to, Why: "pretend verified"}})
		if !errors.Is(err, ErrCommandShape) {
			t.Fatalf("validation bypass %s: %v", to, err)
		}
		after, _ := EncodeSnapshot(store.Snapshot())
		if !bytes.Equal(before, after) {
			t.Fatal("rejection changed source")
		}
	}
	foreign := memoryCommandKey("foreign")
	foreign.Space = "other"
	_, err = store.ApplyCommand(foreign, Command{Transition: &TransitionCommand{ID: id, To: StatusRejected, Why: "cross space"}})
	if !errors.Is(err, ErrCommandScope) {
		t.Fatalf("cross-space transition: %v", err)
	}
	injected := candidateCommand()
	injected.Validation = &Validation{Passed: true, Evidence: []Source{source(1)}}
	if _, err = EncodeCommand(Command{PutCandidate: &injected}); !errors.Is(err, ErrCommandShape) {
		t.Fatalf("injected lifecycle: %v", err)
	}
	validated, err := store.ApplyCommand(memoryCommandKey("validate"), Command{Validate: &ValidateCommand{ID: id, Validation: Validation{Passed: true, StrategyVersion: 1, Evidence: []Source{source(1)}}}})
	if err != nil {
		t.Fatal(err)
	}
	promotedID := validated.Memory.ID
	promoted, err := store.ApplyCommand(memoryCommandKey("promote"), Command{Promote: &promotedID})
	if err != nil || promoted.Memory.Status != StatusPromoted {
		t.Fatalf("promote=%+v err=%v", promoted, err)
	}
}

func TestMemoryApplyCommandOwnsInputsAndConcurrentCandidates(t *testing.T) {
	store := New()
	candidate := candidateCommand()
	result, err := store.ApplyCommand(memoryCommandKey("original"), Command{PutCandidate: &candidate})
	if err != nil {
		t.Fatal(err)
	}
	candidate.Claim[0] = 'x'
	result.Memory.Claim[0] = 'y'
	got, err := store.Get(result.Memory.ID)
	if err != nil || string(got.Claim) != "canonical source fact" {
		t.Fatalf("input/result alias: %+v %v", got, err)
	}
	var wg sync.WaitGroup
	errorsCh := make(chan error, 24)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m := candidateCommand()
			m.Claim = []byte(fmt.Sprintf("candidate %d", i))
			_, err := store.ApplyCommand(memoryCommandKey(fmt.Sprint(i)), Command{PutCandidate: &m})
			errorsCh <- err
		}(i)
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(store.Snapshot().Memories) != 25 {
		t.Fatalf("concurrent candidates lost writes: %d", len(store.Snapshot().Memories))
	}
}
