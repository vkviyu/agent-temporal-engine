package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var errWorkPartialWrite = errors.New("test: work segment write became uncertain")

type workPartialWriteIO struct {
	storage.DurableIO
	armed bool
}

type workPartialWriteFile struct {
	storage.DurableFile
	owner *workPartialWriteIO
}

func (d *workPartialWriteIO) OpenAppend(name string) (storage.DurableFile, error) {
	file, err := d.DurableIO.OpenAppend(name)
	if file == nil {
		return nil, err
	}
	return &workPartialWriteFile{DurableFile: file, owner: d}, err
}

func (f *workPartialWriteFile) Write(p []byte) (int, error) {
	if !f.owner.armed {
		return f.DurableFile.Write(p)
	}
	f.owner.armed = false
	n := len(p) / 2
	if n == 0 {
		n = len(p)
	}
	written, err := f.DurableFile.Write(p[:n])
	if err != nil {
		return written, err
	}
	return written, errWorkPartialWrite
}

func TestWorkMessageConsumptionPartialSegmentWriteRecoversWholePublication(t *testing.T) {
	fixture := newMessageWorkFixture(t)
	s, request := fixture.store, fixture.request
	if err := s.e.Close(); err != nil {
		t.Fatal(err)
	}
	fault := &workPartialWriteIO{DurableIO: s.d, armed: true}
	var err error
	s.e, _, err = engine.OpenExisting(fault, s.options)
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.e.ApplyWork(context.Background(), request)
	if out.Code != api.CodeCommitUnknown || !errors.Is(err, errWorkPartialWrite) {
		t.Fatalf("partial Work publication was not unknown: %+v %v", out, err)
	}
	s.options.Journal.TailPolicy = storage.TailQuarantine
	s.reopen()
	state, err := s.e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: request.Key.Space, Principal: request.Key.Principal})
	if err != nil || len(state.Snapshot.Lifecycle.Consumptions) != 0 || len(state.Snapshot.Lifecycle.Continuations) != 0 {
		t.Fatalf("partial Work recovered control facts: %+v %v", state, err)
	}
	known, err := s.e.GetKnowledgeState(context.Background(), api.ReadKnowledgeStateRequest{Space: request.Key.Space, Principal: request.Key.Principal, Knowledge: &fixture.observed.Key})
	if err != nil || known.Revision == nil || known.Revision.Observation != fixture.prior.ID {
		var got model.ObjectID
		if known.Revision != nil {
			got = known.Revision.Observation
		}
		t.Fatalf("partial Work recovered Knowledge facts: got=%s prior=%s observed=%s state=%+v err=%v", got, fixture.prior.ID, fixture.observed.ID, known, err)
	}
	lookup, err := s.e.LookupRequest(context.Background(), api.LookupRequestRequest{Space: request.Key.Space, Principal: request.Key.Principal, RequestID: request.Key.RequestID})
	if err != nil || lookup.Code != api.CodeNotFoundAtFence {
		t.Fatalf("partial Work retained an uncertain receipt: %+v %v", lookup, err)
	}
	ack := workApply(t, s, request)
	assertWorkSeedCut(t, s, ack, fixture.observed.Key.Execution, 2)
}
