package engine_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func workSeedObservation(t *testing.T, key model.KnowledgeKey, payload model.ObjectID, fence model.CommitFence, sequence uint64, label string) api.WorkStep {
	t.Helper()
	o := knowledge.Observation{Key: key, Payload: payload, Source: knowledge.SourceAnchor{Anchor: model.SnapshotAnchor{Space: "host-space", Fence: fence}}, Sequence: sequence, Label: label}
	o.ID = knowledge.ObservationID(o)
	step, err := api.KnowledgeWorkStep(knowledge.Command{Observation: &o})
	if err != nil {
		t.Fatal(err)
	}
	return step
}

func assertWorkSeedCut(t *testing.T, s *integrationStore, ack api.CommitResult, execution model.ExecutionID, count int) agentprofile.Continuation {
	t.Helper()
	ctx := context.Background()
	control, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &model.SnapshotAnchor{Space: "host-space", Fence: ack.Fence}})
	if err != nil {
		t.Fatal(err)
	}
	var continuation agentprofile.Continuation
	for _, c := range control.Snapshot.Lifecycle.Continuations {
		if c.Execution == execution {
			continuation = c
		}
	}
	if continuation.Position == nil || continuation.Position.KnowledgeSource == nil {
		t.Fatalf("missing atomic seed location: %+v", continuation)
	}
	source := continuation.Position.KnowledgeSource
	if source.ProducerTx != ack.TxID || source.Knowledge == nil || source.Knowledge.Execution != execution || source.Cursor == nil {
		t.Fatalf("wrong seed producer: %+v", source)
	}
	resolved, err := s.e.ResolvePublication(ctx, api.ResolvePublicationRequest{Principal: "host-principal", Source: *source})
	if err != nil || resolved.Anchor.Fence != ack.Fence || resolved.RootSet != ack.RootSet {
		t.Fatalf("seed producer substituted: %+v %v", resolved, err)
	}
	known, err := s.e.GetKnowledgeState(ctx, api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal", Anchor: &resolved.Anchor})
	if err != nil || len(known.Snapshot.Observations) != count || known.Revision == nil || known.Revision.ID != source.Cursor.Revision {
		t.Fatalf("seed cut not exact: %+v %v", known, err)
	}
	return continuation
}

func TestWorkRootSeedCutRemainsExactAcrossNewKnowledgeAndRestart(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
	intake, start := workIntake(t, s)
	workApply(t, s, intake)
	ack := workApply(t, s, start)
	original := assertWorkSeedCut(t, s, ack, "intake-root", 2)
	step, err := knowledge.DecodeCommand(start.Unit.Steps[2].Body)
	if err != nil {
		t.Fatal(err)
	}
	next := step.Observation.Clone()
	next.Sequence = 3
	next.Label = "later explicit adoption"
	next.Source.Anchor.Fence = ack.Fence
	next.ID = knowledge.ObservationID(next)
	applyObservation(t, s.e, "later-root-knowledge", next)
	assertWorkSeedCut(t, s, ack, "intake-root", 2)
	s.reopen()
	got := assertWorkSeedCut(t, s, ack, "intake-root", 2)
	if !reflect.DeepEqual(got, original) {
		t.Fatal("replay rewrote exact seed location")
	}
	if retry := workApply(t, s, start); retry != ack {
		t.Fatal("seed retry changed receipt/producer")
	}
}

func TestWorkRootSeedRequiresExactInputsAndUnpublishedTarget(t *testing.T) {
	for _, name := range []string{"missing-seed", "wrong-query", "wrong-goal", "foreign-key", "future-source", "existing-target"} {
		t.Run(name, func(t *testing.T) {
			s := newIntegrationStore(t, engine.Options{AgentProfile: true})
			intake, start := workIntake(t, s)
			if name == "missing-seed" {
				start.Unit.Steps = append(start.Unit.Steps[:2:2], start.Unit.Steps[4:]...)
			} else if name == "existing-target" {
				task := agentTask("already-existing-task")
				coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("existing-task"), Command: agentprofile.Command{CreateTask: &task}})
				x := agentprofile.Execution{ID: "intake-root", Task: task.ID, Actor: "driver", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
				coreAgentCommit(t, s.e, api.ApplyAgentRequest{Key: agentKey("existing-execution"), Command: agentprofile.Command{Spawn: &x}})
			} else {
				index := 2
				if name == "wrong-goal" {
					index = 3
				}
				command, err := knowledge.DecodeCommand(start.Unit.Steps[index].Body)
				if err != nil {
					t.Fatal(err)
				}
				o := command.Observation
				switch name {
				case "wrong-query", "wrong-goal":
					other, err := knowledge.DecodeCommand(start.Unit.Steps[5-index].Body)
					if err != nil {
						t.Fatal(err)
					}
					o.Payload = other.Observation.Payload
				case "foreign-key":
					o.Key.Actor = "foreign"
				case "future-source":
					o.Source.Anchor.Fence.LSN += 100
				}
				o.ID = knowledge.ObservationID(*o)
				start.Unit.Steps[index], err = api.KnowledgeWorkStep(command)
				if err != nil {
					t.Fatal(err)
				}
			}
			body, err := api.EncodeWorkUnit(start.Unit)
			if err != nil {
				t.Fatal(err)
			}
			intake.Unit.Intake.Next.Body = body
			workApply(t, s, intake)
			before := integrationSnapshot(t, s.e)
			out, err := s.e.ApplyWork(context.Background(), start)
			if err == nil || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
				t.Fatalf("invalid initial seed published: %+v %v", out, err)
			}
		})
	}
}

func TestWorkSeedContinuationRejectsMissingProducerCursorAndEmptyDowngrade(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	intake, start := workIntake(t, s)
	workApply(t, s, intake)
	ack := workApply(t, s, start)
	c := assertWorkSeedCut(t, s, ack, "intake-root", 2)
	authority := agentprofile.AuthorityBinding{Deployment: "local", OwnerEpoch: 1, Incarnation: 1, Holder: "driver"}
	for _, name := range []string{"unknown-producer", "wrong-cursor", "foreign-principal", "empty-knowledge"} {
		t.Run(name, func(t *testing.T) {
			candidate := c
			position := *c.Position
			ref := c.Position.KnowledgeSource.Clone()
			position.KnowledgeSource = &ref
			candidate.Position = &position
			candidate.Version++
			key := agentKey(name)
			switch name {
			case "unknown-producer":
				ref.ProducerTx = model.TxID(strings.Repeat("e", 32))
			case "wrong-cursor":
				ref.Cursor.Revision = model.RevisionID(integrationID('e'))
			case "foreign-principal":
				key.Principal = "foreign"
			case "empty-knowledge":
				position.KnowledgeSource = nil
			}
			before := integrationSnapshot(t, s.e)
			out, err := s.e.ApplyAgent(context.Background(), api.ApplyAgentRequest{Key: key, Command: agentprofile.Command{Control: &agentprofile.ControlCommand{SaveContinuation: &agentprofile.SaveContinuationCommand{Value: candidate, ExpectedVersion: 1, Authority: authority}}}})
			if err == nil || out.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
				t.Fatalf("invalid recovery cut accepted %+v %v", out, err)
			}
			if name == "unknown-producer" && !errors.Is(err, agentprofile.ErrUnknown) {
				t.Fatal(err)
			}
		})
	}
}

func TestWorkSeedContinuationKeepsProducerAfterIncarnationChange(t *testing.T) {
	s := newIntegrationStore(t, engine.Options{AgentProfile: true})
	intake, start := workIntake(t, s)
	workApply(t, s, intake)
	ack := workApply(t, s, start)
	c := assertWorkSeedCut(t, s, ack, "intake-root", 2)
	command, err := knowledge.DecodeCommand(start.Unit.Steps[2].Body)
	if err != nil {
		t.Fatal(err)
	}
	authority := agentprofile.AuthorityBinding{Deployment: "local", OwnerEpoch: 2, Incarnation: 2, Holder: "replacement"}
	recoveryCoreControl(t, s, "fence-seed-worker", agentprofile.ControlCommand{FenceAuthority: &agentprofile.FenceAuthorityCommand{Execution: "intake-root", ExpectedVersion: 1, Evidence: model.ObjectID(integrationID('a'))}})
	recoveryCoreControl(t, s, "resume-seed-worker", agentprofile.ControlCommand{Resume: &agentprofile.ResumePlan{Execution: "intake-root", ExpectedIncarnation: 1, ExpectedAuthorityVersion: 2, Next: authority, AdapterVersion: "work/v1", Checkpoint: command.Observation.Payload, CompatibilityEvidence: model.ObjectID(integrationID('b')), ResourceFenceEvidence: model.ObjectID(integrationID('c'))}})
	c.Version, c.Incarnation = 2, 2
	recoveryCoreControl(t, s, "save-seed-after-resume", agentprofile.ControlCommand{SaveContinuation: &agentprofile.SaveContinuationCommand{Value: c, ExpectedVersion: 1, Authority: authority}})
	s.reopen()
	state, err := s.e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
	if err != nil || len(state.Snapshot.Lifecycle.Continuations) != 1 {
		t.Fatal(state, err)
	}
	got := state.Snapshot.Lifecycle.Continuations[0]
	if got.Incarnation != 2 || !reflect.DeepEqual(got.Position.KnowledgeSource, c.Position.KnowledgeSource) {
		t.Fatalf("takeover changed seed source: %+v", got)
	}
	resolved, err := s.e.ResolvePublication(context.Background(), api.ResolvePublicationRequest{Principal: "host-principal", Source: *got.Position.KnowledgeSource})
	if err != nil || resolved.Anchor.Fence != ack.Fence {
		t.Fatal(resolved, err)
	}
}

func TestWorkChildSeedPublicationUnknownNeverRecoversPartialFacts(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-current", true: "after-current"}[after], func(t *testing.T) {
			s, request, child, _ := workChildFixture(t)
			if err := s.e.Close(); err != nil {
				t.Fatal(err)
			}
			fault := &workCurrentFaultIO{DurableIO: s.d, after: after}
			var err error
			s.e, _, err = engine.OpenExisting(fault, s.options)
			if err != nil {
				t.Fatal(err)
			}
			fault.armed = true
			out, err := s.e.ApplyWork(context.Background(), request)
			if out.Code != api.CodeCommitUnknown || !errors.Is(err, errWorkCurrent) {
				t.Fatalf("unknown child seed: %+v %v", out, err)
			}
			s.options.Journal.TailPolicy = storage.TailQuarantine
			s.reopen()
			state, err := s.e.GetAgentState(context.Background(), api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
			if err != nil {
				t.Fatal(err)
			}
			childExists, continuationExists := false, false
			for _, x := range state.Snapshot.Executions {
				childExists = childExists || x.ID == child.ID
			}
			for _, c := range state.Snapshot.Lifecycle.Continuations {
				continuationExists = continuationExists || c.Execution == child.ID
			}
			known, err := s.e.GetKnowledgeState(context.Background(), api.ReadKnowledgeStateRequest{Space: "host-space", Principal: "host-principal"})
			if err != nil {
				t.Fatal(err)
			}
			observations := 0
			for _, o := range known.Snapshot.Observations {
				if o.Key.Execution == child.ID {
					observations++
				}
			}
			want := 0
			if after {
				want = 2
			}
			if childExists != after || continuationExists != after || observations != want {
				t.Fatalf("mixed child recovery child=%v continuation=%v observations=%d", childExists, continuationExists, observations)
			}
			ack := workApply(t, s, request)
			assertWorkSeedCut(t, s, ack, child.ID, 2)
		})
	}
}
