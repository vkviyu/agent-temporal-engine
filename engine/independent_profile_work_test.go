package engine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	oracle "github.com/vkviyu/agent-temporal-engine/tests/reference"
)

func TestIndependentWorkProjectionNeverPublishesFailedPrefix(t *testing.T) {
	for failureAt := range 4 {
		t.Run(fmt.Sprint(failureAt), func(t *testing.T) {
			ctx := context.Background()
			s := newIntegrationStore(t, engine.Options{AgentProfile: true})
			intake, start := workIntake(t, s)
			bad := workControl(t, agentprofile.ControlCommand{CancelExecution: &agentprofile.CancelExecutionCommand{Execution: "intake-root", ExpectedIncarnation: 99}})
			steps := append([]api.WorkStep(nil), start.Unit.Steps[:failureAt]...)
			steps = append(steps, bad)
			steps = append(steps, start.Unit.Steps[failureAt:]...)
			start.Unit.Steps = steps
			body, err := api.EncodeWorkUnit(start.Unit)
			if err != nil {
				t.Fatal(err)
			}
			intake.Unit.Intake.Next.Body = body
			workApply(t, s, intake)
			want := oracle.NewProfile()
			modelErr := want.Batch(
				func(p *oracle.Profile) error { return p.CreateTask("intake-task") },
				func(p *oracle.Profile) error { return p.Spawn("intake-root", "intake-task", "") },
				func(p *oracle.Profile) error { return p.Grant("intake-root", "driver") },
				func(p *oracle.Profile) error { return p.Resume("intake-root", 99, oracle.Authority{}) },
			)
			if modelErr == nil {
				t.Fatal("reference incorrectly accepted invalid atomic unit")
			}
			before := integrationSnapshot(t, s.e)
			result, err := s.e.ApplyWork(ctx, start)
			if err == nil || result.Code != api.CodeRejected || integrationSnapshot(t, s.e) != before {
				t.Fatalf("failed prefix published: %+v %v", result, err)
			}
			check := func() {
				t.Helper()
				got, err := s.e.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "host-space", Principal: "host-principal"})
				if err != nil || len(got.Snapshot.Tasks) != len(want.Tasks) || len(got.Snapshot.Executions) != len(want.Executions) || len(got.Snapshot.Lifecycle.Continuations) != 0 {
					t.Fatalf("candidate leaked: %+v %v", got, err)
				}
			}
			check()
			s.reopen()
			check()
		})
	}
}
