package engine_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
)

func TestQueryIntakeRawInputWithdrawalBeforeAndAfterStart(t *testing.T) {
	for _, started := range []bool{false, true} {
		for _, kind := range []memory.RetractionKind{memory.RetractionFactualCorrection, memory.RetractionUseBan, memory.RetractionPrivacyErasure, memory.RetractionACLRevocation} {
			name := "pending/" + string(kind)
			if started {
				name = "started/" + string(kind)
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				s := newIntegrationStore(t, engine.Options{AgentProfile: true, EventProfile: true})
				intake, start := workIntake(t, s)
				workApply(t, s, intake)
				if started {
					workApply(t, s, start)
				}
				read := api.ReadQueryIntakeRequest{Space: "host-space", Principal: "host-principal", Query: "query"}
				original, err := s.e.GetQueryIntake(ctx, read)
				if err != nil || !original.Found || !bytes.Equal(original.State.Intake.Query.Input, intake.Unit.Intake.Query.Input) {
					t.Fatalf("intake setup failed: %+v %v", original, err)
				}
				seed, err := knowledge.DecodeCommand(start.Unit.Steps[2].Body)
				if err != nil || seed.Observation == nil {
					t.Fatal("missing explicit Query input source", err)
				}
				applyMemory(t, s.e, "intake-source-withdraw", memory.Command{RetractSource: &memory.RetractSourceCommand{Source: seed.Observation.Payload, Retraction: memory.Retraction{Kind: kind, Reason: "withdraw exact pending intake source"}}})
				privacy := kind == memory.RetractionPrivacyErasure || kind == memory.RetractionACLRevocation
				check := func() {
					t.Helper()
					before := integrationSnapshot(t, s.e)
					for _, historical := range []bool{false, true} {
						r := read
						if historical {
							anchor := original.Anchor.Clone()
							r.Anchor = &anchor
						}
						got, err := s.e.GetQueryIntake(ctx, r)
						if privacy {
							if !errors.Is(err, memory.ErrHistoryAccessDenied) || got.Found || len(got.State.Intake.Query.Input) != 0 || len(got.State.Intake.Next.Body) != 0 {
								t.Errorf("historical=%v: intake disclosed withdrawn source: found=%v input_bytes=%d command_bytes=%d err=%v", historical, got.Found, len(got.State.Intake.Query.Input), len(got.State.Intake.Next.Body), err)
							}
						} else if err != nil || !got.Found || !bytes.Equal(got.State.Intake.Query.Input, original.State.Intake.Query.Input) || !bytes.Equal(got.State.Intake.Next.Body, original.State.Intake.Next.Body) {
							t.Errorf("historical=%v: audit input changed: %+v %v", historical, got, err)
						}
					}
					if after := integrationSnapshot(t, s.e); after != before {
						t.Fatal("intake disclosure rejection changed or fenced store")
					}
				}
				check()
				s.reopen()
				check()
			})
		}
	}
}
