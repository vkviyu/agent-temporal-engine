package engine_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type dependencyOracle struct {
	dependencies map[string][]string
	restrictions map[string]string
}

func newDependencyOracle() *dependencyOracle {
	return &dependencyOracle{
		dependencies: map[string][]string{},
		restrictions: map[string]string{},
	}
}

func (o *dependencyOracle) Add(id string, dependencies ...string) {
	o.dependencies[id] = append([]string(nil), dependencies...)
}

func (o *dependencyOracle) Withdraw(id, kind string) {
	o.restrictions[id] = kind
}

func (o *dependencyOracle) Allowed(id string, audit bool) bool {
	pending := []string{id}
	visited := map[string]bool{}
	for len(pending) > 0 {
		last := len(pending) - 1
		current := pending[last]
		pending = pending[:last]
		if visited[current] {
			continue
		}
		visited[current] = true
		restriction := o.restrictions[current]
		if restriction == "privacy_erasure" || restriction == "acl_revocation" ||
			(!audit && restriction != "") {
			return false
		}
		pending = append(pending, o.dependencies[current]...)
	}
	return true
}

func TestIndependentMemoryDependencyClosureAcrossDisclosureAndRecovery(t *testing.T) {
	for _, kind := range []memory.RetractionKind{memory.RetractionUseBan, memory.RetractionPrivacyErasure} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := context.Background()
			s, primary := memoryFixture(t)
			supportAck := integrationMutate(t, s.e, integrationRequest("support", integrationPut([]byte("support"), integrationBytes('s'))))
			support := memory.Source{SourceID: pathTestStateObject(t, s.e, "support"), Anchor: sourceAnchor(supportAck.Fence)}
			if primary.SourceID == support.SourceID {
				t.Fatal("primary and direct path support must be distinct")
			}
			validation := memory.Validation{StrategyVersion: 1, Passed: true, Evidence: []memory.Source{support}}
			candidate := memory.Memory{
				Space: "host-space", Kind: memory.KindFact, Claim: []byte("primary dependency"),
				Sources: []memory.Source{primary, support},
			}
			applyMemory(t, s.e, "dependency-candidate", memory.Command{PutCandidate: &candidate})
			stored := memoryWithStatus(t, s.e, memory.StatusCandidate)
			applyMemory(t, s.e, "dependency-validate", memory.Command{Validate: &memory.ValidateCommand{ID: stored.ID, Validation: validation}})
			validated := memoryWithStatus(t, s.e, memory.StatusValidated)
			applyMemory(t, s.e, "dependency-promote", memory.Command{Promote: &validated.ID})
			promoted := memoryWithStatus(t, s.e, memory.StatusPromoted)
			pathCommand := memory.BuildPathCommand{
				Space: "host-space", Anchor: sourceAnchor(integrationSnapshot(t, s.e).Fence),
				Steps:      []memory.Step{{Memory: promoted.ID, Node: model.NodeRef{Kind: model.NodeObject, Object: support.SourceID}}},
				Validation: validation,
			}
			applyMemory(t, s.e, "dependency-path", memory.Command{BuildPath: &pathCommand})
			initial, err := s.e.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "host-space"})
			if err != nil || len(initial.Snapshot.Paths) != 1 {
				t.Fatalf("initial path: %+v %v", initial, err)
			}
			path := initial.Snapshot.Paths[0]
			// No direct primary edge in Steps.Node, Validation, Omitted or
			// DerivedFrom: the withdrawal must travel path -> memory -> primary.
			if len(path.Steps) != 1 || path.Steps[0].Memory != promoted.ID ||
				path.Steps[0].Node.Object != support.SourceID ||
				!reflect.DeepEqual(path.Validation, validation) ||
				len(path.Omitted) != 0 || len(path.DerivedFrom) != 0 {
				t.Fatalf("unexpected direct path dependencies: %+v", path)
			}
			want := newDependencyOracle()
			want.Add("memory/promoted", "source/primary", "source/support")
			want.Add("path/procedure", "memory/promoted", "source/support")
			// Also read the individual path, so a whole-space Memory denial
			// cannot hide a missing transitive path disclosure check.
			check := func(t *testing.T, stage string) {
				t.Helper()
				state, err := s.e.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "host-space"})
				if mismatch := want.compareAudit(state, err, promoted.ID, path.ID); mismatch != nil {
					t.Fatalf("%s: %v", stage, mismatch)
				}
				pathAnchor := sourceAnchor(initial.Fence)
				view, err := s.e.OpenView(ctx, api.OpenViewRequest{Space: "host-space", Principal: "host-principal", Anchor: &pathAnchor})
				if err != nil {
					t.Fatal(err)
				}
				ref := model.NodeRef{Kind: model.NodeEntity, Entity: model.EntityKey{
					Profile: "memory", Kind: "experience_path", ID: model.EntityID(path.ID),
				}}
				detail, readErr := s.e.GetNode(ctx, api.GetNodeRequest{View: view.Ref, Ref: ref})
				if err := s.e.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
					t.Fatal(err)
				}
				if mismatch := want.comparePathAudit(detail, readErr, ref); mismatch != nil {
					t.Fatalf("%s: %v", stage, mismatch)
				}
				reuse := pathCommand
				reuse.Anchor = sourceAnchor(integrationSnapshot(t, s.e).Fence)
				reuse.DerivedFrom = []model.ObjectID{path.ID}
				reuse.Steps = []memory.Step{{Memory: promoted.ID, Node: pathCommand.Steps[0].Node, Outcome: stage}}
				before := integrationSnapshot(t, s.e)
				ack, err := s.e.ApplyMemory(ctx, api.ApplyMemoryRequest{
					Key: agentKey("dependency-reuse-" + stage), Command: memory.Command{BuildPath: &reuse},
				})
				if mismatch := want.compareReuse(ack, err); mismatch != nil {
					t.Fatalf("%s: %v", stage, mismatch)
				}
				if !want.Allowed("path/procedure", false) && integrationSnapshot(t, s.e) != before {
					t.Fatal("denied reuse changed publication")
				}
			}
			check(t, "before-withdrawal")
			checkpoint, err := s.e.CreateRecoveryCheckpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			applyMemory(t, s.e, "dependency-withdraw", memory.Command{RetractSource: &memory.RetractSourceCommand{
				Source: primary.SourceID, Retraction: memory.Retraction{Kind: kind, Reason: "independent oracle"},
			}})
			want.Withdraw("source/primary", string(kind))
			check(t, "after-withdrawal")
			independentDisclosureRecovery(t, s, checkpoint.Position.LSN, check)

			t.Run("negative-controls", func(t *testing.T) {
				// Feed deterministic bad API results into the exact comparators
				// used above; these do not change the production implementation.
				if err := want.compareReuse(api.CommitResult{Code: api.CodeDurableAck}, nil); err == nil {
					t.Fatal("comparator accepted forbidden reuse")
				}
				if kind == memory.RetractionPrivacyErasure {
					if err := want.compareAudit(initial, nil, promoted.ID, path.ID); err == nil {
						t.Fatal("comparator accepted private snapshot disclosure")
					}
					if err := want.comparePathAudit(api.NodeDetail{Data: []byte("leaked")}, memory.ErrHistoryAccessDenied, model.NodeRef{}); err == nil {
						t.Fatal("comparator accepted data accompanying denial")
					}
				} else if err := want.compareAudit(api.MemoryStateResult{}, memory.ErrHistoryAccessDenied, promoted.ID, path.ID); err == nil {
					t.Fatal("comparator accepted loss of allowed audit history")
				}
			})
		})
	}
}

func (o *dependencyOracle) compareAudit(got api.MemoryStateResult, err error, memoryID, pathID model.ObjectID) error {
	allowed := o.Allowed("memory/promoted", true) && o.Allowed("path/procedure", true)
	if !allowed {
		if !errors.Is(err, memory.ErrHistoryAccessDenied) || !reflect.DeepEqual(got, api.MemoryStateResult{}) {
			return fmt.Errorf("audit must deny without content: %+v %v", got, err)
		}
		return nil
	}
	pathFound := false
	for _, path := range got.Snapshot.Paths {
		pathFound = pathFound || (path.ID == pathID && path.Status == memory.StatusPromoted)
	}
	if err != nil || !got.Found || !containsMemory(got.Snapshot.Memories, memoryID, memory.StatusPromoted) || !pathFound {
		return fmt.Errorf("allowed audit projection missing: %+v %v", got, err)
	}
	return nil
}

func (o *dependencyOracle) comparePathAudit(got api.NodeDetail, err error, ref model.NodeRef) error {
	if o.Allowed("path/procedure", true) {
		if err != nil || got.Ref != ref || len(got.Data) == 0 {
			return fmt.Errorf("allowed historical path missing: %+v %v", got, err)
		}
	} else if !errors.Is(err, memory.ErrHistoryAccessDenied) || !reflect.DeepEqual(got, api.NodeDetail{}) {
		return fmt.Errorf("historical path must deny without content: %+v %v", got, err)
	}
	return nil
}

func (o *dependencyOracle) compareReuse(got api.CommitResult, err error) error {
	if o.Allowed("path/procedure", false) {
		if err != nil || got.Code != api.CodeDurableAck {
			return fmt.Errorf("allowed reuse failed: %+v %v", got, err)
		}
	} else {
		denial := memory.ErrUseDenied
		if !o.Allowed("path/procedure", true) {
			denial = memory.ErrHistoryAccessDenied
		}
		if got.Code != api.CodeRejected || !errors.Is(err, denial) {
			return fmt.Errorf("reuse must reject with %v: %+v %v", denial, got, err)
		}
	}
	return nil
}

// Both disclosure oracles run the same recovery matrix. A nonzero certified
// prefix plus a withdrawal suffix proves checkpoint use, not merely reopen.
func independentDisclosureRecovery(t *testing.T, s *integrationStore, checkpointLSN uint64, check func(*testing.T, string)) {
	t.Helper()
	ctx := context.Background()
	end := integrationSnapshot(t, s.e)
	if checkpointLSN == 0 || checkpointLSN >= end.Fence.LSN {
		t.Fatal("fixture needs a nonempty checkpoint prefix and withdrawal suffix")
	}
	for _, stage := range []string{"checkpoint", "full-replay", "compaction-checkpoint", "compaction-full-replay"} {
		t.Run(stage, func(t *testing.T) {
			if stage == "compaction-checkpoint" {
				compacted, err := s.e.CompactAuditFull(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if compacted.CopiedBytes == 0 || compacted.After.Generation <= compacted.Before.Generation ||
					compacted.After.Fence() != compacted.Before.Fence() {
					t.Fatalf("compaction did not replace the physical layout at the same fence: %+v", compacted)
				}
				check(t, "after-compaction")
			}
			full := stage == "full-replay" || stage == "compaction-full-replay"
			s.options.DisableRecoveryCheckpoints = full
			report := s.reopen()
			skipped := checkpointLSN
			if full {
				skipped = 0
			}
			if report.CheckpointTransactions != skipped || report.ScannedTransactions != end.Fence.LSN {
				t.Fatalf("unexpected replay coverage: %+v; checkpoint=%d end=%d", report, skipped, end.Fence.LSN)
			}
			t.Logf("checkpoint transactions=%d, scanned transactions=%d", report.CheckpointTransactions, report.ScannedTransactions)
			check(t, stage)
			if integrationSnapshot(t, s.e) != end {
				t.Fatal("recovery/checks changed publication")
			}
		})
	}
	s.options.DisableRecoveryCheckpoints = false
}

func containsMemory(memories []memory.Memory, id model.ObjectID, status memory.Status) bool {
	for _, item := range memories {
		if item.ID == id && item.Status == status {
			return true
		}
	}
	return false
}
