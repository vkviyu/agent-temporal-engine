package reference_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/model"
	oracle "github.com/vkviyu/agent-temporal-engine/tests/reference"
)

func TestIndependentActionGenerationsReconcileOriginalReceiptBeforeRebind(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	options := ate.Options{AgentProfile: true}
	store, err := ate.Create(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			_ = store.Close()
		}
	}()
	good := func(r api.CommitResult, e error) api.CommitResult {
		t.Helper()
		if e != nil || r.Code != api.CodeDurableAck {
			t.Fatalf("%+v %v", r, e)
		}
		return r
	}
	apply := func(id string, c agentprofile.Command) api.CommitResult {
		t.Helper()
		return good(store.ApplyAgent(ctx, api.ApplyAgentRequest{Key: key(id), Command: c}))
	}
	control := func(id string, c agentprofile.ControlCommand) api.CommitResult {
		t.Helper()
		return apply(id, agentprofile.Command{Control: &c})
	}
	checkpoint := good(store.Mutate(ctx, api.MutateRequest{Key: key("checkpoint"), Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte("checkpoint"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("immutable resource checkpoint")}}}}}))
	source := model.SnapshotAnchor{Space: "space", Fence: checkpoint.Fence}
	view, e := store.OpenView(ctx, api.OpenViewRequest{Space: "space", Principal: "owner", Anchor: &source})
	if e != nil {
		t.Fatal(e)
	}
	entries, e := store.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
	if e != nil || len(entries.Items) != 1 {
		t.Fatal(e)
	}
	checkpointID := entries.Items[0].Value
	_ = store.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	task := agentprofile.Task{ID: "task", Space: "space", Owner: "owner", Goal: "recover", GoalEpoch: 1, Status: agentprofile.TaskOpen}
	apply("task", agentprofile.Command{CreateTask: &task})
	execution := agentprofile.Execution{ID: "root", Task: task.ID, Actor: "worker", Incarnation: 1, AcceptedGoalEpoch: 1, Status: agentprofile.ExecutionReady}
	apply("execution", agentprofile.Command{Spawn: &execution})
	authority := agentprofile.AuthorityBinding{Deployment: "host", Holder: "worker", OwnerEpoch: 1, Incarnation: 1}
	control("grant", agentprofile.ControlCommand{GrantAuthority: &agentprofile.GrantAuthorityCommand{Authority: agentprofile.ExecutionAuthority{Execution: execution.ID, Binding: authority, Status: "active", Evidence: digest("a")}}})
	want := oracle.NewActionRecovery(oracle.ActionAuthority{Holder: authority.Holder, Epoch: 1, Incarnation: 1, Active: true, Evidence: string(digest("a"))})
	body, e := engine.EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{{Key: []byte("effect"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("once")}}}})
	if e != nil {
		t.Fatal(e)
	}
	makeRequest := func(id string) api.CommitRequest {
		return api.CommitRequest{Space: "space", Principal: "owner", RequestID: model.RequestID(id), Method: api.MethodMutateState, Body: body}
	}
	publishedRequest := makeRequest("published-before-reservation")
	beforePublished, _ := store.Snapshot(ctx)
	publishedRequest.ExpectedFence = &beforePublished.Fence
	exactFences := map[string]*model.CommitFence{string(publishedRequest.RequestID): publishedRequest.ExpectedFence}
	publishedAck := good(store.Commit(ctx, publishedRequest))
	abstract := func(k string, r api.CommitRequest, a oracle.ActionAuthority) oracle.ExactAction {
		f := ""
		if r.ExpectedFence != nil {
			f = fmt.Sprint(*r.ExpectedFence)
		}
		return oracle.ExactAction{Key: k, Owner: "root", Request: string(r.RequestID), Method: r.Method, Body: string(r.Body), ExpectedFence: f, Generation: 1, Authority: a, Status: "pending"}
	}
	published := abstract("published", publishedRequest, want.Authority)
	want.Published(published, string(publishedAck.ResultDigest), publishedAck.Fence.LSN)
	intent := func(x oracle.ExactAction) agentprofile.ActionIntent {
		return agentprofile.ActionIntent{Action: agentprofile.PendingAction{Key: x.Key, Generation: x.Generation, Request: key(x.Request), Owner: "root", Status: agentprofile.ActionPending, CommandDigest: independentBytesDigest([]byte(x.Body)), InputFence: source.Fence}, Method: x.Method, ExactCommand: []byte(x.Body), ExpectedFence: exactFences[x.Request], Authority: agentprofile.AuthorityBinding{Deployment: "host", Holder: x.Authority.Holder, OwnerEpoch: x.Authority.Epoch, Incarnation: x.Authority.Incarnation}}
	}
	compare := func(label string) {
		t.Helper()
		state, e := store.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "space", Principal: "owner"})
		if e != nil {
			t.Fatal(e)
		}
		current := map[string]oracle.ExactAction{}
		history := map[string]map[uint64]oracle.ExactAction{}
		if r := state.Snapshot.Lifecycle.Recovery; r != nil {
			for _, x := range r.Actions {
				if history[x.Action.Key] == nil {
					history[x.Action.Key] = map[uint64]oracle.ExactAction{}
				}
				f := ""
				if x.ExpectedFence != nil {
					f = fmt.Sprint(*x.ExpectedFence)
				}
				history[x.Action.Key][x.Action.Generation] = oracle.ExactAction{Key: x.Action.Key, Owner: string(x.Action.Owner), Request: string(x.Action.Request.RequestID), Method: x.Method, Body: string(x.ExactCommand), ExpectedFence: f, Generation: x.Action.Generation, Authority: oracle.ActionAuthority{Holder: x.Authority.Holder, Epoch: x.Authority.OwnerEpoch, Incarnation: x.Authority.Incarnation, Active: true, Evidence: map[uint64]string{1: string(digest("a")), 2: string(digest("c"))}[x.Authority.Incarnation]}, Status: string(x.Action.Status)}
			}
		}

		for _, x := range state.Snapshot.Actions {
			value, ok := history[x.Key][x.Generation]
			if !ok {
				t.Fatalf("%s current action has no immutable generation", label)
			}
			value.Key, value.Owner, value.Request, value.Generation, value.Status = x.Key, string(x.Owner), string(x.Request.RequestID), x.Generation, string(x.Status)
			current[x.Key] = value
		}
		if r := state.Snapshot.Lifecycle.Recovery; r != nil {
			for _, actual := range r.Authorities {
				if actual.Execution != "root" {
					continue
				}
				got := oracle.ActionAuthority{Holder: actual.Binding.Holder, Epoch: actual.Binding.OwnerEpoch, Incarnation: actual.Binding.Incarnation, Active: actual.Status == "active", Evidence: string(actual.Evidence)}
				if got != want.Authority {
					t.Fatalf("%s current authority differs: %+v expected=%+v", label, got, want.Authority)
				}
			}
		}

		actualPosition := oracle.ResumePosition{}
		var actualIncarnation uint64
		for _, c := range state.Snapshot.Lifecycle.Continuations {
			if c.Execution != "root" {
				continue
			}
			nextActions := map[string]uint64{}
			for _, ref := range c.Position.NextActions {
				nextActions[ref.Key] = ref.Generation
			}
			actualPosition = oracle.ResumePosition{Execution: string(c.Execution), Space: string(c.Position.Space), Principal: string(c.Position.Principal), Adapter: c.Position.AdapterVersion, Step: c.Position.Step, Version: c.Version, Next: nextActions}
			actualIncarnation = c.Incarnation
		}
		if !reflect.DeepEqual(actualPosition, want.Position) || actualIncarnation != want.PositionIncarnation {
			t.Fatalf("%s continuation changed implicitly or lost exact generations: %+v/%d expected=%+v/%d", label, actualPosition, actualIncarnation, want.Position, want.PositionIncarnation)
		}
		if !reflect.DeepEqual(current, want.Current) || !reflect.DeepEqual(history, want.History) {
			t.Fatalf("%s action generations differ\n current=%+v expected=%+v\n history=%+v expected=%+v", label, current, want.Current, history, want.History)
		}
	}
	sequence := 0
	mutate := func(label string, expected func(*oracle.ActionRecovery) error, c agentprofile.ControlCommand) {
		t.Helper()
		sequence++
		werr := want.Batch(expected)
		before, _ := store.Snapshot(ctx)
		r, e := store.ApplyAgent(ctx, api.ApplyAgentRequest{Key: key(fmt.Sprintf("action-transition-%d", sequence)), Command: agentprofile.Command{Control: &c}})
		after, _ := store.Snapshot(ctx)
		if (werr == nil) != (e == nil && r.Code == api.CodeDurableAck) {
			t.Fatalf("%s acceptance oracle=%v actual=%+v/%v", label, werr, r, e)
		}
		if werr != nil && before != after {
			t.Fatal("rejected recovery changed roots")
		}
		compare(label)
	}
	for _, x := range []oracle.ExactAction{published, abstract("pending", makeRequest("original-unsubmitted"), want.Authority)} {
		x := x
		mutate("reserve "+x.Key, func(a *oracle.ActionRecovery) error { return a.Reserve(x) }, agentprofile.ControlCommand{PutAction: &agentprofile.PutActionCommand{Intent: intent(x), Authority: authority}})
	}

	position := agentprofile.Continuation{Execution: "root", Version: 1, Incarnation: 1, PendingKey: "pending", PendingGeneration: 1, InputFence: source.Fence, Phase: "wait", Position: &agentprofile.ContinuationPosition{Space: "space", Principal: "owner", AdapterVersion: "fixture/v1", NextActions: []agentprofile.PendingActionRef{{Key: "pending", Generation: 1}}}}
	abstractPosition := oracle.ResumePosition{Execution: "root", Space: "space", Principal: "owner", Adapter: "fixture/v1", Version: 1, Next: map[string]uint64{"pending": 1}}
	mutate("checkpoint original action generation", func(a *oracle.ActionRecovery) error { return a.SavePosition(abstractPosition, 1, 0) }, agentprofile.ControlCommand{SaveContinuation: &agentprofile.SaveContinuationCommand{Value: position, ExpectedVersion: 0, Authority: authority}})
	// A matching historical receipt is resolved at its original fence, regardless
	// of the later authority. A false absence must never permit replacement.
	applied := oracle.ActionResolution{Key: "published", Generation: 1, Outcome: "applied", Observed: publishedAck.Fence.LSN, Receipt: string(publishedAck.ResultDigest)}
	mutate("resolve original receipt", func(a *oracle.ActionRecovery) error { return a.Resolve(applied) }, agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: applied.Key, Generation: 1, Outcome: applied.Outcome, ObservedFence: publishedAck.Fence, Receipt: publishedAck.ResultDigest}}})
	observed, _ := store.Snapshot(ctx)
	falseAbsent := oracle.ActionResolution{Key: "published", Generation: 1, Outcome: "absent", Observed: observed.Fence.LSN}
	mutate("published cannot become absent", func(a *oracle.ActionRecovery) error { return a.Resolve(falseAbsent) }, agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: "published", Generation: 1, Outcome: "absent", ObservedFence: observed.Fence}}})
	control("fence-authority", agentprofile.ControlCommand{FenceAuthority: &agentprofile.FenceAuthorityCommand{Execution: "root", ExpectedVersion: 1, Evidence: digest("b")}})
	want.Fence()
	next := authority
	next.Holder = "replacement"
	next.OwnerEpoch = 2
	next.Incarnation = 2
	resumeProof := digest("c")
	control("resume-authority", agentprofile.ControlCommand{Resume: &agentprofile.ResumePlan{Execution: "root", ExpectedIncarnation: 1, ExpectedAuthorityVersion: 2, Next: next, AdapterVersion: "fixture/v1", Checkpoint: checkpointID, CompatibilityEvidence: digest("d"), ResourceFenceEvidence: resumeProof}})
	if e := want.Resume(oracle.ActionAuthority{Holder: next.Holder, Epoch: 2, Incarnation: 2, Active: true, Evidence: string(resumeProof)}); e != nil {
		t.Fatal(e)
	}
	compare("takeover")
	lookup, e := store.LookupRequest(ctx, api.LookupRequestRequest{Space: "space", Principal: "owner", RequestID: publishedRequest.RequestID})
	if e != nil || lookup.Result != publishedAck {
		t.Fatal("takeover changed original receipt", e)
	}
	if retry := good(store.Commit(ctx, publishedRequest)); retry != publishedAck {
		t.Fatal("old authority blocked a published retry")
	}
	observed, _ = store.Snapshot(ctx)
	unknown := oracle.ActionResolution{Key: "pending", Generation: 1, Outcome: "unknown", Observed: observed.Fence.LSN}
	mutate("unknown is not absence", func(a *oracle.ActionRecovery) error { return a.Resolve(unknown) }, agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: "pending", Generation: 1, Outcome: "unknown", ObservedFence: observed.Fence}}})
	rebound := abstract("pending", makeRequest("replacement-request"), want.Authority)
	rebind := func(x oracle.ExactAction, expected uint64, evidence model.ObjectID) agentprofile.ControlCommand {
		return agentprofile.ControlCommand{RebindAction: &agentprofile.RebindActionCommand{Key: x.Key, ExpectedGeneration: expected, NewRequest: key(x.Request), Method: x.Method, ExactCommand: []byte(x.Body), CommandDigest: independentBytesDigest([]byte(x.Body)), Authority: next, FenceEvidence: evidence}}
	}
	mutate("unknown cannot rebind", func(a *oracle.ActionRecovery) error { return a.Rebind(rebound, 1, string(resumeProof)) }, rebind(rebound, 1, resumeProof))
	observed, _ = store.Snapshot(ctx)
	absence := oracle.ActionResolution{Key: "pending", Generation: 1, Outcome: "absent", Observed: observed.Fence.LSN}
	mutate("proved original absence", func(a *oracle.ActionRecovery) error { return a.Resolve(absence) }, agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: "pending", Generation: 1, Outcome: "absent", ObservedFence: observed.Fence}}})
	mutate("wrong resource fence", func(a *oracle.ActionRecovery) error { return a.Rebind(rebound, 1, string(digest("e"))) }, rebind(rebound, 1, digest("e")))
	mutate("wrong generation CAS", func(a *oracle.ActionRecovery) error { return a.Rebind(rebound, 2, string(resumeProof)) }, rebind(rebound, 2, resumeProof))
	reuse := rebound
	reuse.Request = published.Request
	mutate("historical namespace cannot be reused", func(a *oracle.ActionRecovery) error { return a.Rebind(reuse, 1, string(resumeProof)) }, rebind(reuse, 1, resumeProof))
	mutate("new generation", func(a *oracle.ActionRecovery) error { return a.Rebind(rebound, 1, string(resumeProof)) }, rebind(rebound, 1, resumeProof))
	submit := func(label string, r api.CommitRequest) api.CommitResult {
		t.Helper()
		x := abstract("", r, want.Authority)
		old, werr := want.MaySubmit(x)
		before, _ := store.Snapshot(ctx)
		got, e := store.Commit(ctx, r)
		after, _ := store.Snapshot(ctx)
		if (werr == nil) != (e == nil && got.Code == api.CodeDurableAck) {
			t.Fatalf("%s oracle=%v actual=%+v/%v", label, werr, got, e)
		}
		if werr != nil && before != after {
			t.Fatal("rejected dispatch changed publication")
		}
		if werr == nil {
			if old != nil {
				if string(got.ResultDigest) != old.Token || got.Fence.LSN != old.Publication {
					t.Fatal("retry substituted current receipt")
				}
			} else {
				want.Published(x, string(got.ResultDigest), got.Fence.LSN)
			}
		}
		return got
	}

	// Rebind changes the current action, never the already accepted checkpoint.
	compare("rebind retained historical checkpoint generation")
	position.Version, position.Incarnation, position.PendingGeneration = 2, 2, 2
	position.Position.NextActions[0].Generation = 2
	abstractPosition.Version = 2
	abstractPosition.Next = map[string]uint64{"pending": 2}
	mutate("explicit resumed position", func(a *oracle.ActionRecovery) error { return a.SavePosition(abstractPosition, 2, 1) }, agentprofile.ControlCommand{SaveContinuation: &agentprofile.SaveContinuationCommand{Value: position, ExpectedVersion: 1, Authority: next}})
	submit("superseded original key", makeRequest("original-unsubmitted"))
	bad := makeRequest("replacement-request")
	bad.Body = append(append([]byte(nil), body...), 0)
	submit("replacement bytes remain exact", bad)
	newAck := submit("current generation", makeRequest("replacement-request"))
	staleAbsence := oracle.ActionResolution{Key: "pending", Generation: 2, Outcome: "absent", Observed: observed.Fence.LSN}
	mutate("old absence cannot erase new receipt", func(a *oracle.ActionRecovery) error { return a.Resolve(staleAbsence) }, agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: "pending", Generation: 2, Outcome: "absent", ObservedFence: observed.Fence}}})
	wrongReceipt := oracle.ActionResolution{Key: "pending", Generation: 2, Outcome: "applied", Observed: newAck.Fence.LSN, Receipt: string(publishedAck.ResultDigest)}
	mutate("another action receipt cannot settle this generation", func(a *oracle.ActionRecovery) error { return a.Resolve(wrongReceipt) }, agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: "pending", Generation: 2, Outcome: "applied", ObservedFence: newAck.Fence, Receipt: publishedAck.ResultDigest}}})
	resolved := oracle.ActionResolution{Key: "pending", Generation: 2, Outcome: "applied", Observed: newAck.Fence.LSN, Receipt: string(newAck.ResultDigest)}
	mutate("resolve current original receipt", func(a *oracle.ActionRecovery) error { return a.Resolve(resolved) }, agentprofile.ControlCommand{ResolveAction: &agentprofile.ResolveActionCommand{Resolution: agentprofile.ActionResolution{Key: "pending", Generation: 2, Outcome: "applied", ObservedFence: newAck.Fence, Receipt: newAck.ResultDigest}}})
	if _, e := store.CompactAuditFull(ctx); e != nil {
		t.Fatal(e)
	}
	if e := store.Close(); e != nil {
		t.Fatal(e)
	}
	store, _, err = ate.OpenExisting(path, options)
	if err != nil {
		t.Fatal(err)
	}
	compare("reopen retains all generations and evidence")
	submit("applied action retry", makeRequest("replacement-request"))
	droppedFence := publishedRequest
	droppedFence.ExpectedFence = nil
	submit("published outer fence cannot disappear", droppedFence)
	if retry := good(store.Commit(ctx, publishedRequest)); retry != publishedAck {
		t.Fatal("historical ack changed after compaction")
	}
}
