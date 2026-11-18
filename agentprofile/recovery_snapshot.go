package agentprofile

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func sortRecovery(s *RecoverySnapshot) {
	v := reflect.ValueOf(s).Elem()
	for field := 0; field < v.NumField(); field++ {
		xs := v.Field(field)
		type pair struct {
			key   []byte
			value reflect.Value
		}
		pairs := make([]pair, xs.Len())
		for i := 0; i < xs.Len(); i++ {
			b, _ := appendControlValue(nil, xs.Index(i), 0)
			pairs[i] = pair{b, cloneControlValue(xs.Index(i))}
		}
		sort.Slice(pairs, func(i, j int) bool { return bytes.Compare(pairs[i].key, pairs[j].key) < 0 })
		for i, p := range pairs {
			xs.Index(i).Set(p.value)
		}
	}
}
func validateRecoveryShape(s *RecoverySnapshot) error {
	return validateRecoveryShapeMode(s, false)
}

func validateRecoveryShapeMode(s *RecoverySnapshot, records bool) error {
	if s == nil {
		return nil
	}
	v := reflect.ValueOf(*s)
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).Len() > MaxLifecycleRecords {
			return ErrInvalid
		}
		if records {
			for j := 0; j < v.Field(i).Len(); j++ {
				if _, err := appendControlValue(nil, v.Field(i).Index(j), 0); err != nil {
					return ErrInvalid
				}
			}
		}
	}
	if records {
		return nil
	}
	if _, err := appendControlValue(nil, v, 0); err != nil {
		return ErrInvalid
	}
	return nil
}
func uniqueRecord(seen map[string]bool, key string) bool {
	if seen[key] {
		return false
	}
	seen[key] = true
	return true
}
func (s Snapshot) recoveryState() *RecoverySnapshot { return s.Lifecycle.Recovery }

// validateRecoveryState checks authoritative cross-record relations after a
// pure transition and again when an immutable snapshot is restored.
func validateRecoveryState(snapshot Snapshot) error {
	return validateRecoveryStateMode(snapshot, false)
}

func validateRecoveryStateMode(snapshot Snapshot, records bool) error {
	if err := validateOutputState(snapshot); err != nil {
		return err
	}
	if err := validateEffectRecovery(snapshot); err != nil {
		return err
	}
	if err := validateValidationRequirements(snapshot); err != nil {
		return err
	}
	tasks := map[model.EntityID]Task{}
	executions := map[model.ExecutionID]Execution{}
	for _, t := range snapshot.Tasks {
		tasks[t.ID] = t
	}
	for _, e := range snapshot.Executions {
		executions[e.ID] = e
	}
	for _, e := range snapshot.Executions {
		seen := map[model.ExecutionID]bool{}
		for p := e.ID; p != ""; {
			if seen[p] {
				return ErrConflict
			}
			seen[p] = true
			x, ok := executions[p]
			if !ok {
				return ErrUnknown
			}
			p = x.Parent
		}
	}
	for _, c := range snapshot.Lifecycle.Continuations {
		if err := validateContinuationRelations(c, snapshot); err != nil {
			return err
		}
	}
	s := snapshot.recoveryState()
	if s == nil {
		return nil
	}
	if err := validateRecoveryShapeMode(s, records); err != nil {
		return err
	}
	queries := map[string]Query{}
	activities := map[string]Activity{}
	attempts := map[string]Attempt{}
	results := map[string]ActivityResult{}
	actions := map[string]PendingAction{}
	for _, v := range snapshot.Lifecycle.Queries {
		queries[v.ID] = v
	}
	for _, v := range snapshot.Lifecycle.Activities {
		activities[v.ID] = v
	}
	for _, v := range snapshot.Lifecycle.Attempts {
		attempts[v.ID] = v
	}
	for _, v := range snapshot.Lifecycle.Results {
		results[v.ID] = v
	}
	for _, v := range snapshot.Actions {
		actions[v.Key] = v
	}
	invocations := make(map[string]Invocation, len(snapshot.Invocations))
	for _, inv := range snapshot.Invocations {
		invocations[inv.ID] = inv
	}
	seen := map[string]bool{}
	for _, a := range s.Authorities {
		e, ok := executions[a.Execution]
		if !ok || a.Version == 0 || !validBinding(a.Binding) || a.Binding.Incarnation != e.Incarnation || !validDigest(a.Evidence) || (a.Status != "active" && a.Status != "fenced" && a.Status != "canceled") || !uniqueRecord(seen, "authority/"+string(a.Execution)) {
			return ErrInvalid
		}
	}
	specs := map[string]ActivitySpec{}
	for _, a := range s.ActivitySpecs {
		legacy, ok := activities[a.Activity]
		if !ok || a.Activity == "" || legacy.Execution != a.Execution || legacy.Kind != a.Role || legacy.Step != a.Step || legacy.Invocation != a.Invocation || legacy.InputFence != a.InputFence || !validRole(a.Role) || !validEffect(a.Effect) || !validDigest(a.Request) || !validDigest(a.RequestDigest) || a.InputFence.Validate() != nil || !commandText(a.Recipient) || !commandText(a.ResourceScope) || !uniqueRecord(seen, "spec/"+a.Activity) {
			return ErrInvalid
		}
		if !validEffectContract(a) || !validActivityToolLink(a) {
			return ErrInvalid
		}
		if a.Invocation != "" {
			if inv, found := invocations[a.Invocation]; !found || !activityMatchesInvocation(a, inv) {
				return ErrInvalid
			}
		}
		if modelRole(a.Role) && !validDigest(a.ContextPacket) {
			return ErrInvalid
		}
		if a.FinalAnswerInput != "" && (!validDigest(a.FinalAnswerInput) || a.Query == "" || a.Role != "query_decision") {
			return ErrInvalid
		}
		if a.Execution != "" {
			e, ok := executions[a.Execution]
			if !ok || a.Query != "" || a.GoalEpoch == 0 || a.GoalEpoch > tasks[e.Task].GoalEpoch {
				return ErrInvalid
			}
		} else {
			q, ok := queries[a.Query]
			if !ok || a.GoalEpoch == 0 || a.GoalEpoch > q.Version {
				return ErrInvalid
			}
		}
		specs[a.Activity] = a
	}
	controls := map[string]ActivityControl{}
	for _, a := range s.ActivityControls {
		spec, ok := specs[a.Activity]
		if !ok || a.Version == 0 || a.Incarnation == 0 || !uniqueRecord(seen, "activity/"+a.Activity) {
			return ErrInvalid
		}
		if spec.Execution != "" && a.Incarnation > executions[spec.Execution].Incarnation {
			return ErrInvalid
		}
		if a.AcceptedResult != "" {
			r, ok := results[a.AcceptedResult]
			if !ok || r.Activity != a.Activity {
				return ErrInvalid
			}
		}
		controls[a.Activity] = a
	}
	if len(controls) != len(specs) {
		return ErrInvalid
	}
	attemptControls := map[string]AttemptControl{}
	seqs := map[string]map[uint64]bool{}
	for _, a := range s.AttemptControls {
		spec, ok := specs[a.Activity]
		legacy, lok := attempts[a.Attempt]
		if !ok || !lok || legacy.Activity != a.Activity || legacy.Sequence != a.Sequence || legacy.Status != a.Status || a.Sequence == 0 || a.RequestDigest != spec.RequestDigest || !uniqueRecord(seen, "attempt/"+a.Attempt) {
			return ErrInvalid
		}
		if spec.Execution != "" {
			if !validBinding(a.Authority) || a.Authority.Incarnation > executions[spec.Execution].Incarnation || legacy.Incarnation != a.Authority.Incarnation {
				return ErrInvalid
			}
		} else if !zeroBinding(a.Authority) || legacy.Incarnation != 1 {
			return ErrInvalid
		}
		switch a.Status {
		case "created", "fenced_before_dispatch", "dispatch_recorded", "result_recorded", "effect_unknown", "no_effect", "occurred", "compensated":
		default:
			return ErrInvalid
		}
		if a.Status != "created" && a.Status != "fenced_before_dispatch" && !validDigest(a.DispatchEvidence) {
			return ErrInvalid
		}
		if seqs[a.Activity] == nil {
			seqs[a.Activity] = map[uint64]bool{}
		}
		if seqs[a.Activity][a.Sequence] {
			return ErrUniqueSlot
		}
		seqs[a.Activity][a.Sequence] = true
		attemptControls[a.Attempt] = a
	}
	for _, set := range seqs {
		for i := 1; i <= len(set); i++ {
			if !set[uint64(i)] {
				return ErrConflict
			}
		}
	}
	for _, r := range s.Captures {
		v, ok := results[r.Result]
		a, aok := attemptControls[v.Attempt]
		if !ok || !aok || v.Activity != a.Activity || v.RequestDigest != a.RequestDigest || v.Incarnation != attempts[v.Attempt].Incarnation || !uniqueRecord(seen, "capture/"+r.Result) || !uniqueRecord(seen, "attempt-result/"+v.Attempt) {
			return ErrInvalid
		}
	}
	for _, a := range s.Acceptances {
		ctl, ok := controls[a.Activity]
		r, rok := results[a.Result]
		if !ok || !rok || r.Activity != a.Activity || r.Outcome == "unknown" || !acceptableResultCapture(specs[a.Activity], r) || ctl.AcceptedResult != a.Result || a.Version > ctl.Version || !validDigest(a.Evidence) || !uniqueRecord(seen, "acceptance/"+a.Activity) {
			return ErrInvalid
		}
		attempt, ok := attemptControls[r.Attempt]
		imported := false
		for _, v := range s.Imports {
			imported = imported || v == a
		}
		if !ok || (attempt.Authority != a.Authority && !imported) {
			return ErrInvalid
		}
	}
	for id, c := range controls {
		if c.AcceptedResult != "" && !seen["acceptance/"+id] {
			return ErrInvalid
		}
	}
	versions := map[string]map[uint64]bool{}
	for _, a := range s.Settlements {
		if _, ok := specs[a.Activity]; !ok || a.Version == 0 || !validSettlement(a.Status) || !validDigest(a.Evidence) {
			return ErrInvalid
		}
		key := "settle/" + a.Activity
		if versions[key] == nil {
			versions[key] = map[uint64]bool{}
		}
		if versions[key][a.Version] {
			return ErrConflict
		}
		versions[key][a.Version] = true
	}
	for _, a := range s.Reconciliations {
		if _, ok := attemptControls[a.Attempt]; !ok || a.Version == 0 || !validReconciliation(a.Outcome) || !validDigest(a.Evidence) || (a.Outcome == "no_effect" && !a.FutureEffectFenced) {
			return ErrInvalid
		}
		key := "reconcile/" + a.Attempt
		if versions[key] == nil {
			versions[key] = map[uint64]bool{}
		}
		if versions[key][a.Version] {
			return ErrConflict
		}
		versions[key][a.Version] = true
	}
	for _, set := range versions {
		for i := 1; i <= len(set); i++ {
			if !set[uint64(i)] {
				return ErrConflict
			}
		}
	}
	for _, a := range s.Answers {
		q, ok := queries[a.Query]
		if !ok || a.Version == 0 || a.Version > q.Version || !commandText(a.Slot) || len(a.Body) > MaxCommandBytes || !uniqueRecord(seen, "answer/"+a.Query+"/"+a.Slot) {
			return ErrInvalid
		}
		if a.Final && (q.Status != "answered" || !bytes.Equal(a.Body, q.Answer)) {
			return ErrConflict
		}
		if a.Final {
			if err := validateFinalAnswer(snapshot, a.Query, a.GeneratedFrom, a.Body, a.Outcomes, a.InputEvidence); err != nil {
				return err
			}
		}
	}
	for _, a := range s.Starts {
		q, ok := queries[a.Query]
		t, tok := tasks[a.Task]
		e, eok := executions[a.Execution]
		if !ok || !tok || !eok || t.Space != q.Space || t.Owner != q.Owner || e.Task != t.ID || e.Parent != "" || !commandText(a.Slot) || !uniqueRecord(seen, "start/"+a.Query+"/"+a.Slot) {
			return ErrInvalid
		}
		if a.IntentDigest != "" && !validDigest(a.IntentDigest) {
			return ErrInvalid
		}
	}
	for _, a := range s.TaskLinks {
		task, ok := tasks[a.Task]
		parent, pok := executions[a.ParentExecution]
		source, sok := tasks[a.InheritedTask]
		if !ok || !pok || !sok || parent.Task != source.ID || task.Space != source.Space || task.Owner != source.Owner || a.InheritedEpoch == 0 || a.InheritedEpoch > source.GoalEpoch || !validDigest(a.Compatibility) || !commandText(a.Invocation) || !uniqueRecord(seen, "tasklink/"+string(a.Task)) {
			return ErrInvalid
		}
		if (a.IntentDigest != "" && !validDigest(a.IntentDigest)) || (a.GoalObservation != "" && !validDigest(a.GoalObservation)) {
			return ErrInvalid
		}
		inv, ok := findInvocation(snapshot.Invocations, a.Invocation)
		if !ok || inv.Execution != parent.ID {
			return ErrInvalid
		}
	}
	for _, a := range s.Goals {
		e, ok := executions[a.Execution]
		if !ok || a.Epoch == 0 || a.Epoch > tasks[e.Task].GoalEpoch || !validDigest(a.Evidence) || !uniqueRecord(seen, fmt.Sprintf("goal/%s/%d", a.Execution, a.Epoch)) {
			return ErrInvalid
		}
		if a.Observation != "" && !validDigest(a.Observation) {
			return ErrInvalid
		}
	}
	for _, a := range s.Outcomes {
		t, ok := tasks[a.Task]
		e, eok := executions[a.Execution]
		if !ok || !eok || e.Task != a.Task || t.Status != TaskCompleted || e.Status != ExecutionCompleted || a.GoalEpoch != t.GoalEpoch || !validDigest(a.Result) || !validDigest(a.Validation) || a.Sequence == 0 || !uniqueRecord(seen, "outcome/"+string(a.Task)) || !uniqueRecord(seen, fmt.Sprintf("outcome-sequence/%d", a.Sequence)) {
			return ErrInvalid
		}
	}
	for n := 1; n <= len(s.Outcomes); n++ {
		if !seen[fmt.Sprintf("outcome-sequence/%d", n)] {
			return ErrInvalid
		}
	}
	for _, a := range s.Actions {
		if a.ExpectedFence != nil && a.ExpectedFence.Validate() != nil {
			return ErrInvalid
		}
		current, ok := actions[a.Action.Key]
		e, eok := executions[a.Action.Owner]
		if !ok || !eok || a.Action.Generation == 0 || a.Action.Generation > current.Generation || a.Action.Owner != current.Owner || a.Action.Request.Space != tasks[e.Task].Space || a.Action.Request.Principal != tasks[e.Task].Owner || !validBinding(a.Authority) || a.Authority.Incarnation > e.Incarnation || a.Action.InputFence.Validate() != nil || a.Action.CommandDigest != commandHash(a.ExactCommand) || !commandText(a.Method) || len(a.ExactCommand) == 0 || len(a.ExactCommand) > MaxCommandBytes || !uniqueRecord(seen, fmt.Sprintf("action/%s/%d", a.Action.Key, a.Action.Generation)) {
			return ErrInvalid
		}
		for _, id := range a.InputObjects {
			if !validDigest(id) {
				return ErrInvalid
			}
		}
	}
	for _, a := range s.Resolutions {
		current, ok := actions[a.Key]
		if !ok || a.Generation == 0 || a.Generation > current.Generation || a.ObservedFence.Validate() != nil || (a.Outcome != "applied" && a.Outcome != "absent" && a.Outcome != "unknown") || (a.Outcome == "applied" && !validDigest(a.Receipt)) {
			return ErrInvalid
		}
	}
	for _, a := range s.Resumes {
		e, ok := executions[a.Execution]
		if !ok || a.ExpectedIncarnation == 0 || a.Next.Incarnation != a.ExpectedIncarnation+1 || a.Next.Incarnation > e.Incarnation || !validBinding(a.Next) || !validDigest(a.Checkpoint) || !validDigest(a.CompatibilityEvidence) || !validDigest(a.ResourceFenceEvidence) || !uniqueRecord(seen, fmt.Sprintf("resume/%s/%d", a.Execution, a.Next.Incarnation)) {
			return ErrInvalid
		}
	}
	for _, c := range snapshot.Lifecycle.Continuations {
		if !seen["authority/"+string(c.Execution)] {
			return ErrInvalid
		}
		found := false
		for _, a := range s.Actions {
			found = found || (a.Action.Key == c.PendingKey && a.Action.Generation == c.PendingGeneration && a.Action.Owner == c.Execution && a.Action.InputFence == c.InputFence)
		}
		if !found || c.Incarnation == 0 || c.Incarnation > executions[c.Execution].Incarnation || c.InputFence.Validate() != nil || !commandText(c.Phase) {
			return ErrInvalid
		}
	}
	for _, message := range snapshot.Lifecycle.Messages {
		sender, sok := executions[message.Sender]
		recipient, rok := executions[message.Recipient]
		st, rt := tasks[sender.Task], tasks[recipient.Task]
		if !sok || !rok || st.Space != rt.Space || st.Owner != rt.Owner || message.GoalEpoch == 0 || message.GoalEpoch > rt.GoalEpoch {
			return ErrInvalid
		}
	}
	for _, consumption := range snapshot.Lifecycle.Consumptions {
		_, found := findLifecycle(snapshot.Lifecycle.Messages, func(m Message) bool {
			return m.ID == consumption.Message && m.Recipient == consumption.Recipient && m.GoalEpoch == consumption.GoalEpoch
		})
		if !found || consumption.Incarnation == 0 || consumption.Incarnation > executions[consumption.Recipient].Incarnation {
			return ErrInvalid
		}
	}
	for _, a := range s.Joins {
		if _, ok := executions[a.Parent]; !ok || a.ID == "" || a.Version == 0 || !uniqueRecord(seen, "join/"+a.ID) {
			return ErrInvalid
		}
		members := map[model.ExecutionID]bool{}
		for _, id := range a.Members {
			e, ok := executions[id]
			if !ok || e.Parent != a.Parent || members[id] {
				return ErrInvalid
			}
			members[id] = true
		}
		for _, id := range a.Winners {
			if !members[id] {
				return ErrInvalid
			}
		}
		if a.Status != "pending" && a.Status != "completed" && a.Status != "timed_out" {
			return ErrInvalid
		}
	}
	for _, imported := range s.Imports {
		accepted := false
		for _, a := range s.Acceptances {
			accepted = accepted || a == imported
		}
		result, ok := results[imported.Result]
		attempt, aok := attemptControls[result.Attempt]
		var reconciliation AttemptReconciliation
		for _, r := range s.Reconciliations {
			if r.Attempt == result.Attempt && r.Version > reconciliation.Version {
				reconciliation = r
			}
		}
		if !accepted || !ok || !aok || imported.Authority.Incarnation <= attempt.Authority.Incarnation || reconciliation.Outcome != "occurred" || !uniqueRecord(seen, "import/"+imported.Activity) {
			return ErrInvalid
		}
	}
	return nil
}
func findInvocation(xs []Invocation, id string) (Invocation, bool) {
	for _, v := range xs {
		if v.ID == id {
			return v, true
		}
	}
	return Invocation{}, false
}
