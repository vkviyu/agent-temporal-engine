package agentprofile

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const (
	SnapshotVersion    uint64 = 1
	MaxSnapshotRecords        = 65536
	MaxSnapshotBytes          = 32 << 20
)

// Snapshot is an owned, deterministic view of all profile state. It contains
// no mutexes, indexes, or request receipts; those are reconstructed on restore.
type Snapshot struct {
	Tasks       []Task
	Executions  []Execution
	Steps       []Step
	Invocations []Invocation
	Actions     []PendingAction
	Lifecycle   LifecycleSnapshot
}

func cloneSnapshot(s Snapshot) Snapshot {
	out := Snapshot{
		Tasks:       append([]Task(nil), s.Tasks...),
		Executions:  append([]Execution(nil), s.Executions...),
		Steps:       append([]Step(nil), s.Steps...),
		Invocations: make([]Invocation, len(s.Invocations)),
		Actions:     append([]PendingAction(nil), s.Actions...),
		Lifecycle:   cloneLifecycle(s.Lifecycle),
	}
	for i, v := range s.Invocations {
		out.Invocations[i] = v
		out.Invocations[i].Args = bytes.Clone(v.Args)
		out.Invocations[i].Result = bytes.Clone(v.Result)
	}
	return out
}

// Snapshot returns an owned copy. Sorting is performed by EncodeSnapshot, so
// callers can use this method for diagnostics without depending on map order.
func (l *Ledger) Snapshot() Snapshot {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := Snapshot{
		Tasks: make([]Task, 0, len(l.tasks)), Executions: make([]Execution, 0, len(l.executions)),
		Steps: make([]Step, 0, len(l.steps)), Invocations: make([]Invocation, 0, len(l.invocations)),
		Actions:   make([]PendingAction, 0, len(l.actions)),
		Lifecycle: l.lifecycleSnapshot(),
	}
	for _, v := range l.tasks {
		out.Tasks = append(out.Tasks, v)
	}
	for _, v := range l.executions {
		out.Executions = append(out.Executions, v)
	}
	for _, v := range l.steps {
		out.Steps = append(out.Steps, v)
	}
	for _, v := range l.invocations {
		v.Args = bytes.Clone(v.Args)
		v.Result = bytes.Clone(v.Result)
		out.Invocations = append(out.Invocations, v)
	}
	for _, v := range l.actions {
		out.Actions = append(out.Actions, v)
	}
	return out
}

func validTaskStatus(s TaskStatus) bool {
	switch s {
	case TaskOpen, TaskCompleted, TaskCanceled, TaskBlocked:
		return true
	}
	return false
}
func validExecutionStatus(s ExecutionStatus) bool {
	switch s {
	case ExecutionReady, ExecutionRunning, ExecutionCompleted, ExecutionCanceled, ExecutionBlocked:
		return true
	}
	return false
}
func validStepStatus(s StepStatus) bool {
	switch s {
	case StepOpen, StepAccepted, StepCompleted, StepBlocked, StepCanceled:
		return true
	}
	return false
}
func validInvocationStatus(s InvocationStatus) bool {
	switch s {
	case InvocationPending, InvocationAccepted, InvocationRejected, InvocationUnknown, InvocationSettled:
		return true
	}
	return false
}
func validActionStatus(s ActionStatus) bool {
	switch s {
	case ActionPending, ActionApplied, ActionUnknown, ActionFenced, ActionRebound:
		return true
	}
	return false
}

// Validate checks record identity, ownership and all cross-record references.
// It intentionally does not infer a transition history: a recovered snapshot
// is a state image, while the durable command log remains the audit history.
func (s Snapshot) Validate() error {
	if len(s.Tasks) > MaxSnapshotRecords || len(s.Executions) > MaxSnapshotRecords || len(s.Steps) > MaxSnapshotRecords || len(s.Invocations) > MaxSnapshotRecords || len(s.Actions) > MaxSnapshotRecords {
		return ErrInvalid
	}
	if err := validateLifecycleSnapshot(s.Lifecycle); err != nil {
		return err
	}
	tasks := make(map[model.EntityID]Task, len(s.Tasks))
	for _, t := range s.Tasks {
		if !validTaskStatus(t.Status) || t.ID == "" || !validText(string(t.ID)) || t.Goal == "" || t.GoalEpoch == 0 || (t.Status == TaskCanceled && t.CancelEpoch == 0) {
			return ErrInvalid
		}
		if _, ok := tasks[t.ID]; ok {
			return ErrConflict
		}
		if _, err := EncodeTask(t); err != nil {
			return err
		}
		tasks[t.ID] = t
	}
	executions := make(map[model.ExecutionID]Execution, len(s.Executions))
	for _, x := range s.Executions {
		if !validExecutionStatus(x.Status) || x.ID == "" || x.Task == "" || x.Actor == "" || x.Incarnation == 0 || x.AcceptedGoalEpoch == 0 {
			return ErrInvalid
		}
		t, ok := tasks[x.Task]
		if !ok {
			return ErrUnknown
		}
		if x.AcceptedGoalEpoch > t.GoalEpoch || x.CancelEpoch > t.CancelEpoch {
			return ErrGoalEpoch
		}
		if _, ok := executions[x.ID]; ok {
			return ErrConflict
		}
		if _, err := EncodeExecution(x); err != nil {
			return err
		}
		executions[x.ID] = x
	}
	for _, x := range s.Executions {
		if x.Parent != "" {
			p, ok := executions[x.Parent]
			crossTask := false
			if s.Lifecycle.Recovery != nil {
				for _, link := range s.Lifecycle.Recovery.TaskLinks {
					crossTask = crossTask || (link.Task == x.Task && link.ParentExecution == x.Parent && link.InheritedTask == p.Task)
				}
			}
			if !ok || (p.Task != x.Task && !crossTask) {
				return ErrConflict
			}
		}
	}
	steps := make(map[string]Step, len(s.Steps))
	for _, v := range s.Steps {
		if !validStepStatus(v.Status) || v.Execution == "" || v.Sequence == 0 || v.GoalEpoch == 0 {
			return ErrInvalid
		}
		x, ok := executions[v.Execution]
		if !ok {
			return ErrUnknown
		}
		if v.GoalEpoch > x.AcceptedGoalEpoch {
			return ErrGoalEpoch
		}
		key := stepKey(v.Execution, v.Sequence)
		if _, ok := steps[key]; ok {
			return ErrConflict
		}
		if _, err := EncodeStep(v); err != nil {
			return err
		}
		steps[key] = v
	}
	invocations := make(map[string]Invocation, len(s.Invocations))
	slots := make(map[string]string, len(s.Invocations)+len(s.Executions))
	for _, v := range s.Invocations {
		if !validInvocationStatus(v.Status) || v.ID == "" || v.Execution == "" || v.Step == 0 || v.ProviderCallKey == "" || v.Tool == "" || v.Attempt == 0 {
			return ErrInvalid
		}
		x, ok := executions[v.Execution]
		if !ok {
			return ErrUnknown
		}
		if _, ok := steps[stepKey(v.Execution, v.Step)]; !ok {
			return ErrUnknown
		}
		if v.ID == "" || x.Task == "" {
			return ErrInvalid
		}
		if _, ok := invocations[v.ID]; ok {
			return ErrConflict
		}
		slot := "invocation/" + string(v.Execution) + fmt.Sprintf("/%d/", v.Step) + v.ProviderCallKey
		if old, ok := slots[slot]; ok && old != v.ID {
			return ErrUniqueSlot
		}
		if _, err := EncodeInvocation(v); err != nil {
			return err
		}
		if err := validateInvocationSource(v, s.Lifecycle); err != nil {
			return err
		}
		invocations[v.ID] = v
		slots[slot] = v.ID
	}
	for _, v := range s.Actions {
		if !validActionStatus(v.Status) || v.Key == "" || v.Generation == 0 || v.Owner == "" || !model.ValidDigest(string(v.CommandDigest)) {
			return ErrInvalid
		}
		x, ok := executions[v.Owner]
		if !ok {
			return ErrUnknown
		}
		if err := v.Request.Validate(); err != nil {
			return err
		}
		t := tasks[x.Task]
		if v.Request.Space != t.Space || v.Request.Principal != t.Owner {
			return ErrConflict
		}
		if _, err := EncodePendingAction(v); err != nil {
			return err
		}
	}
	return validateRecoveryState(s)
}

func appendRecord(b []byte, raw []byte) ([]byte, error) { return codec.AppendBytes(b, raw) }

// EncodeSnapshot uses sorted records and exact canonical child encodings. It
// never serializes Go map iteration order or the derived slots index.
func EncodeSnapshot(s Snapshot) ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	s = cloneSnapshot(s)
	sort.Slice(s.Tasks, func(i, j int) bool { return s.Tasks[i].ID < s.Tasks[j].ID })
	sort.Slice(s.Executions, func(i, j int) bool { return s.Executions[i].ID < s.Executions[j].ID })
	sort.Slice(s.Steps, func(i, j int) bool {
		if s.Steps[i].Execution != s.Steps[j].Execution {
			return s.Steps[i].Execution < s.Steps[j].Execution
		}
		return s.Steps[i].Sequence < s.Steps[j].Sequence
	})
	sort.Slice(s.Invocations, func(i, j int) bool { return s.Invocations[i].ID < s.Invocations[j].ID })
	sort.Slice(s.Actions, func(i, j int) bool { return s.Actions[i].Key < s.Actions[j].Key })
	b := []byte("ATE-AGENT-SNAPSHOT\x00")
	version := SnapshotVersion
	if !s.Lifecycle.empty() {
		version = SnapshotVersion + 1
	}
	b = codec.AppendUint(b, version)
	encode := func(n int, f func(int) ([]byte, error)) error {
		b = codec.AppendUint(b, uint64(n))
		for i := 0; i < n; i++ {
			raw, err := f(i)
			if err != nil {
				return err
			}
			b, err = appendRecord(b, raw)
			if err != nil {
				return err
			}
			if len(b) > MaxSnapshotBytes {
				return ErrInvalid
			}
		}
		return nil
	}
	if err := encode(len(s.Tasks), func(i int) ([]byte, error) { return EncodeTask(s.Tasks[i]) }); err != nil {
		return nil, err
	}
	if err := encode(len(s.Executions), func(i int) ([]byte, error) { return EncodeExecution(s.Executions[i]) }); err != nil {
		return nil, err
	}
	if err := encode(len(s.Steps), func(i int) ([]byte, error) { return EncodeStep(s.Steps[i]) }); err != nil {
		return nil, err
	}
	if err := encode(len(s.Invocations), func(i int) ([]byte, error) { return EncodeInvocation(s.Invocations[i]) }); err != nil {
		return nil, err
	}
	if err := encode(len(s.Actions), func(i int) ([]byte, error) { return EncodePendingAction(s.Actions[i]) }); err != nil {
		return nil, err
	}
	if version >= SnapshotVersion+1 {
		raw, err := encodeLifecycleSnapshot(s.Lifecycle)
		if err != nil {
			return nil, err
		}
		b, err = appendRecord(b, raw)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

func readRecords(d *codec.Decoder, n int, decode func([]byte) error) error {
	for i := 0; i < n; i++ {
		// Invocation arguments/results are allowed up to codec's bounded byte
		// string size; the enclosing snapshot still has a 32 MiB total cap.
		raw, err := d.Bytes(codec.MaxByteString)
		if err != nil {
			return err
		}
		if err = decode(bytes.Clone(raw)); err != nil {
			return err
		}
	}
	return nil
}

// DecodeSnapshot rejects unknown versions, duplicate records, noncanonical
// child encodings, trailing data and any cross-record reference violation.
func DecodeSnapshot(raw []byte) (Snapshot, error) {
	if len(raw) > MaxSnapshotBytes || len(raw) < len("ATE-AGENT-SNAPSHOT\x00") {
		return Snapshot{}, ErrInvalid
	}
	prefix := "ATE-AGENT-SNAPSHOT\x00"
	if string(raw[:len(prefix)]) != prefix {
		return Snapshot{}, ErrInvalid
	}
	d := codec.NewDecoder(raw[len(prefix):])
	version, err := d.Uint()
	if err != nil || (version != SnapshotVersion && version != SnapshotVersion+1) {
		return Snapshot{}, ErrInvalid
	}
	readCount := func() (int, error) {
		n, err := d.Uint()
		if err != nil || n > MaxSnapshotRecords {
			return 0, ErrInvalid
		}
		return int(n), nil
	}
	out := Snapshot{}
	n, err := readCount()
	if err != nil {
		return Snapshot{}, err
	}
	out.Tasks = make([]Task, 0, n)
	if err = readRecords(d, n, func(raw []byte) error {
		v, e := DecodeTask(raw)
		if e == nil {
			out.Tasks = append(out.Tasks, v)
		}
		return e
	}); err != nil {
		return Snapshot{}, err
	}
	n, err = readCount()
	if err != nil {
		return Snapshot{}, err
	}
	out.Executions = make([]Execution, 0, n)
	if err = readRecords(d, n, func(raw []byte) error {
		v, e := DecodeExecution(raw)
		if e == nil {
			out.Executions = append(out.Executions, v)
		}
		return e
	}); err != nil {
		return Snapshot{}, err
	}
	n, err = readCount()
	if err != nil {
		return Snapshot{}, err
	}
	out.Steps = make([]Step, 0, n)
	if err = readRecords(d, n, func(raw []byte) error {
		v, e := DecodeStep(raw)
		if e == nil {
			out.Steps = append(out.Steps, v)
		}
		return e
	}); err != nil {
		return Snapshot{}, err
	}
	n, err = readCount()
	if err != nil {
		return Snapshot{}, err
	}
	out.Invocations = make([]Invocation, 0, n)
	if err = readRecords(d, n, func(raw []byte) error {
		v, e := DecodeInvocation(raw)
		if e == nil {
			out.Invocations = append(out.Invocations, v)
		}
		return e
	}); err != nil {
		return Snapshot{}, err
	}
	n, err = readCount()
	if err != nil {
		return Snapshot{}, err
	}
	out.Actions = make([]PendingAction, 0, n)
	if err = readRecords(d, n, func(raw []byte) error {
		v, e := DecodePendingAction(raw)
		if e == nil {
			out.Actions = append(out.Actions, v)
		}
		return e
	}); err != nil {
		return Snapshot{}, err
	}
	if version >= SnapshotVersion+1 {
		lifecycleRaw, e := d.Bytes(codec.MaxByteString)
		if e != nil {
			return Snapshot{}, e
		}
		out.Lifecycle, e = decodeLifecycleSnapshot(lifecycleRaw)
		if e != nil {
			return Snapshot{}, e
		}
	}
	if err = d.Done(); err != nil {
		return Snapshot{}, err
	}
	if err = out.Validate(); err != nil {
		return Snapshot{}, err
	}
	canonical, err := EncodeSnapshot(out)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Snapshot{}, ErrInvalid
	}
	return out, nil
}

// RestoreSnapshot creates a new Ledger whose indexes are derived solely from
// the authenticated snapshot records.
func RestoreSnapshot(raw []byte) (*Ledger, error) {
	s, err := DecodeSnapshot(raw)
	if err != nil {
		return nil, err
	}
	l := New()
	for _, v := range s.Tasks {
		l.tasks[v.ID] = v
	}
	for _, v := range s.Executions {
		l.executions[v.ID] = v
		l.slots["spawn/"+string(v.Task)+"/"+string(v.ID)] = string(v.ID)
	}
	for _, v := range s.Steps {
		l.steps[stepKey(v.Execution, v.Sequence)] = v
	}
	for _, v := range s.Invocations {
		v.Args = bytes.Clone(v.Args)
		v.Result = bytes.Clone(v.Result)
		l.invocations[v.ID] = v
		l.slots["invocation/"+string(v.Execution)+fmt.Sprintf("/%d/", v.Step)+v.ProviderCallKey] = v.ID
	}
	for _, v := range s.Actions {
		l.actions[v.Key] = v
	}
	l.lifecycle = cloneLifecycle(s.Lifecycle)
	return l, nil
}
