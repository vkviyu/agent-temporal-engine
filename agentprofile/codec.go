package agentprofile

import (
	"bytes"
	"fmt"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const MaxRecordBytes = 1 << 20

func appendText(b []byte, s string) ([]byte, error) { return codec.AppendString(b, s) }
func EncodeTask(t Task) ([]byte, error) {
	if t.ID == "" || t.Space == "" || t.Owner == "" || t.Goal == "" || t.GoalEpoch == 0 {
		return nil, ErrInvalid
	}
	b := codec.AppendUint(nil, uint64(Version))
	var e error
	for _, s := range []string{string(t.ID), string(t.Space), string(t.Owner), t.Goal, string(t.Status)} {
		b, e = appendText(b, s)
		if e != nil {
			return nil, e
		}
	}
	b = codec.AppendUint(b, t.GoalEpoch)
	b = codec.AppendUint(b, t.CancelEpoch)
	return b, nil
}
func DecodeTask(raw []byte) (Task, error) {
	d := codec.NewDecoder(raw)
	v, e := d.Uint()
	if e != nil || v != uint64(Version) {
		return Task{}, ErrInvalid
	}
	f := make([]string, 5)
	for i := range f {
		f[i], e = d.String(1024)
		if e != nil {
			return Task{}, e
		}
	}
	epoch, e := d.Uint()
	if e != nil {
		return Task{}, e
	}
	cancel, e := d.Uint()
	if e != nil {
		return Task{}, e
	}
	if e = d.Done(); e != nil {
		return Task{}, e
	}
	t := Task{ID: model.EntityID(f[0]), Space: model.SpaceID(f[1]), Owner: model.PrincipalID(f[2]), Goal: f[3], Status: TaskStatus(f[4]), GoalEpoch: epoch, CancelEpoch: cancel}
	canonical, e := EncodeTask(t)
	if e != nil || !bytes.Equal(canonical, raw) {
		return Task{}, fmt.Errorf("%w: noncanonical task", ErrInvalid)
	}
	return t, nil
}
func EncodeExecution(x Execution) ([]byte, error) {
	if x.ID == "" || x.Task == "" || x.Actor == "" || x.Incarnation == 0 || x.AcceptedGoalEpoch == 0 {
		return nil, ErrInvalid
	}
	b := codec.AppendUint(nil, uint64(Version))
	var e error
	for _, s := range []string{string(x.ID), string(x.Task), string(x.Actor), string(x.Parent), string(x.Status)} {
		b, e = appendText(b, s)
		if e != nil {
			return nil, e
		}
	}
	b = codec.AppendUint(b, x.Incarnation)
	b = codec.AppendUint(b, x.AcceptedGoalEpoch)
	b = codec.AppendUint(b, x.CancelEpoch)
	return b, nil
}
func DecodeExecution(raw []byte) (Execution, error) {
	d := codec.NewDecoder(raw)
	v, e := d.Uint()
	if e != nil || v != uint64(Version) {
		return Execution{}, ErrInvalid
	}
	f := make([]string, 5)
	for i := range f {
		f[i], e = d.String(1024)
		if e != nil {
			return Execution{}, e
		}
	}
	inc, e := d.Uint()
	if e != nil {
		return Execution{}, e
	}
	goal, e := d.Uint()
	if e != nil {
		return Execution{}, e
	}
	cancel, e := d.Uint()
	if e != nil {
		return Execution{}, e
	}
	if e = d.Done(); e != nil {
		return Execution{}, e
	}
	x := Execution{ID: model.ExecutionID(f[0]), Task: model.EntityID(f[1]), Actor: model.ActorID(f[2]), Parent: model.ExecutionID(f[3]), Status: ExecutionStatus(f[4]), Incarnation: inc, AcceptedGoalEpoch: goal, CancelEpoch: cancel}
	canonical, e := EncodeExecution(x)
	if e != nil || !bytes.Equal(canonical, raw) {
		return Execution{}, fmt.Errorf("%w: noncanonical execution", ErrInvalid)
	}
	return x, nil
}

func appendFence(b []byte, f model.CommitFence) ([]byte, error) {
	if f.Store == "" && f.LSN == 0 && f.CommitDigest == "" {
		return codec.AppendUint(b, 0), nil
	}
	if err := f.Validate(); err != nil {
		return nil, err
	}
	b = codec.AppendUint(b, 1)
	var e error
	b, e = appendText(b, string(f.Store))
	if e != nil {
		return nil, e
	}
	b = codec.AppendUint(b, f.LSN)
	b, e = appendText(b, string(f.CommitDigest))
	return b, e
}
func readFence(d *codec.Decoder) (model.CommitFence, error) {
	p, e := d.Uint()
	if e != nil {
		return model.CommitFence{}, e
	}
	if p == 0 {
		return model.CommitFence{}, nil
	}
	if p != 1 {
		return model.CommitFence{}, ErrInvalid
	}
	store, e := d.String(1024)
	if e != nil {
		return model.CommitFence{}, e
	}
	lsn, e := d.Uint()
	if e != nil {
		return model.CommitFence{}, e
	}
	digest, e := d.String(64)
	if e != nil {
		return model.CommitFence{}, e
	}
	f := model.CommitFence{Store: model.StoreID(store), LSN: lsn, CommitDigest: model.CommitDigest(digest)}
	if e = f.Validate(); e != nil {
		return model.CommitFence{}, e
	}
	return f, nil
}
func EncodeStep(s Step) ([]byte, error) {
	if s.Execution == "" || s.Sequence == 0 || s.GoalEpoch == 0 || s.Status == "" {
		return nil, ErrInvalid
	}
	b := codec.AppendUint(nil, uint64(Version))
	var e error
	b, e = appendText(b, string(s.Execution))
	if e != nil {
		return nil, e
	}
	b = codec.AppendUint(b, s.Sequence)
	b = codec.AppendUint(b, s.GoalEpoch)
	b, e = appendText(b, string(s.Status))
	if e != nil {
		return nil, e
	}
	b, e = appendFence(b, s.InputAnchor)
	if e != nil {
		return nil, e
	}
	b, e = appendText(b, s.PrimaryInvocation)
	return b, e
}
func DecodeStep(raw []byte) (Step, error) {
	d := codec.NewDecoder(raw)
	v, e := d.Uint()
	if e != nil || v != uint64(Version) {
		return Step{}, ErrInvalid
	}
	execution, e := d.String(1024)
	if e != nil {
		return Step{}, e
	}
	seq, e := d.Uint()
	if e != nil {
		return Step{}, e
	}
	epoch, e := d.Uint()
	if e != nil {
		return Step{}, e
	}
	status, e := d.String(64)
	if e != nil {
		return Step{}, e
	}
	anchor, e := readFence(d)
	if e != nil {
		return Step{}, e
	}
	primary, e := d.String(1024)
	if e != nil {
		return Step{}, e
	}
	if e = d.Done(); e != nil {
		return Step{}, e
	}
	s := Step{Execution: model.ExecutionID(execution), Sequence: seq, GoalEpoch: epoch, Status: StepStatus(status), InputAnchor: anchor, PrimaryInvocation: primary}
	canonical, e := EncodeStep(s)
	if e != nil || !bytes.Equal(canonical, raw) {
		return Step{}, ErrInvalid
	}
	return s, nil
}
func EncodeInvocation(i Invocation) ([]byte, error) {
	return encodeInvocationV1(i)
}

func encodeInvocationV1(i Invocation) ([]byte, error) {
	if i.ID == "" || i.Execution == "" || i.Step == 0 || i.ProviderCallKey == "" || i.Tool == "" || i.Attempt == 0 || i.Status == "" {
		return nil, ErrInvalid
	}
	if !commandText(i.AcceptedResult) || !model.ValidDigest(string(i.ToolDefinition)) || !commandText(i.ToolVersion) || (i.DispatchClass != "pure" && i.DispatchClass != "idempotent" && i.DispatchClass != "non_idempotent") {
		return nil, ErrInvalid
	}
	b := codec.AppendUint(nil, 2)
	var e error
	for _, s := range []string{i.ID, string(i.Execution), i.ProviderCallKey, i.Tool, string(i.Status)} {
		b, e = appendText(b, s)
		if e != nil {
			return nil, e
		}
	}
	b = codec.AppendUint(b, i.Step)
	b, e = codec.AppendBytes(b, i.Args)
	if e != nil {
		return nil, e
	}
	b, e = codec.AppendBytes(b, i.Result)
	if e != nil {
		return nil, e
	}
	b = codec.AppendUint(b, i.Attempt)
	for _, text := range []string{i.AcceptedResult, string(i.ToolDefinition), i.ToolVersion, i.DispatchClass} {
		b, e = appendText(b, text)
		if e != nil {
			return nil, e
		}
	}
	return b, nil
}
func DecodeInvocation(raw []byte) (Invocation, error) {
	return decodeInvocation(raw)
}

func decodeInvocation(raw []byte) (Invocation, error) {
	d := codec.NewDecoder(raw)
	v, e := d.Uint()
	if e != nil || v != 2 {
		return Invocation{}, ErrInvalid
	}
	f := make([]string, 5)
	for j := range f {
		f[j], e = d.String(1 << 20)
		if e != nil {
			return Invocation{}, e
		}
	}
	step, e := d.Uint()
	if e != nil {
		return Invocation{}, e
	}
	args, e := d.Bytes(16 << 20)
	if e != nil {
		return Invocation{}, e
	}
	result, e := d.Bytes(16 << 20)
	if e != nil {
		return Invocation{}, e
	}
	attempt, e := d.Uint()
	if e != nil || e == nil && attempt == 0 {
		return Invocation{}, ErrInvalid
	}
	i := Invocation{ID: f[0], Execution: model.ExecutionID(f[1]), ProviderCallKey: f[2], Tool: f[3], Status: InvocationStatus(f[4]), Step: step, Args: bytes.Clone(args), Result: bytes.Clone(result), Attempt: attempt}
	i.AcceptedResult, e = d.String(model.MaxIdentityBytes)
	if e != nil {
		return Invocation{}, e
	}
	var toolDefinition string
	toolDefinition, e = d.String(64)
	if e != nil {
		return Invocation{}, e
	}
	i.ToolDefinition = model.ObjectID(toolDefinition)
	i.ToolVersion, e = d.String(model.MaxIdentityBytes)
	if e != nil {
		return Invocation{}, e
	}
	i.DispatchClass, e = d.String(model.MaxIdentityBytes)
	if e != nil {
		return Invocation{}, e
	}
	if e = d.Done(); e != nil {
		return Invocation{}, e
	}
	canonical, e := encodeInvocationV1(i)
	if e != nil || !bytes.Equal(canonical, raw) {
		return Invocation{}, ErrInvalid
	}
	return i, nil
}
func EncodePendingAction(a PendingAction) ([]byte, error) {
	if a.Key == "" || a.Generation == 0 || a.Request.RequestID == "" || !model.ValidDigest(string(a.CommandDigest)) || a.Owner == "" || a.Status == "" {
		return nil, ErrInvalid
	}
	b := codec.AppendUint(nil, uint64(Version))
	var e error
	for _, s := range []string{a.Key, string(a.Request.Space), string(a.Request.Principal), string(a.Request.RequestID), string(a.CommandDigest), string(a.Owner), string(a.Status), a.Supersedes} {
		b, e = appendText(b, s)
		if e != nil {
			return nil, e
		}
	}
	b = codec.AppendUint(b, a.Generation)
	b, e = appendFence(b, a.InputFence)
	return b, e
}
func DecodePendingAction(raw []byte) (PendingAction, error) {
	d := codec.NewDecoder(raw)
	v, e := d.Uint()
	if e != nil || v != uint64(Version) {
		return PendingAction{}, ErrInvalid
	}
	f := make([]string, 8)
	for j := range f {
		f[j], e = d.String(1 << 20)
		if e != nil {
			return PendingAction{}, e
		}
	}
	gen, e := d.Uint()
	if e != nil {
		return PendingAction{}, e
	}
	fence, e := readFence(d)
	if e != nil {
		return PendingAction{}, e
	}
	if e = d.Done(); e != nil {
		return PendingAction{}, e
	}
	a := PendingAction{Key: f[0], Request: model.RequestKey{Space: model.SpaceID(f[1]), Principal: model.PrincipalID(f[2]), RequestID: model.RequestID(f[3])}, CommandDigest: model.ObjectID(f[4]), Owner: model.ExecutionID(f[5]), Status: ActionStatus(f[6]), Supersedes: f[7], Generation: gen, InputFence: fence}
	canonical, e := EncodePendingAction(a)
	if e != nil || !bytes.Equal(canonical, raw) {
		return PendingAction{}, ErrInvalid
	}
	return a, nil
}
