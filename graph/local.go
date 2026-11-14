package graph

import (
	"bytes"
	"slices"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// ExecutionAuthority supplies the live authority for an Agent branch command.
// The engine resolves Actor, Task, and authority version from its ledger.
type ExecutionAuthority struct {
	Execution   model.ExecutionID
	Deployment  string
	Holder      string
	OwnerEpoch  uint64
	Incarnation uint64
}

// BranchPosition pins both the contents and the version of a movable branch.
type BranchPosition struct {
	Branch  model.BranchID
	Head    model.RevisionID
	Version uint64
}

type ForkLocalCommand struct {
	Branch    model.BranchID
	Source    model.RevisionID
	Authority ExecutionAuthority
}

// WriteBranchCommand applies canonical engine.EncodeStateBatch bytes to Target.
// Batch ExpectedRoot and per-key conditions refer to the branch's state tree.
type WriteBranchCommand struct {
	Target    BranchPosition
	Authority ExecutionAuthority
	Batch     []byte
}

// JoinLocalCommand merges Source into Target and advances only Target.
type JoinLocalCommand struct {
	Target, Source BranchPosition
	Authority      ExecutionAuthority
	Resolutions    []Resolution
	Events         []model.EventID
}

// PublishBranchCommand applies the changes from the last published branch
// revision (or the immutable fork base) to the selected branch head. The
// current domain root is guarded independently.
// A divergent current value on any changed key rejects the entire publication.
type PublishBranchCommand struct {
	Source             BranchPosition
	Authority          ExecutionAuthority
	ExpectedDomainRoot model.ObjectID
}

func (s BranchScope) valid() bool {
	return validText(string(s.Principal)) && (s.Execution == "" || validText(string(s.Execution))) && validDigest(string(s.Base))
}

func (p RevisionProducer) valid() bool {
	if !validText(string(p.Branch)) || p.BranchVersion == 0 || !validText(string(p.Principal)) {
		return false
	}
	if p.Execution == "" {
		return p.Task == "" && p.Actor == "" && p.GoalEpoch == 0 && p.Incarnation == 0 && p.AuthorityVersion == 0
	}
	return validText(string(p.Execution)) && validText(string(p.Task)) && validText(string(p.Actor)) && p.GoalEpoch != 0 && p.Incarnation != 0 && p.AuthorityVersion != 0
}

func appendProducer(raw []byte, p RevisionProducer) []byte {
	raw, _ = codec.AppendString(raw, string(p.Branch))
	raw = codec.AppendUint(raw, p.BranchVersion)
	for _, field := range []string{string(p.Principal), string(p.Execution), string(p.Task), string(p.Actor)} {
		raw, _ = codec.AppendString(raw, field)
	}
	raw = codec.AppendUint(raw, p.GoalEpoch)
	raw = codec.AppendUint(raw, p.Incarnation)
	return codec.AppendUint(raw, p.AuthorityVersion)
}

func readProducer(d *codec.Decoder) (RevisionProducer, error) {
	branch, err := d.String(model.MaxIdentityBytes)
	if err != nil {
		return RevisionProducer{}, err
	}
	version, err := d.Uint()
	if err != nil {
		return RevisionProducer{}, err
	}
	fields, err := readTextFields(d, 4)
	if err != nil {
		return RevisionProducer{}, err
	}
	epoch, err := d.Uint()
	if err != nil {
		return RevisionProducer{}, err
	}
	incarnation, err := d.Uint()
	if err != nil {
		return RevisionProducer{}, err
	}
	authority, err := d.Uint()
	if err != nil {
		return RevisionProducer{}, err
	}
	return RevisionProducer{Branch: model.BranchID(branch), BranchVersion: version, Principal: model.PrincipalID(fields[0]), Execution: model.ExecutionID(fields[1]), Task: model.EntityID(fields[2]), Actor: model.ActorID(fields[3]), GoalEpoch: epoch, Incarnation: incarnation, AuthorityVersion: authority}, nil
}

func readTextFields(d *codec.Decoder, count int) ([]string, error) {
	fields := make([]string, count)
	for i := range fields {
		value, err := d.String(model.MaxIdentityBytes)
		if err != nil {
			return nil, err
		}
		fields[i] = value
	}
	return fields, nil
}

func appendExecutionAuthority(raw []byte, a ExecutionAuthority) ([]byte, error) {
	if a != (ExecutionAuthority{}) && (!validText(string(a.Execution)) || !validText(a.Deployment) || !validText(a.Holder) || a.OwnerEpoch == 0 || a.Incarnation == 0) {
		return nil, ErrCommandShape
	}
	for _, field := range []string{string(a.Execution), a.Deployment, a.Holder} {
		raw, _ = codec.AppendString(raw, field)
	}
	raw = codec.AppendUint(raw, a.OwnerEpoch)
	return codec.AppendUint(raw, a.Incarnation), nil
}

func readExecutionAuthority(d *codec.Decoder) (ExecutionAuthority, error) {
	fields, err := readTextFields(d, 3)
	if err != nil {
		return ExecutionAuthority{}, err
	}
	epoch, err := d.Uint()
	if err != nil {
		return ExecutionAuthority{}, err
	}
	incarnation, err := d.Uint()
	if err != nil {
		return ExecutionAuthority{}, err
	}
	return ExecutionAuthority{Execution: model.ExecutionID(fields[0]), Deployment: fields[1], Holder: fields[2], OwnerEpoch: epoch, Incarnation: incarnation}, nil
}

func appendBranchPosition(raw []byte, p BranchPosition) ([]byte, error) {
	if !validText(string(p.Branch)) || !validDigest(string(p.Head)) {
		return nil, ErrCommandShape
	}
	raw, _ = codec.AppendString(raw, string(p.Branch))
	raw, _ = codec.AppendString(raw, string(p.Head))
	return codec.AppendUint(raw, p.Version), nil
}

func readBranchPosition(d *codec.Decoder) (BranchPosition, error) {
	id, err := d.String(model.MaxIdentityBytes)
	if err != nil {
		return BranchPosition{}, err
	}
	head, err := readDigestString(d, false)
	if err != nil {
		return BranchPosition{}, err
	}
	version, err := d.Uint()
	if err != nil {
		return BranchPosition{}, err
	}
	return BranchPosition{Branch: model.BranchID(id), Head: model.RevisionID(head), Version: version}, nil
}

func appendLocalJoinEvidence(raw []byte, resolutions []Resolution, events []model.EventID) ([]byte, error) {
	if len(resolutions) > MaxRevisionEntries || len(events) > MaxRevisionEvents {
		return nil, ErrLimit
	}
	resolutions = slices.Clone(resolutions)
	sort.Slice(resolutions, func(i, j int) bool { return bytes.Compare(resolutions[i].Key, resolutions[j].Key) < 0 })
	raw = codec.AppendUint(raw, uint64(len(resolutions)))
	for i, r := range resolutions {
		if len(r.Key) > MaxEntryKeyBytes || (r.Value != "" && !validDigest(string(r.Value))) || (i > 0 && bytes.Equal(resolutions[i-1].Key, r.Key)) {
			return nil, ErrCommandShape
		}
		raw, _ = codec.AppendBytes(raw, r.Key)
		raw, _ = codec.AppendString(raw, string(r.Value))
		if len(raw) > MaxCommandBytes {
			return nil, ErrLimit
		}
	}
	raw = codec.AppendUint(raw, uint64(len(events)))
	seen := map[model.EventID]bool{}
	for _, id := range events {
		if !validDigest(string(id)) || seen[id] {
			return nil, ErrCommandShape
		}
		seen[id] = true
		raw, _ = codec.AppendString(raw, string(id))
		if len(raw) > MaxCommandBytes {
			return nil, ErrLimit
		}
	}
	return raw, nil
}

func readLocalJoinEvidence(d *codec.Decoder) ([]Resolution, []model.EventID, error) {
	count, err := checkedCount(d, MaxRevisionEntries, 2)
	if err != nil {
		return nil, nil, err
	}
	resolutions := make([]Resolution, count)
	for i := range resolutions {
		key, err := d.Bytes(MaxEntryKeyBytes)
		if err != nil {
			return nil, nil, err
		}
		value, err := readDigestString(d, true)
		if err != nil {
			return nil, nil, err
		}
		resolutions[i] = Resolution{Key: bytes.Clone(key), Value: model.ObjectID(value)}
	}
	count, err = checkedCount(d, MaxRevisionEvents, 65)
	if err != nil {
		return nil, nil, err
	}
	events := make([]model.EventID, count)
	for i := range events {
		id, err := readDigestString(d, false)
		if err != nil {
			return nil, nil, err
		}
		events[i] = model.EventID(id)
	}
	return resolutions, events, nil
}
