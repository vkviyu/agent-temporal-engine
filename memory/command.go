package memory

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/model"
)

const MaxCommandBytes = MaxRecordBytes

var (
	ErrCommandShape = errors.New("invalid memory command")
	ErrCommandScope = errors.New("memory command crosses space authority")
)

type ValidateCommand struct {
	ID         model.ObjectID
	Validation Validation
}
type TransitionCommand struct {
	ID  model.ObjectID
	To  Status
	Why string
}
type RetractSourceCommand struct {
	Source     model.ObjectID
	Retraction Retraction
}
type BuildPathCommand struct {
	Space         model.SpaceID
	Anchor        model.SnapshotAnchor
	Steps         []Step
	Omitted       []model.NodeRef
	Validation    Validation
	Applicability Applicability
	DerivedFrom   []model.ObjectID
}
type RetractPathCommand struct {
	ID         model.ObjectID
	Retraction Retraction
}
type Command struct {
	PutCandidate  *Memory
	Validate      *ValidateCommand
	Promote       *model.ObjectID
	Transition    *TransitionCommand
	RetractSource *RetractSourceCommand
	BuildPath     *BuildPathCommand
	RetractPath   *RetractPathCommand
}
type CommandResult struct {
	Memory  *Memory
	Path    *ExperiencePath
	Changed []model.ObjectID
}

func (c Command) count() int {
	n := 0
	if c.PutCandidate != nil {
		n++
	}
	if c.Validate != nil {
		n++
	}
	if c.Promote != nil {
		n++
	}
	if c.Transition != nil {
		n++
	}
	if c.RetractSource != nil {
		n++
	}
	if c.BuildPath != nil {
		n++
	}
	if c.RetractPath != nil {
		n++
	}
	return n
}

// durableCommand owns the exact input and selects one closed operation. The
// standalone adapter keeps its existing disk schema; the core uses these same
// canonical bytes and repeats the reducer under its coordinator.
func (c Command) durableCommand() (durableCommand, error) {
	if c.count() != 1 {
		return durableCommand{}, ErrCommandShape
	}
	out := durableCommand{}
	switch {
	case c.PutCandidate != nil:
		m := cloneMemory(*c.PutCandidate)
		if (m.Status != "" && m.Status != StatusCandidate) || m.Validation != nil || m.Retraction != nil || len(m.Supersedes) != 0 || len(m.Retracts) != 0 {
			return out, ErrCommandShape
		}
		m.Status = StatusCandidate
		if m.VisibleAt.IsZero() {
			m.VisibleAt = m.RecordedAt
		}
		if m.ID != "" && m.ID != IDFor(m) {
			return out, ErrCommandShape
		}
		m.ID = ""
		if err := Validate(m); err != nil {
			return out, fmt.Errorf("%w: %w", ErrCommandShape, err)
		}
		out.op = durablePutCandidate
		out.memory = m
	case c.Validate != nil:
		x := c.Validate
		if !model.ValidDigest(string(x.ID)) || !x.Validation.Passed || len(x.Validation.Evidence) == 0 || !validValidation(x.Validation, x.Validation.Evidence[0].Anchor.Space) {
			return out, ErrCommandShape
		}
		out.op = durableValidate
		out.id = x.ID
		out.validation = cloneValidation(x.Validation)
	case c.Promote != nil:
		if !model.ValidDigest(string(*c.Promote)) {
			return out, ErrCommandShape
		}
		out.op = durablePromote
		out.id = *c.Promote
	case c.Transition != nil:
		x := c.Transition
		if !model.ValidDigest(string(x.ID)) || !validText(x.Why, false) || (x.To != StatusRejected && x.To != StatusRetracted && x.To != StatusSuperseded) {
			return out, ErrCommandShape
		}
		out.op = durableTransition
		out.id = x.ID
		out.to = x.To
		out.why = x.Why
	case c.RetractSource != nil:
		x := c.RetractSource
		if !model.ValidDigest(string(x.Source)) || !validRetraction(&x.Retraction) {
			return out, ErrCommandShape
		}
		out.op = durableRetractSource
		out.id = x.Source
		out.retraction = x.Retraction
	case c.BuildPath != nil:
		x := c.BuildPath
		p := ExperiencePath{Space: x.Space, SourceAnchor: x.Anchor, Steps: x.Steps, Omitted: x.Omitted, Validation: x.Validation, Applicability: x.Applicability, DerivedFrom: x.DerivedFrom, Status: StatusPromoted}
		p = clonePath(p)
		if _, err := CanonicalPath(p); err != nil {
			return out, fmt.Errorf("%w: %w", ErrCommandShape, err)
		}
		out.op = durableBuildPath
		out.path = p
	case c.RetractPath != nil:
		x := c.RetractPath
		if !model.ValidDigest(string(x.ID)) || !validRetraction(&x.Retraction) {
			return out, ErrCommandShape
		}
		out.op = durableRetractPath
		out.id = x.ID
		out.retraction = x.Retraction
	}
	return out, nil
}
func commandFromDurable(c durableCommand) Command {
	out := Command{}
	switch c.op {
	case durablePutCandidate:
		m := cloneMemory(c.memory)
		out.PutCandidate = &m
	case durableValidate:
		out.Validate = &ValidateCommand{ID: c.id, Validation: cloneValidation(c.validation)}
	case durablePromote:
		id := c.id
		out.Promote = &id
	case durableTransition:
		out.Transition = &TransitionCommand{ID: c.id, To: c.to, Why: c.why}
	case durableRetractSource:
		out.RetractSource = &RetractSourceCommand{Source: c.id, Retraction: c.retraction}
	case durableBuildPath:
		p := clonePath(c.path)
		out.BuildPath = &BuildPathCommand{Space: p.Space, Anchor: p.SourceAnchor, Steps: p.Steps, Omitted: p.Omitted, Validation: p.Validation, Applicability: p.Applicability, DerivedFrom: p.DerivedFrom}
	case durableRetractPath:
		out.RetractPath = &RetractPathCommand{ID: c.id, Retraction: c.retraction}
	}
	return out
}
func (c Command) Check() error { _, err := c.durableCommand(); return err }
func EncodeCommand(c Command) ([]byte, error) {
	dc, err := c.durableCommand()
	if err != nil {
		return nil, err
	}
	raw, err := encodeDurableCommand(dc)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCommandShape, err)
	}
	if len(raw) > MaxCommandBytes {
		return nil, ErrCommandShape
	}
	return raw, nil
}
func DecodeCommand(raw []byte) (Command, error) {
	if len(raw) > MaxCommandBytes {
		return Command{}, ErrCommandShape
	}
	dc, err := decodeDurableCommand(raw)
	if err != nil {
		return Command{}, fmt.Errorf("%w: %w", ErrCommandShape, err)
	}
	c := commandFromDurable(dc)
	canonical, err := EncodeCommand(c)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Command{}, ErrCommandShape
	}
	return c, nil
}

// ApplyCommand serializes read/candidate/install under one mutex. A rejected
// command cannot alter the source, and concurrent candidates cannot overwrite
// each other. The core uses a private Store; only its publisher grants durability.
func (s *Store) ApplyCommand(key model.RequestKey, c Command) (CommandResult, error) {
	if err := key.Validate(); err != nil {
		return CommandResult{}, err
	}
	raw, err := EncodeCommand(c)
	if err != nil {
		return CommandResult{}, err
	}
	c, err = DecodeCommand(raw)
	if err != nil {
		return CommandResult{}, err
	}
	dc, err := c.durableCommand()
	if err != nil {
		return CommandResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate := New()
	for id, m := range s.memories {
		candidate.memories[id] = cloneMemory(m)
		candidate.index(m)
	}
	for id, p := range s.paths {
		candidate.paths[id] = clonePath(p)
	}
	for id, r := range s.sourceBan {
		candidate.sourceBan[id] = r
	}
	for id, next := range s.successors {
		candidate.successors[id] = next
	}
	for id, next := range s.pathSuccessors {
		candidate.pathSuccessors[id] = next
	}
	if err = validateDurableScope(candidate, key, dc); err != nil {
		if errors.Is(err, ErrInvalidMemory) {
			return CommandResult{}, fmt.Errorf("%w: %w", ErrCommandScope, err)
		}
		return CommandResult{}, err
	}
	result, err := applyDurableCommand(candidate, dc)
	if err != nil {
		return CommandResult{}, err
	}
	image, err := EncodeSnapshot(candidate.Snapshot())
	if err != nil {
		return CommandResult{}, err
	}
	candidate, err = RestoreSnapshot(image)
	if err != nil {
		return CommandResult{}, err
	}
	s.memories = candidate.memories
	s.paths = candidate.paths
	s.sourceBan = candidate.sourceBan
	s.successors = candidate.successors
	s.pathSuccessors = candidate.pathSuccessors
	s.tokens = candidate.tokens
	out := CommandResult{Changed: append([]model.ObjectID(nil), result.changed...)}
	if result.memory.ID != "" {
		m := cloneMemory(result.memory)
		out.Memory = &m
	}
	if result.path.ID != "" {
		p := clonePath(result.path)
		out.Path = &p
	}
	return out, nil
}

// Sources names newly supplied provenance, which the embedding core must
// authenticate against each exact source Anchor before it can publish.
func (c Command) Sources() []Source {
	var out []Source
	switch {
	case c.PutCandidate != nil:
		out = allSources(*c.PutCandidate)
	case c.Validate != nil:
		out = append(append([]Source(nil), c.Validate.Validation.Evidence...), c.Validate.Validation.Counterexamples...)
	case c.BuildPath != nil:
		out = append(append([]Source(nil), c.BuildPath.Validation.Evidence...), c.BuildPath.Validation.Counterexamples...)
	}
	for i := range out {
		out[i] = cloneSource(out[i])
	}
	return out
}
