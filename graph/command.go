package graph

import (
	"bytes"
	"errors"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const CommandSchema uint64 = 1
const MaxCommandBytes = 1 << 20
const commandDomain = "ATE-GRAPH-COMMAND\x00"

var ErrCommandShape = errors.New("graph command must select one valid operation")

// Command is the small, closed graph command set used by the embedded
// API. The canonical intent is admitted before the core validates it against
// its private BranchRefs candidate. Callers cannot submit replacement state.
type Command struct {
	AddRevision *AddRevisionCommand
	Fork        *ForkCommand
	Advance     *AdvanceBranchCommand
	Join        *JoinCommand
	Prune       *PruneBranchCommand
}

type AddRevisionCommand struct {
	Parent  model.RevisionID
	Joins   []model.RevisionID
	Root    model.ObjectID
	Events  []model.EventID
	Entries []Entry
}
type ForkCommand struct {
	Branch model.BranchID
	Source model.RevisionID
}
type AdvanceBranchCommand struct {
	Branch   model.BranchID
	Expected uint64
	Head     model.RevisionID
}

// JoinCommand names immutable inputs and the exact live roots to replace.
// The engine computes the resulting tree and Revision; no output root is
// supplied by the caller. Resolutions may address existing conflicts only.
type JoinCommand struct {
	Branch             model.BranchID
	Expected           uint64
	Left, Right        model.RevisionID
	ExpectedDomainRoot model.ObjectID
	Resolutions        []Resolution
	Events             []model.EventID
}

type PruneBranchCommand struct {
	Branch   model.BranchID
	Expected uint64
	Head     model.RevisionID
	Reason   string
}

func (c Command) Kind() string {
	if c.selected() != 1 {
		return ""
	}
	switch {
	case c.AddRevision != nil:
		return "add_revision"
	case c.Fork != nil:
		return "fork"
	case c.Advance != nil:
		return "advance"
	case c.Join != nil:
		return "join"
	case c.Prune != nil:
		return "prune"
	default:
		return ""
	}
}

func (c Command) selected() int {
	n := 0
	if c.AddRevision != nil {
		n++
	}
	if c.Fork != nil {
		n++
	}
	if c.Advance != nil {
		n++
	}
	if c.Join != nil {
		n++
	}
	if c.Prune != nil {
		n++
	}
	return n
}

// EncodeCommand freezes the intent, including its authenticated Space. The
// AddRevision child uses the existing canonical revision codec, so set/key
// ordering cannot give the same graph intent different request identities.
func EncodeCommand(space model.SpaceID, c Command) ([]byte, error) {
	if !validText(string(space)) || c.selected() != 1 {
		return nil, ErrCommandShape
	}
	b := codec.AppendUint([]byte(commandDomain), CommandSchema)
	b, _ = codec.AppendString(b, string(space))
	switch {
	case c.AddRevision != nil:
		x := c.AddRevision
		r, err := BuildRevision(string(space), x.Parent, x.Joins, x.Root, x.Events, x.Entries)
		if err != nil {
			return nil, err
		}
		raw, err := EncodeRevision(r)
		if err != nil {
			return nil, err
		}
		b = codec.AppendUint(b, 1)
		b, err = codec.AppendBytes(b, raw)
		if err != nil {
			return nil, err
		}
	case c.Fork != nil:
		x := c.Fork
		if !validText(string(x.Branch)) || !validDigest(string(x.Source)) {
			return nil, ErrCommandShape
		}
		if _, err := EncodeBranch(Branch{ID: x.Branch, Head: x.Source}); err != nil {
			return nil, err
		}
		b = codec.AppendUint(b, 2)
		b, _ = codec.AppendString(b, string(x.Branch))
		b, _ = codec.AppendString(b, string(x.Source))
	case c.Advance != nil:
		x := c.Advance
		if !validText(string(x.Branch)) || !validDigest(string(x.Head)) {
			return nil, ErrCommandShape
		}
		if _, err := EncodeBranch(Branch{ID: x.Branch, Head: x.Head, Version: x.Expected}); err != nil {
			return nil, err
		}
		b = codec.AppendUint(b, 3)
		b, _ = codec.AppendString(b, string(x.Branch))
		b = codec.AppendUint(b, x.Expected)
		b, _ = codec.AppendString(b, string(x.Head))
	case c.Join != nil:
		x := c.Join
		if !validText(string(x.Branch)) || !validDigest(string(x.Left)) || !validDigest(string(x.Right)) || (x.ExpectedDomainRoot != "" && !validDigest(string(x.ExpectedDomainRoot))) || len(x.Resolutions) > MaxRevisionEntries || len(x.Events) > MaxRevisionEvents {
			return nil, ErrCommandShape
		}
		if _, err := EncodeBranch(Branch{ID: x.Branch, Head: x.Left, Version: x.Expected}); err != nil {
			return nil, err
		}
		resolutions := make([]Resolution, len(x.Resolutions))
		for i, r := range x.Resolutions {
			if len(r.Key) > MaxEntryKeyBytes || (r.Value != "" && !validDigest(string(r.Value))) {
				return nil, ErrCommandShape
			}
			resolutions[i] = Resolution{Key: bytes.Clone(r.Key), Value: r.Value}
		}
		sort.Slice(resolutions, func(i, j int) bool { return bytes.Compare(resolutions[i].Key, resolutions[j].Key) < 0 })
		b = codec.AppendUint(b, 4)
		b, _ = codec.AppendString(b, string(x.Branch))
		b = codec.AppendUint(b, x.Expected)
		b, _ = codec.AppendString(b, string(x.Left))
		b, _ = codec.AppendString(b, string(x.Right))
		b, _ = codec.AppendString(b, string(x.ExpectedDomainRoot))
		b = codec.AppendUint(b, uint64(len(resolutions)))
		for i, r := range resolutions {
			if i > 0 && bytes.Equal(resolutions[i-1].Key, r.Key) {
				return nil, ErrCommandShape
			}
			b, _ = codec.AppendBytes(b, r.Key)
			b, _ = codec.AppendString(b, string(r.Value))
		}
		b = codec.AppendUint(b, uint64(len(x.Events)))
		seen := map[model.EventID]bool{}
		for _, id := range x.Events {
			if !validDigest(string(id)) || seen[id] {
				return nil, ErrCommandShape
			}
			seen[id] = true
			b, _ = codec.AppendString(b, string(id))
		}
	case c.Prune != nil:
		x := c.Prune
		if !validText(string(x.Branch)) || !validDigest(string(x.Head)) || !validText(x.Reason) {
			return nil, ErrCommandShape
		}
		if _, err := EncodeBranch(Branch{ID: x.Branch, Head: x.Head, Version: x.Expected}); err != nil {
			return nil, err
		}
		b = codec.AppendUint(b, 5)
		b, _ = codec.AppendString(b, string(x.Branch))
		b = codec.AppendUint(b, x.Expected)
		b, _ = codec.AppendString(b, string(x.Head))
		b, _ = codec.AppendString(b, x.Reason)
	}
	if len(b) > MaxCommandBytes {
		return nil, ErrLimit
	}
	return b, nil
}

func DecodeCommand(raw []byte) (model.SpaceID, Command, error) {
	if len(raw) > MaxCommandBytes {
		return "", Command{}, ErrLimit
	}
	if !bytes.HasPrefix(raw, []byte(commandDomain)) {
		return "", Command{}, ErrCommandShape
	}
	d := codec.NewDecoder(raw[len(commandDomain):])
	v, err := d.Uint()
	if err != nil {
		return "", Command{}, err
	}
	if v != CommandSchema {
		return "", Command{}, ErrUnsupportedSchema
	}
	s, err := d.String(model.MaxIdentityBytes)
	if err != nil {
		return "", Command{}, err
	}
	space := model.SpaceID(s)
	op, err := d.Uint()
	if err != nil {
		return "", Command{}, err
	}
	c := Command{}
	switch op {
	case 1:
		child, err := d.Bytes(MaxCommandBytes)
		if err != nil {
			return "", Command{}, err
		}
		r, err := DecodeRevision(child)
		if err != nil {
			return "", Command{}, err
		}
		if r.Space != space {
			return "", Command{}, ErrSpaceMismatch
		}
		c.AddRevision = &AddRevisionCommand{Parent: r.StateParent, Joins: r.JoinParents, Root: r.StateRoot, Events: r.Events, Entries: r.Entries}
	case 2:
		id, err := d.String(model.MaxIdentityBytes)
		if err != nil {
			return "", Command{}, err
		}
		source, err := d.String(64)
		if err != nil {
			return "", Command{}, err
		}
		c.Fork = &ForkCommand{Branch: model.BranchID(id), Source: model.RevisionID(source)}
	case 3:
		id, err := d.String(model.MaxIdentityBytes)
		if err != nil {
			return "", Command{}, err
		}
		expected, err := d.Uint()
		if err != nil {
			return "", Command{}, err
		}
		head, err := d.String(64)
		if err != nil {
			return "", Command{}, err
		}
		c.Advance = &AdvanceBranchCommand{Branch: model.BranchID(id), Expected: expected, Head: model.RevisionID(head)}
	case 4:
		id, err := d.String(model.MaxIdentityBytes)
		if err != nil {
			return "", Command{}, err
		}
		expected, err := d.Uint()
		if err != nil {
			return "", Command{}, err
		}
		left, err := readDigestString(d, false)
		if err != nil {
			return "", Command{}, err
		}
		right, err := readDigestString(d, false)
		if err != nil {
			return "", Command{}, err
		}
		root, err := readDigestString(d, true)
		if err != nil {
			return "", Command{}, err
		}
		n, err := checkedCount(d, MaxRevisionEntries, 2)
		if err != nil {
			return "", Command{}, err
		}
		x := &JoinCommand{Branch: model.BranchID(id), Expected: expected, Left: model.RevisionID(left), Right: model.RevisionID(right), ExpectedDomainRoot: model.ObjectID(root), Resolutions: make([]Resolution, int(n))}
		for i := range x.Resolutions {
			key, err := d.Bytes(MaxEntryKeyBytes)
			if err != nil {
				return "", Command{}, err
			}
			v, err := readDigestString(d, true)
			if err != nil {
				return "", Command{}, err
			}
			x.Resolutions[i] = Resolution{Key: bytes.Clone(key), Value: model.ObjectID(v)}
		}
		count, err := checkedCount(d, MaxRevisionEvents, 65)
		if err != nil {
			return "", Command{}, err
		}
		x.Events = make([]model.EventID, int(count))
		for i := range x.Events {
			v, err := readDigestString(d, false)
			if err != nil {
				return "", Command{}, err
			}
			x.Events[i] = model.EventID(v)
		}
		c.Join = x
	case 5:
		id, err := d.String(model.MaxIdentityBytes)
		if err != nil {
			return "", Command{}, err
		}
		expected, err := d.Uint()
		if err != nil {
			return "", Command{}, err
		}
		head, err := readDigestString(d, false)
		if err != nil {
			return "", Command{}, err
		}
		reason, err := d.String(model.MaxIdentityBytes)
		if err != nil {
			return "", Command{}, err
		}
		c.Prune = &PruneBranchCommand{Branch: model.BranchID(id), Expected: expected, Head: model.RevisionID(head), Reason: reason}
	default:
		return "", Command{}, ErrUnsupportedSchema
	}
	if err = d.Done(); err != nil {
		return "", Command{}, err
	}
	canonical, err := EncodeCommand(space, c)
	if err != nil {
		return "", Command{}, err
	}
	if !bytes.Equal(canonical, raw) {
		return "", Command{}, ErrCommandShape
	}
	return space, c, nil
}
