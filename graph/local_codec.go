package graph

import (
	"bytes"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func encodeLocalCommand(space model.SpaceID, c Command) ([]byte, error) {
	raw := codec.AppendUint([]byte(commandDomain), LocalCommandSchema)
	raw, _ = codec.AppendString(raw, string(space))
	var err error
	switch {
	case c.ForkLocal != nil:
		x := c.ForkLocal
		raw = codec.AppendUint(raw, 1)
		raw, err = appendBranchPosition(raw, BranchPosition{Branch: x.Branch, Head: x.Source})
		if err == nil {
			raw, err = appendExecutionAuthority(raw, x.Authority)
		}
	case c.WriteBranch != nil:
		x := c.WriteBranch
		if len(x.Batch) == 0 || len(x.Batch) > MaxCommandBytes {
			return nil, ErrCommandShape
		}
		raw = codec.AppendUint(raw, 2)
		raw, err = appendBranchPosition(raw, x.Target)
		if err == nil {
			raw, err = appendExecutionAuthority(raw, x.Authority)
		}
		if err == nil {
			raw, err = codec.AppendBytes(raw, x.Batch)
		}
	case c.JoinLocal != nil:
		x := c.JoinLocal
		if x.Target.Branch == x.Source.Branch || x.Target.Head == x.Source.Head {
			return nil, ErrCommandShape
		}
		raw = codec.AppendUint(raw, 3)
		raw, err = appendBranchPosition(raw, x.Target)
		if err == nil {
			raw, err = appendExecutionAuthority(raw, x.Authority)
		}
		if err == nil {
			raw, err = appendBranchPosition(raw, x.Source)
		}
		if err == nil {
			raw, err = appendLocalJoinEvidence(raw, x.Resolutions, x.Events)
		}
	case c.PublishBranch != nil:
		x := c.PublishBranch
		if x.ExpectedDomainRoot != "" && !validDigest(string(x.ExpectedDomainRoot)) {
			return nil, ErrCommandShape
		}
		raw = codec.AppendUint(raw, 4)
		raw, err = appendBranchPosition(raw, x.Source)
		if err == nil {
			raw, err = appendExecutionAuthority(raw, x.Authority)
		}
		if err == nil {
			raw, err = codec.AppendString(raw, string(x.ExpectedDomainRoot))
		}
	}
	if err != nil {
		return nil, err
	}
	if len(raw) > MaxCommandBytes {
		return nil, ErrLimit
	}
	return raw, nil
}

func decodeLocalCommand(raw []byte, d *codec.Decoder) (model.SpaceID, Command, error) {
	space, err := d.String(model.MaxIdentityBytes)
	if err != nil {
		return "", Command{}, err
	}
	op, err := d.Uint()
	if err != nil {
		return "", Command{}, err
	}
	if op < 1 || op > 4 {
		return "", Command{}, ErrUnsupportedSchema
	}
	position, err := readBranchPosition(d)
	if err != nil {
		return "", Command{}, err
	}
	authority, err := readExecutionAuthority(d)
	if err != nil {
		return "", Command{}, err
	}
	var c Command
	switch op {
	case 1:
		if position.Version != 0 {
			return "", Command{}, ErrCommandShape
		}
		c.ForkLocal = &ForkLocalCommand{Branch: position.Branch, Source: position.Head, Authority: authority}
	case 2:
		batch, err := d.Bytes(MaxCommandBytes)
		if err != nil {
			return "", Command{}, err
		}
		c.WriteBranch = &WriteBranchCommand{Target: position, Authority: authority, Batch: bytes.Clone(batch)}
	case 3:
		source, err := readBranchPosition(d)
		if err != nil {
			return "", Command{}, err
		}
		resolutions, events, err := readLocalJoinEvidence(d)
		if err != nil {
			return "", Command{}, err
		}
		c.JoinLocal = &JoinLocalCommand{Target: position, Source: source, Authority: authority, Resolutions: resolutions, Events: events}
	case 4:
		root, err := readDigestString(d, true)
		if err != nil {
			return "", Command{}, err
		}
		c.PublishBranch = &PublishBranchCommand{Source: position, Authority: authority, ExpectedDomainRoot: model.ObjectID(root)}
	}
	if err := d.Done(); err != nil {
		return "", Command{}, err
	}
	canonical, err := EncodeCommand(model.SpaceID(space), c)
	if err != nil || !bytes.Equal(canonical, raw) {
		return "", Command{}, ErrCommandShape
	}
	return model.SpaceID(space), c, nil
}
