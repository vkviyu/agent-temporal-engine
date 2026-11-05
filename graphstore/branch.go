package graphstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

const branchCommandSchema uint64 = 1
const branchCommandMagic = "ATE-GRAPH-BRANCH\x00"
const branchRequestPrefix = "branch/v1/"

type branchOperation uint64

const (
	branchFork branchOperation = iota + 1
	branchAdvance
)

type branchCommand struct {
	operation branchOperation
	id        model.BranchID
	source    model.RevisionID
	expected  uint64
	head      model.RevisionID
}

func validBranchText(s string) bool {
	return s != "" && len(s) <= model.MaxIdentityBytes && utf8.ValidString(s)
}

func encodeBranchCommand(c branchCommand) ([]byte, error) {
	if !validBranchText(string(c.id)) || !model.ValidDigest(string(c.head)) && c.operation == branchAdvance {
		return nil, graph.ErrInvalidRevision
	}
	if c.operation == branchFork {
		if !model.ValidDigest(string(c.source)) {
			return nil, graph.ErrInvalidRevision
		}
	} else if c.operation != branchAdvance {
		return nil, storage.ErrUnsupported
	}
	b := []byte(branchCommandMagic)
	b = codec.AppendUint(b, branchCommandSchema)
	b = codec.AppendUint(b, uint64(c.operation))
	var err error
	b, err = codec.AppendString(b, string(c.id))
	if err != nil {
		return nil, err
	}
	switch c.operation {
	case branchFork:
		b, err = codec.AppendString(b, string(c.source))
	case branchAdvance:
		b = codec.AppendUint(b, c.expected)
		b, err = codec.AppendString(b, string(c.head))
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

func decodeBranchCommand(raw []byte) (branchCommand, error) {
	if len(raw) > graph.MaxBranchBytes+128 || !bytes.HasPrefix(raw, []byte(branchCommandMagic)) {
		return branchCommand{}, ErrInvalidGraphTransaction
	}
	d := codec.NewDecoder(raw[len(branchCommandMagic):])
	schema, err := d.Uint()
	if err != nil || schema != branchCommandSchema {
		return branchCommand{}, ErrUnsupportedAdapterSchema
	}
	op, err := d.Uint()
	if err != nil {
		return branchCommand{}, err
	}
	id, err := d.String(model.MaxIdentityBytes)
	if err != nil {
		return branchCommand{}, err
	}
	c := branchCommand{operation: branchOperation(op), id: model.BranchID(id)}
	switch c.operation {
	case branchFork:
		v, e := d.String(64)
		if e != nil {
			return branchCommand{}, e
		}
		c.source = model.RevisionID(v)
	case branchAdvance:
		c.expected, err = d.Uint()
		if err != nil {
			return branchCommand{}, err
		}
		v, e := d.String(64)
		if e != nil {
			return branchCommand{}, e
		}
		c.head = model.RevisionID(v)
	default:
		return branchCommand{}, storage.ErrUnsupported
	}
	if err = d.Done(); err != nil {
		return branchCommand{}, err
	}
	canonical, err := encodeBranchCommand(c)
	if err != nil || !bytes.Equal(canonical, raw) {
		return branchCommand{}, ErrInvalidGraphTransaction
	}
	return c, nil
}

func branchRequestID(raw []byte) model.RequestID {
	sum := sha256.Sum256(append([]byte("ATE-GRAPH-BRANCH-REQUEST\x00"), raw...))
	return model.RequestID(branchRequestPrefix + hex.EncodeToString(sum[:]))
}

func branchDigest(raw []byte) storage.Digest { return storage.Digest(sha256.Sum256(raw)) }

func validateBranchCommand(c branchCommand, candidate *graph.Store) (model.SpaceID, error) {
	switch c.operation {
	case branchFork:
		if _, err := candidate.PreviewFork(c.id, c.source); err != nil {
			return "", err
		}
		r, err := candidate.Get(c.source)
		if err != nil {
			return "", err
		}
		return r.Space, nil
	case branchAdvance:
		if _, err := candidate.PreviewAdvanceBranch(c.id, c.expected, c.head); err != nil {
			return "", err
		}
		b, err := candidate.Branch(c.id)
		if err != nil {
			return "", err
		}
		if b.Version != c.expected {
			return "", graph.ErrBranchCAS
		}
		old, err := candidate.Get(b.Head)
		if err != nil {
			return "", err
		}
		next, err := candidate.Get(c.head)
		if err != nil {
			return "", err
		}
		if old.Space != next.Space {
			return "", graph.ErrSpaceMismatch
		}
		return old.Space, nil
	default:
		return "", storage.ErrUnsupported
	}
}

func applyBranchCommand(candidate *graph.Store, c branchCommand) (graph.Branch, error) {
	switch c.operation {
	case branchFork:
		return candidate.Fork(c.id, c.source)
	case branchAdvance:
		return candidate.AdvanceBranch(c.id, c.expected, c.head)
	default:
		return graph.Branch{}, storage.ErrUnsupported
	}
}
