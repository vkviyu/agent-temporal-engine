package state

import (
	"bytes"
	"context"
	"fmt"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// Entry is an owned absolute key and its immutable value identity.
type Entry struct {
	Key   []byte
	Value model.ObjectID
}

// ScanLimits bound traversal work, retained node bytes, and returned entries.
// Zero fields select defaults; exceeding a limit returns no partial snapshot.
type ScanLimits struct {
	MaxNodes     int
	MaxEntries   int
	MaxBytes     int
	MaxReadBytes int
}

func (l ScanLimits) checked() (ScanLimits, error) {
	if l.MaxNodes == 0 {
		l.MaxNodes = MaxReadNodes
	}
	if l.MaxEntries == 0 {
		l.MaxEntries = 65536
	}
	if l.MaxBytes == 0 {
		l.MaxBytes = 16 << 20
	}
	if l.MaxReadBytes == 0 {
		l.MaxReadBytes = 64 << 20
	}
	if l.MaxNodes < 1 || l.MaxNodes > MaxReadNodes || l.MaxEntries < 1 || l.MaxEntries > 65536 || l.MaxBytes < 1 || l.MaxBytes > 64<<20 || l.MaxReadBytes < 1 || l.MaxReadBytes > 256<<20 {
		return l, ErrLimit
	}
	return l, nil
}

type scanReader struct {
	source     ObjectReader
	bytes, max int
}

func (r *scanReader) ReadObject(ctx context.Context, id model.ObjectID) ([]byte, error) {
	if r.source == nil {
		return nil, fmt.Errorf("%w: reader required", ErrInvalidNode)
	}
	if r.bytes >= r.max {
		return nil, ErrLimit
	}
	b, err := r.source.ReadObject(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(b) > r.max-r.bytes {
		return nil, ErrLimit
	}
	r.bytes += len(b)
	return b, nil
}

type scanFrame struct {
	id      model.ObjectID
	node    node
	key     []byte
	child   int
	emitted bool
}

// Entries authenticates the whole fixed radix snapshot and returns sorted,
// owned absolute keys. It does not silently truncate. Identical suffix node IDs
// mounted at different prefixes are visited separately; only active-path IDs
// detect cycles. The stack grows with path depth, not the breadth of the tree.
func Entries(ctx context.Context, reader ObjectReader, space model.SpaceID, root model.ObjectID, limits ScanLimits) ([]Entry, error) {
	l, err := limits.checked()
	if err != nil {
		return nil, err
	}
	bounded := &scanReader{source: reader, max: l.MaxReadBytes}
	tree, err := newTree(ctx, bounded, space, root)
	if err != nil {
		return nil, err
	}
	if root == EmptyRoot {
		return []Entry{}, nil
	}
	n, err := tree.load(root, 0, nil)
	if err != nil {
		return nil, err
	}
	stack := []scanFrame{{id: root, node: n, key: bytes.Clone(n.prefix)}}
	active := map[model.ObjectID]bool{root: true}
	visited := 1
	used := 0
	out := []Entry{}
	for len(stack) > 0 {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		frame := &stack[len(stack)-1]
		if !frame.emitted {
			frame.emitted = true
			if frame.node.value != "" {
				cost := len(frame.key) + 64
				if len(out) >= l.MaxEntries || cost > l.MaxBytes-used {
					return nil, ErrLimit
				}
				used += cost
				out = append(out, Entry{Key: bytes.Clone(frame.key), Value: frame.node.value})
			}
		}
		if frame.child == len(frame.node.children) {
			delete(active, frame.id)
			stack = stack[:len(stack)-1]
			continue
		}
		edge := frame.node.children[frame.child]
		frame.child++
		if active[edge.ref] {
			return nil, fmt.Errorf("%w: radix cycle", ErrInvalidNode)
		}
		if visited >= l.MaxNodes {
			return nil, ErrLimit
		}
		visited++
		child, err := tree.load(edge.ref, len(frame.key), &edge.label)
		if err != nil {
			return nil, err
		}
		key := append(bytes.Clone(frame.key), child.prefix...)
		if len(key) > MaxKeyBytes {
			return nil, ErrInvalidNode
		}
		active[edge.ref] = true
		stack = append(stack, scanFrame{id: edge.ref, node: child, key: key})
	}
	return out, nil
}
