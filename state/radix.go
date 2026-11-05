// Package state implements an immutable, path-compressed byte radix tree.
// It depends only on model, codec and the standard library; readers and writers
// provide persistence outside this package.
package state

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const (
	// EmptyRoot represents an absent tree. It is not an object identity.
	EmptyRoot     model.ObjectID = ""
	NodeType                     = "ate.radix"
	NodeVersion   uint64         = 1
	MaxKeyBytes                  = 1024
	MaxMutations                 = 1024
	MaxReadNodes                 = 65536
	MaxNewNodes                  = 65536
	MaxDeltaBytes                = 16 << 20
)

var (
	ErrInvalidNode = errors.New("invalid radix node")
	ErrInvalidKey  = errors.New("invalid radix key")
	ErrLimit       = errors.New("radix resource limit exceeded")
)

// ObjectReader returns encoded immutable objects. Implementations may reuse
// their buffers on the next ReadObject call; this package retains no borrowed data.
type ObjectReader interface {
	ReadObject(context.Context, model.ObjectID) ([]byte, error)
}

type Mutation struct {
	Key    []byte
	Value  model.ObjectID
	Delete bool
}

// TreeDelta contains only newly constructed nodes reachable from Root, once
// each, in child-before-parent order. Apply never writes through its reader.
type TreeDelta struct {
	Root    model.ObjectID
	Objects []codec.Object
}

type edge struct {
	label byte
	ref   model.ObjectID
}

type node struct {
	prefix   []byte
	value    model.ObjectID
	children []edge
}

type stagedNode struct {
	node   node
	object codec.Object
}

type tree struct {
	ctx      context.Context
	reader   ObjectReader
	space    model.SpaceID
	loaded   map[model.ObjectID]node
	staged   map[model.ObjectID]stagedNode
	reads    int
	created  int
	newBytes int
}

// Apply uses copy-on-write path splitting and merging. Mutations are applied
// in input order, so repeated keys use last-write-wins semantics. Deleting a
// missing key is a no-op. Returned slices do not alias input or reader buffers.
func Apply(ctx context.Context, reader ObjectReader, space model.SpaceID, root model.ObjectID, mutations []Mutation) (TreeDelta, error) {
	t, err := newTree(ctx, reader, space, root)
	if err != nil {
		return TreeDelta{}, err
	}
	if len(mutations) > MaxMutations {
		return TreeDelta{}, fmt.Errorf("%w: mutations", ErrLimit)
	}
	// Validate the whole batch before reading any object or constructing nodes.
	for _, m := range mutations {
		if len(m.Key) > MaxKeyBytes {
			return TreeDelta{}, ErrInvalidKey
		}
		if !m.Delete && !validID(m.Value) {
			return TreeDelta{}, fmt.Errorf("%w: value identity", ErrInvalidNode)
		}
	}
	if root != EmptyRoot {
		if _, err := t.load(root, 0, nil); err != nil {
			return TreeDelta{}, err
		}
	}
	for _, m := range mutations {
		root, err = t.mutate(root, m.Key, m.Value, m.Delete, 0, nil)
		if err != nil {
			return TreeDelta{}, err
		}
	}
	delta := TreeDelta{Root: root}
	seen := make(map[model.ObjectID]bool)
	if err := t.collect(root, seen, &delta.Objects); err != nil {
		return TreeDelta{}, err
	}
	return delta, nil
}

// Get follows only the lookup path. Every visited node is hash-verified and
// schema-checked; child labels and total path length are checked when followed.
func Get(ctx context.Context, reader ObjectReader, space model.SpaceID, root model.ObjectID, key []byte) (model.ObjectID, bool, error) {
	t, err := newTree(ctx, reader, space, root)
	if err != nil {
		return "", false, err
	}
	if len(key) > MaxKeyBytes {
		return "", false, ErrInvalidKey
	}
	depth := 0
	var expected *byte
	for root != EmptyRoot {
		n, err := t.load(root, depth, expected)
		if err != nil {
			return "", false, err
		}
		if !bytes.HasPrefix(key, n.prefix) {
			return "", false, nil
		}
		key = key[len(n.prefix):]
		depth += len(n.prefix)
		if len(key) == 0 {
			return n.value, n.value != "", nil
		}
		i, ok := childIndex(n.children, key[0])
		if !ok {
			return "", false, nil
		}
		label := n.children[i].label
		expected = &label
		root = n.children[i].ref
	}
	return "", false, nil
}

// ValidateNodeObject checks an envelope and its payload, then verifies every
// direct child and its incoming label. Value object types and spaces belong to
// the caller's registry. Descendants are validated as they are visited, not by
// an unbounded recursive scan of the entire tree.
func ValidateNodeObject(ctx context.Context, obj codec.Object, reader ObjectReader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := codec.EncodeObject(obj); err != nil {
		return err
	}
	n, err := decodeNode(obj, obj.Space)
	if err != nil {
		return err
	}
	t, err := newTree(ctx, reader, obj.Space, EmptyRoot)
	if err != nil {
		return err
	}
	for _, child := range n.children {
		if _, err := t.load(child.ref, len(n.prefix), &child.label); err != nil {
			return err
		}
	}
	return nil
}

func newTree(ctx context.Context, reader ObjectReader, space model.SpaceID, root model.ObjectID) (*tree, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if space == "" || len(space) > model.MaxIdentityBytes || !utf8.ValidString(string(space)) {
		return nil, fmt.Errorf("%w: space", ErrInvalidNode)
	}
	if root != EmptyRoot && !validID(root) {
		return nil, fmt.Errorf("%w: root identity", ErrInvalidNode)
	}
	return &tree{ctx: ctx, reader: reader, space: space, loaded: make(map[model.ObjectID]node), staged: make(map[model.ObjectID]stagedNode)}, nil
}

func (t *tree) load(id model.ObjectID, depth int, expected *byte) (node, error) {
	if err := t.ctx.Err(); err != nil {
		return node{}, err
	}
	n, ok := t.loaded[id]
	if !ok {
		if staged, exists := t.staged[id]; exists {
			n, ok = staged.node, true
		}
	}
	if !ok {
		if t.reader == nil {
			return node{}, fmt.Errorf("%w: reader required", ErrInvalidNode)
		}
		if t.reads >= MaxReadNodes {
			return node{}, fmt.Errorf("%w: reads", ErrLimit)
		}
		t.reads++
		raw, err := t.reader.ReadObject(t.ctx, id)
		if err != nil {
			return node{}, err
		}
		if err := t.ctx.Err(); err != nil {
			return node{}, err
		}
		obj, err := codec.VerifyObject(id, raw)
		if err != nil {
			return node{}, err
		}
		n, err = decodeNode(obj, t.space)
		if err != nil {
			return node{}, err
		}
		t.loaded[id] = n
	}
	if depth+len(n.prefix) > MaxKeyBytes {
		return node{}, fmt.Errorf("%w: path length", ErrInvalidNode)
	}
	if expected != nil && (len(n.prefix) == 0 || n.prefix[0] != *expected) {
		return node{}, fmt.Errorf("%w: child label", ErrInvalidNode)
	}
	return n, nil
}

func (t *tree) mutate(id model.ObjectID, key []byte, value model.ObjectID, remove bool, depth int, expected *byte) (model.ObjectID, error) {
	if err := t.ctx.Err(); err != nil {
		return "", err
	}
	if id == EmptyRoot {
		if remove {
			return EmptyRoot, nil
		}
		return t.put(node{prefix: bytes.Clone(key), value: value})
	}
	n, err := t.load(id, depth, expected)
	if err != nil {
		return "", err
	}
	shared := 0
	for shared < len(key) && shared < len(n.prefix) && key[shared] == n.prefix[shared] {
		shared++
	}
	if shared != len(n.prefix) {
		if remove {
			return id, nil
		}
		old := cloneNode(n)
		old.prefix = bytes.Clone(n.prefix[shared:])
		oldID, err := t.put(old)
		if err != nil {
			return "", err
		}
		branch := node{prefix: bytes.Clone(key[:shared]), children: []edge{{old.prefix[0], oldID}}}
		if shared == len(key) {
			branch.value = value
		} else {
			leaf := node{prefix: bytes.Clone(key[shared:]), value: value}
			leafID, err := t.put(leaf)
			if err != nil {
				return "", err
			}
			branch.children = append(branch.children, edge{leaf.prefix[0], leafID})
			sort.Slice(branch.children, func(i, j int) bool { return branch.children[i].label < branch.children[j].label })
		}
		return t.put(branch)
	}
	rest := key[shared:]
	if len(rest) == 0 {
		if (!remove && n.value == value) || (remove && n.value == "") {
			return id, nil
		}
		n = cloneNode(n)
		if remove {
			n.value = ""
		} else {
			n.value = value
		}
		return t.normalize(n, depth)
	}
	i, exists := childIndex(n.children, rest[0])
	if !exists && remove {
		return id, nil
	}
	childID := EmptyRoot
	if exists {
		childID = n.children[i].ref
	}
	label := rest[0]
	newID, err := t.mutate(childID, rest, value, remove, depth+len(n.prefix), &label)
	if err != nil {
		return "", err
	}
	if childID == newID {
		return id, nil
	}
	n = cloneNode(n)
	if exists {
		if newID == EmptyRoot {
			n.children = append(n.children[:i], n.children[i+1:]...)
		} else {
			n.children[i].ref = newID
		}
	} else {
		n.children = append(n.children, edge{})
		copy(n.children[i+1:], n.children[i:])
		n.children[i] = edge{label, newID}
	}
	return t.normalize(n, depth)
}

func (t *tree) normalize(n node, depth int) (model.ObjectID, error) {
	if n.value == "" {
		if len(n.children) == 0 {
			return EmptyRoot, nil
		}
		if len(n.children) == 1 {
			child := n.children[0]
			loaded, err := t.load(child.ref, depth+len(n.prefix), &child.label)
			if err != nil {
				return "", err
			}
			merged := cloneNode(loaded)
			merged.prefix = append(bytes.Clone(n.prefix), loaded.prefix...)
			return t.put(merged)
		}
	}
	return t.put(n)
}

func (t *tree) put(n node) (model.ObjectID, error) {
	if err := t.ctx.Err(); err != nil {
		return "", err
	}
	obj := encodeNode(t.space, n)
	id, err := codec.ObjectID(obj)
	if err != nil {
		return "", err
	}
	if _, exists := t.loaded[id]; exists {
		return id, nil
	}
	if _, exists := t.staged[id]; exists {
		return id, nil
	}
	if t.created >= MaxNewNodes {
		return "", fmt.Errorf("%w: new nodes", ErrLimit)
	}
	encoded, err := codec.EncodeObject(obj)
	if err != nil {
		return "", err
	}
	if len(encoded) > MaxDeltaBytes-t.newBytes {
		return "", fmt.Errorf("%w: constructed object bytes", ErrLimit)
	}
	t.created++
	t.newBytes += len(encoded)
	t.staged[id] = stagedNode{cloneNode(n), obj}
	return id, nil
}

func (t *tree) collect(id model.ObjectID, seen map[model.ObjectID]bool, out *[]codec.Object) error {
	if err := t.ctx.Err(); err != nil {
		return err
	}
	if id == EmptyRoot || seen[id] {
		return nil
	}
	seen[id] = true
	s, ok := t.staged[id]
	if !ok {
		return nil
	}
	for _, child := range s.node.children {
		if err := t.collect(child.ref, seen, out); err != nil {
			return err
		}
	}
	obj := s.object
	obj.Refs = append([]model.ObjectID(nil), obj.Refs...)
	obj.Payload = bytes.Clone(obj.Payload)
	*out = append(*out, obj)
	return nil
}

func childIndex(children []edge, label byte) (int, bool) {
	i := sort.Search(len(children), func(i int) bool { return children[i].label >= label })
	return i, i < len(children) && children[i].label == label
}

func cloneNode(n node) node {
	n.prefix = bytes.Clone(n.prefix)
	n.children = append([]edge(nil), n.children...)
	return n
}

func validID(id model.ObjectID) bool {
	return model.ValidDigest(string(id)) && id != "0000000000000000000000000000000000000000000000000000000000000000"
}

func nodeRefs(n node) []model.ObjectID {
	refs := make([]model.ObjectID, 0, len(n.children)+1)
	if n.value != "" {
		refs = append(refs, n.value)
	}
	for _, child := range n.children {
		refs = append(refs, child.ref)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i] < refs[j] })
	unique := refs[:0]
	for _, ref := range refs {
		if len(unique) == 0 || unique[len(unique)-1] != ref {
			unique = append(unique, ref)
		}
	}
	return unique
}

func encodeNode(space model.SpaceID, n node) codec.Object {
	payload, _ := codec.AppendBytes(nil, n.prefix)
	if n.value == "" {
		payload = codec.AppendUint(payload, 0)
	} else {
		payload = codec.AppendUint(payload, 1)
		digest, _ := hex.DecodeString(string(n.value))
		payload, _ = codec.AppendBytes(payload, digest)
	}
	payload = codec.AppendUint(payload, uint64(len(n.children)))
	for _, child := range n.children {
		payload = codec.AppendUint(payload, uint64(child.label))
		digest, _ := hex.DecodeString(string(child.ref))
		payload, _ = codec.AppendBytes(payload, digest)
	}
	return codec.Object{Type: NodeType, Space: space, Version: NodeVersion, Refs: nodeRefs(n), Payload: payload}
}

func decodeNode(obj codec.Object, space model.SpaceID) (node, error) {
	if obj.Type != NodeType || obj.Space != space || obj.Version != NodeVersion {
		return node{}, fmt.Errorf("%w: type, space or version", ErrInvalidNode)
	}
	d := codec.NewDecoder(obj.Payload)
	prefix, err := d.Bytes(MaxKeyBytes)
	if err != nil {
		return node{}, fmt.Errorf("%w: prefix: %w", ErrInvalidNode, err)
	}
	n := node{prefix: bytes.Clone(prefix)}
	present, err := d.Uint()
	if err != nil || present > 1 {
		return node{}, fmt.Errorf("%w: value presence", ErrInvalidNode)
	}
	if present == 1 {
		n.value, err = readID(d)
		if err != nil {
			return node{}, err
		}
	}
	count, err := d.Uint()
	if err != nil || count > 256 {
		return node{}, fmt.Errorf("%w: child count", ErrInvalidNode)
	}
	if count != 0 {
		n.children = make([]edge, 0, int(count))
	}
	for i := uint64(0); i < count; i++ {
		label, err := d.Uint()
		if err != nil || label > 255 || (len(n.children) != 0 && byte(label) <= n.children[len(n.children)-1].label) {
			return node{}, fmt.Errorf("%w: child label order", ErrInvalidNode)
		}
		ref, err := readID(d)
		if err != nil {
			return node{}, err
		}
		n.children = append(n.children, edge{byte(label), ref})
	}
	if err := d.Done(); err != nil {
		return node{}, fmt.Errorf("%w: %w", ErrInvalidNode, err)
	}
	if n.value == "" && len(n.children) < 2 {
		return node{}, fmt.Errorf("%w: empty or uncompressed node", ErrInvalidNode)
	}
	refs := nodeRefs(n)
	if len(refs) != len(obj.Refs) {
		return node{}, fmt.Errorf("%w: reference set", ErrInvalidNode)
	}
	for i, ref := range refs {
		if obj.Refs[i] != ref {
			return node{}, fmt.Errorf("%w: reference set", ErrInvalidNode)
		}
	}
	return n, nil
}

func readID(d *codec.Decoder) (model.ObjectID, error) {
	raw, err := d.Bytes(32)
	if err != nil || len(raw) != 32 {
		return "", fmt.Errorf("%w: digest length", ErrInvalidNode)
	}
	id := model.ObjectID(hex.EncodeToString(raw))
	if !validID(id) {
		return "", fmt.Errorf("%w: zero digest", ErrInvalidNode)
	}
	return id, nil
}
