package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const testSpace model.SpaceID = "radix-test"

type memoryReader struct {
	objects map[model.ObjectID][]byte
	reads   int
}

func newMemoryReader() *memoryReader { return &memoryReader{objects: make(map[model.ObjectID][]byte)} }

func (m *memoryReader) ReadObject(ctx context.Context, id model.ObjectID) ([]byte, error) {
	m.reads++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if raw, ok := m.objects[id]; ok {
		return raw, nil
	}
	return nil, fmt.Errorf("missing object %s", id)
}

func valueID(s string) model.ObjectID {
	digest := sha256.Sum256([]byte(s))
	return model.ObjectID(hex.EncodeToString(digest[:]))
}

func storeObject(t *testing.T, m *memoryReader, obj codec.Object) model.ObjectID {
	t.Helper()
	id, err := codec.ObjectID(obj)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := codec.EncodeObject(obj)
	if err != nil {
		t.Fatal(err)
	}
	m.objects[id] = raw
	return id
}

// install also checks the delta's reachability, uniqueness and child ordering.
func install(t *testing.T, m *memoryReader, delta TreeDelta) {
	t.Helper()
	all := make(map[model.ObjectID]codec.Object)
	positions := make(map[model.ObjectID]int)
	for i, obj := range delta.Objects {
		id, err := codec.ObjectID(obj)
		if err != nil {
			t.Fatal(err)
		}
		if _, exists := all[id]; exists {
			t.Fatal("duplicate delta object")
		}
		all[id], positions[id] = obj, i
	}
	reachable := make(map[model.ObjectID]bool)
	var visit func(model.ObjectID)
	visit = func(id model.ObjectID) {
		obj, ok := all[id]
		if !ok || reachable[id] {
			return
		}
		reachable[id] = true
		n, err := decodeNode(obj, testSpace)
		if err != nil {
			t.Fatal(err)
		}
		for _, child := range n.children {
			if pos, isNew := positions[child.ref]; isNew && pos >= positions[id] {
				t.Fatal("delta parent precedes child")
			}
			visit(child.ref)
		}
	}
	visit(delta.Root)
	if len(reachable) != len(all) {
		t.Fatalf("unreachable intermediate nodes in delta: %d of %d reachable", len(reachable), len(all))
	}
	for _, obj := range delta.Objects {
		storeObject(t, m, obj)
	}
	for _, obj := range delta.Objects {
		if err := ValidateNodeObject(context.Background(), obj, m); err != nil {
			t.Fatal(err)
		}
	}
}

func apply(t *testing.T, m *memoryReader, root model.ObjectID, mutations ...Mutation) TreeDelta {
	t.Helper()
	delta, err := Apply(context.Background(), m, testSpace, root, mutations)
	if err != nil {
		t.Fatal(err)
	}
	install(t, m, delta)
	return delta
}

func requireGet(t *testing.T, m ObjectReader, root model.ObjectID, key []byte, want model.ObjectID, found bool) {
	t.Helper()
	got, ok, err := Get(context.Background(), m, testSpace, root, key)
	if err != nil || got != want || ok != found {
		t.Fatalf("Get(%x): (%s,%v,%v), want (%s,%v)", key, got, ok, err, want, found)
	}
}

func TestRadixSplitsMergesAndOldRoots(t *testing.T) {
	m := newMemoryReader()
	keys := [][]byte{nil, {0}, {0, 1}, []byte("a"), []byte("ab"), []byte("ac"), []byte("xb"), {255, 0, 255}}
	root := EmptyRoot
	roots := []model.ObjectID{root}
	for i, key := range keys {
		root = apply(t, m, root, Mutation{Key: key, Value: valueID(fmt.Sprint(i))}).Root
		roots = append(roots, root)
	}
	for i, old := range roots {
		for j, key := range keys {
			if j < i {
				requireGet(t, m, old, key, valueID(fmt.Sprint(j)), true)
			} else {
				requireGet(t, m, old, key, "", false)
			}
		}
	}
	noChange := apply(t, m, root, Mutation{Key: []byte("missing"), Delete: true}, Mutation{Key: keys[3], Value: valueID("3")})
	if noChange.Root != root || len(noChange.Objects) != 0 {
		t.Fatal("no-op must preserve root and produce no objects")
	}
	for i := len(keys) - 1; i >= 0; i-- {
		root = apply(t, m, root, Mutation{Key: keys[i], Delete: true}).Root
		if root != roots[i] {
			t.Fatalf("delete did not restore canonical old root at %d", i)
		}
	}
	if root != EmptyRoot {
		t.Fatal("empty tree must use EmptyRoot")
	}
}

func TestRadixIndependentMapOracle(t *testing.T) {
	m := newMemoryReader()
	rng := rand.New(rand.NewSource(73149))
	keys := [][]byte{nil, {0}, {255}, []byte("same-suffix"), []byte("different-suffix")}
	for i := 0; i < 90; i++ {
		key := make([]byte, rng.Intn(20)+1)
		_, _ = rng.Read(key)
		if i%3 == 0 {
			key = append([]byte("shared-prefix/"), key...)
		}
		keys = append(keys, key)
	}
	oracle := make(map[string]model.ObjectID)
	root := EmptyRoot
	for batch := 0; batch < 30; batch++ {
		var mutations []Mutation
		for i := 0; i < 20; i++ {
			key := keys[rng.Intn(len(keys))]
			mutation := Mutation{Key: key, Value: valueID(fmt.Sprintf("%d/%d", batch, i)), Delete: rng.Intn(3) == 0}
			mutations = append(mutations, mutation)
			if mutation.Delete {
				delete(oracle, string(key))
			} else {
				oracle[string(key)] = mutation.Value
			}
		}
		root = apply(t, m, root, mutations...).Root
		for _, key := range keys {
			want, found := oracle[string(key)]
			requireGet(t, m, root, key, want, found)
		}
	}
}

func TestRadixInsertionOrderAndGolden(t *testing.T) {
	mutations := []Mutation{
		{Key: []byte("abc"), Value: valueID("abc-value")},
		{Key: []byte("abd"), Value: valueID("abd-value")},
		{Key: nil, Value: valueID("empty-value")},
		{Key: []byte{0, 255}, Value: valueID("binary-value")},
		{Key: []byte("a"), Value: valueID("prefix-value")},
	}
	m := newMemoryReader()
	want := apply(t, m, EmptyRoot, mutations...).Root
	const goldenRoot = "dbb37bff77b4fd907ff4bafcf09932317ecede43dfea6e01e90161013ceb28da"
	if want != model.ObjectID(goldenRoot) {
		t.Fatalf("golden root = %q", want)
	}
	rng := rand.New(rand.NewSource(4096))
	for i := 0; i < 40; i++ {
		rng.Shuffle(len(mutations), func(a, b int) { mutations[a], mutations[b] = mutations[b], mutations[a] })
		got := apply(t, newMemoryReader(), EmptyRoot, mutations...).Root
		if got != want {
			t.Fatalf("insertion order changed root: %s != %s", got, want)
		}
	}
}

func TestRadixCopyOnWriteAndOwnership(t *testing.T) {
	m := newMemoryReader()
	left := bytes.Repeat([]byte{'a'}, 100)
	right := bytes.Repeat([]byte{'z'}, 100)
	first := apply(t, m, EmptyRoot, Mutation{Key: left, Value: valueID("left")}, Mutation{Key: right, Value: valueID("right")})
	left[0] = 'b'
	requireGet(t, m, first.Root, bytes.Repeat([]byte{'a'}, 100), valueID("left"), true)
	m.reads = 0
	second := apply(t, m, first.Root, Mutation{Key: right, Value: valueID("right-2")})
	if len(second.Objects) != 2 {
		t.Fatalf("one-leaf update should copy leaf and root, got %d objects", len(second.Objects))
	}
	// The earlier root and its reader-owned encoded buffers remain unchanged.
	requireGet(t, m, first.Root, right, valueID("right"), true)
	requireGet(t, m, second.Root, right, valueID("right-2"), true)
	for _, obj := range second.Objects {
		for i := range obj.Payload {
			obj.Payload[i] ^= 255
		}
		for i := range obj.Refs {
			obj.Refs[i] = valueID("mutated output")
		}
	}
	requireGet(t, m, second.Root, right, valueID("right-2"), true)
}

func TestRadixLastWriteWinsAndPrunesIntermediateObjects(t *testing.T) {
	m := newMemoryReader()
	mutations := []Mutation{
		{Key: []byte("a"), Value: valueID("a1")},
		{Key: []byte("ab"), Value: valueID("ab")},
		{Key: []byte("a"), Value: valueID("a2")},
		{Key: []byte("ab"), Delete: true},
	}
	delta := apply(t, m, EmptyRoot, mutations...)
	if len(delta.Objects) != 1 {
		t.Fatalf("final single key should retain one object, got %d", len(delta.Objects))
	}
	requireGet(t, m, delta.Root, []byte("a"), valueID("a2"), true)
	requireGet(t, m, delta.Root, []byte("ab"), "", false)
}

func TestRadixRejectsUntrustedObjects(t *testing.T) {
	leaf := encodeNode(testSpace, node{prefix: []byte("x"), value: valueID("v")})
	for _, tc := range []struct {
		name string
		edit func(*codec.Object)
	}{
		{"wrong type", func(o *codec.Object) { o.Type = "other" }},
		{"wrong space", func(o *codec.Object) { o.Space = "other" }},
		{"wrong version", func(o *codec.Object) { o.Version = 2 }},
		{"missing refs", func(o *codec.Object) { o.Refs = nil }},
		{"extra refs", func(o *codec.Object) {
			o.Refs = []model.ObjectID{valueID("extra")}
			o.Payload = encodeNode(testSpace, node{prefix: []byte("x"), value: valueID("extra")}).Payload
			o.Refs = append(o.Refs, valueID("v"))
			if o.Refs[0] > o.Refs[1] {
				o.Refs[0], o.Refs[1] = o.Refs[1], o.Refs[0]
			}
		}},
		{"empty", func(o *codec.Object) { *o = encodeNode(testSpace, node{}) }},
		{"one child without value", func(o *codec.Object) {
			*o = encodeNode(testSpace, node{prefix: []byte("x"), children: []edge{{'a', valueID("child")}}})
		}},
		{"presence", func(o *codec.Object) { o.Payload[2] = 2 }},
		{"trailing", func(o *codec.Object) { o.Payload = append(o.Payload, 0) }},
		{"long prefix", func(o *codec.Object) {
			*o = encodeNode(testSpace, node{prefix: make([]byte, MaxKeyBytes+1), value: valueID("v")})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obj := leaf
			obj.Payload = bytes.Clone(obj.Payload)
			obj.Refs = append([]model.ObjectID(nil), obj.Refs...)
			tc.edit(&obj)
			m := newMemoryReader()
			id := storeObject(t, m, obj)
			if _, _, err := Get(context.Background(), m, testSpace, id, []byte("x")); err == nil {
				t.Fatal("malformed node accepted")
			}
		})
	}
	m := newMemoryReader()
	id := storeObject(t, m, leaf)
	otherID := valueID("untrusted claimed hash")
	m.objects[otherID] = m.objects[id]
	if _, _, err := Get(context.Background(), m, testSpace, otherID, []byte("x")); !errors.Is(err, codec.ErrObjectDigest) {
		t.Fatalf("untrusted object digest: %v", err)
	}
}

func TestRadixChildRelationshipsAndRefSet(t *testing.T) {
	for _, prefix := range [][]byte{nil, []byte("wrong"), bytes.Repeat([]byte{'x'}, MaxKeyBytes)} {
		t.Run(fmt.Sprintf("prefix-%d", len(prefix)), func(t *testing.T) {
			m := newMemoryReader()
			child := storeObject(t, m, encodeNode(testSpace, node{prefix: prefix, value: valueID("child-value")}))
			parent := encodeNode(testSpace, node{prefix: []byte("p"), value: valueID("parent-value"), children: []edge{{'x', child}}})
			root := storeObject(t, m, parent)
			if err := ValidateNodeObject(context.Background(), parent, m); !errors.Is(err, ErrInvalidNode) {
				t.Fatalf("child relationship: %v", err)
			}
			if _, _, err := Get(context.Background(), m, testSpace, root, []byte("px")); !errors.Is(err, ErrInvalidNode) {
				t.Fatalf("lookup child relationship: %v", err)
			}
		})
	}
	// Refs are the sorted set, even if value and child identities coincide.
	id := valueID("same-value-and-child")
	obj := encodeNode(testSpace, node{value: id, children: []edge{{'x', id}}})
	if len(obj.Refs) != 1 {
		t.Fatal("refs not deduplicated")
	}
	if _, err := decodeNode(obj, testSpace); err != nil {
		t.Fatal(err)
	}
}

func TestRadixLimitsAndCancellation(t *testing.T) {
	ctx := context.Background()
	m := newMemoryReader()
	maxKey := bytes.Repeat([]byte{255}, MaxKeyBytes)
	delta := apply(t, m, EmptyRoot, Mutation{Key: maxKey, Value: valueID("max")})
	requireGet(t, m, delta.Root, maxKey, valueID("max"), true)
	if _, err := Apply(ctx, m, testSpace, delta.Root, []Mutation{{Key: make([]byte, MaxKeyBytes+1), Delete: true}}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("long mutation key: %v", err)
	}
	if _, _, err := Get(ctx, m, testSpace, delta.Root, make([]byte, MaxKeyBytes+1)); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("long lookup key: %v", err)
	}
	if _, err := Apply(ctx, m, testSpace, EmptyRoot, make([]Mutation, MaxMutations+1)); !errors.Is(err, ErrLimit) {
		t.Fatalf("batch count: %v", err)
	}
	for _, badID := range []model.ObjectID{"", "abc", model.ObjectID(strings.Repeat("0", 64)), model.ObjectID(strings.Repeat("A", 64))} {
		if _, err := Apply(ctx, m, testSpace, EmptyRoot, []Mutation{{Value: badID}}); err == nil {
			t.Fatalf("invalid value %q accepted", badID)
		}
	}
	for _, badRoot := range []model.ObjectID{"abc", model.ObjectID(strings.Repeat("0", 64))} {
		if _, _, err := Get(ctx, m, testSpace, badRoot, nil); err == nil {
			t.Fatalf("invalid root %q accepted", badRoot)
		}
	}
	if _, err := Apply(ctx, nil, "", EmptyRoot, nil); err == nil {
		t.Fatal("empty space accepted")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := Apply(cancelled, m, testSpace, delta.Root, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled apply: %v", err)
	}
	if _, _, err := Get(cancelled, m, testSpace, delta.Root, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled get: %v", err)
	}
	if err := ValidateNodeObject(cancelled, delta.Objects[0], m); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled validate: %v", err)
	}
	// Exercise each hard budget at its exact boundary without allocating an
	// enormous fixture. The mutation/map-oracle tests exercise normal paths.
	for _, budget := range []string{"reads", "nodes", "bytes"} {
		t.Run(budget, func(t *testing.T) {
			tree, err := newTree(ctx, m, testSpace, EmptyRoot)
			if err != nil {
				t.Fatal(err)
			}
			var got error
			switch budget {
			case "reads":
				tree.reads = MaxReadNodes
				_, got = tree.load(delta.Root, 0, nil)
			case "nodes":
				tree.created = MaxNewNodes
				_, got = tree.put(node{value: valueID("new")})
			case "bytes":
				tree.newBytes = MaxDeltaBytes
				_, got = tree.put(node{value: valueID("new")})
			}
			if !errors.Is(got, ErrLimit) {
				t.Fatalf("%s: %v", budget, got)
			}
		})
	}
}
