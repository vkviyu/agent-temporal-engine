package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

const agentRecordType = "ate.agent.record"
const agentRecordRootType = "ate.agent.record-root"
const agentRecordMutationBatch = 256
const agentRecordKeyBytes = 40 // SHA-256 content digest + uint64 duplicate ordinal.
const agentRecordScanBytes = 4 << 20
const agentRecordScanReadBytes = 32 << 20

type agentRecordCatalog struct {
	id      model.ObjectID
	space   model.SpaceID
	owner   model.PrincipalID
	root    model.ObjectID
	objects map[string]model.ObjectID
	image   agentprofile.RecordImage
}

type agentRecordRoot struct {
	owner        model.PrincipalID
	root         model.ObjectID
	count, bytes uint64
}

func makeAgentRecordRoot(space model.SpaceID, r agentRecordRoot) (codec.Object, error) {
	if validAgentScope(space, r.owner) != nil || r.count > agentprofile.MaxStateRecords ||
		r.bytes > agentprofile.MaxStateBytes || (r.count == 0) != (r.root == "") ||
		(r.count == 0) != (r.bytes == 0) {
		return codec.Object{}, ErrInvalidStateData
	}
	b, err := codec.AppendString(corePrefix(), string(r.owner))
	if err != nil {
		return codec.Object{}, err
	}
	b, err = appendOptionalID(b, r.root)
	if err != nil {
		return codec.Object{}, err
	}
	b = codec.AppendUint(b, r.count)
	b = codec.AppendUint(b, r.bytes)
	return coreObject(agentRecordRootType, space, canonicalRefs(r.root), b)
}

func readAgentRecordRoot(obj codec.Object) (agentRecordRoot, error) {
	if err := checkCoreObject(obj, agentRecordRootType, false); err != nil {
		return agentRecordRoot{}, err
	}
	d, err := coreDecoder(obj.Payload)
	if err != nil {
		return agentRecordRoot{}, err
	}
	owner, err := d.String(model.MaxIdentityBytes)
	if err != nil {
		return agentRecordRoot{}, err
	}
	r := agentRecordRoot{owner: model.PrincipalID(owner)}
	if r.root, err = readOptionalID(d); err != nil {
		return r, err
	}
	if r.count, err = d.Uint(); err != nil {
		return r, err
	}
	if r.bytes, err = d.Uint(); err != nil {
		return r, err
	}
	if err = d.Done(); err != nil {
		return r, err
	}
	canonical, err := makeAgentRecordRoot(obj.Space, r)
	if err != nil || !bytes.Equal(canonical.Payload, obj.Payload) || checkRefs(obj, canonical.Refs) != nil {
		return r, ErrInvalidStateData
	}
	return r, nil
}

func makeAgentRecord(space model.SpaceID, owner model.PrincipalID, raw []byte) (codec.Object, error) {
	if len(raw) > agentprofile.MaxStateRecordBytes {
		return codec.Object{}, storage.ErrLimit
	}
	b, err := codec.AppendString(corePrefix(), string(owner))
	if err != nil {
		return codec.Object{}, err
	}
	b, err = codec.AppendBytes(b, raw)
	if err != nil {
		return codec.Object{}, err
	}
	return coreObject(agentRecordType, space, nil, b)
}

func readAgentRecord(obj codec.Object) ([]byte, model.PrincipalID, error) {
	raw, owner, err := readAgentRecordEnvelope(obj)
	if err != nil {
		return nil, "", err
	}
	if err = agentprofile.ValidateStateRecord(raw); err != nil {
		if errors.Is(err, agentprofile.ErrStateLimit) {
			return nil, "", fmt.Errorf("%w: %w", ErrStoredObject, err)
		}
		return nil, "", err
	}
	return bytes.Clone(raw), owner, nil
}

// The returned bytes borrow obj.Payload and are not yet schema-authenticated.
// Full image loading authenticates them once through DecodeRecordImage; the
// standalone registry path above still validates individual record objects.
func readAgentRecordEnvelope(obj codec.Object) ([]byte, model.PrincipalID, error) {
	if err := checkCoreObject(obj, agentRecordType, false); err != nil {
		return nil, "", err
	}
	d, err := coreDecoder(obj.Payload)
	if err != nil {
		return nil, "", err
	}
	owner, err := d.String(model.MaxIdentityBytes)
	if err != nil {
		return nil, "", err
	}
	raw, err := d.Bytes(agentprofile.MaxStateRecordBytes)
	if err != nil {
		return nil, "", err
	}
	if err = d.Done(); err != nil {
		return nil, "", err
	}
	if err = validAgentScope(obj.Space, model.PrincipalID(owner)); err != nil {
		return nil, "", err
	}
	if err = checkRefs(obj, nil); err != nil {
		return nil, "", err
	}
	return raw, model.PrincipalID(owner), nil
}

// Keep source failures distinct from the fixed scanner's own limits. A reader
// can have an independent work budget, which says nothing about stored validity.
type agentRecordScanReader struct {
	state.ObjectReader
	err error
}

func (r *agentRecordScanReader) ReadObject(ctx context.Context, id model.ObjectID) ([]byte, error) {
	if r.ObjectReader == nil {
		r.err = fmt.Errorf("%w: reader required", state.ErrInvalidNode)
		return nil, r.err
	}
	raw, err := r.ObjectReader.ReadObject(ctx, id)
	r.err = err
	return raw, err
}

func agentRecordReadFailure(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrObjectIO) || errors.Is(err, ErrStoredObject) ||
		errors.Is(err, storage.ErrLimit) || errors.Is(err, state.ErrLimit) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrStoredObject, err)
}

func agentRecordEntries(ctx context.Context, reader state.ObjectReader, space model.SpaceID, root model.ObjectID) ([]state.Entry, error) {
	// RecordImage emits N <= 16384 fixed 40-byte keys. Apply compresses every
	// valueless unary node, so the final tree has V <= 2N-1 nodes and E = V-1
	// edges (also counting shared suffixes at each absolute path).
	// Entries costs at most N*(40+64) = 1,703,936 bytes.
	// With a 256-byte space, the canonical envelope/prefix/count fields cost
	// <360 bytes/node; each value costs 66 bytes and each edge <=68 bytes,
	// including their envelope refs. Thus reads <=360V+66N+68E = 15,105,552
	// bytes, below 32 MiB; V <=32767 is below MaxReadNodes. Record payloads
	// are read separately. These are fixed format-covering limits, NOT a
	// configurable work budget. Revisit the proof when keys/codecs change.
	source := &agentRecordScanReader{ObjectReader: reader}
	entries, err := state.Entries(ctx, source, space, root, state.ScanLimits{
		MaxEntries: agentprofile.MaxStateRecords, MaxNodes: state.MaxReadNodes,
		MaxBytes: agentRecordScanBytes, MaxReadBytes: agentRecordScanReadBytes,
	})
	if err != nil {
		if source.err != nil {
			return nil, agentRecordReadFailure(source.err)
		}
		if errors.Is(err, state.ErrLimit) {
			return nil, fmt.Errorf("%w: agent record radix: %w", ErrStoredObject, err)
		}
		return nil, agentRecordReadFailure(err)
	}
	for _, entry := range entries {
		if len(entry.Key) != agentRecordKeyBytes {
			return nil, agentRecordReadFailure(ErrInvalidStateData)
		}
	}
	return entries, nil
}

func agentRecordRootEntries(ctx context.Context, reader state.ObjectReader, space model.SpaceID, r agentRecordRoot) ([]state.Entry, error) {
	entries, err := agentRecordEntries(ctx, reader, space, r.root)
	if err != nil {
		return nil, err
	}
	if uint64(len(entries)) != r.count {
		return nil, agentRecordReadFailure(ErrInvalidStateData)
	}
	return entries, nil
}

func loadAgentRecordCatalog(ctx context.Context, reader state.ObjectReader, obj codec.Object) (agentRecordCatalog, model.PrincipalID, error) {
	fail := func(err error) (agentRecordCatalog, model.PrincipalID, error) {
		return agentRecordCatalog{}, "", agentRecordReadFailure(err)
	}
	r, err := readAgentRecordRoot(obj)
	if err != nil {
		return fail(err)
	}
	entries, err := agentRecordRootEntries(ctx, reader, obj.Space, r)
	if err != nil {
		return fail(err)
	}
	records := make([]agentprofile.StateRecord, 0, len(entries))
	objects := make(map[string]model.ObjectID, len(entries))
	var total uint64
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		child, err := readObject(ctx, reader, entry.Value)
		if err != nil {
			return fail(err)
		}
		raw, owner, err := readAgentRecordEnvelope(child)
		if err != nil {
			return fail(err)
		}
		if child.Space != obj.Space || owner != r.owner || uint64(len(raw)) > r.bytes-total {
			return fail(ErrInvalidStateData)
		}
		total += uint64(len(raw))
		records = append(records, agentprofile.StateRecord{Key: entry.Key, Raw: raw})
		objects[string(entry.Key)] = entry.Value
	}
	if total != r.bytes {
		return fail(ErrInvalidStateData)
	}
	image, err := agentprofile.DecodeRecordImage(records)
	if err != nil {
		return fail(err)
	}
	if err = image.CheckScope(obj.Space, r.owner); err != nil {
		return fail(err)
	}
	return agentRecordCatalog{space: obj.Space, owner: r.owner, root: r.root, objects: objects, image: image}, r.owner, nil
}

func loadAgentRecordImage(ctx context.Context, reader state.ObjectReader, obj codec.Object) (agentprofile.RecordImage, model.PrincipalID, error) {
	catalog, owner, err := loadAgentRecordCatalog(ctx, reader, obj)
	return catalog.image, owner, err
}

// storeAgentRecords reuses every unchanged leaf and applies only changed keys
// to the previous persistent radix. Builder dedup also spans Work substeps.
func storeAgentRecords(
	ctx context.Context,
	b *objectBuilder,
	space model.SpaceID,
	owner model.PrincipalID,
	before model.ObjectID,
	image agentprofile.RecordImage,
) (model.ObjectID, agentRecordCatalog, error) {
	if err := image.CheckScope(space, owner); err != nil {
		return "", agentRecordCatalog{}, err
	}
	root := model.ObjectID("")
	old := map[string]model.ObjectID{}
	final := map[string]model.ObjectID{}
	if before != "" {
		if catalog, ok := workAgentRecordCatalogAt(ctx, before, space, owner); ok {
			root = catalog.root
			for key, id := range catalog.objects {
				old[key] = id
				final[key] = id
			}
		} else {
			obj, err := readObject(ctx, b, before)
			if err != nil {
				return "", agentRecordCatalog{}, agentRecordReadFailure(err)
			}
			if obj.Type != agentRecordRootType {
				return "", agentRecordCatalog{}, agentRecordReadFailure(ErrUnsupportedSchema)
			}
			r, err := readAgentRecordRoot(obj)
			if err != nil {
				return "", agentRecordCatalog{}, agentRecordReadFailure(err)
			}
			if obj.Space != space || r.owner != owner {
				return "", agentRecordCatalog{}, agentRecordReadFailure(ErrInvalidStateData)
			}
			root = r.root
			entries, err := agentRecordRootEntries(ctx, b, space, r)
			if err != nil {
				return "", agentRecordCatalog{}, err
			}
			for _, entry := range entries {
				old[string(entry.Key)] = entry.Value
				final[string(entry.Key)] = entry.Value
			}
		}
	}
	keys, err := image.RecordKeys()
	if err != nil {
		return "", agentRecordCatalog{}, err
	}
	mutations := make([]state.Mutation, 0)
	for i, recordKey := range keys {
		key := string(recordKey)
		if _, found := old[key]; found {
			delete(old, key)
			continue
		}
		record, err := image.RecordAt(i)
		if err != nil {
			return "", agentRecordCatalog{}, err
		}
		obj, err := makeAgentRecord(space, owner, record.Raw)
		if err != nil {
			return "", agentRecordCatalog{}, err
		}
		id, err := b.add(obj, storage.KindObject)
		if err != nil {
			return "", agentRecordCatalog{}, err
		}
		final[key] = id
		mutations = append(mutations, state.Mutation{Key: record.Key, Value: id})
	}
	for key := range old {
		delete(final, key)
		mutations = append(mutations, state.Mutation{Key: []byte(key), Delete: true})
	}
	sort.Slice(mutations, func(i, j int) bool { return bytes.Compare(mutations[i].Key, mutations[j].Key) < 0 })
	// Migration and multi-record commands can exceed radix's per-Apply limit.
	// Batches are deterministic; intermediate radix nodes remain bounded/deduped.
	for start := 0; start < len(mutations); start += agentRecordMutationBatch {
		delta, err := state.Apply(ctx, b, space, root, mutations[start:min(start+agentRecordMutationBatch, len(mutations))])
		if err != nil {
			return "", agentRecordCatalog{}, err
		}
		for _, obj := range delta.Objects {
			if _, err := b.add(obj, storage.KindObject); err != nil {
				return "", agentRecordCatalog{}, err
			}
		}
		root = delta.Root
	}
	obj, err := makeAgentRecordRoot(space, agentRecordRoot{owner, root, uint64(len(keys)), image.Size()})
	if err != nil {
		return "", agentRecordCatalog{}, err
	}
	id, err := b.add(obj, storage.KindObject)
	if err != nil {
		return "", agentRecordCatalog{}, err
	}
	return id, agentRecordCatalog{id: id, space: space, owner: owner, root: root, objects: final, image: image}, nil
}
