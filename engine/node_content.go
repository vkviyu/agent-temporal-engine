package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

// ReadContent reuses the same authenticated membership projection as inspect.
// The current object format has a whole-object SHA-256, so immutable content is
// still authenticated with bounded full-object reads internally. Chunking bounds
// output; it does not claim independent disk-range/Merkle proofs.
func (e *Engine) ReadContent(ctx context.Context, req api.ReadContentRequest) (api.ContentRange, error) {
	grant, err := e.prepareContentRead(ctx, req, api.ReadDestination{})
	if err != nil {
		return api.ContentRange{}, err
	}
	return grant.Consume(grant.destination, req)
}

func (e *Engine) PrepareContentRead(ctx context.Context, req api.ReadContentRequest, destination api.ReadDestination) (api.ReadEmissionGrant, error) {
	if !validReadDestination(destination) {
		return nil, api.ErrReadEmissionDenied
	}
	grant, err := e.prepareContentRead(ctx, req, destination)
	if err != nil {
		return nil, err
	}
	return grant, nil
}

func (e *Engine) prepareContentRead(ctx context.Context, req api.ReadContentRequest, destination api.ReadDestination) (*contentReadGrant, error) {
	binding := req
	if req.Ref.Validate() != nil {
		return nil, model.ErrInvalidNodeRef
	}
	if req.Ref.Kind != model.NodeObject {
		return nil, ErrUnsupportedMethod
	}
	if req.Projection == "" {
		req.Projection = "payload"
	}
	if req.Projection != "payload" && req.Projection != "value_bytes" {
		return nil, api.ErrInvalidRequest
	}
	if req.Length == 0 {
		req.Length = api.DefaultContentChunkBytes
	}
	if req.Length > api.MaxContentChunkBytes {
		return nil, storage.ErrLimit
	}
	if req.MaxBytes == 0 {
		req.MaxBytes = 1 << 20
	}
	if req.MaxBytes < 1 || req.MaxBytes > 1<<20 {
		return nil, storage.ErrLimit
	}
	ctx, release, err := e.beginReadCut(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	entry, err := e.acquireView(ctx, req.View)
	if err != nil {
		return nil, err
	}
	defer e.releaseView(req.View, entry)
	if destination == (api.ReadDestination{}) {
		destination = embeddedReadDestination(entry.principal)
	} else if destination.Principal != entry.principal {
		return nil, api.ErrReadEmissionDenied
	}
	_, catalog, err := e.projectedNode(ctx, entry, req.Ref)
	if err != nil {
		if err == model.ErrInvalidNodeRef {
			return nil, err
		}
		return nil, e.nodeReadError(ctx, err)
	}
	object, visible := catalog.objects[req.Ref.Object]
	if !visible {
		return nil, ErrUnsupportedMethod
	}
	data, err := objectContentProjection(object, req.Projection)
	if err != nil {
		return nil, e.nodeReadError(ctx, err)
	}
	if req.Offset > uint64(len(data)) {
		return nil, api.ErrInvalidRequest
	}
	end := req.Offset + uint64(req.Length)
	if end > uint64(len(data)) {
		end = uint64(len(data))
	}
	sum := sha256.Sum256(data)
	out := api.ContentRange{Ref: req.Ref, Anchor: entry.descriptor.Anchor.Clone(), ObjectType: object.Type, ObjectVersion: object.Version, Projection: req.Projection, Offset: req.Offset, TotalBytes: uint64(len(data)), Digest: model.ObjectID(hex.EncodeToString(sum[:])), Data: bytes.Clone(data[int(req.Offset):int(end)]), EOF: end == uint64(len(data))}
	if jsonBytes(out) > req.MaxBytes {
		return nil, storage.ErrLimit
	}
	ids, err := e.contentReadDependencies(ctx, entry, catalog, req.Ref.Object)
	if err != nil {
		return nil, e.nodeReadError(ctx, err)
	}
	grant := &contentReadGrant{
		destination: destination,
		request:     binding,
		content:     out,
		proof: contentReadProof{
			viewDigest:  entry.descriptor.ViewDigest,
			rangeDigest: emissionDigest(out.Data),
		},
	}
	if err := e.issueContentRead(ctx, entry, grant, ids); err != nil {
		return nil, err
	}
	return grant, nil
}

var _ api.ContentReader = (*Engine)(nil)
var _ api.ContentEmissionReader = (*Engine)(nil)
