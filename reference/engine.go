// Package reference provides an experimental, in-memory receipt oracle. It has
// no persistence, authorization, domain execution, or durable-absence guarantee.
// Use it for command-contract tests, never as a production store.
package reference

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"sync"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type Engine struct {
	mu      sync.Mutex
	fence   model.CommitFence
	results map[api.RequestKey]entry
}

type entry struct {
	digest model.ObjectID
	result api.CommitResult
}

var _ api.Committer = (*Engine)(nil)

func New() *Engine {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}
	store := model.StoreID("reference-" + hex.EncodeToString(id[:]))
	b, _ := codec.AppendString([]byte("ATE-REFERENCE-GENESIS\x00"), string(store))
	h := sha256.Sum256(b)
	return &Engine{fence: model.CommitFence{Store: store, CommitDigest: model.CommitDigest(hex.EncodeToString(h[:]))}, results: make(map[api.RequestKey]entry)}
}

// Fence returns the current volatile model prefix. It cannot survive New().
func (e *Engine) Fence() model.CommitFence { e.mu.Lock(); defer e.mu.Unlock(); return e.fence }

// Commit hashes the opaque command and records its receipt; it does not execute
// Method. Inputs must not be mutated concurrently. Cancellation before model
// admission is NotSubmitted; once admitted this synchronous oracle returns its
// known result rather than implying cancellation rolls back a commit.
func (e *Engine) Commit(ctx context.Context, req api.CommitRequest) (api.CommitResult, error) {
	notSubmitted := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: req.RequestID}
	if err := ctx.Err(); err != nil {
		return notSubmitted, err
	}
	digest, err := CommandDigest(req)
	if err != nil {
		return notSubmitted, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return notSubmitted, err
	}
	if old, ok := e.results[req.Key()]; ok {
		if old.digest != digest {
			return api.CommitResult{Code: api.CodeConflict, RequestID: req.RequestID, Conflict: api.ConflictIdempotency}, nil
		}
		return old.result, nil
	}
	if req.ExpectedFence != nil && *req.ExpectedFence != e.fence {
		return api.CommitResult{Code: api.CodeConflict, RequestID: req.RequestID, Conflict: api.ConflictPrecondition}, nil
	}
	b := []byte("ATE-REFERENCE-COMMIT\x00")
	b, _ = codec.AppendString(b, string(e.fence.Store))
	b = codec.AppendUint(b, e.fence.LSN+1)
	b, _ = codec.AppendString(b, string(e.fence.CommitDigest))
	b, _ = codec.AppendString(b, string(digest))
	h := sha256.Sum256(b)
	txID := model.TxID(hex.EncodeToString(h[:16]))
	// Logical receipt data excludes its final fence, avoiding self-reference.
	receipt := []byte("ATE-REFERENCE-RESULT\x00")
	receipt, _ = codec.AppendString(receipt, string(api.CodeVolatileAck))
	receipt, _ = codec.AppendString(receipt, string(txID))
	receipt = codec.AppendUint(receipt, e.fence.LSN+1)
	receipt, _ = codec.AppendString(receipt, string(digest))
	resultHash := sha256.Sum256(receipt)
	b, _ = codec.AppendBytes(b, resultHash[:])
	h = sha256.Sum256(b)
	e.fence.LSN++
	e.fence.CommitDigest = model.CommitDigest(hex.EncodeToString(h[:]))
	result := api.CommitResult{Code: api.CodeVolatileAck, RequestID: req.RequestID,
		TxID: txID, Fence: e.fence, RequestDigest: digest, ResultDigest: model.ObjectID(hex.EncodeToString(resultHash[:]))}
	e.results[req.Key()] = entry{digest, result}
	return result, nil
}

func (e *Engine) LookupRequest(ctx context.Context, req api.LookupRequestRequest) (api.LookupRequestResult, error) {
	if err := req.Validate(); err != nil {
		return api.LookupRequestResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return api.LookupRequestResult{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return api.LookupRequestResult{}, err
	}
	if old, ok := e.results[req.Key()]; ok {
		return api.LookupRequestResult{Code: old.result.Code, Result: old.result, ObservedFence: e.fence}, nil
	}
	return api.LookupRequestResult{Code: api.CodeVolatileNotFound, ObservedFence: e.fence}, nil
}
