package engine

import (
	"context"
	"errors"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var ErrNodeNotFound = errors.New("node is not visible in the selected view")

const maxNodeQueryRecords = 65536

type nodeQueryLimits struct{ nodes, edges, bytes, depth int }

func checkedNodeLimits(q api.QueryLimits) (nodeQueryLimits, error) {
	l := nodeQueryLimits{q.MaxNodes, q.MaxEdges, q.MaxBytes, q.MaxDepth}
	if l.nodes == 0 {
		l.nodes = 1024
	}
	if l.edges == 0 {
		l.edges = 2048
	}
	if l.bytes == 0 {
		l.bytes = 4 << 20
	}
	if l.depth == 0 {
		l.depth = 1
	}
	if l.nodes < 1 || l.nodes > maxNodeQueryRecords || l.edges < 1 || l.edges > maxNodeQueryRecords || l.bytes < 1 || l.bytes > 8<<20 || l.depth < 1 || l.depth > 8 {
		return l, storage.ErrLimit
	}
	return l, nil
}

// The traversal ceiling is independent of the number of returned summaries.
type nodeQueryReader struct {
	source       state.ObjectReader
	reads, bytes int
}

func (r *nodeQueryReader) ReadObject(ctx context.Context, id model.ObjectID) ([]byte, error) {
	if r.reads >= maxNodeQueryRecords || r.bytes >= 64<<20 {
		return nil, storage.ErrLimit
	}
	r.reads++
	b, e := r.source.ReadObject(ctx, id)
	if e != nil {
		return nil, e
	}
	if len(b) > 64<<20-r.bytes {
		return nil, storage.ErrLimit
	}
	r.bytes += len(b)
	return b, nil
}
func (e *Engine) nodeReadError(ctx context.Context, err error) error {
	if isLimit(err) || errors.Is(err, memory.ErrHistoryAccessDenied) || errors.Is(err, memory.ErrUseDenied) || errors.Is(err, ErrUnsupportedMethod) || errors.Is(err, ErrNodeNotFound) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return e.readFailureAt(ctx, err)
}
