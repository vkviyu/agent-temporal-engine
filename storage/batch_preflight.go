package storage

import (
	"fmt"
	"math"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func (j *journalWriterState) validatePreparedBatch(batch []*PreparedTransaction) (uint64, error) {
	if len(batch) == 0 {
		return 0, fmt.Errorf("%w: empty batch", ErrMalformed)
	}
	if len(batch) > MaxPreparedBatchTransactions {
		return 0, ErrLimit
	}
	p := j.publication
	count := uint64(len(batch))
	if p.Generation == math.MaxUint64 || count > math.MaxUint64-p.Position.LSN ||
		p.Position.LSN > j.options.MaxPublishedTransactions || count > j.options.MaxPublishedTransactions-p.Position.LSN ||
		p.PublishedEnd > j.options.MaxPublishedBytes {
		return 0, ErrLimit
	}
	position := p.Position
	var total uint64
	seen := make(map[model.TxID]struct{}, len(batch))
	for _, prepared := range batch {
		if prepared == nil || len(prepared.encoded) == 0 || prepared.position.LSN == 0 {
			return 0, fmt.Errorf("%w: unprepared transaction", ErrMalformed)
		}
		size := prepared.Size()
		if size > MaxPreparedBatchBytes || size > j.options.Limits.MaxTransactionBytes ||
			prepared.maxPayload > j.options.Limits.MaxPayload || prepared.frameCount > j.options.Limits.MaxRecords ||
			size > MaxPreparedBatchBytes-total {
			return 0, ErrLimit
		}
		total += size
		if prepared.position.LSN != position.LSN+1 || prepared.previous != position.Digest {
			return 0, ErrPrecondition
		}
		if _, exists := j.txIDs[prepared.txID]; exists {
			return 0, ErrDuplicateTransaction
		}
		if _, exists := seen[prepared.txID]; exists {
			return 0, ErrDuplicateTransaction
		}
		seen[prepared.txID] = struct{}{}
		position = prepared.position
	}
	if total > j.options.MaxPublishedBytes-p.PublishedEnd {
		return 0, ErrLimit
	}
	return total, nil
}
