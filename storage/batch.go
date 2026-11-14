package storage

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/model"
)

const (
	MaxPreparedBatchTransactions        = 256
	MaxPreparedBatchBytes        uint64 = 64 << 20
)

// PreparedTransaction owns the canonical encoding and all metadata used by
// publication. PrepareTransaction is the only constructor; later changes to
// the caller's Transaction cannot change this value. It is safe to reuse a
// prepared value across calls. Its fields deliberately expose no mutable bytes.
type PreparedTransaction struct {
	encoded          []byte
	txID             model.TxID
	request          model.RequestKey
	requestDigest    Digest
	position         Position
	previous         Digest
	receipt          Digest
	postRootSet      Digest
	payloadLocations []RecordLocation
	maxPayload       uint32
	frameCount       uint32
}

// PrepareTransaction performs bounded encoding without IO or publication.
// Inputs must not be changed until it returns.
func PrepareTransaction(tx Transaction, limits Limits) (*PreparedTransaction, error) {
	b, digest, err := EncodeTransaction(tx, limits)
	if err != nil {
		return nil, err
	}
	p := &PreparedTransaction{encoded: b, txID: tx.TxID, request: tx.Request,
		requestDigest: tx.RequestDigest, position: Position{LSN: tx.LSN, Digest: digest},
		previous: tx.PreviousDigest, receipt: tx.Receipt, postRootSet: tx.PostRootSet,
		payloadLocations: publishedRecordLocations(b, 0)}
	// Read only canonical headers emitted by EncodeTransaction; external bytes
	// never enter this path. Retain limit facts so publication can honor stricter
	// journal options without encoding a second time.
	for at := 0; at < len(b); {
		n := binary.LittleEndian.Uint32(b[at+20 : at+24])
		if n > p.maxPayload {
			p.maxPayload = n
		}
		p.frameCount++
		at += HeaderSize + int(n) + padding(n) + TrailerSize
	}
	return p, nil
}

func (p *PreparedTransaction) Position() Position {
	if p == nil {
		return Position{}
	}
	return p.position
}

func (p *PreparedTransaction) Size() uint64 {
	if p == nil {
		return 0
	}
	return uint64(len(p.encoded))
}

// PublishedBatch grants all transaction results together after the final
// CURRENT directory sync. Publication is the final transaction's tip; each
// transaction retains its own position, root, end, and payload locations under
// the same new generation. Every failed call returns the strict zero value.
type PublishedBatch struct {
	Publication  Publication
	Transactions []PublishedTransaction
}

// PublishPreparedBatch serializes and publishes one nonempty bounded batch.
// Cancellation is checked until the first append attempt; once P1 starts it
// runs to completion or fences the journal with ErrCommitUnknown.
func (j *Journal) PublishPreparedBatch(ctx context.Context, batch []*PreparedTransaction) (PublishedBatch, error) {
	return j.publishBatch(ctx, batch, nil)
}

func unknownBatch(previous Publication, stage string, batch []*PreparedTransaction, cause error) error {
	ids := make([]model.TxID, len(batch))
	for i, p := range batch {
		ids[i] = p.txID
	}
	err := &PublicationError{Stage: stage, TxID: ids[0], TxIDs: ids, Previous: previous, Cause: cause}
	return err
}

// publishPreparedBatch executes P1 on a private mutex-free writer candidate.
func (j *journalWriterState) publishPreparedBatch(ctx context.Context, batch []*PreparedTransaction, guard func() error) (PublishedBatch, error) {
	if err := ctx.Err(); err != nil {
		return PublishedBatch{}, err
	}
	p := j.publication
	total, err := j.validatePreparedBatch(batch)
	if err != nil {
		return PublishedBatch{}, err
	}
	manifestID, err := randomID()
	if err != nil {
		return PublishedBatch{}, err
	}
	layout, err := j.prepareLayout(batch, total, manifestID)
	if err != nil {
		return PublishedBatch{}, err
	}
	if err = ctx.Err(); err != nil {
		return PublishedBatch{}, err
	}
	activeEnd := p.PublishedEnd
	// The candidate already includes new frames/seal. Size must match the old
	// published physical end, not the candidate or the logical stream end.
	if len(j.segments) > 0 {
		activeEnd = j.segments[len(j.segments)-1].descriptor.DataEnd
	}
	size, err := j.segment.Size()
	if err != nil || size < 0 || uint64(size) != activeEnd {
		if err == nil {
			err = fmt.Errorf("%w: segment length changed outside the publisher", ErrCorruption)
		}
		return PublishedBatch{}, fmt.Errorf("%w before append: %w", ErrStoreFenced, err)
	}
	if err = ctx.Err(); err != nil {
		return PublishedBatch{}, err
	}
	if err = guard(); err != nil {
		return PublishedBatch{}, err
	}
	// There are no cancellation checks after the first attempted layout IO.
	// A seal/new successor is not a separate logical publication.
	if err = j.writeLayout(layout, guard); err != nil {
		return PublishedBatch{}, unknownBatch(p, "segmented publication", batch, err)
	}
	m := layout.manifest
	result := PublishedBatch{Publication: publicationFor(j.identity, m), Transactions: make([]PublishedTransaction, len(batch))}
	end := p.PublishedEnd
	for i, prepared := range batch {
		locations := make([]RecordLocation, len(prepared.payloadLocations))
		for n, location := range prepared.payloadLocations {
			location.PayloadOffset += int64(end)
			locations[n] = location
		}
		if len(locations) == 0 {
			locations = nil
		}
		end += prepared.Size()
		own := result.Publication
		own.Position, own.PostRootSet, own.PublishedEnd = prepared.position, prepared.postRootSet, end
		result.Transactions[i] = PublishedTransaction{Publication: own, RecordLocations: locations}
		j.txIDs[prepared.txID] = struct{}{}
	}
	// The sole in-memory publication point follows the final directory sync.
	j.segments = layout.segments
	j.pendingSegments = nil
	active := j.segments[len(j.segments)-1]
	j.segment, j.segmentID, j.headerDigest, j.prefix = active.file, active.descriptor.SegmentID, active.descriptor.HeaderDigest, active.prefix
	j.publication = result.Publication
	j.manifest = m
	return result, nil
}
