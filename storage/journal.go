package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"

	"github.com/vkviyu/agent-temporal-engine/model"
)

var (
	ErrClosed               = errors.New("journal is closed")
	ErrClosing              = errors.New("journal is closing")
	ErrStoreFenced          = errors.New("journal is fenced")
	ErrCommitUnknown        = errors.New("journal publication outcome is unknown")
	ErrNotEmpty             = errors.New("store directory is not empty")
	ErrIncompleteStore      = errors.New("store initialization is incomplete")
	ErrRecoveryRequired     = errors.New("unpublished journal tail requires explicit recovery")
	ErrRecoveryIncomplete   = errors.New("journal recovery did not finish")
	ErrCorruption           = errors.New("published store data is corrupt")
	ErrPrecondition         = errors.New("prepared transaction does not extend the published position")
	ErrDuplicateTransaction = errors.New("transaction ID is already published")
)

const DefaultMaxPublishedBytes uint64 = 1 << 30
const MaxPublishedBytes uint64 = 64 << 30
const DefaultMaxPublishedTransactions uint64 = 100_000
const MaxPublishedTransactions uint64 = 1 << 20

const DefaultSegmentTargetBytes uint64 = 64 << 20
const MinSegmentTargetBytes uint64 = 1024
const MaxSegmentTargetBytes uint64 = 1 << 30

type TailPolicy string

const (
	TailReject     TailPolicy = "reject"
	TailQuarantine TailPolicy = "quarantine"
)

type JournalOptions struct {
	// SegmentTargetBytes is a soft physical data-end target. Frames are never
	// split; one frame larger than the target occupies a segment by itself.
	SegmentTargetBytes       uint64
	Limits                   Limits
	MaxPublishedBytes        uint64
	MaxPublishedTransactions uint64
	TailPolicy               TailPolicy
}

// NormalizeJournalOptions validates options and applies defaults without IO.
func NormalizeJournalOptions(o JournalOptions) (JournalOptions, error) { return o.checked() }

func (o JournalOptions) checked() (JournalOptions, error) {
	var err error
	if o.Limits, err = o.Limits.checked(); err != nil {
		return o, err
	}
	if o.MaxPublishedBytes == 0 {
		o.MaxPublishedBytes = DefaultMaxPublishedBytes
	}
	if o.MaxPublishedBytes < SegmentHeaderSize || o.MaxPublishedBytes > MaxPublishedBytes {
		return o, ErrLimit
	}
	if o.MaxPublishedTransactions == 0 {
		o.MaxPublishedTransactions = DefaultMaxPublishedTransactions
	}
	if o.MaxPublishedTransactions > MaxPublishedTransactions {
		return o, ErrLimit
	}
	if o.SegmentTargetBytes == 0 {
		o.SegmentTargetBytes = DefaultSegmentTargetBytes
	}
	if o.SegmentTargetBytes < MinSegmentTargetBytes || o.SegmentTargetBytes > MaxSegmentTargetBytes {
		return o, ErrLimit
	}
	if o.TailPolicy == "" {
		o.TailPolicy = TailReject
	}
	if o.TailPolicy != TailReject && o.TailPolicy != TailQuarantine {
		return o, fmt.Errorf("%w: tail policy", ErrUnsupported)
	}
	return o, nil
}

type JournalState string

const (
	JournalReady       JournalState = "ready"
	JournalMaintenance JournalState = "maintenance"
	JournalFenced      JournalState = "fenced"
	JournalClosing     JournalState = "closing"
	JournalClosed      JournalState = "closed"
)

// Publication proves the physical journal prefix under Profile. Domain roots,
// permissions and command receipts still require the semantic engine's replay.
type Publication struct {
	Store       model.StoreID
	Generation  uint64
	Position    Position
	PostRootSet Digest
	// PublishedEnd is a logical stream offset: the initial header followed by
	// canonical frames, excluding all successor headers and seal footers.
	PublishedEnd uint64
	Profile      DurabilityProfile
}

func (p Publication) Fence() model.CommitFence {
	return model.CommitFence{Store: p.Store, LSN: p.Position.LSN, CommitDigest: model.CommitDigest(p.Position.Digest.String())}
}

// JournalStatus is diagnostic only. LastPublication must not grant a new read
// or dispatch after State becomes fenced. Use Published for an active fence.
type JournalStatus struct {
	State           JournalState
	LastPublication Publication
}

type PublicationError struct {
	Stage    string
	TxID     model.TxID
	TxIDs    []model.TxID
	Previous Publication
	Cause    error
}

func (e *PublicationError) Error() string {
	return fmt.Sprintf("%v at %s (transaction %s): %v", ErrCommitUnknown, e.Stage, e.TxID, e.Cause)
}
func (e *PublicationError) Unwrap() []error { return []error{ErrCommitUnknown, e.Cause} }

// journalSegment owns one opened segment and its last published descriptor.
// The active prefix hash excludes the seal footer; sealed hashes need not be
// retained after recovery. closed records confirmed resource retirement.
type journalSegment struct {
	descriptor SegmentDescriptor
	file       DurableFile
	prefix     hash.Hash
	closed     bool
}

// Journal is a segmented physical publisher. It serializes publication and
// retains read layouts and its owner lock until Close. Inputs must not be mutated
// during preparation. It deliberately does not implement api.Committer: domain execution,
// durable receipt lookup, authorization and state replay belong above it.
type Journal struct {
	mu       sync.Mutex
	writerMu sync.Mutex
	journalWriterState
	state          JournalState
	fenceCause     error
	closeRequested bool
	readerPins     map[*PinnedReader]struct{}
	activeReads    map[*logicalReader]struct{}
	readsDone      chan struct{}
}

// The writer owns this mutex-free state while preparing P1. Published layout
// fields are installed together under Journal.mu; readers retain value snapshots.
// txIDs and physical retirement progress belong exclusively to writerMu.
type journalWriterState struct {
	io               DurableIO
	owner            OwnerLock
	segment          DurableFile
	segments         []*journalSegment
	pendingSegments  []*journalSegment
	identity         Identity
	identityDigest   Digest
	headerDigest     Digest
	segmentID        string
	options          JournalOptions
	publication      Publication
	prefix           hash.Hash
	txIDs            map[model.TxID]struct{}
	segmentClosed    bool
	ownerClosed      bool
	manifest         Manifest
	retiredSegments  []*retiredSegment
	retirementIntent string
}

func segmentName(id string) string  { return "segments/SEGMENT." + id }
func manifestName(id string) string { return "manifests/MANIFEST." + id }

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func checkIO(d DurableIO, options JournalOptions) (JournalOptions, error) {
	o, err := options.checked()
	if err != nil {
		return o, err
	}
	if d == nil || d.Profile() != FileSyncExperimental {
		return o, fmt.Errorf("%w: durability profile", ErrUnsupported)
	}
	return o, nil
}

// CreateJournal initializes an existing empty directory. The lock file is the
// only permitted preexisting entry. Partial initialization is left for explicit
// inspection; Create never erases or guesses how to repair an existing store.
func CreateJournal(d DurableIO, options JournalOptions) (_ *Journal, err error) {
	o, err := checkIO(d, options)
	if err != nil {
		return nil, err
	}
	owner, err := d.AcquireOwnerLock()
	if err != nil {
		return nil, cleanupOnError(err, owner)
	}
	var segment DurableFile
	defer func() {
		if err != nil {
			var resources []io.Closer
			if segment != nil {
				resources = append(resources, segment)
			}
			err = cleanupOnError(err, append(resources, owner)...)
		}
	}()
	entries, err := d.List(".")
	if err != nil {
		return nil, err
	}
	for _, name := range entries {
		if name != ".owner.lock" {
			return nil, ErrNotEmpty
		}
	}
	storeID, err := randomID()
	if err != nil {
		return nil, err
	}
	segmentID, err := randomID()
	if err != nil {
		return nil, err
	}
	manifestID, err := randomID()
	if err != nil {
		return nil, err
	}
	id := Identity{Store: model.StoreID(storeID), Profile: d.Profile()}
	idBytes, err := EncodeIdentity(id)
	if err != nil {
		return nil, err
	}
	idDigest, err := IdentityDigest(id)
	if err != nil {
		return nil, err
	}
	genesis, err := GenesisPosition(id)
	if err != nil {
		return nil, err
	}
	header, err := EncodeSegmentHeader(SegmentHeader{Store: id.Store, SegmentID: segmentID, FirstLSN: 1, PreviousDigest: genesis.Digest})
	if err != nil {
		return nil, err
	}
	// From the first layout mutation onward a failed initialization must not be
	// mistaken for an untouched empty directory, even when CURRENT is absent.
	stage := "create directories"
	defer func() {
		if err != nil {
			err = fmt.Errorf("%w at %s: %w", ErrIncompleteStore, stage, err)
		}
	}()
	for _, name := range []string{"segments", "manifests"} {
		if err = d.Mkdir(name); err != nil {
			return nil, err
		}
	}
	if err = d.SyncDir("."); err != nil {
		return nil, err
	}
	stage = "identity"
	if err = writeSyncedFile(d, "IDENTITY", idBytes); err != nil {
		return nil, err
	}
	if err = d.SyncDir("."); err != nil {
		return nil, err
	}
	stage = "genesis segment"
	segment, err = d.CreateExclusive(segmentName(segmentID))
	if err != nil {
		return nil, err
	}
	if err = writeAll(segment, header); err != nil {
		return nil, err
	}
	if err = segment.Sync(); err != nil {
		return nil, err
	}
	if err = d.SyncDir("segments"); err != nil {
		return nil, err
	}
	prefix := sha256.New()
	prefix.Write(header)
	headerDigest := Digest(sha256.Sum256(header))
	m := Manifest{Store: id.Store, ManifestID: manifestID, SegmentID: segmentID, SegmentHeaderDigest: headerDigest,
		PublishedEnd: SegmentHeaderSize, PrefixDigest: hashDigest(prefix), Position: genesis}
	stage = "genesis publication"
	if err = installPublication(d, m, idDigest); err != nil {
		return nil, err
	}
	p := publicationFor(id, m)
	return &Journal{state: JournalReady, journalWriterState: journalWriterState{io: d, owner: owner, segment: segment, identity: id, identityDigest: idDigest, headerDigest: headerDigest,
		segmentID: segmentID, options: o, publication: p, prefix: prefix, txIDs: make(map[model.TxID]struct{}), manifest: m,
		segments: []*journalSegment{{descriptor: SegmentDescriptor{SegmentID: segmentID, HeaderSize: SegmentHeaderSize,
			HeaderDigest: headerDigest, DataEnd: SegmentHeaderSize, PrefixDigest: hashDigest(prefix), LogicalStart: SegmentHeaderSize,
			LogicalEnd: SegmentHeaderSize, StartPosition: genesis, EndPosition: genesis}, file: segment, prefix: prefix}}}}, nil
}

func publicationFor(id Identity, m Manifest) Publication {
	return Publication{Store: id.Store, Generation: m.Generation, Position: m.Position, PostRootSet: m.PostRootSet, PublishedEnd: m.PublishedEnd, Profile: id.Profile}
}

// Published is unavailable after an uncertain IO result; stale authorization or
// dispatch must not use the last cached fence. Status remains diagnostic.
func (j *Journal) Published() (Publication, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return Publication{}, err
	}
	return j.publication, nil
}

func (j *Journal) Status() JournalStatus {
	j.mu.Lock()
	defer j.mu.Unlock()
	return JournalStatus{State: j.state, LastPublication: j.publication}
}

func (j *Journal) ready() error {
	if j.closeRequested && (j.state == JournalReady || j.state == JournalMaintenance) {
		return ErrClosing
	}
	switch j.state {
	case JournalReady, JournalMaintenance:
		return nil
	case JournalFenced:
		return fmt.Errorf("%w: %w", ErrStoreFenced, j.fenceCause)
	case JournalClosing:
		return ErrClosing
	default:
		return ErrClosed
	}
}

func (j *Journal) writable() error {
	if err := j.ready(); err != nil {
		return err
	}
	if j.state == JournalMaintenance {
		return ErrMaintenance
	}
	return nil
}

// Publish validates before writing and publishes one transaction through the
// same P1 path as PublishPreparedBatch. Inputs must not change during this call.
func (j *Journal) Publish(ctx context.Context, tx Transaction) (Publication, error) {
	result, err := j.publish(ctx, tx)
	return result.Publication, err
}

func (j *Journal) publish(ctx context.Context, tx Transaction) (PublishedTransaction, error) {
	batch, err := j.publishBatch(ctx, nil, &tx)
	if err != nil {
		return PublishedTransaction{}, err
	}
	return batch.Transactions[0], nil
}

// Close waits for the synchronous publisher, closes all its files, and finally
// releases ownership. Failed closures remain Closing and can be retried;
// ownership is retained until the segment is confirmed closed. Close does not
// sync or turn unconfirmed bytes into commits. The caller separately closes
// its DurableIO object after Journal.Close succeeds.
func (j *Journal) Close() error {
	j.mu.Lock()
	if j.state == JournalClosed {
		j.mu.Unlock()
		return nil
	}
	// Close revokes new grants before waiting for the IO owner. A running P1
	// or maintenance operation keeps its writer ownership until completion.
	j.closeRequested = true
	j.mu.Unlock()
	j.writerMu.Lock()
	defer j.writerMu.Unlock()
	j.mu.Lock()
	if j.state == JournalClosed {
		j.mu.Unlock()
		return nil
	}
	j.state = JournalClosing
	if len(j.readerPins) > 0 {
		j.mu.Unlock()
		return ErrReadersPinned
	}
	for len(j.activeReads) > 0 {
		done := j.readsDone
		j.mu.Unlock()
		<-done
		j.mu.Lock()
	}
	cause := j.fenceCause
	j.mu.Unlock()
	// These handles and their retry flags belong to writerMu. The metadata
	// mutex stays available while a native Close or owner unlock is stalled.
	for _, pending := range pendingCleanups(cause) {
		if err := pending.Close(); err != nil {
			return err
		}
	}
	if !j.segmentClosed {
		// New files acquired by a failed publication remain owned too. A file
		// failure never allows ownership to be released behind its handle.
		for _, segment := range j.pendingSegments {
			if !segment.closed && segment.file != nil {
				if err := segment.file.Close(); err != nil {
					return err
				}
				segment.closed = true
			}
		}
		for _, retired := range j.retiredSegments {
			segment := retired.segment
			if !segment.closed && segment.file != nil {
				if err := segment.file.Close(); err != nil {
					return err
				}
				segment.closed = true
			}
		}
		for i, segment := range j.segments {
			if !segment.closed && segment.file != nil {
				file := segment.file
				if i == len(j.segments)-1 {
					file = j.segment
				}
				if err := file.Close(); err != nil {
					return err
				}
				segment.closed = true
			}
		}
		// Keep cleanup compatible with a legacy constructor that has not yet
		// materialized its one-segment catalog.
		if len(j.segments) == 0 && j.segment != nil {
			if err := j.segment.Close(); err != nil {
				return err
			}
		}
		j.segmentClosed = true
	}
	if !j.ownerClosed {
		if j.owner != nil {
			if err := j.owner.Close(); err != nil {
				return err
			}
		}
		j.ownerClosed = true
	}
	j.mu.Lock()
	j.state = JournalClosed
	j.mu.Unlock()
	return nil
}
