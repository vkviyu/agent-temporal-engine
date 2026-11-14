package memory

import (
	"errors"
	"sync"
	"time"

	"github.com/vkviyu/agent-temporal-engine/model"
)

type Kind string

const (
	KindFact           Kind = "fact"
	KindDecision       Kind = "decision"
	KindProcedure      Kind = "procedure"
	KindFailurePattern Kind = "failure_pattern"
	KindPreference     Kind = "preference"
)

type Status string

const (
	StatusCandidate  Status = "candidate"
	StatusValidated  Status = "validated"
	StatusPromoted   Status = "promoted"
	StatusSuperseded Status = "superseded"
	StatusRetracted  Status = "retracted"
	StatusRejected   Status = "rejected"
)

type RetractionKind string

const (
	RetractionFactualCorrection RetractionKind = "factual_correction"
	RetractionUseBan            RetractionKind = "use_ban"
	RetractionPrivacyErasure    RetractionKind = "privacy_erasure"
	RetractionACLRevocation     RetractionKind = "acl_revocation"
)

var (
	ErrInvalidMemory        = errors.New("invalid memory record")
	ErrInvalidTransition    = errors.New("invalid memory lifecycle transition")
	ErrSourceUnavailable    = errors.New("memory source unavailable")
	ErrNotFound             = errors.New("memory not found")
	ErrAlreadyExists        = errors.New("memory already exists")
	ErrNotEligible          = errors.New("memory is not eligible for reuse")
	ErrPathInvalid          = errors.New("invalid experience path")
	ErrStepEvidenceMismatch = errors.New("experience step node is not a declared source or supporting evidence of its memory")
)

// Source identifies an immutable source event/activity/run. SourceID is a
// content identity, while Anchor fixes the publication cut at which it was seen.
type Source struct {
	SourceID model.ObjectID
	Anchor   model.SnapshotAnchor
}
type Evidence struct {
	Source  Source
	Counter bool
	Note    string
}
type Applicability struct {
	Goal          string
	Environment   string
	SchemaVersion uint32
	Tags          []string
}

// Memory is a versioned, scoped assertion with provenance and lifecycle.
// Promotion records the caller's evidence-backed validation. It does not infer
// the assertion's truth or adopt it into another execution's context.
type Memory struct {
	ID              model.ObjectID
	Space           model.SpaceID
	Kind            Kind
	Claim           []byte
	Sources         []Source
	Evidence        []Evidence
	Applicability   Applicability
	Status          Status
	Confidence      uint32
	StrategyVersion uint32
	RecordedAt      time.Time
	VisibleAt       time.Time
	ValidFrom       time.Time
	ValidUntil      *time.Time
	Supersedes      []model.ObjectID
	Retracts        []model.ObjectID
	Retraction      *Retraction
	Validation      *Validation
}
type Retraction struct {
	ID         model.ObjectID
	Kind       RetractionKind
	SourceID   model.ObjectID
	Reason     string
	RecordedAt time.Time
}

// Step selects a supporting node at the owning path's SourceAnchor and a
// promoted Memory version. Order is procedural sequence, not event causality.
type Step struct {
	Node    model.NodeRef
	Memory  model.ObjectID
	Order   uint32
	Outcome string
}
type Validation struct {
	StrategyVersion uint32
	Passed          bool
	Evidence        []Source
	Counterexamples []Source
	RecordedAt      time.Time
}

// ExperiencePath is an ordered composition of promoted memory versions with
// validation and applicability of its own. Omitted records evidence locations
// excluded from the procedure, while retaining them for authorized audit.
type ExperiencePath struct {
	ID            model.ObjectID
	Space         model.SpaceID
	SourceAnchor  model.SnapshotAnchor
	Steps         []Step
	Omitted       []model.NodeRef
	Validation    Validation
	Applicability Applicability
	Status        Status
	DerivedFrom   []model.ObjectID
	Retraction    *Retraction
}

// Query supplies explicit reuse conditions. An unset scope selector applies no
// filter for that dimension. AtTime is required for time-limited assertions.
type Query struct {
	Space         model.SpaceID
	Text          string
	Goal          string
	Environment   string
	SchemaVersion uint32
	Tags          []string
	At            model.SnapshotAnchor
	AtTime        time.Time
}
type Result struct {
	Memory Memory
	Score  uint32
}
type Store struct {
	mu             sync.RWMutex
	successors     map[model.ObjectID]model.ObjectID
	pathSuccessors map[model.ObjectID]model.ObjectID
	memories       map[model.ObjectID]Memory
	paths          map[model.ObjectID]ExperiencePath
	sourceBan      map[model.ObjectID]Retraction
	tokens         map[string]map[model.ObjectID]struct{}
}

func New() *Store {
	return &Store{successors: map[model.ObjectID]model.ObjectID{}, pathSuccessors: map[model.ObjectID]model.ObjectID{}, memories: make(map[model.ObjectID]Memory), paths: make(map[model.ObjectID]ExperiencePath), sourceBan: make(map[model.ObjectID]Retraction), tokens: make(map[string]map[model.ObjectID]struct{})}
}
