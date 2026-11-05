package knowledge

import (
	"errors"
	"sort"
	"time"

	"github.com/vkviyu/agent-temporal-engine/model"
)

const (
	SummarySchema            uint64 = 1
	MaxSummaryTopics                = 256
	MaxSummaryReviewEvidence        = 256
	MaxSummaryReferences            = 4 * MaxItems
)

var ErrInvalidSummary = errors.New("invalid knowledge summary")

// SummarySource distinguishes adopted logical projection IDs from raw core
// objects. Observation and typed Content IDs are not codec object addresses.
type SummarySource struct {
	Observation model.ObjectID
	Content     model.ObjectID
	PayloadKind string
}

type SummaryTopic struct {
	Name         string
	Observations []model.ObjectID
}

type SummaryOmission struct {
	Observation model.ObjectID
	Reason      string
}

type SummaryApplicability struct {
	Goal, Environment string
	SchemaVersion     uint64
	Tags              []string
}

// RecordedContent addresses exact immutable bytes with their publication
// anchor and encoding. It records evidence, not an assertion of model truth.
type RecordedContent struct {
	Object   model.ObjectID
	Anchor   model.SnapshotAnchor
	Encoding string
}

type SummaryGeneration struct {
	Activity, Attempt, Result string
	Model, PromptVersion      string
	PolicyVersion             string
	Input, Output             RecordedContent
}

type SummaryValidation struct {
	Status                  string // unreviewed, validated, rejected
	Reviewer, PolicyVersion string
	Evidence                []RecordedContent
}

// Summary is an immutable derived record. GeneratedAt/VisibleAt are recorded
// host timestamps, never authority to insert a new summary into an old View.
// The engine must authenticate publication, adopted knowledge, the auxiliary
// generation Activity/accepted Result, and today's access/withdrawal policy.
type Summary struct {
	ID              model.ObjectID
	Space           model.SpaceID
	Source          SourceAnchor
	Sources         []SummarySource
	SourceSetDigest model.ObjectID
	Topics          []SummaryTopic
	Omissions       []SummaryOmission
	Applicability   SummaryApplicability
	Confidence      uint32 // 0..1000; a host/model proposal, not a probability proof
	Generation      SummaryGeneration
	GeneratedAt     time.Time
	VisibleAt       time.Time
	Validation      SummaryValidation
	Replaces        model.ObjectID // prior logical SummaryID, never overwritten
}

func validSummaryPayloadKind(kind string) bool {
	return kind == "" || kind == "memory" || kind == "experience_path" || kind == "summary"
}

func validSummaryTime(stamp time.Time) bool {
	return !stamp.IsZero() && stamp.UTC().Year() >= 1 && stamp.UTC().Year() <= 9999
}

func (content RecordedContent) validate(space model.SpaceID, store model.StoreID) error {
	if !model.ValidDigest(string(content.Object)) || !validText(content.Encoding) || content.Anchor.Validate() != nil || content.Anchor.Space != space || content.Anchor.Fence.Store != store || content.Anchor.Knowledge != nil {
		return ErrInvalidSummary
	}
	return nil
}

func (s Summary) Validate() error {
	if !validText(string(s.Space)) || s.Source.Validate() != nil || s.Source.Anchor.Space != s.Space || s.Source.Anchor.Knowledge == nil || s.Source.Anchor.Cursor == nil || s.Source.Frontier == "" || len(s.Sources) == 0 || len(s.Sources) > MaxItems || !model.ValidDigest(string(s.SourceSetDigest)) || len(s.Topics) == 0 || len(s.Topics) > MaxSummaryTopics || len(s.Omissions) > MaxItems || s.Confidence > 1000 || !validSummaryTime(s.GeneratedAt) || !validSummaryTime(s.VisibleAt) || s.VisibleAt.Before(s.GeneratedAt) || (s.Replaces != "" && (!model.ValidDigest(string(s.Replaces)) || s.Replaces == s.ID)) {
		return ErrInvalidSummary
	}
	sources := make(map[model.ObjectID]bool, len(s.Sources))
	references := len(s.Sources) + len(s.Omissions)
	for _, source := range s.Sources {
		if !model.ValidDigest(string(source.Observation)) || !model.ValidDigest(string(source.Content)) || !validSummaryPayloadKind(source.PayloadKind) || sources[source.Observation] {
			return ErrInvalidSummary
		}
		sources[source.Observation] = true
	}
	if SummarySourceSetDigest(s.Source, s.Sources) != s.SourceSetDigest {
		return ErrInvalidSummary
	}
	topics := make(map[string]bool, len(s.Topics))
	for _, topic := range s.Topics {
		if !validText(topic.Name) || topics[topic.Name] || len(topic.Observations) == 0 || len(topic.Observations) > MaxItems {
			return ErrInvalidSummary
		}
		topics[topic.Name] = true
		references += len(topic.Observations)
		seen := make(map[model.ObjectID]bool, len(topic.Observations))
		for _, id := range topic.Observations {
			if !sources[id] || seen[id] {
				return ErrInvalidSummary
			}
			seen[id] = true
		}
	}
	omitted := make(map[model.ObjectID]bool, len(s.Omissions))
	for _, omission := range s.Omissions {
		if !model.ValidDigest(string(omission.Observation)) || !validText(omission.Reason) || sources[omission.Observation] || omitted[omission.Observation] {
			return ErrInvalidSummary
		}
		omitted[omission.Observation] = true
	}
	app := s.Applicability
	if app.SchemaVersion == 0 || (app.Goal != "" && !validText(app.Goal)) || (app.Environment != "" && !validText(app.Environment)) || len(app.Tags) > MaxSummaryTopics {
		return ErrInvalidSummary
	}
	tags := make(map[string]bool, len(app.Tags))
	for _, tag := range app.Tags {
		if !validText(tag) || tags[tag] {
			return ErrInvalidSummary
		}
		tags[tag] = true
	}
	generation := s.Generation
	for _, field := range []string{generation.Activity, generation.Attempt, generation.Result, generation.Model, generation.PromptVersion, generation.PolicyVersion} {
		if !validText(field) {
			return ErrInvalidSummary
		}
	}
	store := s.Source.Anchor.Fence.Store
	if generation.Input.validate(s.Space, store) != nil || generation.Output.validate(s.Space, store) != nil {
		return ErrInvalidSummary
	}
	// Accepted output cannot be published before the exact request existed.
	input, output := generation.Input.Anchor.Fence, generation.Output.Anchor.Fence
	if output.LSN < input.LSN || (output.LSN == input.LSN && output.CommitDigest != input.CommitDigest) {
		return ErrFutureSource
	}
	validation := s.Validation
	if len(validation.Evidence) > MaxSummaryReviewEvidence {
		return ErrInvalidSummary
	}
	switch validation.Status {
	case "unreviewed":
		if validation.Reviewer != "" || validation.PolicyVersion != "" || len(validation.Evidence) != 0 {
			return ErrInvalidSummary
		}
	case "validated", "rejected":
		if !validText(validation.Reviewer) || !validText(validation.PolicyVersion) || len(validation.Evidence) == 0 {
			return ErrInvalidSummary
		}
	default:
		return ErrInvalidSummary
	}
	seenEvidence := make(map[model.ObjectID]bool, len(validation.Evidence))
	for _, evidence := range validation.Evidence {
		if evidence.validate(s.Space, store) != nil || seenEvidence[evidence.Object] {
			return ErrInvalidSummary
		}
		seenEvidence[evidence.Object] = true
	}
	if references+len(validation.Evidence) > MaxSummaryReferences {
		return ErrInvalidSummary
	}
	return nil
}

func (s Summary) ValidateBefore(consumer model.CommitFence) error {
	if err := s.Validate(); err != nil {
		return err
	}
	for _, source := range SummarySourceAnchors(s) {
		if err := source.ValidateBefore(consumer); err != nil {
			return err
		}
	}
	return nil
}

// VerifySummarySources authenticates the selected observation membership and
// content/kind at the exact Key+Cursor+Frontier. The supplied snapshot must
// itself have been authenticated at Source.Fence by the engine; this pure
// check cannot prove that a caller owns an arbitrary in-memory snapshot.
func VerifySummarySources(s Summary, snapshot Snapshot) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if s.ID == "" || SummaryID(s) != s.ID {
		return ErrInvalidSummary
	}
	key := *s.Source.Anchor.Knowledge
	selected, _, err := SelectRevision(snapshot, s.Space, key, s.Source.Anchor.Cursor.Revision)
	if err != nil {
		return err
	}
	frontier, err := Frontier(selected, key)
	if err != nil || frontier != s.Source.Frontier {
		return model.ErrAnchorMismatch
	}
	observations := make(map[model.ObjectID]Observation, len(selected.Observations))
	for _, observation := range selected.Observations {
		before, at := observation.Source.Anchor.Fence, s.Source.Anchor.Fence
		if before.Store != at.Store || before.LSN > at.LSN || (before.LSN == at.LSN && before.CommitDigest != at.CommitDigest) {
			return ErrFutureSource
		}
		observations[observation.ID] = observation
	}
	for _, source := range s.Sources {
		observation, ok := observations[source.Observation]
		if !ok {
			return ErrUnknownObservation
		}
		if observation.Payload != source.Content || observation.PayloadKind != source.PayloadKind {
			return ErrInvalidSummary
		}
	}
	for _, omission := range s.Omissions {
		if _, ok := observations[omission.Observation]; !ok {
			return ErrUnknownObservation
		}
	}
	return nil
}

func (s Summary) Clone() Summary {
	s.Source = s.Source.clone()
	s.Sources = append([]SummarySource(nil), s.Sources...)
	s.Topics = append([]SummaryTopic(nil), s.Topics...)
	for i := range s.Topics {
		s.Topics[i].Observations = append([]model.ObjectID(nil), s.Topics[i].Observations...)
	}
	s.Omissions = append([]SummaryOmission(nil), s.Omissions...)
	s.Applicability.Tags = append([]string(nil), s.Applicability.Tags...)
	s.Generation.Input.Anchor = s.Generation.Input.Anchor.Clone()
	s.Generation.Output.Anchor = s.Generation.Output.Anchor.Clone()
	s.Validation.Evidence = append([]RecordedContent(nil), s.Validation.Evidence...)
	for i := range s.Validation.Evidence {
		s.Validation.Evidence[i].Anchor = s.Validation.Evidence[i].Anchor.Clone()
	}
	return s
}

// SummaryContentReferences excludes logical Observation/Memory/Path/Summary IDs
// and source commitments. Those require the corresponding typed projections
// and historical anchors; they must not be blindly read as codec objects.
func SummaryContentReferences(s Summary) []model.ObjectID {
	ids := []model.ObjectID{s.Generation.Input.Object, s.Generation.Output.Object}
	for _, source := range s.Sources {
		if source.PayloadKind == "" {
			ids = append(ids, source.Content)
		}
	}
	for _, evidence := range s.Validation.Evidence {
		ids = append(ids, evidence.Object)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	unique := ids[:0]
	for _, id := range ids {
		if id != "" && (len(unique) == 0 || unique[len(unique)-1] != id) {
			unique = append(unique, id)
		}
	}
	return unique
}

func SummarySourceAnchors(s Summary) []SourceAnchor {
	out := []SourceAnchor{s.Source.clone(), {Anchor: s.Generation.Input.Anchor.Clone()}, {Anchor: s.Generation.Output.Anchor.Clone()}}
	for _, evidence := range s.Validation.Evidence {
		out = append(out, SourceAnchor{Anchor: evidence.Anchor.Clone()})
	}
	return out
}
