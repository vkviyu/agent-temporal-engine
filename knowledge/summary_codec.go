package knowledge

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const summaryDomain = "ATE-KNOWLEDGE-SUMMARY\x00"

type summaryWriter struct {
	b   []byte
	err error
}

func (w *summaryWriter) text(value string) {
	if w.err == nil {
		w.b, w.err = codec.AppendString(w.b, value)
	}
}
func (w *summaryWriter) number(value uint64) { w.b = codec.AppendUint(w.b, value) }
func (w *summaryWriter) source(source SourceAnchor) {
	if w.err == nil {
		w.b, w.err = encodeSource(w.b, source)
	}
}
func (w *summaryWriter) ids(ids []model.ObjectID) {
	w.number(uint64(len(ids)))
	for _, id := range ids {
		w.text(string(id))
	}
}
func (w *summaryWriter) content(content RecordedContent) {
	w.text(string(content.Object))
	w.source(SourceAnchor{Anchor: content.Anchor})
	w.text(content.Encoding)
}

func sortedSummarySources(sources []SummarySource) []SummarySource {
	out := append([]SummarySource(nil), sources...)
	sort.Slice(out, func(i, j int) bool { return out[i].Observation < out[j].Observation })
	return out
}

// SummarySourceSetDigest binds the exact source position and selected adopted
// source identities/content/kinds. Topic labels, omission reasons and output
// bytes belong to the complete SummaryID instead of rewriting source identity.
func SummarySourceSetDigest(source SourceAnchor, sources []SummarySource) model.ObjectID {
	if source.Validate() != nil || source.Anchor.Knowledge == nil || source.Anchor.Cursor == nil || source.Frontier == "" || len(sources) == 0 || len(sources) > MaxItems {
		return ""
	}
	w := summaryWriter{b: codec.AppendUint([]byte("ATE-SUMMARY-SOURCESET\x00"), SummarySchema)}
	w.source(source)
	w.number(uint64(len(sources)))
	var previous model.ObjectID
	for _, item := range sortedSummarySources(sources) {
		if !model.ValidDigest(string(item.Observation)) || !model.ValidDigest(string(item.Content)) || !validSummaryPayloadKind(item.PayloadKind) || item.Observation == previous {
			return ""
		}
		previous = item.Observation
		w.text(string(item.Observation))
		w.text(string(item.Content))
		w.text(item.PayloadKind)
	}
	if w.err != nil {
		return ""
	}
	return knowledgeDigest(w.b)
}

func SummaryID(s Summary) model.ObjectID {
	raw, err := encodeSummary(s, false)
	if err != nil {
		return ""
	}
	return knowledgeDigest(append([]byte("ATE-SUMMARY-ID\x00"), raw...))
}

func (s Summary) Canonical() ([]byte, error) { return encodeSummary(s, true) }

func encodeSummary(s Summary, withID bool) ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	w := summaryWriter{b: codec.AppendUint([]byte(summaryDomain), SummarySchema)}
	w.text(string(s.Space))
	w.source(s.Source)
	w.number(uint64(len(s.Sources)))
	for _, source := range sortedSummarySources(s.Sources) {
		w.text(string(source.Observation))
		w.text(string(source.Content))
		w.text(source.PayloadKind)
	}
	w.text(string(s.SourceSetDigest))
	topics := append([]SummaryTopic(nil), s.Topics...)
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	w.number(uint64(len(topics)))
	for _, topic := range topics {
		w.text(topic.Name)
		ids := append([]model.ObjectID(nil), topic.Observations...)
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		w.ids(ids)
	}
	omissions := append([]SummaryOmission(nil), s.Omissions...)
	sort.Slice(omissions, func(i, j int) bool { return omissions[i].Observation < omissions[j].Observation })
	w.number(uint64(len(omissions)))
	for _, omission := range omissions {
		w.text(string(omission.Observation))
		w.text(omission.Reason)
	}
	app := s.Applicability
	w.text(app.Goal)
	w.text(app.Environment)
	w.number(app.SchemaVersion)
	tags := append([]string(nil), app.Tags...)
	sort.Strings(tags)
	w.number(uint64(len(tags)))
	for _, tag := range tags {
		w.text(tag)
	}
	w.number(uint64(s.Confidence))
	generation := s.Generation
	for _, value := range []string{generation.Activity, generation.Attempt, generation.Result, generation.Model, generation.PromptVersion, generation.PolicyVersion} {
		w.text(value)
	}
	w.content(generation.Input)
	w.content(generation.Output)
	w.text(s.GeneratedAt.UTC().Format(time.RFC3339Nano))
	w.text(s.VisibleAt.UTC().Format(time.RFC3339Nano))
	validation := s.Validation
	w.text(validation.Status)
	w.text(validation.Reviewer)
	w.text(validation.PolicyVersion)
	evidence := append([]RecordedContent(nil), validation.Evidence...)
	sort.Slice(evidence, func(i, j int) bool { return evidence[i].Object < evidence[j].Object })
	w.number(uint64(len(evidence)))
	for _, content := range evidence {
		w.content(content)
	}
	w.text(string(s.Replaces))
	if withID {
		if s.ID == "" || SummaryID(s) != s.ID {
			return nil, ErrInvalidSummary
		}
		w.text(string(s.ID))
	}
	if w.err != nil || len(w.b) > MaxRecordBytes {
		return nil, ErrInvalidSummary
	}
	return w.b, nil
}

type summaryReader struct {
	d   *codec.Decoder
	err error
}

func (r *summaryReader) text(maximum int) string {
	if r.err != nil {
		return ""
	}
	var value string
	value, r.err = r.d.String(uint64(maximum))
	return value
}
func (r *summaryReader) number() uint64 {
	if r.err != nil {
		return 0
	}
	var value uint64
	value, r.err = r.d.Uint()
	return value
}
func (r *summaryReader) count(maximum, minimumBytes int) int {
	n := r.number()
	if r.err != nil || n > uint64(maximum) || n > uint64(r.d.Remaining()/minimumBytes) {
		r.err = ErrInvalidSummary
		return 0
	}
	return int(n)
}
func (r *summaryReader) source() SourceAnchor {
	if r.err != nil {
		return SourceAnchor{}
	}
	var source SourceAnchor
	source, r.err = decodeSource(r.d)
	return source
}
func (r *summaryReader) ids() []model.ObjectID {
	n := r.count(MaxItems, 65)
	ids := make([]model.ObjectID, 0, n)
	for ; n > 0; n-- {
		ids = append(ids, model.ObjectID(r.text(64)))
	}
	return ids
}
func (r *summaryReader) content() RecordedContent {
	object := model.ObjectID(r.text(64))
	source := r.source()
	if source.ViewDigest != "" || source.Frontier != "" {
		r.err = ErrInvalidSummary
	}
	return RecordedContent{Object: object, Anchor: source.Anchor, Encoding: r.text(model.MaxIdentityBytes)}
}
func (r *summaryReader) timestamp() time.Time {
	raw := r.text(64)
	if r.err != nil {
		return time.Time{}
	}
	stamp, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || stamp.UTC().Format(time.RFC3339Nano) != raw {
		r.err = ErrInvalidSummary
	}
	return stamp
}

// DecodeSummary checks all counts before allocating, owns nested data, verifies
// both source-set and record identity, and rejects noncanonical permutations.
func DecodeSummary(raw []byte) (Summary, error) {
	if len(raw) > MaxRecordBytes || !bytes.HasPrefix(raw, []byte(summaryDomain)) {
		return Summary{}, ErrInvalidSummary
	}
	r := summaryReader{d: codec.NewDecoder(raw[len(summaryDomain):])}
	version := r.number()
	if r.err != nil {
		return Summary{}, ErrInvalidSummary
	}
	if version != SummarySchema {
		return Summary{}, fmt.Errorf("%w: summary %d", ErrUnsupportedSchema, version)
	}
	s := Summary{Space: model.SpaceID(r.text(model.MaxIdentityBytes)), Source: r.source()}
	for n := r.count(MaxItems, 131); n > 0; n-- {
		s.Sources = append(s.Sources, SummarySource{Observation: model.ObjectID(r.text(64)), Content: model.ObjectID(r.text(64)), PayloadKind: r.text(model.MaxIdentityBytes)})
	}
	s.SourceSetDigest = model.ObjectID(r.text(64))
	for n := r.count(MaxSummaryTopics, 2); n > 0; n-- {
		s.Topics = append(s.Topics, SummaryTopic{Name: r.text(model.MaxIdentityBytes), Observations: r.ids()})
	}
	for n := r.count(MaxItems, 66); n > 0; n-- {
		s.Omissions = append(s.Omissions, SummaryOmission{Observation: model.ObjectID(r.text(64)), Reason: r.text(model.MaxIdentityBytes)})
	}
	s.Applicability = SummaryApplicability{Goal: r.text(model.MaxIdentityBytes), Environment: r.text(model.MaxIdentityBytes), SchemaVersion: r.number()}
	for n := r.count(MaxSummaryTopics, 2); n > 0; n-- {
		s.Applicability.Tags = append(s.Applicability.Tags, r.text(model.MaxIdentityBytes))
	}
	confidence := r.number()
	if confidence > 1000 {
		r.err = ErrInvalidSummary
	}
	s.Confidence = uint32(confidence)
	s.Generation = SummaryGeneration{Activity: r.text(model.MaxIdentityBytes), Attempt: r.text(model.MaxIdentityBytes), Result: r.text(model.MaxIdentityBytes), Model: r.text(model.MaxIdentityBytes), PromptVersion: r.text(model.MaxIdentityBytes), PolicyVersion: r.text(model.MaxIdentityBytes), Input: r.content(), Output: r.content()}
	s.GeneratedAt, s.VisibleAt = r.timestamp(), r.timestamp()
	s.Validation = SummaryValidation{Status: r.text(model.MaxIdentityBytes), Reviewer: r.text(model.MaxIdentityBytes), PolicyVersion: r.text(model.MaxIdentityBytes)}
	for n := r.count(MaxSummaryReviewEvidence, 66); n > 0; n-- {
		s.Validation.Evidence = append(s.Validation.Evidence, r.content())
	}
	s.Replaces = model.ObjectID(r.text(64))
	s.ID = model.ObjectID(r.text(64))
	if r.err != nil || r.d.Done() != nil || !model.ValidDigest(string(s.ID)) {
		return Summary{}, ErrInvalidSummary
	}
	canonical, err := s.Canonical()
	if err != nil || !bytes.Equal(canonical, raw) {
		return Summary{}, ErrInvalidSummary
	}
	return s, nil
}
