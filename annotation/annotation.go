// Package annotation stores host-accepted, sourced navigation judgments.
// Records are immutable and retractions append records; neither changes facts,
// execution state, access policy, required context, or causal trace edges.
package annotation

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/model"
)

var (
	ErrInvalid     = errors.New("invalid annotation")
	ErrConflict    = errors.New("annotation conflict")
	ErrUnsupported = errors.New("unsupported annotation schema")
	ErrLimit       = errors.New("annotation limit")
)

const (
	MaxRecords       = 1024
	MaxRecordBytes   = 64 << 10
	MaxSnapshotBytes = 1 << 20
	MaxEvidence      = 64
	LabelDuplicate   = "duplicate"
	LabelSuperseded  = "superseded"
	LabelIrrelevant  = "irrelevant"
	LabelRetracted   = "retracted"
	LabelTitle       = "title"
	LabelNote        = "note"
)

// Record is an accepted judgment, not an authoritative replacement for Target.
// Source fixes the read cut at which Target and every Evidence ref were visible.
// Retracts names one earlier record in this owner's partition. A retraction has
// no Label/Text/Hide; its Source, Target, Evidence and Reason remain auditable.
type Record struct {
	ID       model.ObjectID
	Target   model.NodeRef
	Label    string
	Text     string
	Reason   string
	Hide     bool
	Source   model.SnapshotAnchor
	Evidence []model.NodeRef
	Retracts model.ObjectID
}

type Snapshot struct{ Records []Record }

func (r Record) Clone() Record {
	r.Source = r.Source.Clone()
	r.Evidence = append([]model.NodeRef(nil), r.Evidence...)
	return r
}

func validText(s string, max int, required bool) bool {
	return (!required || len(s) > 0) && len(s) <= max && utf8.ValidString(s)
}

// Judgments may cite factual nodes. Another annotation is not new evidence;
// withdrawals name prior judgments only through the explicit Retracts field.
func validFactRef(ref model.NodeRef) bool {
	return ref.Validate() == nil && !(ref.Kind == model.NodeEntity && ref.Entity.Profile == "annotation")
}

func validateRecord(r Record) error {
	if !validFactRef(r.Target) || r.Source.Validate() != nil || !validText(r.Reason, 8192, true) || !validText(r.Text, 32768, false) {
		return ErrInvalid
	}
	if len(r.Evidence) > MaxEvidence {
		return ErrLimit
	}
	seen := map[model.NodeRef]bool{}
	for _, ref := range r.Evidence {
		if !validFactRef(ref) || seen[ref] {
			return ErrInvalid
		}
		seen[ref] = true
	}
	if r.Retracts != "" {
		if !model.ValidDigest(string(r.Retracts)) || r.Label != "" || r.Text != "" || r.Hide {
			return ErrInvalid
		}
	} else {
		switch r.Label {
		case LabelDuplicate, LabelSuperseded, LabelIrrelevant, LabelRetracted, LabelTitle, LabelNote:
		default:
			return ErrInvalid
		}
		if r.Label == LabelTitle && r.Text == "" {
			return ErrInvalid
		}
	}
	return nil
}

// NewRecord computes the identity over all judgment and provenance fields.
func NewRecord(r Record) (Record, error) {
	r = r.Clone()
	r.ID = ""
	if err := validateRecord(r); err != nil {
		return Record{}, err
	}
	raw, err := json.Marshal(r)
	if err != nil || len(raw) > MaxRecordBytes {
		return Record{}, ErrLimit
	}
	h := sha256.New()
	h.Write([]byte("ATE-ANNOTATION\x00v1\x00"))
	h.Write(raw)
	r.ID = model.ObjectID(hex.EncodeToString(h.Sum(nil)))
	return r, nil
}

func EncodeRecord(r Record) ([]byte, error) {
	expected, err := NewRecord(r)
	if err != nil {
		return nil, err
	}
	if r.ID != expected.ID {
		return nil, ErrInvalid
	}
	raw, err := json.Marshal(expected)
	if err != nil || len(raw) > MaxRecordBytes {
		return nil, ErrLimit
	}
	return append([]byte("ATE-ANNOTATION\x00v1\x00"), raw...), nil
}

func decodeJSON(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return ErrInvalid
	}
	if d.Decode(new(any)) != io.EOF {
		return ErrInvalid
	}
	return nil
}

func DecodeRecord(raw []byte) (Record, error) {
	const prefix = "ATE-ANNOTATION\x00v1\x00"
	if len(raw) > MaxRecordBytes+len(prefix) {
		return Record{}, ErrLimit
	}
	if !bytes.HasPrefix(raw, []byte(prefix)) {
		if bytes.HasPrefix(raw, []byte("ATE-ANNOTATION\x00")) {
			return Record{}, ErrUnsupported
		}
		return Record{}, ErrInvalid
	}
	var r Record
	if err := decodeJSON(raw[len(prefix):], &r); err != nil {
		return Record{}, err
	}
	canonical, err := EncodeRecord(r)
	if err != nil {
		return Record{}, err
	}
	if !bytes.Equal(raw, canonical) {
		return Record{}, ErrInvalid
	}
	return r, nil
}

// Validate checks append order as well as the identity of every retained record.
func (s Snapshot) Validate() error {
	if len(s.Records) > MaxRecords {
		return ErrLimit
	}
	seen := map[model.ObjectID]Record{}
	withdrawn := map[model.ObjectID]bool{}
	for _, r := range s.Records {
		if _, err := EncodeRecord(r); err != nil {
			return err
		}
		if _, ok := seen[r.ID]; ok {
			return ErrConflict
		}
		if r.Retracts != "" {
			prior, ok := seen[r.Retracts]
			if !ok || prior.Retracts != "" || prior.Target != r.Target || withdrawn[r.Retracts] {
				return ErrConflict
			}
			if prior.Source.Space != r.Source.Space || prior.Source.Fence.Store != r.Source.Fence.Store || prior.Source.Fence.LSN > r.Source.Fence.LSN {
				return ErrInvalid
			}
			withdrawn[r.Retracts] = true
		}
		seen[r.ID] = r
	}
	return nil
}

// Apply appends an owned record. Request-key idempotency belongs to the engine;
// a second request cannot silently manufacture a second acceptance of one ID.
func Apply(s Snapshot, r Record) (Snapshot, error) {
	if err := s.Validate(); err != nil {
		return Snapshot{}, err
	}
	next := Snapshot{Records: make([]Record, 0, len(s.Records)+1)}
	for _, prior := range s.Records {
		next.Records = append(next.Records, prior.Clone())
	}
	next.Records = append(next.Records, r.Clone())
	if err := next.Validate(); err != nil {
		return Snapshot{}, err
	}
	return next, nil
}

// Hidden returns the union of active hide judgments. Withdrawing one judgment
// never cancels an independent host judgment for the same target.
func (s Snapshot) Hidden() map[model.NodeRef]bool {
	withdrawn := map[model.ObjectID]bool{}
	for _, r := range s.Records {
		if r.Retracts != "" {
			withdrawn[r.Retracts] = true
		}
	}
	hidden := map[model.NodeRef]bool{}
	for _, r := range s.Records {
		if r.Retracts == "" && r.Hide && !withdrawn[r.ID] {
			hidden[r.Target] = true
		}
	}
	return hidden
}

func EncodeSnapshot(s Snapshot) ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(s)
	if err != nil || len(raw) > MaxSnapshotBytes {
		return nil, ErrLimit
	}
	return append([]byte("ATE-ANNOTATIONS\x00v1\x00"), raw...), nil
}
func DecodeSnapshot(raw []byte) (Snapshot, error) {
	const prefix = "ATE-ANNOTATIONS\x00v1\x00"
	if len(raw) > MaxSnapshotBytes+len(prefix) {
		return Snapshot{}, ErrLimit
	}
	if !bytes.HasPrefix(raw, []byte(prefix)) {
		if bytes.HasPrefix(raw, []byte("ATE-ANNOTATIONS\x00")) {
			return Snapshot{}, ErrUnsupported
		}
		return Snapshot{}, ErrInvalid
	}
	var s Snapshot
	if err := decodeJSON(raw[len(prefix):], &s); err != nil {
		return Snapshot{}, err
	}
	canonical, err := EncodeSnapshot(s)
	if err != nil {
		return Snapshot{}, err
	}
	if !bytes.Equal(raw, canonical) {
		return Snapshot{}, ErrInvalid
	}
	return s, nil
}
