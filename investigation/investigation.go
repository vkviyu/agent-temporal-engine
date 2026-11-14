// Package investigation defines durable, bounded checkpoints for inspecting
// published evidence. The engine authenticates their citations and owners.
package investigation

import (
	"errors"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/model"
)

var (
	ErrInvalid     = errors.New("invalid investigation")
	ErrConflict    = errors.New("investigation version conflict")
	ErrLimit       = errors.New("investigation limit")
	ErrUnsupported = errors.New("unsupported investigation schema")
)

const (
	MaxFindings           = 32
	MaxFindingEdits       = 16
	MaxEvidencePerFinding = 8
	MaxQuestions          = 16
	MaxCitations          = 32
	MaxFindingTextBytes   = 8192
	MaxNoteBytes          = 2048
)

// Citation identifies a node as seen at one published cut. It carries no
// permission to read the referenced content and makes no claim of causality.
type Citation struct {
	Ref    model.NodeRef
	Anchor model.SnapshotAnchor
}

func (c Citation) Validate() error {
	a := c.Anchor
	if c.Ref.Validate() != nil || a.Validate() != nil || !ValidID(string(a.Space)) || !ValidID(string(a.Fence.Store)) {
		return ErrInvalid
	}
	if k := a.Knowledge; k != nil && (!ValidID(string(k.Actor)) || !ValidID(string(k.Execution))) {
		return ErrInvalid
	}
	if c.Ref.Kind == model.NodeEntity {
		e := c.Ref.Entity
		if !ValidID(string(e.Profile)) || !ValidID(string(e.Kind)) || !validText(string(e.ID), 4096) {
			return ErrInvalid
		}
	}
	return nil
}

func (c Citation) clone() Citation {
	c.Anchor = c.Anchor.Clone()
	return c
}

// Finding is a caller-authored assertion. Evidence records its cited sources.
type Finding struct {
	ID       string
	Text     string
	Evidence []Citation
}

// Position saves the exact focus node and selectors for a new context read.
// Page tokens belong to the live view and are excluded from the checkpoint.
type Position struct {
	Focus    Citation
	Mode     string
	Sections []string `json:",omitempty"`
}

// ViewMode selects the OpenView mode for a Position's GetNodeContext request.
func (p Position) ViewMode() string {
	if p.Focus.Anchor.Knowledge != nil {
		return "execution_knowledge"
	}
	if p.Mode == "retrospective" {
		return "audit_snapshot"
	}
	return "state"
}

func (p Position) Validate() error {
	if err := p.Focus.Validate(); err != nil {
		return err
	}
	switch p.Mode {
	case "recorded_input", "evidence_neighborhood":
	case "as_known_at_anchor":
		if p.Focus.Anchor.Knowledge == nil {
			return ErrInvalid
		}
	case "retrospective":
		if p.Focus.Anchor.Knowledge != nil {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	if len(p.Sections) > 8 {
		return ErrLimit
	}
	seen := make(map[string]bool, len(p.Sections))
	for _, section := range p.Sections {
		switch section {
		case "node", "inputs":
		case "upstream", "downstream", "stateDiff", "messages", "activityReceipts", "artifacts":
			if p.Mode == "recorded_input" {
				return ErrInvalid
			}
		default:
			return ErrInvalid
		}
		if seen[section] {
			return ErrInvalid
		}
		seen[section] = true
	}
	return nil
}

// Checkpoint is the current version of one investigation. Prior committed
// versions can be read at their original publication fences.
type Checkpoint struct {
	ID            string
	Version       uint64
	Subject       Citation
	Findings      []Finding  `json:",omitempty"`
	OpenQuestions []string   `json:",omitempty"`
	NextFocus     string     `json:",omitempty"`
	Position      *Position  `json:",omitempty"`
	Sources       []Citation `json:",omitempty"`
}

func (c Checkpoint) Validate() error {
	if !ValidID(c.ID) || c.Version == 0 || !validText(c.NextFocus, MaxNoteBytes) {
		return ErrInvalid
	}
	if len(c.Findings) > MaxFindings || len(c.Sources) > MaxCitations {
		return ErrLimit
	}
	if err := validateQuestions(c.OpenQuestions); err != nil {
		return err
	}
	for i, finding := range c.Findings {
		if i > 0 && c.Findings[i-1].ID >= finding.ID {
			return ErrInvalid
		}
		if err := validateFinding(finding); err != nil {
			return err
		}
	}
	if c.Position != nil {
		if err := c.Position.Validate(); err != nil {
			return err
		}
	}
	if len(uniqueCitations(c.Sources)) != len(c.Sources) {
		return ErrInvalid
	}
	return validateCitations(c.Citations())
}

// Citations returns the unique direct dependencies of the checkpoint,
// including the immutable subject and the current position's focus.
func (c Checkpoint) Citations() []Citation {
	return collectCitations(&c.Subject, c.Position, c.Findings, c.Sources)
}

func validText(value string, maxBytes int) bool {
	return len(value) <= maxBytes && utf8.ValidString(value)
}

// ValidID reports whether a caller-provided identifier is valid.
func ValidID(value string) bool {
	return value != "" && validText(value, model.MaxIdentityBytes)
}

func validateQuestions(questions []string) error {
	if len(questions) > MaxQuestions {
		return ErrLimit
	}
	for _, question := range questions {
		if question == "" || !validText(question, MaxNoteBytes) {
			return ErrInvalid
		}
	}
	return nil
}

func validateFinding(f Finding) error {
	if !ValidID(f.ID) || f.Text == "" || !validText(f.Text, MaxFindingTextBytes) || len(f.Evidence) == 0 {
		return ErrInvalid
	}
	if len(f.Evidence) > MaxEvidencePerFinding {
		return ErrLimit
	}
	if len(uniqueCitations(f.Evidence)) != len(f.Evidence) {
		return ErrInvalid
	}
	return nil
}

func validateCitations(citations []Citation) error {
	if len(citations) > MaxCitations {
		return ErrLimit
	}
	for _, citation := range citations {
		if err := citation.Validate(); err != nil {
			return err
		}
		if citation.Anchor.Space != citations[0].Anchor.Space || citation.Anchor.Fence.Store != citations[0].Anchor.Fence.Store {
			return ErrInvalid
		}
	}
	return nil
}

type citationIdentity struct {
	ref       model.NodeRef
	space     model.SpaceID
	fence     model.CommitFence
	cursor    model.Cursor
	knowledge model.KnowledgeKey
	hasCursor bool
	hasKey    bool
}

func uniqueCitations(citations []Citation) []Citation {
	var result []Citation
	seen := make(map[citationIdentity]bool, len(citations))
	for _, citation := range citations {
		a := citation.Anchor
		id := citationIdentity{ref: citation.Ref, space: a.Space, fence: a.Fence, hasCursor: a.Cursor != nil, hasKey: a.Knowledge != nil}
		if a.Cursor != nil {
			id.cursor = *a.Cursor
		}
		if a.Knowledge != nil {
			id.knowledge = *a.Knowledge
		}
		if !seen[id] {
			seen[id] = true
			result = append(result, citation.clone())
		}
	}
	return result
}

func collectCitations(subject *Citation, position *Position, findings []Finding, sources []Citation) []Citation {
	var citations []Citation
	if subject != nil {
		citations = append(citations, *subject)
	}
	if position != nil {
		citations = append(citations, position.Focus)
	}
	for _, finding := range findings {
		citations = append(citations, finding.Evidence...)
	}
	return uniqueCitations(append(citations, sources...))
}
