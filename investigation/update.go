package investigation

import (
	"math"
	"slices"
	"strings"
)

// Update creates an investigation when ExpectedVersion is zero and Subject is
// set. Later updates keep the original subject, and compare ExpectedVersion
// against this investigation only. Findings upserts by ID. A nonnil question
// list or next-focus note replaces that field, including an empty value.
// Position, when supplied, replaces the saved browse target. Sources declares
// the origins of derived text, including origins omitted from a finding's
// citations; these dependencies are retained by later versions.
type Update struct {
	ID              string
	ExpectedVersion uint64
	Subject         *Citation  `json:",omitempty"`
	Findings        []Finding  `json:",omitempty"`
	RemoveFindings  []string   `json:",omitempty"`
	OpenQuestions   *[]string  `json:",omitempty"`
	NextFocus       *string    `json:",omitempty"`
	Position        *Position  `json:",omitempty"`
	Sources         []Citation `json:",omitempty"`
}

func (u Update) Validate() error {
	if !ValidID(u.ID) || (u.ExpectedVersion == 0) != (u.Subject != nil) {
		return ErrInvalid
	}
	if len(u.Findings)+len(u.RemoveFindings) > MaxFindingEdits || len(u.Sources) > MaxCitations || u.ExpectedVersion == math.MaxUint64 {
		return ErrLimit
	}
	if u.Subject != nil && len(u.RemoveFindings) != 0 {
		return ErrInvalid
	}
	if u.Subject == nil && len(u.Findings)+len(u.RemoveFindings)+len(u.Sources) == 0 && u.OpenQuestions == nil && u.NextFocus == nil && u.Position == nil {
		return ErrInvalid
	}
	if u.OpenQuestions != nil {
		if err := validateQuestions(*u.OpenQuestions); err != nil {
			return err
		}
	}
	if u.NextFocus != nil && !validText(*u.NextFocus, MaxNoteBytes) {
		return ErrInvalid
	}
	if u.Position != nil {
		if err := u.Position.Validate(); err != nil {
			return err
		}
	}
	seen := make(map[string]bool, len(u.Findings)+len(u.RemoveFindings))
	for _, id := range u.RemoveFindings {
		if !ValidID(id) || seen[id] {
			return ErrInvalid
		}
		seen[id] = true
	}
	for _, finding := range u.Findings {
		if err := validateFinding(finding); err != nil {
			return err
		}
		if seen[finding.ID] {
			return ErrInvalid
		}
		seen[finding.ID] = true
	}
	return validateCitations(u.Citations())
}

// Citations returns the unique explicit references supplied by the update.
func (u Update) Citations() []Citation {
	return collectCitations(u.Subject, u.Position, u.Findings, u.Sources)
}

// Apply computes the next version without I/O. The engine must independently
// verify ownership, the authenticated sources, and publication of the result.
func Apply(current *Checkpoint, update Update) (Checkpoint, error) {
	if err := update.Validate(); err != nil {
		return Checkpoint{}, err
	}
	next := Checkpoint{ID: update.ID, Version: update.ExpectedVersion + 1}
	if current == nil {
		if update.ExpectedVersion != 0 {
			return Checkpoint{}, ErrConflict
		}
		next.Subject = update.Subject.clone()
	} else {
		if current.ID != update.ID || current.Version != update.ExpectedVersion {
			return Checkpoint{}, ErrConflict
		}
		if err := current.Validate(); err != nil {
			return Checkpoint{}, err
		}
		next = *current
		next.Version++
		next.Subject = next.Subject.clone()
		next.OpenQuestions = slices.Clone(next.OpenQuestions)
	}
	if update.OpenQuestions != nil {
		next.OpenQuestions = slices.Clone(*update.OpenQuestions)
	}
	if update.NextFocus != nil {
		next.NextFocus = *update.NextFocus
	}
	if update.Position != nil {
		next.Position = update.Position
	}
	if next.Position != nil {
		position := *next.Position
		position.Focus = position.Focus.clone()
		position.Sections = slices.Clone(position.Sections)
		next.Position = &position
	}
	sources := append(slices.Clone(next.Sources), update.Sources...)
	next.Sources = uniqueCitations(sources)
	findings, err := editFindings(next.Findings, update.Findings, update.RemoveFindings)
	if err != nil {
		return Checkpoint{}, err
	}
	next.Findings = findings
	if err = next.Validate(); err != nil {
		return Checkpoint{}, err
	}
	return next, nil
}

func editFindings(prior, edits []Finding, removals []string) ([]Finding, error) {
	byID := make(map[string]Finding, len(prior)+len(edits))
	for _, finding := range prior {
		byID[finding.ID] = finding
	}
	for _, id := range removals {
		if _, found := byID[id]; !found {
			return nil, ErrConflict
		}
		delete(byID, id)
	}
	for _, finding := range edits {
		byID[finding.ID] = finding
	}
	result := make([]Finding, 0, len(byID))
	for _, finding := range byID {
		finding.Evidence = uniqueCitations(finding.Evidence)
		result = append(result, finding)
	}
	slices.SortFunc(result, func(a, b Finding) int { return strings.Compare(a.ID, b.ID) })
	return result, nil
}
