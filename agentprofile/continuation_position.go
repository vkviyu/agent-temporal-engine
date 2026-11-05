package agentprofile

import (
	"reflect"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func controlPointerAllowed(t reflect.Type) bool {
	return t == reflect.TypeOf((*model.CommitFence)(nil)) || t == reflect.TypeOf((*ContinuationPosition)(nil)) || t == reflect.TypeOf((*model.SourcePublicationRef)(nil)) || t == reflect.TypeOf((*model.Cursor)(nil)) || t == reflect.TypeOf((*model.KnowledgeKey)(nil))
}

func continuationKnowledgeEmpty(p *ContinuationPosition) bool {
	return p.KnowledgeSource == nil && p.KnowledgeFence == (model.CommitFence{}) && p.KnowledgeCursor == (model.Cursor{}) && p.KnowledgeKey == (model.KnowledgeKey{})
}

func validateContinuationPosition(c Continuation) error {
	p := c.Position
	if p == nil {
		return ErrInvalid
	}
	knowledgeEmpty := continuationKnowledgeEmpty(p)
	if p.KnowledgeSource == nil && !knowledgeEmpty && (p.KnowledgeFence.Validate() != nil || p.KnowledgeCursor.Store == "" || p.KnowledgeCursor.Space == "" || !model.ValidDigest(string(p.KnowledgeCursor.Revision)) || p.KnowledgeKey.Execution != c.Execution) {
		return ErrInvalid
	}
	if source := p.KnowledgeSource; source != nil {
		if source.Validate() != nil || source.Space != p.Space || source.Store != c.InputFence.Store || source.Knowledge == nil || source.Cursor == nil || source.Knowledge.Execution != c.Execution || p.KnowledgeFence != (model.CommitFence{}) || p.KnowledgeCursor != (model.Cursor{}) || p.KnowledgeKey != (model.KnowledgeKey{}) {
			return ErrInvalid
		}
	}
	if !commandText(string(p.Principal)) || !commandText(string(p.Space)) || !commandText(p.AdapterVersion) ||
		len(p.NextActions) == 0 || len(p.NextActions) > MaxLifecycleRecords {
		return ErrInvalid
	}
	primary := false
	for i, ref := range p.NextActions {
		if !commandText(ref.Key) || ref.Generation == 0 ||
			(i > 0 && p.NextActions[i-1].Key >= ref.Key) {
			return ErrInvalid
		}
		primary = primary || ref.Key == c.PendingKey && ref.Generation == c.PendingGeneration
	}
	if !primary {
		return ErrInvalid
	}
	return nil
}

// validateContinuationRelations also runs on snapshot restore. Rebinding may
// advance the current action generation, but cannot erase the exact historical
// generations from which an accepted continuation must resume.
func validateContinuationRelations(c Continuation, snapshot Snapshot) error {
	if err := validateContinuationPosition(c); err != nil {
		return err
	}
	p := c.Position
	if p == nil {
		return nil
	}
	var execution Execution
	found := false
	for _, x := range snapshot.Executions {
		if x.ID == c.Execution {
			execution, found = x, true
		}
	}
	if !found {
		return ErrInvalid
	}
	knowledgeKey := p.KnowledgeKey
	if p.KnowledgeSource != nil {
		knowledgeKey = *p.KnowledgeSource.Knowledge
	}
	if knowledgeKey != (model.KnowledgeKey{}) && execution.Actor != knowledgeKey.Actor {
		return ErrInvalid
	}
	found = false
	for _, task := range snapshot.Tasks {
		found = found || task.ID == execution.Task && task.Space == p.Space && task.Owner == p.Principal
	}
	if !found {
		return ErrInvalid
	}
	if p.Step != 0 {
		found = false
		for _, step := range snapshot.Steps {
			found = found || step.Execution == c.Execution && step.Sequence == p.Step
		}
		if !found {
			return ErrInvalid
		}
	}
	if snapshot.Lifecycle.Recovery != nil {
		for _, resume := range snapshot.Lifecycle.Recovery.Resumes {
			if resume.Execution == c.Execution && resume.Next.Incarnation >= c.Incarnation && resume.AdapterVersion != p.AdapterVersion {
				return ErrInvalid
			}
		}
	}
	for _, ref := range p.NextActions {
		found = false
		if snapshot.Lifecycle.Recovery != nil {
			for _, intent := range snapshot.Lifecycle.Recovery.Actions {
				a := intent.Action
				found = found || a.Key == ref.Key && a.Generation == ref.Generation && a.Owner == c.Execution && a.Request.Space == p.Space && a.Request.Principal == p.Principal
			}
		}
		for _, a := range snapshot.Actions {
			found = found || a.Key == ref.Key && a.Generation == ref.Generation && a.Owner == c.Execution && a.Request.Space == p.Space && a.Request.Principal == p.Principal
		}
		if !found {
			return ErrInvalid
		}
	}
	return nil
}
