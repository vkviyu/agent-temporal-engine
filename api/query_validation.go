package api

import (
	"github.com/vkviyu/agent-temporal-engine/model"
	"unicode/utf8"
)

func queryText(s string) bool { return s != "" && len(s) <= MaxIdentityBytes && utf8.ValidString(s) }

func validateQueryTurn(t *QueryTurn, kind string) error {
	if t == nil || t.ExpectedVersion == ^uint64(0) || !queryText(t.AdapterVersion) || !queryText(t.Phase) || t.Source.Validate() != nil || t.Source.Knowledge != nil || t.Source.Cursor != nil || len(t.Invocations) > MaxWorkSteps {
		return ErrInvalidRequest
	}
	if (kind == "query_answer") != (t.Next == nil) || (t.Next != nil && t.Next.Validate() != nil) || (kind != "query_decision" && len(t.Invocations) != 0) {
		return ErrInvalidRequest
	}
	if kind == "query_open" && (t.Sequence != 0 || t.ExpectedVersion != 0) {
		return ErrInvalidRequest
	}
	if t.Activity != "" && (len(t.Activity) > 1024 || !utf8.ValidString(t.Activity)) {
		return ErrInvalidRequest
	}
	seenCalls, seenActivities := map[string]bool{}, map[string]bool{}
	for i, inv := range t.Invocations {
		if !queryText(inv.ID) || !queryText(inv.Activity) || !queryText(inv.ProviderCallKey) || !queryText(inv.Tool) || !queryText(inv.ToolVersion) || !model.ValidDigest(string(inv.Definition)) || inv.AcceptedResult != "" || len(inv.Arguments) > MaxCommandBody || seenCalls[inv.ProviderCallKey] || seenActivities[inv.Activity] || (i > 0 && t.Invocations[i-1].ID >= inv.ID) {
			return ErrInvalidRequest
		}
		if inv.DispatchClass != "pure" && inv.DispatchClass != "idempotent" && inv.DispatchClass != "non_idempotent" {
			return ErrInvalidRequest
		}
		seenCalls[inv.ProviderCallKey], seenActivities[inv.Activity] = true, true
	}
	return nil
}
