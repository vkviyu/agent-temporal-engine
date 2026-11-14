package agentprofile

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// ExactRequestDigest commits to the complete framed bytes sent to the model or
// executor. Framing and encoding are part of those bytes; transport deadlines,
// connection identity and returned result bytes are not. A digest binds the
// recorded request, not the truth of any external result or attestation.
func ExactRequestDigest(request []byte) model.ObjectID {
	sum := sha256.Sum256(request)
	return model.ObjectID(hex.EncodeToString(sum[:]))
}

// ContentReferences extracts the schema's strong content references. Entity
// identities, attestation/request digests and embedded Knowledge PacketIDs are
// different reference classes. They must not be passed to a raw object reader.
// This extractor leaves the original snapshot wire bytes unchanged.
func ContentReferences(s Snapshot) []model.ObjectID {
	if s.Lifecycle.Recovery == nil {
		return nil
	}
	var refs []model.ObjectID
	r := s.Lifecycle.Recovery
	for _, invocation := range s.Invocations {
		refs = append(refs, invocation.ToolDefinition)
	}
	for _, a := range r.ActivitySpecs {
		refs = append(refs, a.Request, a.Contract.Definition, a.FinalAnswerInput)
	}
	for _, a := range r.Actions {
		refs = append(refs, a.InputObjects...)
	}
	for _, a := range r.Resumes {
		refs = append(refs, a.Checkpoint)
	}
	for _, a := range r.Outcomes {
		refs = append(refs, a.Result, a.Validation)
	}
	for _, requirement := range r.ValidationRequirements {
		refs = append(refs, requirement.Spec.Result, requirement.Spec.Command, requirement.Spec.Environment)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i] < refs[j] })
	out := refs[:0]
	for _, id := range refs {
		if id != "" && (len(out) == 0 || out[len(out)-1] != id) {
			out = append(out, id)
		}
	}
	return out
}

// PublicationReferences extracts immutable transaction dependencies separately
// from raw content ObjectIDs. A producer TxID must be resolved through the
// authenticated publication index; it is never read as a content address.
// AuditFull retention preserves these producers and their exact root sets.
func PublicationReferences(s Snapshot) []model.SourcePublicationRef {
	byKey := map[string]model.SourcePublicationRef{}
	for _, continuation := range s.Lifecycle.Continuations {
		if continuation.Position == nil || continuation.Position.KnowledgeSource == nil {
			continue
		}
		source := continuation.Position.KnowledgeSource.Clone()
		raw, _ := json.Marshal(source) // fixed value struct, used only as a dedup key
		byKey[string(raw)] = source
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]model.SourcePublicationRef, 0, len(keys))
	for _, key := range keys {
		out = append(out, byKey[key])
	}
	return out
}
