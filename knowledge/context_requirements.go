package knowledge

import (
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// ContextRequirement binds a structural obligation to exact adopted bytes in
// this packet. Kind/ID are logical identity, never a substitute for content.
// The agent-profile engine independently discovers and verifies this set.
type ContextRequirement struct {
	Kind        string
	ID          string
	Observation model.ObjectID
	Content     model.ObjectID
}

// ContextRequirements records controller responsibility separately from the
// pure benefit/cost heuristic. HostPolicyVersion names the host's renderer,
// semantic-candidate and token-counting policy. It is an auditable assertion,
// not evidence that a model has read or understood any bytes.
type ContextRequirements struct {
	RulesVersion      string
	HostPolicyVersion string
	Facts             []ContextRequirement
}

func (r ContextRequirements) Clone() ContextRequirements {
	r.Facts = append([]ContextRequirement(nil), r.Facts...)
	return r
}

func validateContextRequirements(p ContextPacket) error {
	if p.Requirements == nil {
		return nil
	}
	r := p.Requirements
	if p.Key == nil || !validText(r.RulesVersion) || !validText(r.HostPolicyVersion) || len(r.Facts) == 0 || len(r.Facts) > MaxItems {
		return ErrContextSelectionInvalid
	}
	items := map[model.ObjectID]model.ObjectID{}
	for _, item := range p.Items {
		items[item.Observation] = item.Content
	}
	mandatory := map[model.ObjectID]bool{}
	if p.Selection != nil {
		for _, id := range p.Selection.Proposal.Mandatory {
			mandatory[id] = true
		}
	}
	for i, fact := range r.Facts {
		if !validText(fact.Kind) || !validText(fact.ID) || !model.ValidDigest(string(fact.Observation)) || !model.ValidDigest(string(fact.Content)) || items[fact.Observation] != fact.Content {
			return ErrContextSelectionInvalid
		}
		if i > 0 {
			prev := r.Facts[i-1]
			if prev.Kind > fact.Kind || (prev.Kind == fact.Kind && prev.ID >= fact.ID) {
				return ErrContextSelectionInvalid
			}
		}
		if p.Selection != nil && !mandatory[fact.Observation] {
			return ErrContextSelectionInvalid
		}
	}
	return nil
}

func encodeContextRequirements(r ContextRequirements) []byte {
	raw := codec.AppendUint(nil, 1)
	raw, _ = codec.AppendString(raw, r.RulesVersion)
	raw, _ = codec.AppendString(raw, r.HostPolicyVersion)
	raw = codec.AppendUint(raw, uint64(len(r.Facts)))
	for _, fact := range r.Facts {
		for _, text := range []string{fact.Kind, fact.ID, string(fact.Observation), string(fact.Content)} {
			raw, _ = codec.AppendString(raw, text)
		}
	}
	return raw
}

func decodeContextRequirements(raw []byte) (ContextRequirements, error) {
	d := codec.NewDecoder(raw)
	v, err := d.Uint()
	if err != nil || v != 1 {
		return ContextRequirements{}, ErrContextSelectionInvalid
	}
	r := ContextRequirements{}
	r.RulesVersion, err = d.String(model.MaxIdentityBytes)
	if err != nil {
		return r, err
	}
	r.HostPolicyVersion, err = d.String(model.MaxIdentityBytes)
	if err != nil {
		return r, err
	}
	n, err := d.Uint()
	if err != nil || n == 0 || n > MaxItems || n > uint64(d.Remaining()/134) {
		return r, ErrContextSelectionInvalid
	}
	r.Facts = make([]ContextRequirement, int(n))
	for i := range r.Facts {
		values := []string{}
		for _, limit := range []uint64{model.MaxIdentityBytes, model.MaxIdentityBytes, 64, 64} {
			value, err := d.String(limit)
			if err != nil {
				return r, err
			}
			values = append(values, value)
		}
		r.Facts[i] = ContextRequirement{Kind: values[0], ID: values[1], Observation: model.ObjectID(values[2]), Content: model.ObjectID(values[3])}
	}
	if err = d.Done(); err != nil {
		return r, err
	}
	return r, nil
}
