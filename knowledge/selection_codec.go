package knowledge

import (
	"bytes"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// ContextSelectionProposal is the complete recorded selector input without a
// caller-supplied Snapshot. The core resolves Snapshot from Source's published
// owner/knowledge cut. Costs and benefits are explicit host proposals; this
// record never claims that the engine inferred semantic relevance or tokens.
type ContextSelectionProposal struct {
	Source    SourceAnchor
	Capacity  ContextCapacity
	Mandatory []model.ObjectID
	// Costs is the complete bounded candidate set considered by this proposal.
	// Other adopted observations were not evaluated and are not in Omitted.
	Costs         []ContextCost
	Bundles       []ContextBundle
	Tokenizer     string
	CountQuality  string
	PolicyVersion string
}

// ContextSelectionRecord persists both the original mandatory proposal and
// the expanded, deterministic selection including every proposed omission.
// It is metadata, not a read grant for omitted source content.
type ContextSelectionRecord struct {
	Proposal ContextSelectionProposal
	Result   ContextSelection
}

func (p ContextSelectionProposal) request(snapshot Snapshot) ContextSelectionRequest {
	return ContextSelectionRequest{Snapshot: snapshot, Source: p.Source.clone(), Capacity: p.Capacity, Mandatory: append([]model.ObjectID(nil), p.Mandatory...), Costs: append([]ContextCost(nil), p.Costs...), Bundles: cloneContextBundles(p.Bundles), Tokenizer: p.Tokenizer, CountQuality: p.CountQuality, PolicyVersion: p.PolicyVersion}
}

func cloneContextBundles(values []ContextBundle) []ContextBundle {
	out := append([]ContextBundle(nil), values...)
	for i := range out {
		out[i].Evidence = append([]model.ObjectID(nil), values[i].Evidence...)
		out[i].Counterexamples = append([]model.ObjectID(nil), values[i].Counterexamples...)
	}
	return out
}

func (r ContextSelectionRecord) Clone() ContextSelectionRecord {
	r.Proposal.Source = r.Proposal.Source.clone()
	r.Proposal.Mandatory = append([]model.ObjectID(nil), r.Proposal.Mandatory...)
	r.Proposal.Costs = append([]ContextCost(nil), r.Proposal.Costs...)
	r.Proposal.Bundles = cloneContextBundles(r.Proposal.Bundles)
	r.Result.Source = r.Result.Source.clone()
	r.Result.Mandatory = append([]model.ObjectID(nil), r.Result.Mandatory...)
	r.Result.Selected = append([]model.ObjectID(nil), r.Result.Selected...)
	r.Result.SelectedBundles = append([]model.ObjectID(nil), r.Result.SelectedBundles...)
	r.Result.Omitted = append([]ContextOmission(nil), r.Result.Omitted...)
	return r
}

// RecordContextSelection records owned, canonical inputs and the pure result.
// ContextPacket.EstimatedTokens must be UsedTokens + FramingReserve: input
// segment costs plus input framing, excluding the output reserve. Exact versus
// estimated remains declared by CountQuality and does not replace counting the
// complete RecordedInput at the model adapter.
func RecordContextSelection(req ContextSelectionRequest) (ContextSelectionRecord, error) {
	if req.PolicyVersion == "" {
		req.PolicyVersion = ContextSelectionPolicy
	}
	result, err := SelectContext(req)
	if err != nil {
		return ContextSelectionRecord{}, err
	}
	p := ContextSelectionProposal{Source: req.Source.clone(), Capacity: req.Capacity, Mandatory: req.Mandatory, Costs: req.Costs, Bundles: req.Bundles, Tokenizer: req.Tokenizer, CountQuality: req.CountQuality, PolicyVersion: req.PolicyVersion}
	p, _, err = canonicalContextProposal(p)
	if err != nil {
		return ContextSelectionRecord{}, err
	}
	return (ContextSelectionRecord{Proposal: p, Result: result}).Clone(), nil
}

func sameContextSource(a, b SourceAnchor) bool {
	x, err := encodeSource(nil, a)
	if err != nil {
		return false
	}
	y, err := encodeSource(nil, b)
	return err == nil && bytes.Equal(x, y)
}

// canonicalContextProposal uses the same commitment domain as SelectContext.
// Structural validation is bounded independently of historical membership.
func canonicalContextProposal(p ContextSelectionProposal) (ContextSelectionProposal, []byte, error) {
	if len(p.Mandatory) > MaxItems || len(p.Costs) > MaxItems || len(p.Bundles) > MaxContextBundles {
		return ContextSelectionProposal{}, nil, ErrContextSelectionLimit
	}
	c := p.Capacity
	if p.Source.Validate() != nil || p.Source.Anchor.Knowledge == nil || p.Source.Anchor.Cursor == nil || p.Source.Frontier == "" || !validText(p.Tokenizer) || (p.CountQuality != TokenCountEstimated && p.CountQuality != TokenCountExact) || p.PolicyVersion != ContextSelectionPolicy || c.Window == 0 || c.Window > MaxEstimatedTokens || c.OutputReserve > c.Window || c.FramingReserve > c.Window-c.OutputReserve {
		return ContextSelectionProposal{}, nil, ErrContextSelectionInvalid
	}
	var err error
	p.Source = p.Source.clone()
	p.Mandatory, err = sortedContextIDs(p.Mandatory)
	if err != nil {
		return ContextSelectionProposal{}, nil, err
	}
	p.Costs = append([]ContextCost(nil), p.Costs...)
	sort.Slice(p.Costs, func(i, j int) bool { return p.Costs[i].Observation < p.Costs[j].Observation })
	for i, cost := range p.Costs {
		if !model.ValidDigest(string(cost.Observation)) || cost.Tokens > MaxEstimatedTokens || (i > 0 && p.Costs[i-1].Observation == cost.Observation) {
			return ContextSelectionProposal{}, nil, ErrContextSelectionInvalid
		}
	}
	p.Bundles = cloneContextBundles(p.Bundles)
	sort.Slice(p.Bundles, func(i, j int) bool { return p.Bundles[i].ID < p.Bundles[j].ID })
	references := 0
	conclusions := map[model.ObjectID]bool{}
	for i := range p.Bundles {
		b := &p.Bundles[i]
		references += 1 + len(b.Evidence) + len(b.Counterexamples)
		if references > maxContextReferences {
			return ContextSelectionProposal{}, nil, ErrContextSelectionLimit
		}
		if !model.ValidDigest(string(b.ID)) || ContextBundleID(*b) != b.ID || conclusions[b.Conclusion] || (i > 0 && p.Bundles[i-1].ID == b.ID) {
			return ContextSelectionProposal{}, nil, ErrContextSelectionInvalid
		}
		conclusions[b.Conclusion] = true
		b.Evidence, _ = sortedContextIDs(b.Evidence)
		b.Counterexamples, _ = sortedContextIDs(b.Counterexamples)
	}
	raw, _ := encodeSource(codec.AppendUint([]byte("ATE-CONTEXT-PROPOSAL\x00"), 1), p.Source)
	for _, v := range []uint64{c.Window, c.OutputReserve, c.FramingReserve} {
		raw = codec.AppendUint(raw, v)
	}
	for _, value := range []string{p.PolicyVersion, p.Tokenizer, p.CountQuality} {
		raw, _ = codec.AppendString(raw, value)
	}
	raw = appendContextIDs(raw, p.Mandatory)
	raw = codec.AppendUint(raw, uint64(len(p.Costs)))
	for _, cost := range p.Costs {
		raw, _ = codec.AppendString(raw, string(cost.Observation))
		raw = codec.AppendUint(raw, cost.Tokens)
	}
	raw = codec.AppendUint(raw, uint64(len(p.Bundles)))
	for _, bundle := range p.Bundles {
		encoded, _ := contextBundleBytes(bundle)
		raw, _ = codec.AppendBytes(raw, encoded)
	}
	if len(raw) > MaxRecordBytes {
		return ContextSelectionProposal{}, nil, ErrContextSelectionLimit
	}
	return p, raw, nil
}

func encodeContextSelection(r ContextSelectionRecord) ([]byte, error) {
	p, proposal, err := canonicalContextProposal(r.Proposal)
	if err != nil {
		return nil, err
	}
	v := r.Result
	if !sameContextSource(p.Source, v.Source) || p.Capacity != v.Capacity || p.Tokenizer != v.Tokenizer || p.CountQuality != v.CountQuality || p.PolicyVersion != v.PolicyVersion || knowledgeDigest(proposal) != v.ProposalDigest || v.InputLimit != p.Capacity.Window-p.Capacity.OutputReserve-p.Capacity.FramingReserve || v.UsedTokens > v.InputLimit || len(v.Selected) > MaxItems || len(v.Mandatory) > MaxItems || len(v.SelectedBundles) > MaxContextBundles || len(v.Omitted) > MaxItems {
		return nil, ErrContextSelectionInvalid
	}
	seen := map[model.ObjectID]bool{}
	for _, id := range v.Selected {
		if !model.ValidDigest(string(id)) || seen[id] {
			return nil, ErrContextSelectionInvalid
		}
		seen[id] = true
	}
	for i, id := range v.Mandatory {
		if !seen[id] || (i > 0 && v.Mandatory[i-1] >= id) {
			return nil, ErrContextSelectionInvalid
		}
	}
	for i, omitted := range v.Omitted {
		if !model.ValidDigest(string(omitted.Observation)) || seen[omitted.Observation] || (i > 0 && v.Omitted[i-1].Observation >= omitted.Observation) || (omitted.Reason != "capacity" && omitted.Reason != "zero_benefit" && omitted.Reason != "not_proposed") {
			return nil, ErrContextSelectionInvalid
		}
	}
	bundles := map[model.ObjectID]bool{}
	for _, bundle := range p.Bundles {
		bundles[bundle.ID] = true
	}
	for _, id := range v.SelectedBundles {
		if !bundles[id] {
			return nil, ErrContextSelectionInvalid
		}
		delete(bundles, id)
	}
	raw := codec.AppendUint([]byte("ATE-CONTEXT-SELECTION\x00"), 1)
	raw, _ = codec.AppendBytes(raw, proposal)
	raw = codec.AppendUint(raw, v.InputLimit)
	raw = codec.AppendUint(raw, v.UsedTokens)
	raw = appendContextIDs(raw, v.Mandatory)
	raw = appendContextIDs(raw, v.Selected)
	raw = appendContextIDs(raw, v.SelectedBundles)
	raw = codec.AppendUint(raw, uint64(len(v.Omitted)))
	for _, omitted := range v.Omitted {
		raw, _ = codec.AppendString(raw, string(omitted.Observation))
		raw, _ = codec.AppendString(raw, omitted.Reason)
	}
	raw, _ = codec.AppendString(raw, string(v.ProposalDigest))
	if len(raw) > MaxRecordBytes {
		return nil, ErrContextSelectionLimit
	}
	return raw, nil
}

type contextSelectionReader struct {
	d   *codec.Decoder
	err error
}

func (r *contextSelectionReader) uint() uint64 {
	if r.err != nil {
		return 0
	}
	v, err := r.d.Uint()
	r.err = err
	return v
}
func (r *contextSelectionReader) text(limit uint64) string {
	if r.err != nil {
		return ""
	}
	v, err := r.d.String(limit)
	r.err = err
	return v
}
func (r *contextSelectionReader) blob(limit uint64) []byte {
	if r.err != nil {
		return nil
	}
	v, err := r.d.Bytes(limit)
	r.err = err
	return v
}
func (r *contextSelectionReader) count(maximum, minimum uint64) int {
	n := r.uint()
	if r.err != nil || n > maximum || n > uint64(r.d.Remaining())/minimum {
		r.err = ErrContextSelectionInvalid
		return 0
	}
	return int(n)
}
func (r *contextSelectionReader) ids(maximum uint64) []model.ObjectID {
	n := r.count(maximum, 65)
	if r.err != nil {
		return nil
	}
	out := make([]model.ObjectID, n)
	for i := range out {
		out[i] = model.ObjectID(r.text(64))
	}
	return out
}

func decodeContextProposal(raw []byte) (ContextSelectionProposal, error) {
	const prefix = "ATE-CONTEXT-PROPOSAL\x00"
	if len(raw) > MaxRecordBytes || !bytes.HasPrefix(raw, []byte(prefix)) {
		return ContextSelectionProposal{}, ErrContextSelectionInvalid
	}
	r := &contextSelectionReader{d: codec.NewDecoder(raw[len(prefix):])}
	if r.uint() != 1 {
		return ContextSelectionProposal{}, ErrContextSelectionInvalid
	}
	source, err := decodeSource(r.d)
	if err != nil {
		return ContextSelectionProposal{}, err
	}
	p := ContextSelectionProposal{Source: source, Capacity: ContextCapacity{Window: r.uint(), OutputReserve: r.uint(), FramingReserve: r.uint()}, PolicyVersion: r.text(model.MaxIdentityBytes), Tokenizer: r.text(model.MaxIdentityBytes), CountQuality: r.text(model.MaxIdentityBytes), Mandatory: r.ids(MaxItems)}
	for n := r.count(MaxItems, 66); n > 0 && r.err == nil; n-- {
		p.Costs = append(p.Costs, ContextCost{Observation: model.ObjectID(r.text(64)), Tokens: r.uint()})
	}
	for n := r.count(MaxContextBundles, 1); n > 0 && r.err == nil; n-- {
		encoded := r.blob(MaxRecordBytes)
		const domain = "ATE-CONTEXT-BUNDLE\x00"
		if !bytes.HasPrefix(encoded, []byte(domain)) {
			return ContextSelectionProposal{}, ErrContextSelectionInvalid
		}
		br := &contextSelectionReader{d: codec.NewDecoder(encoded[len(domain):])}
		if br.uint() != 1 {
			return ContextSelectionProposal{}, ErrContextSelectionInvalid
		}
		b := ContextBundle{Conclusion: model.ObjectID(br.text(64)), Evidence: br.ids(MaxItems), Counterexamples: br.ids(MaxItems), Benefit: br.uint()}
		if br.err != nil || br.d.Done() != nil {
			return ContextSelectionProposal{}, ErrContextSelectionInvalid
		}
		b.ID = ContextBundleID(b)
		p.Bundles = append(p.Bundles, b)
	}
	if r.err != nil || r.d.Done() != nil {
		return ContextSelectionProposal{}, ErrContextSelectionInvalid
	}
	p, canonical, err := canonicalContextProposal(p)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ContextSelectionProposal{}, ErrContextSelectionInvalid
	}
	return p, nil
}

func decodeContextSelection(raw []byte) (ContextSelectionRecord, error) {
	const prefix = "ATE-CONTEXT-SELECTION\x00"
	if len(raw) > MaxRecordBytes || !bytes.HasPrefix(raw, []byte(prefix)) {
		return ContextSelectionRecord{}, ErrContextSelectionInvalid
	}
	r := &contextSelectionReader{d: codec.NewDecoder(raw[len(prefix):])}
	if r.uint() != 1 {
		return ContextSelectionRecord{}, ErrContextSelectionInvalid
	}
	p, err := decodeContextProposal(r.blob(MaxRecordBytes))
	if err != nil {
		return ContextSelectionRecord{}, err
	}
	v := ContextSelection{Source: p.Source.clone(), Capacity: p.Capacity, PolicyVersion: p.PolicyVersion, Tokenizer: p.Tokenizer, CountQuality: p.CountQuality, InputLimit: r.uint(), UsedTokens: r.uint(), Mandatory: r.ids(MaxItems), Selected: r.ids(MaxItems), SelectedBundles: r.ids(MaxContextBundles)}
	for n := r.count(MaxItems, 66); n > 0 && r.err == nil; n-- {
		v.Omitted = append(v.Omitted, ContextOmission{Observation: model.ObjectID(r.text(64)), Reason: r.text(32)})
	}
	v.ProposalDigest = model.ObjectID(r.text(64))
	if r.err != nil || r.d.Done() != nil {
		return ContextSelectionRecord{}, ErrContextSelectionInvalid
	}
	out := ContextSelectionRecord{Proposal: p, Result: v}
	canonical, err := encodeContextSelection(out)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ContextSelectionRecord{}, ErrContextSelectionInvalid
	}
	return out, nil
}

func validatePacketSelectionShape(p ContextPacket) error {
	if p.Selection == nil {
		return nil
	}
	r := p.Selection
	if _, err := encodeContextSelection(*r); err != nil {
		return err
	}
	if p.Key == nil || !sameContextSource(r.Proposal.Source, p.Source) || p.Tokenizer != r.Proposal.Tokenizer || p.EstimatedTokens != r.Result.UsedTokens+r.Proposal.Capacity.FramingReserve || len(p.Items) != len(r.Result.Selected) {
		return ErrContextSelectionInvalid
	}
	for i, item := range p.Items {
		if item.Observation != r.Result.Selected[i] {
			return ErrContextSelectionInvalid
		}
	}
	return nil
}

// VerifyPacketSelection authenticates a recorded plan against the supplied
// historical observation corpus, never current/latest knowledge. The core must
// first prove that corpus's owner, Source.Fence/Cursor, Frontier and ViewDigest.
func VerifyPacketSelection(p ContextPacket, snapshot Snapshot) error {
	if p.Selection == nil {
		return nil
	}
	if err := validatePacketSelectionShape(p); err != nil {
		return err
	}
	// Selection only uses observations. Dropping derived packets also avoids
	// recursively verifying a packet while computing its knowledge revision.
	req := p.Selection.Proposal.request(Snapshot{Observations: snapshot.Observations})
	want, err := RecordContextSelection(req)
	if err != nil {
		return err
	}
	expected, err := encodeContextSelection(want)
	if err != nil {
		return err
	}
	actual, err := encodeContextSelection(*p.Selection)
	if err != nil || !bytes.Equal(expected, actual) {
		return ErrContextSelectionInvalid
	}
	mandatory := make(map[model.ObjectID]bool, len(want.Result.Mandatory))
	for _, id := range want.Result.Mandatory {
		mandatory[id] = true
	}
	for _, observation := range snapshot.Observations {
		if observation.Key == *p.Key && observation.Payload == p.GoalRef && mandatory[observation.ID] {
			return nil
		}
	}
	return ErrContextSelectionInvalid
}
