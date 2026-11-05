package knowledge

import (
	"errors"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const (
	ContextSelectionPolicy = "benefit_per_marginal_token/v1"
	TokenCountEstimated    = "estimated"
	TokenCountExact        = "exact_framed_segments"
	MaxContextBundles      = 256
	maxContextReferences   = 16384
	maxSelectionWork       = 4 << 20
)

var (
	ErrContextSelectionInvalid  = errors.New("invalid context selection")
	ErrContextSelectionLimit    = errors.New("context selection resource limit")
	ErrContextBudgetUnsatisfied = errors.New("mandatory context exceeds input capacity")
)

// ContextCapacity concerns model capacity only. Account balance, monetary
// budgets and execution authority belong to dispatch and never to selection.
type ContextCapacity struct {
	Window, OutputReserve, FramingReserve uint64
}

// ContextCost is the cost of the exact segment the host intends to render for
// an adopted Observation. Exact counts require separately framed segments whose
// token costs are additive. Otherwise the host must declare estimated quality;
// the complete provider request must still be counted before dispatch.
type ContextCost struct {
	Observation model.ObjectID
	Tokens      uint64
}

// ContextBundle keeps a conclusion, its supporting evidence and its relevant
// counterexamples together. Benefit is a recorded host proposal, not a claim
// that the engine understands semantic relevance. IDs derive from these exact
// fields. Each conclusion has one bundle within a selection proposal.
type ContextBundle struct {
	ID              model.ObjectID
	Conclusion      model.ObjectID
	Evidence        []model.ObjectID
	Counterexamples []model.ObjectID
	Benefit         uint64
}

type ContextSelectionRequest struct {
	Snapshot      Snapshot
	Source        SourceAnchor
	Capacity      ContextCapacity
	Mandatory     []model.ObjectID
	Costs         []ContextCost
	Bundles       []ContextBundle
	Tokenizer     string
	CountQuality  string
	PolicyVersion string
}

type ContextOmission struct {
	Observation model.ObjectID
	Reason      string // capacity, zero_benefit, not_proposed
}

// ContextSelection is a deterministic plan, not a model request or a grant.
// Selected is presentation order: mandatory IDs first, then whole selected
// bundles, with stable ID order inside each addition. Omitted is sorted by ID.
// The host must persist this plan and the resulting exact provider input; a
// rerun with today's knowledge is not a substitute for a historical packet.
type ContextSelection struct {
	Source          SourceAnchor
	Capacity        ContextCapacity
	InputLimit      uint64
	UsedTokens      uint64
	Mandatory       []model.ObjectID
	Selected        []model.ObjectID
	SelectedBundles []model.ObjectID
	Omitted         []ContextOmission
	PolicyVersion   string
	Tokenizer       string
	CountQuality    string
	ProposalDigest  model.ObjectID
}

func sortedContextIDs(ids []model.ObjectID) ([]model.ObjectID, error) {
	out := append([]model.ObjectID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	for i, id := range out {
		if !model.ValidDigest(string(id)) || (i > 0 && out[i-1] == id) {
			return nil, ErrContextSelectionInvalid
		}
	}
	return out, nil
}

func appendContextIDs(raw []byte, ids []model.ObjectID) []byte {
	raw = codec.AppendUint(raw, uint64(len(ids)))
	for _, id := range ids {
		raw, _ = codec.AppendString(raw, string(id))
	}
	return raw
}

func contextBundleBytes(bundle ContextBundle) ([]byte, error) {
	if !model.ValidDigest(string(bundle.Conclusion)) || bundle.Benefit > MaxEstimatedTokens || len(bundle.Evidence)+len(bundle.Counterexamples) > MaxItems {
		return nil, ErrContextSelectionInvalid
	}
	evidence, err := sortedContextIDs(bundle.Evidence)
	if err != nil {
		return nil, err
	}
	counters, err := sortedContextIDs(bundle.Counterexamples)
	if err != nil {
		return nil, err
	}
	seen := map[model.ObjectID]bool{bundle.Conclusion: true}
	for _, id := range append(append([]model.ObjectID(nil), evidence...), counters...) {
		if seen[id] {
			return nil, ErrContextSelectionInvalid
		}
		seen[id] = true
	}
	raw := codec.AppendUint([]byte("ATE-CONTEXT-BUNDLE\x00"), 1)
	raw, _ = codec.AppendString(raw, string(bundle.Conclusion))
	raw = appendContextIDs(raw, evidence)
	raw = appendContextIDs(raw, counters)
	return codec.AppendUint(raw, bundle.Benefit), nil
}

func ContextBundleID(bundle ContextBundle) model.ObjectID {
	raw, err := contextBundleBytes(bundle)
	if err != nil {
		return ""
	}
	return knowledgeDigest(raw)
}

type contextProposal struct {
	costs        map[model.ObjectID]uint64
	bundles      []ContextBundle
	mandatory    []model.ObjectID
	dependencies map[model.ObjectID][]model.ObjectID
	digest       model.ObjectID
}

func validateContextProposal(req ContextSelectionRequest) (contextProposal, error) {
	if len(req.Costs) > MaxItems || len(req.Mandatory) > MaxItems || len(req.Bundles) > MaxContextBundles {
		return contextProposal{}, ErrContextSelectionLimit
	}
	if req.Source.Validate() != nil || req.Source.Anchor.Knowledge == nil || req.Source.Anchor.Cursor == nil || req.Source.Frontier == "" || !validText(req.Tokenizer) || (req.CountQuality != TokenCountEstimated && req.CountQuality != TokenCountExact) || req.PolicyVersion != ContextSelectionPolicy {
		return contextProposal{}, ErrContextSelectionInvalid
	}
	cap := req.Capacity
	if cap.Window == 0 || cap.Window > MaxEstimatedTokens || cap.OutputReserve > cap.Window || cap.FramingReserve > cap.Window-cap.OutputReserve {
		return contextProposal{}, ErrContextSelectionInvalid
	}
	selected, _, err := SelectRevision(req.Snapshot, req.Source.Anchor.Space, *req.Source.Anchor.Knowledge, req.Source.Anchor.Cursor.Revision)
	if err != nil {
		return contextProposal{}, err
	}
	frontier, err := Frontier(selected, *req.Source.Anchor.Knowledge)
	if err != nil || frontier != req.Source.Frontier {
		return contextProposal{}, model.ErrAnchorMismatch
	}
	known := make(map[model.ObjectID]bool, len(selected.Observations))
	for _, observation := range selected.Observations {
		fence := observation.Source.Anchor.Fence
		if fence.Store != req.Source.Anchor.Fence.Store || fence.LSN > req.Source.Anchor.Fence.LSN || (fence.LSN == req.Source.Anchor.Fence.LSN && fence.CommitDigest != req.Source.Anchor.Fence.CommitDigest) {
			return contextProposal{}, ErrFutureSource
		}
		known[observation.ID] = true
	}
	p := contextProposal{costs: make(map[model.ObjectID]uint64, len(req.Costs)), dependencies: make(map[model.ObjectID][]model.ObjectID)}
	for _, cost := range req.Costs {
		if !known[cost.Observation] {
			return contextProposal{}, ErrUnknownObservation
		}
		if _, duplicate := p.costs[cost.Observation]; duplicate || cost.Tokens > MaxEstimatedTokens {
			return contextProposal{}, ErrContextSelectionInvalid
		}
		p.costs[cost.Observation] = cost.Tokens
	}
	p.mandatory, err = sortedContextIDs(req.Mandatory)
	if err != nil {
		return contextProposal{}, err
	}
	for _, id := range p.mandatory {
		if _, exists := p.costs[id]; !exists {
			return contextProposal{}, ErrUnknownObservation
		}
	}
	ids := map[model.ObjectID]bool{}
	references := 0
	for _, bundle := range req.Bundles {
		if bundle.ID == "" || ContextBundleID(bundle) != bundle.ID || ids[bundle.ID] {
			return contextProposal{}, ErrContextSelectionInvalid
		}
		ids[bundle.ID] = true
		if _, exists := p.dependencies[bundle.Conclusion]; exists {
			return contextProposal{}, ErrContextSelectionInvalid
		}
		members := append(append([]model.ObjectID{bundle.Conclusion}, bundle.Evidence...), bundle.Counterexamples...)
		references += len(members)
		if references > maxContextReferences {
			return contextProposal{}, ErrContextSelectionLimit
		}
		for _, id := range members {
			if _, exists := p.costs[id]; !exists {
				return contextProposal{}, ErrUnknownObservation
			}
		}
		p.dependencies[bundle.Conclusion] = members
		owned := bundle
		owned.Evidence, _ = sortedContextIDs(bundle.Evidence)
		owned.Counterexamples, _ = sortedContextIDs(bundle.Counterexamples)
		p.bundles = append(p.bundles, owned)
	}
	sort.Slice(p.bundles, func(i, j int) bool { return p.bundles[i].ID < p.bundles[j].ID })
	raw, _ := encodeSource(codec.AppendUint([]byte("ATE-CONTEXT-PROPOSAL\x00"), 1), req.Source)
	for _, value := range []uint64{cap.Window, cap.OutputReserve, cap.FramingReserve} {
		raw = codec.AppendUint(raw, value)
	}
	for _, text := range []string{req.PolicyVersion, req.Tokenizer, req.CountQuality} {
		raw, _ = codec.AppendString(raw, text)
	}
	raw = appendContextIDs(raw, p.mandatory)
	ordered := append([]ContextCost(nil), req.Costs...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Observation < ordered[j].Observation })
	raw = codec.AppendUint(raw, uint64(len(ordered)))
	for _, cost := range ordered {
		raw, _ = codec.AppendString(raw, string(cost.Observation))
		raw = codec.AppendUint(raw, cost.Tokens)
	}
	raw = codec.AppendUint(raw, uint64(len(p.bundles)))
	for _, bundle := range p.bundles {
		encoded, _ := contextBundleBytes(bundle)
		raw, _ = codec.AppendBytes(raw, encoded)
	}
	p.digest = knowledgeDigest(raw)
	return p, nil
}

// SelectContext implements a bounded deterministic heuristic over an already
// authenticated knowledge cut. It performs no IO, permission checks, model
// calls or hidden discovery. The engine/host must authenticate the supplied
// snapshot, derive mandatory constraints and recheck authorization at dispatch.
func SelectContext(req ContextSelectionRequest) (ContextSelection, error) {
	if req.PolicyVersion == "" {
		req.PolicyVersion = ContextSelectionPolicy
	}
	p, err := validateContextProposal(req)
	if err != nil {
		return ContextSelection{}, err
	}
	work := 0
	closure := func(seeds []model.ObjectID) ([]model.ObjectID, error) {
		seen := make(map[model.ObjectID]bool)
		queue := append([]model.ObjectID(nil), seeds...)
		for len(queue) > 0 {
			id := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			work++
			if work > maxSelectionWork {
				return nil, ErrContextSelectionLimit
			}
			if seen[id] {
				continue
			}
			seen[id] = true
			queue = append(queue, p.dependencies[id]...)
		}
		out := make([]model.ObjectID, 0, len(seen))
		for id := range seen {
			out = append(out, id)
		}
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return out, nil
	}
	mandatory, err := closure(p.mandatory)
	if err != nil {
		return ContextSelection{}, err
	}
	out := ContextSelection{Source: req.Source.clone(), Capacity: req.Capacity, InputLimit: req.Capacity.Window - req.Capacity.OutputReserve - req.Capacity.FramingReserve, Mandatory: mandatory, PolicyVersion: req.PolicyVersion, Tokenizer: req.Tokenizer, CountQuality: req.CountQuality, ProposalDigest: p.digest, Selected: []model.ObjectID{}, SelectedBundles: []model.ObjectID{}, Omitted: []ContextOmission{}}
	chosen := make(map[model.ObjectID]bool)
	add := func(ids []model.ObjectID) {
		for _, id := range ids {
			if !chosen[id] {
				chosen[id] = true
				out.Selected = append(out.Selected, id)
				out.UsedTokens += p.costs[id]
			}
		}
	}
	var mandatoryCost uint64
	for _, id := range mandatory {
		mandatoryCost += p.costs[id]
	}
	if mandatoryCost > out.InputLimit {
		return ContextSelection{}, ErrContextBudgetUnsatisfied
	}
	add(mandatory)
	members := make([][]model.ObjectID, len(p.bundles))
	proposed := make(map[model.ObjectID]bool)
	positive := make(map[model.ObjectID]bool)
	for i, bundle := range p.bundles {
		members[i], err = closure([]model.ObjectID{bundle.Conclusion})
		if err != nil {
			return ContextSelection{}, err
		}
		for _, id := range members[i] {
			proposed[id] = true
			positive[id] = positive[id] || bundle.Benefit > 0
		}
	}
	consumed := make([]bool, len(p.bundles))
	for {
		best := -1
		var bestCost uint64
		for i, bundle := range p.bundles {
			if consumed[i] || bundle.Benefit == 0 {
				continue
			}
			var marginal uint64
			for _, id := range members[i] {
				work++
				if work > maxSelectionWork {
					return ContextSelection{}, ErrContextSelectionLimit
				}
				if !chosen[id] {
					marginal += p.costs[id]
				}
			}
			if marginal > out.InputLimit-out.UsedTokens {
				continue
			}
			// Bounded integer cross-products avoid floating point ratios. A
			// zero-cost bundle ranks first; equality uses stable bundle ID.
			better := best < 0
			if best >= 0 {
				if marginal == 0 || bestCost == 0 {
					better = marginal == 0 && bestCost != 0
				} else {
					better = bundle.Benefit*bestCost > p.bundles[best].Benefit*marginal
				}
			}
			if better {
				best, bestCost = i, marginal
			}
		}
		if best < 0 {
			break
		}
		consumed[best] = true
		out.SelectedBundles = append(out.SelectedBundles, p.bundles[best].ID)
		add(members[best])
	}
	for id := range p.costs {
		if chosen[id] {
			continue
		}
		reason := "capacity"
		if !proposed[id] {
			reason = "not_proposed"
		} else if !positive[id] {
			reason = "zero_benefit"
		}
		out.Omitted = append(out.Omitted, ContextOmission{Observation: id, Reason: reason})
	}
	sort.Slice(out.Omitted, func(i, j int) bool { return out.Omitted[i].Observation < out.Omitted[j].Observation })
	return out, nil
}
