package reference

import "maps"

// ActionRecovery models logical reservations and evidence, with receipt tokens
// treated as opaque identities. Authority is current; historical command bytes
// and receipt identities never change when a new worker takes over.
type ActionAuthority struct {
	Holder, Evidence   string
	Incarnation, Epoch uint64
	Active             bool
}
type ExactAction struct {
	Key, Owner, Request, Method, Body, ExpectedFence string
	Generation                                       uint64
	Authority                                        ActionAuthority
	Status                                           string
}
type ActionReceipt struct {
	Command     ExactAction
	Token       string
	Publication uint64
}
type ActionResolution struct {
	Key, Outcome, Receipt string
	Generation, Observed  uint64
}
type ActionRecovery struct {
	Authority           ActionAuthority
	Current             map[string]ExactAction
	History             map[string]map[uint64]ExactAction
	Receipts            map[string]ActionReceipt
	Resolutions         []ActionResolution
	Position            ResumePosition
	PositionIncarnation uint64
}

func NewActionRecovery(a ActionAuthority) *ActionRecovery {
	return &ActionRecovery{Authority: a, Current: map[string]ExactAction{}, History: map[string]map[uint64]ExactAction{}, Receipts: map[string]ActionReceipt{}}
}
func (a *ActionRecovery) Clone() *ActionRecovery {
	c := *a
	c.Current = maps.Clone(a.Current)
	c.History = map[string]map[uint64]ExactAction{}
	for k, g := range a.History {
		c.History[k] = maps.Clone(g)
	}
	c.Receipts = maps.Clone(a.Receipts)
	c.Resolutions = append([]ActionResolution(nil), a.Resolutions...)
	c.Position.Next = maps.Clone(a.Position.Next)
	return &c
}
func (a *ActionRecovery) Batch(change func(*ActionRecovery) error) error {
	c := a.Clone()
	if err := change(c); err != nil {
		return err
	}
	*a = *c
	return nil
}
func sameExactAction(a, b ExactAction) bool {
	return a.Request == b.Request && a.Method == b.Method && a.Body == b.Body && a.ExpectedFence == b.ExpectedFence
}
func (a *ActionRecovery) Reserve(x ExactAction) error {
	if !a.Authority.Active || x.Authority != a.Authority || x.Generation != 1 || a.Current[x.Key].Key != "" {
		return ErrRejected
	}
	for _, history := range a.History {
		for _, old := range history {
			if old.Request == x.Request {
				return ErrRejected
			}
		}
	}
	if r, ok := a.Receipts[x.Request]; ok && !sameExactAction(r.Command, x) {
		return ErrRejected
	}
	x.Status = "pending"
	a.Current[x.Key] = x
	a.History[x.Key] = map[uint64]ExactAction{1: x}
	return nil
}
func (a *ActionRecovery) MaySubmit(x ExactAction) (*ActionReceipt, error) {
	if r, ok := a.Receipts[x.Request]; ok {
		if !sameExactAction(r.Command, x) {
			return nil, ErrRejected
		}
		return &r, nil
	}
	for key, history := range a.History {
		for generation, reserved := range history {
			if reserved.Request != x.Request {
				continue
			}
			current := a.Current[key]
			if !sameExactAction(reserved, x) || current.Generation != generation || (current.Status != "pending" && current.Status != "rebound") || !a.Authority.Active || reserved.Authority != a.Authority {
				return nil, ErrRejected
			}
		}
	}
	return nil, nil
}
func (a *ActionRecovery) Published(x ExactAction, token string, publication uint64) {
	a.Receipts[x.Request] = ActionReceipt{x, token, publication}
}
func (a *ActionRecovery) Fence() {
	a.Authority.Active = false
	for k, x := range a.Current {
		if x.Status == "pending" || x.Status == "rebound" {
			x.Status = "fenced"
			a.Current[k] = x
		}
	}
}
func (a *ActionRecovery) Resume(next ActionAuthority) error {
	if a.Authority.Active || !next.Active || next.Incarnation != a.Authority.Incarnation+1 || next.Epoch <= a.Authority.Epoch || next.Evidence == "" {
		return ErrRejected
	}
	a.Authority = next
	return nil
}
func (a *ActionRecovery) Resolve(r ActionResolution) error {
	x, ok := a.Current[r.Key]
	if !ok || x.Generation != r.Generation {
		return ErrRejected
	}
	receipt, found := a.Receipts[x.Request]
	if r.Outcome == "applied" {
		if !found || receipt.Publication > r.Observed || r.Receipt != receipt.Token {
			return ErrRejected
		}
		x.Status = "applied"
	} else {
		if found {
			return ErrRejected
		}
		switch r.Outcome {
		case "absent":
			x.Status = "fenced"
		case "unknown":
			x.Status = "unknown"
		default:
			return ErrRejected
		}
	}
	a.Current[x.Key] = x
	a.Resolutions = append(a.Resolutions, r)
	return nil
}
func (a *ActionRecovery) Rebind(x ExactAction, expected uint64, evidence string) error {
	old, ok := a.Current[x.Key]
	if !ok || old.Generation != expected || old.Status != "fenced" || x.Request == old.Request || x.Authority != a.Authority || !a.Authority.Active || x.Authority.Incarnation <= old.Authority.Incarnation || evidence != a.Authority.Evidence {
		return ErrRejected
	}
	absent := false
	for _, r := range a.Resolutions {
		absent = absent || r.Key == old.Key && r.Generation == old.Generation && r.Outcome == "absent"
	}
	if !absent {
		return ErrRejected
	}
	for _, history := range a.History {
		for _, r := range history {
			if r.Request == x.Request {
				return ErrRejected
			}
		}
	}
	x.Generation = old.Generation + 1
	x.Owner = old.Owner
	x.Status = "rebound"
	a.Current[x.Key] = x
	a.History[x.Key][x.Generation] = x
	return nil
}

func (a *ActionRecovery) SavePosition(p ResumePosition, incarnation, expected uint64) error {
	old := a.Position
	if !a.Authority.Active || incarnation != a.Authority.Incarnation || p.Version != expected+1 || old.Version != expected || p.Execution == "" || p.Adapter == "" || len(p.Next) == 0 {
		return ErrRejected
	}
	if old.Version > 0 && (old.Adapter != p.Adapter || old.Knowledge != "" && p.Knowledge == "") {
		return ErrRejected
	}
	for key, generation := range p.Next {
		current, ok := a.Current[key]
		if !ok || current.Owner != p.Execution || current.Generation != generation {
			return ErrRejected
		}
	}
	p.Next = maps.Clone(p.Next)
	a.Position = p
	a.PositionIncarnation = incarnation
	return nil
}
