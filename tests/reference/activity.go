package reference

import (
	"maps"
	"slices"
)

// Activity is an abstract external-operation oracle. It uses independent
// booleans for dispatch, result quarantine, and no-effect evidence rather than
// replaying the production status machine. No local acceptance claims that an
// external non-idempotent operation occurred exactly once.
type Activity struct {
	Effect                                             string
	AuthorityIncarnation, DecisionIncarnation, Version uint64
	Accepted                                           string
	Attempts                                           map[string]Attempt
	Results                                            map[string]Result
	guarantee                                          FiniteGuarantee
	permits                                            map[string]RetryPermit
	retry                                              RetryCheck
}
type Attempt struct {
	Incarnation, Sequence                  uint64
	Sent, Live, FencedBeforeSend, NoEffect bool
	Result, Reconciliation                 string
	ClaimCheck, DispatchCheck              RetryCheck
}
type Result struct {
	Attempt, Outcome string
	Quarantined      bool
}

func NewActivity(effect string) *Activity {
	return &Activity{Effect: effect, AuthorityIncarnation: 1, DecisionIncarnation: 1, Version: 1, Attempts: map[string]Attempt{}, Results: map[string]Result{}, permits: map[string]RetryPermit{}}
}
func (a *Activity) Clone() *Activity {
	c := *a
	c.Attempts, c.Results = maps.Clone(a.Attempts), maps.Clone(a.Results)
	c.permits = maps.Clone(a.permits)
	return &c
}
func (a *Activity) Batch(step func(*Activity) error) error {
	candidate := a.Clone()
	if err := step(candidate); err != nil {
		return err
	}
	*a = *candidate
	return nil
}
func (a *Activity) safeRetry(version, sequence uint64, check RetryCheck) bool {
	if check.Permit != "" {
		p, issued := a.permits[check.Permit]
		g := a.guarantee
		return a.Effect == "idempotent" && issued && p.Guarantee == g.ID && !g.Revoked &&
			g.Incarnation == a.AuthorityIncarnation && p.Version == version && g.Sequence == sequence &&
			p.From <= check.At && check.At < check.SendBefore && check.SendBefore <= p.Until
	}
	if a.Effect == "pure" {
		return true
	}
	for _, attempt := range a.Attempts {
		if attempt.Sent && !attempt.NoEffect {
			return false
		}
	}
	return true
}
func (a *Activity) Claim(id string, sequence, incarnation uint64, check RetryCheck) error {
	if incarnation != a.AuthorityIncarnation || incarnation != a.DecisionIncarnation || a.Accepted != "" {
		return ErrRejected
	}
	if old, exists := a.Attempts[id]; exists {
		if old.Incarnation == incarnation && old.Sequence == sequence && old.ClaimCheck == check {
			return nil
		}
		return ErrRejected
	}
	if sequence != uint64(len(a.Attempts))+1 {
		return ErrRejected
	}
	if sequence > 1 && (!a.safeRetry(a.Version, sequence, check) ||
		(check.Permit != "" && (check.Permit != a.retry.Permit || check.At < a.retry.At))) {
		return ErrRejected
	}
	for _, attempt := range a.Attempts {
		if attempt.Live {
			return ErrRejected
		}
	}
	a.Attempts[id] = Attempt{Incarnation: incarnation, Sequence: sequence, Live: true, ClaimCheck: check}
	return nil
}
func (a *Activity) Dispatch(id string, incarnation uint64, check RetryCheck) error {
	attempt, exists := a.Attempts[id]
	if !exists || incarnation != a.AuthorityIncarnation || incarnation != attempt.Incarnation || attempt.FencedBeforeSend {
		return ErrRejected
	}
	if attempt.Sent && attempt.Live && attempt.DispatchCheck == check {
		return nil
	}
	if !attempt.Live {
		return ErrRejected
	}
	if attempt.Sent || attempt.Sequence > 1 &&
		(!a.safeRetry(a.Version, attempt.Sequence, check) || check.At < attempt.ClaimCheck.At ||
			(check.Permit != "" && (check.Permit != a.retry.Permit || check.Permit != attempt.ClaimCheck.Permit))) {
		return ErrRejected
	}
	attempt.Sent = true
	attempt.DispatchCheck = check
	a.Attempts[id] = attempt
	return nil
}
func (a *Activity) Capture(id, attemptID, outcome string) error {
	attempt, exists := a.Attempts[attemptID]
	if !exists || !attempt.Sent || attempt.Result != "" {
		return ErrRejected
	}
	quarantined := attempt.Incarnation != a.AuthorityIncarnation || attempt.FencedBeforeSend || attempt.Reconciliation != ""
	a.Results[id] = Result{Attempt: attemptID, Outcome: outcome, Quarantined: quarantined}
	if !quarantined {
		attempt.Result = id
		attempt.Live = false
		a.Attempts[attemptID] = attempt
	}
	return nil
}
func (a *Activity) Accept(id string, incarnation uint64) error {
	r, exists := a.Results[id]
	if !exists || r.Quarantined || r.Outcome == "unknown" || incarnation != a.AuthorityIncarnation || a.Attempts[r.Attempt].Incarnation != incarnation {
		return ErrRejected
	}
	if a.Accepted != "" {
		if a.Accepted == id {
			return nil
		}
		return ErrRejected
	}
	a.Accepted = id
	a.Version++
	return nil
}
func (a *Activity) Takeover() {
	for id, attempt := range a.Attempts {
		if attempt.Live {
			attempt.Live = false
			if !attempt.Sent {
				attempt.FencedBeforeSend = true
			} else {
				attempt.Reconciliation = "unresolved"
			}
			a.Attempts[id] = attempt
		}
	}
	a.AuthorityIncarnation++
}
func (a *Activity) Reconcile(id string, noEffect, futureFenced bool) error {
	attempt, exists := a.Attempts[id]
	if !exists || !attempt.Sent || attempt.NoEffect || noEffect && !futureFenced {
		return ErrRejected
	}
	if noEffect {
		attempt.NoEffect = true
		attempt.Reconciliation = "no_effect"
	} else {
		attempt.Reconciliation = "unresolved"
	}
	attempt.Live = false
	a.Attempts[id] = attempt
	return nil
}
func (a *Activity) Retry(expected, incarnation uint64, check RetryCheck) error {
	if expected != a.Version || incarnation != a.AuthorityIncarnation || a.Accepted != "" ||
		!a.safeRetry(expected+1, uint64(len(a.Attempts))+1, check) {
		return ErrRejected
	}
	a.Version++
	a.DecisionIncarnation = incarnation
	a.retry = check
	return nil
}

// FiniteGuarantee projects the receiver's promise onto the sequence it covers.
// The oracle takes the receiver and host statements as inputs and models when
// they can be used, independent of the production codec and effect contract.
type FiniteGuarantee struct {
	ID                             string
	Version, Incarnation, Sequence uint64
	From, Until                    uint64
	Revoked                        bool
}

type RetryPermit struct {
	ID, Guarantee        string
	Version, From, Until uint64
}

type RetryCheck struct {
	Permit         string
	At, SendBefore uint64
}

func (a *Activity) PublishGuarantee(g FiniteGuarantee, covered []string) error {
	if a.Effect != "idempotent" || a.Accepted != "" || g.Incarnation != a.AuthorityIncarnation ||
		g.Version != a.guarantee.Version+1 || g.From >= g.Until || g.Sequence != uint64(len(a.Attempts))+1 ||
		!slices.Equal(covered, slices.Sorted(maps.Keys(a.Attempts))) {
		return ErrRejected
	}
	a.guarantee = g
	return nil
}

func (a *Activity) AuthorizeRetry(p RetryPermit) error {
	g := a.guarantee
	if a.Accepted != "" || g.ID == "" || p.Guarantee != g.ID || g.Revoked || g.Incarnation != a.AuthorityIncarnation ||
		p.Version != a.Version+1 || g.Sequence != uint64(len(a.Attempts))+1 ||
		p.From < g.From || p.From >= p.Until || p.Until > g.Until {
		return ErrRejected
	}
	for _, attempt := range a.Attempts {
		if attempt.Live {
			return ErrRejected
		}
	}
	a.permits[p.ID] = p
	return nil
}
func (a *Activity) Status(id string) string {
	attempt := a.Attempts[id]
	if attempt.FencedBeforeSend {
		return "fenced_before_dispatch"
	}
	if attempt.NoEffect {
		return "no_effect"
	}
	if attempt.Reconciliation != "" {
		return "effect_unknown"
	}
	if attempt.Result != "" {
		if a.Results[attempt.Result].Outcome == "unknown" {
			return "effect_unknown"
		}
		return "result_recorded"
	}
	if attempt.Sent {
		return "dispatch_recorded"
	}
	return "created"
}
