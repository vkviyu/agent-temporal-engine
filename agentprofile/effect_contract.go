package agentprofile

import (
	"reflect"
	"slices"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// EffectContract is an immutable, versioned declaration by the trusted resource
// adapter. Capabilities are independent: a queryable result or a compensation
// operation does not imply absence or permit retry. An empty contract grants no
// capability; only the separate pure classification and terminal no-effect
// reconciliations can justify retry without an idempotency guarantee.
type EffectContract struct {
	Version           string
	Definition        model.ObjectID // optional published tool definition, not an attestation
	Recipient         string
	AllowsMutation    bool
	VerifiedPure      bool
	HistoricalRead    bool
	KeyNamespace      string
	RetentionNanos    uint64
	DifferentRequest  string // reject, unsupported
	ResultQuery       string // none, eventual, strong
	ResourceFence     string // none, supported
	Compensation      string // none, supported
	CompensationLimit string
	LateArrivalFence  string // none, request_deadline, receiver_epoch
}

// IdempotencyGuarantee is a finite attestation, distinct from a capability
// declaration. The receiver binding covers every prior Attempt and the next
// sequence using this exact operation key and request. TerminalEvidence asserts
// that delayed packets cannot produce effects after ValidUntil, including the
// next authorized send. Merely observing "key exists now" is insufficient.
// Times are explicit trusted-host facts in nanoseconds; replay never reads a
// machine clock. A newer version (including revoked) supersedes older grants.
type IdempotencyGuarantee struct {
	ID                model.ObjectID
	Activity          string
	Version           uint64
	Status            string // active, revoked
	Contract          model.ObjectID
	RequestDigest     model.ObjectID
	KeyNamespace      string
	IdempotencyKey    string
	ReceiverEpoch     string
	CoveredAttempts   []string // exact sorted set at issuance
	NextSequence      uint64
	ValidFrom         uint64
	ValidUntil        uint64 // exclusive receiver arrival cutoff
	TerminalCondition string // same as EffectContract.LateArrivalFence
	TerminalEvidence  model.ObjectID
	Evidence          model.ObjectID
	Authority         AuthorityBinding
}

// RetryAuthorization is a single-sequence host decision. It names a specific
// finite guarantee, current control successor and authority. It cannot be reused
// after another attempt, takeover, guarantee renewal/revocation or control CAS.
type RetryAuthorization struct {
	ID             model.ObjectID
	Activity       string
	ControlVersion uint64 // version produced by RetryActivity
	Sequence       uint64
	Guarantee      model.ObjectID
	CheckedAt      uint64
	NotBefore      uint64
	NotAfter       uint64
	Evidence       model.ObjectID
	Authority      AuthorityBinding
}

// RetryUse records a fresh host timing check at retry, claim and dispatch. The
// adapter must enforce SendNotAfter at the receiver (or through the declared
// epoch fence). Recording this fact does not itself send or cancel a packet.
type RetryUse struct {
	Authorization model.ObjectID
	ObservedAt    uint64
	SendNotAfter  uint64
	Evidence      model.ObjectID
}

type PutIdempotencyGuaranteeCommand struct{ Guarantee IdempotencyGuarantee }
type AuthorizeRetryCommand struct{ Authorization RetryAuthorization }

func EffectContractDigest(c EffectContract) model.ObjectID {
	b, err := appendControlValue([]byte("ATE-EFFECT-CONTRACT\x00\x01"), reflect.ValueOf(c), 0)
	if err != nil {
		return ""
	}
	return commandHash(b)
}
func IdempotencyGuaranteeID(g IdempotencyGuarantee) model.ObjectID {
	g.ID = ""
	b, err := appendControlValue([]byte("ATE-IDEMPOTENCY-GUARANTEE\x00\x01"), reflect.ValueOf(g), 0)
	if err != nil {
		return ""
	}
	return commandHash(b)
}
func RetryAuthorizationID(a RetryAuthorization) model.ObjectID {
	a.ID = ""
	b, err := appendControlValue([]byte("ATE-RETRY-AUTHORIZATION\x00\x01"), reflect.ValueOf(a), 0)
	if err != nil {
		return ""
	}
	return commandHash(b)
}

func validEffectContract(s ActivitySpec) bool {
	return (s.Effect != "idempotent" || commandText(s.IdempotencyKey)) && validEffectCapabilities(s.Effect, s.Recipient, s.Contract)
}

func validEffectCapabilities(effect, recipient string, c EffectContract) bool {
	if c == (EffectContract{}) {
		return effect != "idempotent"
	}
	if !commandText(c.Version) || c.Recipient != recipient || (c.Definition != "" && !validDigest(c.Definition)) ||
		(c.DifferentRequest != "reject" && c.DifferentRequest != "unsupported") ||
		(c.ResultQuery != "none" && c.ResultQuery != "eventual" && c.ResultQuery != "strong") ||
		(c.ResourceFence != "none" && c.ResourceFence != "supported") ||
		(c.Compensation != "none" && c.Compensation != "supported") ||
		(c.LateArrivalFence != "none" && c.LateArrivalFence != "request_deadline" && c.LateArrivalFence != "receiver_epoch") {
		return false
	}
	if c.VerifiedPure && c.AllowsMutation || effect == "pure" && !c.VerifiedPure {
		return false
	}
	if c.Compensation == "supported" && !commandText(c.CompensationLimit) || c.Compensation == "none" && c.CompensationLimit != "" {
		return false
	}
	if c.LateArrivalFence == "receiver_epoch" && c.ResourceFence != "supported" {
		return false
	}
	if effect == "idempotent" {
		return commandText(c.KeyNamespace) && c.RetentionNanos > 0 && c.DifferentRequest == "reject" && c.LateArrivalFence != "none"
	}
	return true
}
func sortedAttemptIDs(ids []string) bool {
	if len(ids) > MaxLifecycleRecords {
		return false
	}
	for i, id := range ids {
		if !commandText(id) || i > 0 && ids[i-1] >= id {
			return false
		}
	}
	return true
}
func validGuaranteeShape(g IdempotencyGuarantee) bool {
	return validDigest(g.ID) && g.ID == IdempotencyGuaranteeID(g) && commandText(g.Activity) && g.Version > 0 &&
		(g.Status == "active" || g.Status == "revoked") && validDigest(g.Contract) && validDigest(g.RequestDigest) &&
		commandText(g.KeyNamespace) && commandText(g.IdempotencyKey) && commandText(g.ReceiverEpoch) && sortedAttemptIDs(g.CoveredAttempts) &&
		g.NextSequence > 1 && g.ValidFrom > 0 && g.ValidUntil > g.ValidFrom &&
		(g.TerminalCondition == "request_deadline" || g.TerminalCondition == "receiver_epoch") && validDigest(g.TerminalEvidence) && validDigest(g.Evidence)
}
func validRetryAuthorizationShape(a RetryAuthorization) bool {
	return validDigest(a.ID) && a.ID == RetryAuthorizationID(a) && commandText(a.Activity) && a.ControlVersion > 1 && a.Sequence > 1 &&
		validDigest(a.Guarantee) && a.CheckedAt > 0 && a.NotBefore >= a.CheckedAt && a.NotAfter > a.NotBefore && validDigest(a.Evidence)
}
func validRetryUse(use RetryUse) bool {
	return use == (RetryUse{}) || validDigest(use.Authorization) && use.ObservedAt > 0 && use.SendNotAfter > use.ObservedAt && validDigest(use.Evidence)
}
func (l *Ledger) latestGuarantee(activity string) (IdempotencyGuarantee, bool) {
	var latest IdempotencyGuarantee
	for _, g := range l.recovery().IdempotencyGuarantees {
		if g.Activity == activity && g.Version > latest.Version {
			latest = g
		}
	}
	return latest, latest.Version != 0
}
func guaranteeMatches(spec ActivitySpec, g IdempotencyGuarantee) bool {
	return spec.Effect == "idempotent" && validEffectContract(spec) && g.Activity == spec.Activity && g.Contract == EffectContractDigest(spec.Contract) &&
		g.RequestDigest == spec.RequestDigest && g.KeyNamespace == spec.Contract.KeyNamespace && g.IdempotencyKey == spec.IdempotencyKey &&
		g.TerminalCondition == spec.Contract.LateArrivalFence && g.ValidUntil-g.ValidFrom <= spec.Contract.RetentionNanos
}
func (l *Ledger) retryAttemptIDs(activity, excluded string) ([]string, uint64) {
	ids := []string{}
	var last uint64
	for _, a := range l.recovery().AttemptControls {
		if a.Activity != activity || a.Attempt == excluded {
			continue
		}
		ids = append(ids, a.Attempt)
		if a.Sequence > last {
			last = a.Sequence
		}
	}
	sort.Strings(ids)
	return ids, last
}
func (l *Ledger) allEffectsAbsent(spec ActivitySpec, excluded string) bool {
	for _, a := range l.recovery().AttemptControls {
		if a.Activity != spec.Activity || a.Attempt == excluded || a.Status == "created" || a.Status == "fenced_before_dispatch" {
			continue
		}
		var latest AttemptReconciliation
		for _, r := range l.recovery().Reconciliations {
			if r.Attempt == a.Attempt && r.Version > latest.Version {
				latest = r
			}
		}
		if latest.Outcome != "no_effect" || !latest.FutureEffectFenced {
			return false
		}
	}
	return true
}
func (l *Ledger) validRetryGrant(spec ActivitySpec, binding AuthorityBinding, controlVersion, sequence uint64, excluded string, use RetryUse) bool {
	if use.Authorization == "" || !validRetryUse(use) {
		return false
	}
	var a RetryAuthorization
	for _, candidate := range l.recovery().RetryAuthorizations {
		if candidate.ID == use.Authorization {
			a = candidate
			break
		}
	}
	if a.ID == "" || a.Activity != spec.Activity || a.Authority != binding || a.ControlVersion != controlVersion || a.Sequence != sequence ||
		use.ObservedAt < a.NotBefore || use.ObservedAt < a.CheckedAt || use.ObservedAt >= a.NotAfter || use.SendNotAfter > a.NotAfter {
		return false
	}
	g, found := l.latestGuarantee(spec.Activity)
	ids, last := l.retryAttemptIDs(spec.Activity, excluded)
	return found && g.ID == a.Guarantee && g.Status == "active" && g.Authority == binding && guaranteeMatches(spec, g) &&
		slices.Equal(g.CoveredAttempts, ids) && g.NextSequence == sequence && last+1 == sequence &&
		a.CheckedAt >= g.ValidFrom && a.NotBefore >= g.ValidFrom && a.NotAfter <= g.ValidUntil
}
func (l *Ledger) safeRetry(spec ActivitySpec, binding AuthorityBinding, version, sequence uint64, excluded string, use RetryUse) bool {
	if use.Authorization == "" {
		return spec.Effect == "pure" || l.allEffectsAbsent(spec, excluded)
	}
	return spec.Effect == "idempotent" && l.validRetryGrant(spec, binding, version, sequence, excluded, use)
}
func (l *Ledger) putIdempotencyGuarantee(g IdempotencyGuarantee) (Result, error) {
	spec, err := l.activitySpec(g.Activity)
	if err != nil {
		return Result{}, err
	}
	if err = l.checkActivity(spec, g.Authority); err != nil {
		return Result{}, err
	}
	if !guaranteeMatches(spec, g) {
		return Result{}, ErrConflict
	}
	for _, old := range l.recovery().IdempotencyGuarantees {
		if old.ID == g.ID {
			return Result{}, nil
		}
	}
	old, found := l.latestGuarantee(g.Activity)
	if found && (old.Version == ^uint64(0) || g.Version != old.Version+1) || !found && g.Version != 1 {
		return Result{}, ErrConflict
	}
	ids, last := l.retryAttemptIDs(g.Activity, "")
	if !slices.Equal(g.CoveredAttempts, ids) || g.NextSequence != last+1 {
		return Result{}, ErrConflict
	}
	_, ctl, err := l.activityControl(g.Activity)
	if err != nil || ctl.AcceptedResult != "" {
		return Result{}, ErrConflict
	}
	l.recovery().IdempotencyGuarantees = append(l.recovery().IdempotencyGuarantees, g)
	return Result{}, nil
}
func (l *Ledger) authorizeRetry(a RetryAuthorization) (Result, error) {
	spec, err := l.activitySpec(a.Activity)
	if err != nil {
		return Result{}, err
	}
	if err = l.checkActivity(spec, a.Authority); err != nil {
		return Result{}, err
	}
	for _, old := range l.recovery().RetryAuthorizations {
		if old.ID == a.ID {
			return Result{}, nil
		}
	}
	_, ctl, err := l.activityControl(a.Activity)
	if err != nil || ctl.AcceptedResult != "" || ctl.Version == ^uint64(0) || a.ControlVersion != ctl.Version+1 {
		return Result{}, ErrConflict
	}
	g, found := l.latestGuarantee(a.Activity)
	ids, last := l.retryAttemptIDs(a.Activity, "")
	if !found || g.ID != a.Guarantee || g.Status != "active" || g.Authority != a.Authority || !guaranteeMatches(spec, g) ||
		!slices.Equal(g.CoveredAttempts, ids) || a.Sequence != last+1 || g.NextSequence != a.Sequence ||
		a.CheckedAt < g.ValidFrom || a.NotBefore < g.ValidFrom || a.NotAfter > g.ValidUntil {
		return Result{}, ErrActionUnknown
	}
	for _, attempt := range l.recovery().AttemptControls {
		if attempt.Activity == spec.Activity && (attempt.Status == "created" || attempt.Status == "dispatch_recorded") {
			return Result{}, ErrConflict
		}
	}
	l.recovery().RetryAuthorizations = append(l.recovery().RetryAuthorizations, a)
	return Result{}, nil
}

// EncodeEffectContract exposes the fixed schema for independent wire fixtures;
// no open JSON or caller-provided runtime type participates in its identity.
func EncodeEffectContract(c EffectContract) ([]byte, error) {
	return appendControlValue(codec.AppendUint([]byte("ATE-EFFECT-CONTRACT\x00"), 1), reflect.ValueOf(c), 0)
}

func validateEffectRecovery(s Snapshot) error {
	r := s.Lifecycle.Recovery
	if r == nil {
		return nil
	}
	specs := map[string]ActivitySpec{}
	controls := map[string]ActivityControl{}
	attempts := map[string]AttemptControl{}
	for _, spec := range r.ActivitySpecs {
		if !validEffectContract(spec) {
			return ErrInvalid
		}
		for _, old := range specs {
			if keyRequestConflict(old, spec) {
				return ErrConflict
			}
		}
		specs[spec.Activity] = spec
	}
	for _, c := range r.ActivityControls {
		controls[c.Activity] = c
	}
	for _, a := range r.AttemptControls {
		attempts[a.Attempt] = a
	}
	guarantees := map[model.ObjectID]IdempotencyGuarantee{}
	versions := map[string]map[uint64]bool{}
	validAuthority := func(spec ActivitySpec, b AuthorityBinding) bool {
		if spec.Execution == "" {
			return zeroBinding(b)
		}
		for _, execution := range s.Executions {
			if execution.ID == spec.Execution {
				return validBinding(b) && b.Incarnation <= execution.Incarnation
			}
		}
		return false
	}
	for _, g := range r.IdempotencyGuarantees {
		spec, exists := specs[g.Activity]
		if !exists || !validGuaranteeShape(g) || !guaranteeMatches(spec, g) || !validAuthority(spec, g.Authority) {
			return ErrInvalid
		}
		if _, exists = guarantees[g.ID]; exists {
			return ErrUniqueSlot
		}
		if versions[g.Activity] == nil {
			versions[g.Activity] = map[uint64]bool{}
		}
		if versions[g.Activity][g.Version] {
			return ErrUniqueSlot
		}
		versions[g.Activity][g.Version] = true
		var ids []string
		for _, a := range attempts {
			if a.Activity == g.Activity && a.Sequence < g.NextSequence {
				ids = append(ids, a.Attempt)
			}
		}
		sort.Strings(ids)
		if len(ids)+1 != int(g.NextSequence) || !slices.Equal(ids, g.CoveredAttempts) {
			return ErrConflict
		}
		guarantees[g.ID] = g
	}
	for _, set := range versions {
		for i := uint64(1); i <= uint64(len(set)); i++ {
			if !set[i] {
				return ErrConflict
			}
		}
	}
	authorizations := map[model.ObjectID]RetryAuthorization{}
	for _, a := range r.RetryAuthorizations {
		spec, exists := specs[a.Activity]
		g, found := guarantees[a.Guarantee]
		if !exists || !found || !validRetryAuthorizationShape(a) || !validAuthority(spec, a.Authority) ||
			a.Activity != g.Activity || a.Sequence != g.NextSequence || a.Authority != g.Authority || g.Status != "active" ||
			a.ControlVersion > controls[a.Activity].Version+1 || a.CheckedAt < g.ValidFrom || a.NotBefore < g.ValidFrom || a.NotAfter > g.ValidUntil {
			return ErrInvalid
		}
		if _, exists = authorizations[a.ID]; exists {
			return ErrUniqueSlot
		}
		authorizations[a.ID] = a
	}
	for _, c := range controls {
		if c.RetryAuthorization == "" {
			if c.RetryObservedAt != 0 {
				return ErrInvalid
			}
			continue
		}
		a, exists := authorizations[c.RetryAuthorization]
		if !exists || a.Activity != c.Activity || a.ControlVersion > c.Version || c.RetryObservedAt < a.NotBefore || c.RetryObservedAt >= a.NotAfter {
			return ErrInvalid
		}
	}
	for _, attempt := range attempts {
		if !validRetryUse(attempt.ClaimUse) || !validRetryUse(attempt.DispatchUse) {
			return ErrInvalid
		}
		for _, use := range []RetryUse{attempt.ClaimUse, attempt.DispatchUse} {
			if use.Authorization == "" {
				continue
			}
			a, exists := authorizations[use.Authorization]
			if !exists || a.Activity != attempt.Activity || a.Sequence != attempt.Sequence || a.Authority != attempt.Authority ||
				use.ObservedAt < a.CheckedAt || use.ObservedAt < a.NotBefore || use.ObservedAt >= a.NotAfter || use.SendNotAfter > a.NotAfter {
				return ErrInvalid
			}
		}
		c := controls[attempt.Activity]
		if attempt.ClaimUse.Authorization != "" && attempt.ClaimUse.Authorization == c.RetryAuthorization && attempt.ClaimUse.ObservedAt < c.RetryObservedAt {
			return ErrInvalid
		}
		if attempt.DispatchUse.Authorization != "" && (attempt.DispatchUse.Authorization != attempt.ClaimUse.Authorization || attempt.DispatchUse.ObservedAt < attempt.ClaimUse.ObservedAt) {
			return ErrInvalid
		}
	}
	return nil
}

func keyRequestConflict(a, b ActivitySpec) bool {
	return a.Effect == "idempotent" && b.Effect == "idempotent" && a.Recipient == b.Recipient && a.Contract.KeyNamespace == b.Contract.KeyNamespace &&
		a.IdempotencyKey == b.IdempotencyKey && (a.RequestDigest != b.RequestDigest || a.Request != b.Request)
}
