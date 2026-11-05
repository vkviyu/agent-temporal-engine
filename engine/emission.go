package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var ErrEmissionRejected = errors.New("emission grant rejected")
var _ api.EmissionCommands = (*Engine)(nil)

// emissionKey uses a simple framed hash to keep maximum key sizes bounded.
func emissionStorageKey(space model.SpaceID, principal model.PrincipalID, id string) []byte {
	h := sha256.New()
	h.Write([]byte("ATE-EMISSION-KEY\x00"))
	for _, s := range []string{string(space), string(principal), id} {
		var length [10]byte
		n := binary.PutUvarint(length[:], uint64(len(s)))
		h.Write(length[:n])
		h.Write([]byte(s))
	}
	return []byte("ate/emission/v1/" + hex.EncodeToString(h.Sum(nil)))
}

func encodeEmissionRecord(r api.EmissionRecord) ([]byte, error) {
	if r.Status != "issued" && r.Status != "claimed" && r.Status != "revoked" || r.Version == 0 {
		return nil, ErrEmissionRejected
	}
	if r.Proof == nil {
		return nil, ErrEmissionRejected
	}
	if r.Proof != nil {
		p := r.Proof
		digest, err := api.EmissionDependencyDigest(p.Dependencies)
		if err != nil || digest != p.DependencyDigest || p.Space != r.Spec.Anchor.Space || p.Principal == "" || p.PolicyFence.Validate() != nil || p.PolicyFence.Store != r.Spec.Anchor.Fence.Store || p.PolicyFence.LSN < r.Spec.Anchor.Fence.LSN || (p.PolicyRoot != "" && !model.ValidDigest(string(p.PolicyRoot))) {
			return nil, ErrEmissionRejected
		}
		contentFound := false
		for _, id := range p.Dependencies {
			contentFound = contentFound || id == r.Spec.Content
		}
		if !contentFound {
			return nil, ErrEmissionRejected
		}
		if r.Spec.Anchor.Validate() != nil || ((r.Spec.ContentKind == "" || r.Spec.ContentKind == "raw") && (r.Spec.Anchor.Cursor != nil || r.Spec.Anchor.Knowledge != nil)) {
			return nil, ErrEmissionRejected
		}
		projection := r.Spec.Projection
		if r.Spec.ContentKind == "packet" || r.Spec.ContentKind == "summary" {
			projection = "value_bytes"
		} else if projection == "" {
			projection = "payload"
		}
		if (p.Projection != "payload" && p.Projection != "value_bytes") || p.Projection != projection || !model.ValidDigest(string(p.ContentDigest)) || !model.ValidDigest(string(p.RangeDigest)) || r.Spec.Offset > p.TotalBytes || r.Spec.Length == 0 || r.Spec.Length > p.TotalBytes-r.Spec.Offset {
			return nil, ErrEmissionRejected
		}
		if r.Spec.Offset == 0 && r.Spec.Length == p.TotalBytes && p.ContentDigest != p.RangeDigest {
			return nil, ErrEmissionRejected
		}
	}
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return b, nil
}
func decodeEmissionRecord(b []byte) (api.EmissionRecord, error) {
	var r api.EmissionRecord
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return r, err
	}
	raw, err := encodeEmissionRecord(r)
	if err != nil || !bytes.Equal(raw, b) {
		return r, ErrInvalidStateData
	}
	return r, nil
}

func (e *Engine) loadEmission(ctx context.Context, roots model.RootSet, space model.SpaceID, principal model.PrincipalID, id string) (api.EmissionRecord, bool, error) {
	root := roots.LiveControl
	oid, found, err := state.Get(ctx, e.index, systemSpace, root, emissionStorageKey(space, principal, id))
	if err != nil || !found {
		return api.EmissionRecord{}, found, err
	}
	obj, err := readObject(ctx, e.index, oid)
	if err != nil {
		return api.EmissionRecord{}, false, err
	}
	v, err := readValue(obj)
	if err != nil || v.Kind != api.ValueBytes {
		return api.EmissionRecord{}, false, ErrInvalidStateData
	}
	r, err := decodeEmissionRecord(v.Data)
	if err != nil || r.Spec.ID != id || obj.Space != space {
		return api.EmissionRecord{}, false, ErrInvalidStateData
	}
	if r.Proof != nil && (r.Proof.Space != space || r.Proof.Principal != principal) {
		return api.EmissionRecord{}, false, ErrInvalidStateData
	}
	return r, true, nil
}

func emissionAuthority(l *agentprofile.Ledger, execution model.ExecutionID) (agentprofile.Execution, agentprofile.Task, agentprofile.ExecutionAuthority, error) {
	x, err := l.GetExecution(execution)
	if err != nil {
		return agentprofile.Execution{}, agentprofile.Task{}, agentprofile.ExecutionAuthority{}, err
	}
	t, err := l.GetTask(x.Task)
	if err != nil {
		return agentprofile.Execution{}, agentprofile.Task{}, agentprofile.ExecutionAuthority{}, err
	}
	s := l.Snapshot()
	if s.Lifecycle.Recovery != nil {
		for _, a := range s.Lifecycle.Recovery.Authorities {
			if a.Execution == execution && a.Status == "active" {
				return x, t, a, nil
			}
		}
	}
	return agentprofile.Execution{}, agentprofile.Task{}, agentprofile.ExecutionAuthority{}, agentprofile.ErrConflict
}
func (e *Engine) checkEmissionContent(ctx context.Context, req api.CommitRequest, s api.EmissionSpec) error {
	roots, _, err := e.rootsAtFence(ctx, s.Anchor.Fence, true)
	if err != nil {
		return err
	}
	if s.Anchor.Space != req.Space {
		return agentprofile.ErrCommandScope
	}
	if s.Anchor.Fence.LSN >= e.publication.Position.LSN+1 {
		return model.ErrAnchorMismatch
	}
	return e.visibleSourceObject(ctx, roots, s.Anchor, req.Principal, s.Content)
}

func (e *Engine) checkEmissionReuse(ctx context.Context, space model.SpaceID, ids []model.ObjectID) error {
	current, _, _, err := e.loadMemory(ctx, e.roots, space)
	if err != nil {
		return err
	}
	return current.AccessPolicy().Check(ids, memory.AccessReuse)
}

func (e *Engine) ApplyEmission(ctx context.Context, req api.ApplyEmissionRequest) (api.CommitResult, error) {
	b, err := api.EncodeEmissionCommand(req.Command)
	if err != nil {
		return admissionFailure(req.Key.RequestID, err)
	}
	return e.Commit(ctx, api.CommitRequest{Space: req.Key.Space, Principal: req.Key.Principal, RequestID: req.Key.RequestID, Method: api.MethodApplyEmission, Body: b, ExpectedFence: req.ExpectedFence})
}

func (e *Engine) prepareEmission(ctx context.Context, req api.CommitRequest, digest model.ObjectID, txID model.TxID, lsn uint64, recorded *operationsRecord) (*commitPlan, error) {
	if !e.options.AgentProfile {
		return nil, ErrUnsupportedMethod
	}
	if req.ExpectedFence != nil && *req.ExpectedFence != e.publication.Fence() {
		return nil, errCAS
	}
	c, err := api.DecodeEmissionCommand(req.Body)
	if err != nil {
		return nil, err
	}
	if c.SchemaVersion != 3 {
		return nil, agentprofile.ErrCommandVersion
	}
	ledger, _, found, err := loadAgentLedger(ctx, e.index, e.roots.LiveControl, req.Space, req.Principal)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, agentprofile.ErrCommandScope
	}
	keyID := ""
	var next api.EmissionRecord
	var existing bool
	if c.Issue != nil {
		keyID = c.Issue.ID
		_, existing, err = e.loadEmission(ctx, e.roots, req.Space, req.Principal, keyID)
		if err != nil {
			return nil, err
		}
		if existing {
			return nil, agentprofile.ErrUniqueSlot
		}
		if err := checkEmissionOwner(ledger, req, *c.Issue, c.Issue.Authority); err != nil {
			return nil, err
		}
		if c.Issue.NotAfterLSN != 0 && lsn > c.Issue.NotAfterLSN {
			return nil, ErrEmissionRejected
		}
		proof, err := e.prepareEmissionProof(ctx, req, ledger, *c.Issue)
		if err != nil {
			return nil, err
		}
		if err := checkEmissionActivity(ledger, *c.Issue, proof); err != nil {
			return nil, err
		}
		next = api.EmissionRecord{Spec: *c.Issue, Status: "issued", Version: 1, Proof: proof}
	} else if c.ClaimID != "" {
		keyID = c.ClaimID
		next, existing, err = e.loadEmission(ctx, e.roots, req.Space, req.Principal, keyID)
		if err != nil {
			return nil, err
		}
		if !existing {
			return nil, fmt.Errorf("%w: unknown grant", ErrEmissionRejected)
		}
		if next.Status != "issued" || next.Version != c.ClaimVersion {
			return nil, ErrEmissionRejected
		}
		if next.Spec.Activity != "" && !e.privateReduction {
			return nil, ErrWorkBoundary
		}
		if err := checkEmissionOwner(ledger, req, next.Spec, c.ClaimAuthority); err != nil {
			return nil, err
		}
		if next.Spec.NotAfterLSN != 0 && lsn > next.Spec.NotAfterLSN {
			return nil, ErrEmissionRejected
		}
		proof, err := e.prepareEmissionProof(ctx, req, ledger, next.Spec)
		if err != nil {
			return nil, err
		}
		if err := checkEmissionActivity(ledger, next.Spec, proof); err != nil {
			return nil, err
		}
		// Re-resolve the immutable projection and compare its exact bytes. A
		// historical proof is evidence, never a substitute for today's gate.
		if proof.ContentDigest != next.Proof.ContentDigest || proof.RangeDigest != next.Proof.RangeDigest || proof.TotalBytes != next.Proof.TotalBytes || proof.Projection != next.Proof.Projection {
			return nil, ErrEmissionRejected
		}
		if err := e.checkEmissionReuse(ctx, req.Space, next.Proof.Dependencies); err != nil {
			return nil, err
		}
		next.Status = "claimed"
		next.Version++
		next.ClaimAuthority = c.ClaimAuthority
	} else {
		keyID = c.RevokeID
		next, existing, err = e.loadEmission(ctx, e.roots, req.Space, req.Principal, keyID)
		if err != nil {
			return nil, err
		}
		if !existing {
			return nil, fmt.Errorf("%w: unknown grant", ErrEmissionRejected)
		}
		if next.Version != c.RevokeVersion || next.Status == "revoked" {
			return nil, ErrEmissionRejected
		}
		next.Status = "revoked"
		next.Version++
	}
	raw, err := encodeEmissionRecord(next)
	if err != nil {
		return nil, err
	}
	b := newBuilder(ctx, e.index, e.options.MaxObjects)
	cmd, err := makeCommand(req)
	if err != nil {
		return nil, err
	}
	cmdID, err := b.add(cmd, storage.KindObject)
	if err != nil {
		return nil, err
	}
	val, err := makeValue(req.Space, api.Value{Kind: api.ValueBytes, Data: raw})
	if err != nil {
		return nil, err
	}
	valID, err := b.add(val, storage.KindObject)
	if err != nil {
		return nil, err
	}
	beforeID, _, err := state.Get(ctx, e.index, systemSpace, e.roots.LiveControl, emissionStorageKey(req.Space, req.Principal, keyID))
	if err != nil {
		return nil, err
	}
	delta, err := state.Apply(ctx, b, systemSpace, e.roots.LiveControl, []state.Mutation{{Key: emissionStorageKey(req.Space, req.Principal, keyID), Value: valID}})
	if err != nil {
		return nil, err
	}
	for _, o := range delta.Objects {
		if _, err = b.add(o, storage.KindObject); err != nil {
			return nil, err
		}
	}
	ops, err := makeOperations(operationsRecord{Space: req.Space, BeforeRoot: e.roots.LiveControl, AfterRoot: delta.Root, Changes: []stateChange{{Key: emissionStorageKey(req.Space, req.Principal, keyID), Before: beforeID, After: valID}}})
	if err != nil {
		return nil, err
	}
	if recorded != nil {
		old, er := makeOperations(*recorded)
		if er != nil || !bytes.Equal(old.Payload, ops.Payload) {
			return nil, ErrInvalidStateData
		}
	}
	opsID, err := b.add(ops, storage.KindStateOps)
	if err != nil {
		return nil, err
	}
	roots := e.roots
	roots.LiveControl = delta.Root
	return e.finishPlan(ctx, req, digest, txID, lsn, b, roots, roots.LiveControl, cmdID, opsID)
}

func emissionCommandRejection(err error) bool {
	return knowledgeCommandRejection(err) || errors.Is(err, ErrWorkBoundary) || errors.Is(err, ErrEmissionRejected) || errors.Is(err, agentprofile.ErrCommandVersion) || errors.Is(err, agentprofile.ErrConflict) || errors.Is(err, agentprofile.ErrUniqueSlot) || errors.Is(err, agentprofile.ErrCommandScope) || errors.Is(err, memory.ErrUseDenied) || errors.Is(err, memory.ErrHistoryAccessDenied)
}

func (e *Engine) GetEmission(ctx context.Context, req api.ReadEmissionRequest) (api.EmissionResult, error) {
	if err := ctx.Err(); err != nil {
		return api.EmissionResult{}, err
	}
	if !e.options.AgentProfile {
		return api.EmissionResult{}, ErrUnsupportedMethod
	}
	if req.ID == "" || validAgentScope(req.Space, req.Principal) != nil {
		return api.EmissionResult{}, api.ErrInvalidRequest
	}
	if err := e.publicGate(); err != nil {
		return api.EmissionResult{}, err
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return api.EmissionResult{}, err
	}
	defer e.mu.Unlock()
	if err := e.ready(); err != nil {
		return api.EmissionResult{}, err
	}
	a := model.SnapshotAnchor{Space: req.Space, Fence: e.publication.Fence()}
	if req.Anchor != nil {
		a = req.Anchor.Clone()
	}
	if a.Validate() != nil || a.Space != req.Space || a.Cursor != nil || a.Knowledge != nil {
		return api.EmissionResult{}, model.ErrAnchorMismatch
	}
	roots, root, err := e.rootsAtFence(ctx, a.Fence, false)
	if err != nil {
		return api.EmissionResult{}, e.knowledgeReadFailure(err)
	}
	r, found, err := e.loadEmission(ctx, roots, req.Space, req.Principal, req.ID)
	if err != nil {
		return api.EmissionResult{}, e.knowledgeReadFailure(err)
	}
	var ids []model.ObjectID
	if found {
		ids = []model.ObjectID{r.Spec.Content}
		if r.Proof != nil {
			ids = r.Proof.Dependencies
		}
		if err := e.checkDisclosureIDs(ctx, req.Space, ids); err != nil {
			return api.EmissionResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return api.EmissionResult{}, err
	}
	if err := e.viewOutputGate(); err != nil {
		return api.EmissionResult{}, err
	}
	out := api.EmissionResult{Found: found, Record: r, Anchor: a, Fence: a.Fence, RootSet: root}
	return finishReadDelivery(ctx, e, "get_emission", req.Principal, out, a, "", "", ids)
}
