package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var (
	ErrInvalidView = errors.New("view is unavailable or closed")
	ErrViewExpired = errors.New("view lease expired")
)

const DefaultMaxViews = 128
const MaxViews = 4096
const DefaultViewTTL = 5 * time.Minute
const MaxViewTTL = time.Hour

type ViewOptions struct {
	MaxViews int
	TTL      time.Duration
}

func (o ViewOptions) checked() (ViewOptions, error) {
	if o.MaxViews == 0 {
		o.MaxViews = DefaultMaxViews
	}
	if o.TTL == 0 {
		o.TTL = DefaultViewTTL
	}
	if o.MaxViews < 1 || o.MaxViews > MaxViews || o.TTL < time.Millisecond || o.TTL > MaxViewTTL {
		return o, storage.ErrLimit
	}
	return o, nil
}

type viewEntry struct {
	// coreLocked is set only on a synthetic synchronous read cut by a caller
	// already holding the engine mutex (or owning its isolated private reducer).
	// It prevents event metadata enumeration from reacquiring that same mutex.
	coreLocked bool
	descriptor api.ViewDescriptor
	roots      model.RootSet
	rootSet    model.ObjectID
	cursorRoot *model.ObjectID
	principal  model.PrincipalID
	mode       string
	expires    time.Time
	active     int
	closing    bool
	timer      *time.Timer
}
type viewRegistry struct {
	mu       sync.Mutex
	entries  map[model.ViewRef]*viewEntry
	pending  int
	stopping bool
	changed  chan struct{}
}

func (v *viewRegistry) init() {
	if v.entries == nil {
		v.entries = make(map[model.ViewRef]*viewEntry)
	}
	if v.changed == nil {
		v.changed = make(chan struct{})
	}
}
func (v *viewRegistry) notify() {
	if v.changed != nil {
		close(v.changed)
	}
	v.changed = make(chan struct{})
}
func (v *viewRegistry) retire(ref model.ViewRef, entry *viewEntry) {
	if entry.closing && entry.active == 0 {
		if entry.timer != nil {
			entry.timer.Stop()
		}
		if v.entries[ref] == entry {
			delete(v.entries, ref)
		}
		v.notify()
	}
}
func (e *Engine) stopViews() {
	v := &e.views
	v.mu.Lock()
	defer v.mu.Unlock()
	v.init()
	v.stopping = true
	for ref, entry := range v.entries {
		entry.closing = true
		v.retire(ref, entry)
	}
	v.notify()
}
func (e *Engine) waitViews(ctx context.Context) error {
	v := &e.views
	for {
		v.mu.Lock()
		v.init()
		if v.pending == 0 && len(v.entries) == 0 {
			v.mu.Unlock()
			return nil
		}
		changed := v.changed
		v.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// OpenView reserves a provisional pin before loading historical roots. The
// registry lock never spans IO; a concurrent close cannot retire resources until
// the provisional pin has either registered or been released.
func (e *Engine) OpenView(ctx context.Context, req api.OpenViewRequest) (api.ViewDescriptor, error) {
	if err := ctx.Err(); err != nil {
		return api.ViewDescriptor{}, err
	}
	if err := validSpace(req.Space); err != nil {
		return api.ViewDescriptor{}, err
	}
	if req.Principal == "" || len(req.Principal) > model.MaxIdentityBytes || !utf8.ValidString(string(req.Principal)) {
		return api.ViewDescriptor{}, model.ErrInvalidRequestKey
	}
	mode := req.Mode
	if mode == "" {
		mode = "state"
	}
	if mode != "state" && mode != "audit_snapshot" && mode != "execution_knowledge" {
		return api.ViewDescriptor{}, ErrUnsupportedMethod
	}
	if err := e.publicGate(); err != nil {
		return api.ViewDescriptor{}, err
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return api.ViewDescriptor{}, err
	}
	if err := e.ready(); err != nil {
		e.mu.Unlock()
		return api.ViewDescriptor{}, err
	}
	anchor := model.SnapshotAnchor{Fence: e.publication.Fence(), Space: req.Space}
	proof := commitEnvelope{fence: anchor.Fence, rootSet: e.rootSet}
	if req.Anchor != nil {
		anchor = req.Anchor.Clone()
		if err := anchor.Validate(); err != nil {
			e.mu.Unlock()
			return api.ViewDescriptor{}, err
		}
		if anchor.Space != req.Space {
			e.mu.Unlock()
			return api.ViewDescriptor{}, model.ErrAnchorMismatch
		}
		known, ok := e.history[anchor.Fence.LSN]
		if !ok || known.fence != anchor.Fence {
			e.mu.Unlock()
			return api.ViewDescriptor{}, model.ErrAnchorMismatch
		}
		proof = known
	}
	if req.Knowledge != nil {
		if anchor.Knowledge != nil && *anchor.Knowledge != *req.Knowledge {
			e.mu.Unlock()
			return api.ViewDescriptor{}, model.ErrAnchorMismatch
		}
		k := *req.Knowledge
		anchor.Knowledge = &k
	}
	if (mode == "execution_knowledge") != (anchor.Knowledge != nil) {
		e.mu.Unlock()
		return api.ViewDescriptor{}, ErrUnsupportedMethod
	}
	v := &e.views
	v.mu.Lock()
	v.init()
	if v.stopping || e.draining.Load() {
		v.mu.Unlock()
		e.mu.Unlock()
		return api.ViewDescriptor{}, storage.ErrClosing
	}
	if len(v.entries)+v.pending >= e.options.Views.MaxViews {
		v.mu.Unlock()
		e.mu.Unlock()
		return api.ViewDescriptor{}, storage.ErrLimit
	}
	v.pending++
	v.mu.Unlock()
	e.mu.Unlock()
	defer func() { v.mu.Lock(); v.pending--; v.notify(); v.mu.Unlock() }()
	roots := model.RootSet{}
	if proof.rootSet != "" {
		obj, err := readObject(ctx, e.index, proof.rootSet)
		if err != nil {
			return api.ViewDescriptor{}, e.viewReadFailure(err)
		}
		var err2 error
		roots, err2 = readRootSet(obj)
		if err2 != nil {
			return api.ViewDescriptor{}, e.viewReadFailure(err2)
		}
	}
	if anchor.Knowledge != nil {
		if err := e.authorizeKnowledge(ctx, roots, req.Space, req.Principal, *anchor.Knowledge); err != nil {
			if knowledgeCommandRejection(err) {
				return api.ViewDescriptor{}, err
			}
			return api.ViewDescriptor{}, e.viewReadFailure(err)
		}
	}
	var cursorRoot *model.ObjectID
	if anchor.Knowledge != nil {
		if _, _, _, _, err := e.selectedKnowledge(ctx, roots, anchor, req.Principal); err != nil {
			if knowledgeCommandRejection(err) || isLimit(err) {
				return api.ViewDescriptor{}, err
			}
			return api.ViewDescriptor{}, e.viewReadFailure(err)
		}
	} else if anchor.Cursor != nil {
		root, err := e.resolveDomainCursor(ctx, roots, anchor)
		if err != nil {
			if errors.Is(err, model.ErrAnchorMismatch) || errors.Is(err, ErrUnsupportedMethod) || isLimit(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return api.ViewDescriptor{}, err
			}
			return api.ViewDescriptor{}, e.viewReadFailure(err)
		}
		cursorRoot = &root
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return api.ViewDescriptor{}, err
	}
	ref := model.ViewRef(hex.EncodeToString(raw[:]))
	desc := api.ViewDescriptor{Ref: ref, Anchor: anchor, ViewDigest: viewDigest(anchor, proof.rootSet, req.Principal, mode)}
	if err := ctx.Err(); err != nil {
		return api.ViewDescriptor{}, err
	}
	if err := e.viewOutputGate(); err != nil {
		return api.ViewDescriptor{}, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.stopping || e.draining.Load() {
		return api.ViewDescriptor{}, storage.ErrClosing
	}
	if _, ok := v.entries[ref]; ok {
		return api.ViewDescriptor{}, fmt.Errorf("%w: view identity collision", ErrInvalidView)
	}
	entry := &viewEntry{descriptor: desc, roots: roots, rootSet: proof.rootSet, cursorRoot: cursorRoot, principal: req.Principal, mode: mode, expires: time.Now().Add(e.options.Views.TTL)}
	v.entries[ref] = entry
	entry.timer = time.AfterFunc(e.options.Views.TTL, func() {
		v.mu.Lock()
		defer v.mu.Unlock()
		if v.entries[ref] == entry {
			entry.closing = true
			v.retire(ref, entry)
		}
	})
	desc.Anchor = desc.Anchor.Clone()
	if _, ok := ctx.Value(readDeliveryKey{}).(*readDeliveryCapture); ok {
		if err := e.mu.LockContext(ctx); err != nil {
			return api.ViewDescriptor{}, err
		}
		defer e.mu.Unlock()
		return finishReadDelivery(ctx, e, "open_view", req.Principal, desc, desc.Anchor, desc.Ref, desc.ViewDigest, nil)
	}
	return desc, nil
}

func viewDigest(anchor model.SnapshotAnchor, rootSet model.ObjectID, principal model.PrincipalID, mode string) model.ObjectID {
	b := []byte("ATE-VIEW\x00")
	for _, s := range []string{string(anchor.Fence.Store), string(anchor.Space), string(anchor.Fence.CommitDigest), string(principal), mode, string(rootSet)} {
		b, _ = codec.AppendString(b, s)
	}
	b = codec.AppendUint(b, anchor.Fence.LSN)
	if k := anchor.Knowledge; k != nil {
		b, _ = codec.AppendString(b, string(k.Actor))
		b, _ = codec.AppendString(b, string(k.Execution))
	}
	if c := anchor.Cursor; c != nil {
		b, _ = codec.AppendString(b, "cursor")
		b, _ = codec.AppendString(b, string(c.Store))
		b, _ = codec.AppendString(b, string(c.Space))
		b, _ = codec.AppendString(b, string(c.Revision))
	}
	sum := sha256.Sum256(b)
	return model.ObjectID(hex.EncodeToString(sum[:]))
}

func (e *Engine) viewDomainRoot(ctx context.Context, entry *viewEntry) (model.ObjectID, error) {
	if entry.cursorRoot != nil {
		return *entry.cursorRoot, nil
	}
	return domainRoot(ctx, e.index, entry.roots, entry.descriptor.Anchor.Space)
}

func (e *Engine) CloseView(ctx context.Context, req api.CloseViewRequest) error {
	v := &e.views
	v.mu.Lock()
	v.init()
	entry := v.entries[req.Ref]
	if entry == nil {
		v.mu.Unlock()
		return nil
	}
	entry.closing = true
	v.retire(req.Ref, entry)
	for entry.active > 0 {
		changed := v.changed
		v.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
		v.mu.Lock()
	}
	v.mu.Unlock()
	return nil
}
func (e *Engine) acquireView(ctx context.Context, ref model.ViewRef) (*viewEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := e.publicGate(); err != nil {
		return nil, err
	}
	v := &e.views
	v.mu.Lock()
	defer v.mu.Unlock()
	v.init()
	if v.stopping || e.draining.Load() {
		return nil, storage.ErrClosing
	}
	entry := v.entries[ref]
	if entry == nil || entry.closing {
		return nil, ErrInvalidView
	}
	if !time.Now().Before(entry.expires) {
		entry.closing = true
		v.retire(ref, entry)
		return nil, ErrViewExpired
	}
	entry.active++
	return entry, nil
}
func (e *Engine) releaseView(ref model.ViewRef, entry *viewEntry) {
	v := &e.views
	v.mu.Lock()
	defer v.mu.Unlock()
	entry.active--
	v.retire(ref, entry)
	v.notify()
}
func (e *Engine) viewOutputGate() error {
	// Draining already-acquired operations may finish, but a fenced publication
	// can never grant new output from a previously captured root.
	if e.journal == nil {
		return storage.ErrClosed
	}
	_, err := e.journal.Published()
	return err
}
func (e *Engine) viewReadFailure(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.readFailure(err)
}
func (e *Engine) GetViewState(ctx context.Context, req api.ReadViewStateRequest) (api.StateValueResult, error) {
	if len(req.Key) > api.MaxStateKeyBytes {
		return api.StateValueResult{}, state.ErrInvalidKey
	}
	entry, err := e.acquireView(ctx, req.View)
	if err != nil {
		return api.StateValueResult{}, err
	}
	defer e.releaseView(req.View, entry)
	if entry.mode == "execution_knowledge" {
		return api.StateValueResult{}, ErrUnsupportedMethod
	}
	root, err := e.viewDomainRoot(ctx, entry)
	if err != nil {
		return api.StateValueResult{}, e.viewReadFailure(err)
	}
	id, found, err := state.Get(ctx, e.index, entry.descriptor.Anchor.Space, root, req.Key)
	if err != nil {
		return api.StateValueResult{}, e.viewReadFailure(err)
	}
	result := api.StateValueResult{Found: found, StateRoot: root, RootSet: entry.rootSet, Fence: entry.descriptor.Anchor.Fence}
	if found {
		obj, err := readObject(ctx, e.index, id)
		if err != nil {
			return api.StateValueResult{}, e.viewReadFailure(err)
		}
		if obj.Space != entry.descriptor.Anchor.Space {
			return api.StateValueResult{}, e.viewReadFailure(ErrInvalidStateData)
		}
		result.Value, err = readValue(obj)
		if err != nil {
			return api.StateValueResult{}, e.viewReadFailure(err)
		}
	}
	if err = ctx.Err(); err != nil {
		return api.StateValueResult{}, err
	}
	var dependencies []model.ObjectID
	if found && readDeliveryPrincipal(ctx) != "" {
		dependencies = []model.ObjectID{id}
	}
	if found {
		derived, derivedErr := e.derivedDisclosureIDs(ctx, entry.descriptor.Anchor.Space, []model.ObjectID{id})
		if derivedErr != nil {
			return api.StateValueResult{}, e.viewReadFailure(derivedErr)
		}
		dependencies = append(dependencies, derived...)
	}
	return finishViewReadDelivery(ctx, e, entry, "get_view_state", result, dependencies)
}

func (e *Engine) GetViewEntries(ctx context.Context, req api.ReadViewEntriesRequest) (api.StateEntriesResult, error) {
	entry, err := e.acquireView(ctx, req.View)
	if err != nil {
		return api.StateEntriesResult{}, err
	}
	defer e.releaseView(req.View, entry)
	if entry.mode == "execution_knowledge" {
		return api.StateEntriesResult{}, ErrUnsupportedMethod
	}
	space := entry.descriptor.Anchor.Space
	root, err := e.viewDomainRoot(ctx, entry)
	if err != nil {
		return api.StateEntriesResult{}, e.viewReadFailure(err)
	}
	entries, err := state.Entries(ctx, e.index, space, root, state.ScanLimits{
		MaxNodes: req.Limits.MaxNodes, MaxEntries: req.Limits.MaxEntries,
		MaxBytes: req.Limits.MaxBytes, MaxReadBytes: req.Limits.MaxReadBytes,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return api.StateEntriesResult{}, err
		}
		return api.StateEntriesResult{}, e.viewReadFailure(err)
	}
	items := make([]api.StateEntry, len(entries))
	for i, item := range entries {
		obj, readErr := readObject(ctx, e.index, item.Value)
		if readErr != nil || obj.Space != space {
			if readErr == nil {
				readErr = ErrInvalidStateData
			}
			return api.StateEntriesResult{}, e.viewReadFailure(readErr)
		}
		if _, readErr = readValue(obj); readErr != nil {
			return api.StateEntriesResult{}, e.viewReadFailure(readErr)
		}
		items[i] = api.StateEntry{Key: append([]byte(nil), item.Key...), Value: item.Value}
	}
	if err = ctx.Err(); err != nil {
		return api.StateEntriesResult{}, err
	}
	var dependencies []model.ObjectID
	if readDeliveryPrincipal(ctx) != "" {
		dependencies = make([]model.ObjectID, len(items))
		for i, item := range items {
			dependencies[i] = item.Value
		}
	}
	if len(items) != 0 {
		ids := make([]model.ObjectID, len(items))
		for i, item := range items {
			ids[i] = item.Value
		}
		derived, derivedErr := e.derivedDisclosureIDs(ctx, space, ids)
		if derivedErr != nil {
			return api.StateEntriesResult{}, e.viewReadFailure(derivedErr)
		}
		dependencies = append(dependencies, derived...)
	}
	result := api.StateEntriesResult{Anchor: entry.descriptor.Anchor.Clone(), StateRoot: root, RootSet: entry.rootSet, Fence: entry.descriptor.Anchor.Fence, Items: items}
	return finishViewReadDelivery(ctx, e, entry, "get_view_entries", result, dependencies)
}
