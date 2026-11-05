package engine

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func typedContextMode(mode string) bool {
	return mode == "recorded_input" || mode == "as_known_at_anchor" || mode == "evidence_neighborhood" || mode == "retrospective"
}

var typedContextSections = []string{"node", "inputs", "upstream", "downstream", "stateDiff", "messages", "activityReceipts", "artifacts"}

func contextSections(req api.NodeContextRequest) (string, []string, error) {
	mode := req.Mode
	if mode == "" {
		mode = "evidence_neighborhood"
	}
	if !typedContextMode(mode) || (req.Projection != "" && req.Projection != "summary") {
		return "", nil, ErrUnsupportedMethod
	}
	sections := append([]string(nil), req.Sections...)
	if len(sections) == 0 {
		if mode == "recorded_input" {
			sections = []string{"inputs"}
		} else {
			sections = append(sections, typedContextSections...)
		}
	}
	if len(sections) > len(typedContextSections) {
		return "", nil, api.ErrInvalidRequest
	}
	sort.Strings(sections)
	for i, s := range sections {
		valid := false
		for _, known := range typedContextSections {
			valid = valid || s == known
		}
		if !valid || (i > 0 && s == sections[i-1]) || (mode == "recorded_input" && s != "node" && s != "inputs") {
			return "", nil, api.ErrInvalidRequest
		}
	}
	return mode, sections, nil
}

type typedContextItem struct {
	key, section string
	summary      *api.NodeSummary
	change       *api.NodeStateChange
}

func nodeContextRelations(c nodeCatalog) (map[model.NodeRef][]model.NodeRef, map[model.NodeRef][]model.NodeRef, error) {
	parents, children := map[model.NodeRef][]model.NodeRef{}, map[model.NodeRef][]model.NodeRef{}
	edges := 0
	for ref, n := range c.nodes {
		seen := map[model.NodeRef]bool{}
		for _, p := range append(append([]model.NodeRef(nil), n.parents...), n.detail.Sources...) {
			if _, visible := c.nodes[p]; !visible || p == ref || seen[p] {
				continue
			}
			seen[p] = true
			edges++
			if edges > maxNodeQueryRecords {
				return nil, nil, storage.ErrLimit
			}
			parents[ref] = append(parents[ref], p)
			children[p] = append(children[p], ref)
		}
	}
	for _, m := range []map[model.NodeRef][]model.NodeRef{parents, children} {
		for ref := range m {
			sort.Slice(m[ref], func(i, j int) bool { return nodeSortKey(m[ref][i]) < nodeSortKey(m[ref][j]) })
		}
	}
	return parents, children, nil
}

func contextWalk(start model.NodeRef, links map[model.NodeRef][]model.NodeRef, maxDepth int) (map[model.NodeRef]int, bool) {
	seen := map[model.NodeRef]int{start: 0}
	queue := []model.NodeRef{start}
	limited := false
	for len(queue) > 0 {
		ref := queue[0]
		queue = queue[1:]
		for _, next := range links[ref] {
			if _, exists := seen[next]; exists {
				continue
			}
			if seen[ref] == maxDepth {
				limited = true
				continue
			}
			seen[next] = seen[ref] + 1
			queue = append(queue, next)
		}
	}
	return seen, limited
}

func typedNodePartition(n nodeProjection) string {
	switch n.detail.Summary.Kind {
	case "ActivitySpec", "ContextPacket", "recorded_input", "ActionIntent", "declared_input":
		return "inputs"
	case "Message", "MessageConsumption":
		return "messages"
	case "Attempt", "ActivityResult", "ActivityControl", "AttemptControl", "ResultCapture", "ActivityAcceptance", "ActivityResultImport", "ActivitySettlement", "AttemptReconciliation", "ActionResolution", "receipt":
		return "activityReceipts"
	case "TaskOutcome", "ResumePlan", "ValidationRequirement", "artifact":
		return "artifacts"
	}
	return ""
}

func (e *Engine) getTypedNodeContext(ctx context.Context, req api.NodeContextRequest) (api.NodeContext, error) {
	l, err := checkedNodeLimits(req.Limits)
	if err != nil {
		return api.NodeContext{}, err
	}
	mode, sections, err := contextSections(req)
	if err != nil {
		return api.NodeContext{}, err
	}
	if req.Ref.Validate() != nil {
		return api.NodeContext{}, model.ErrInvalidNodeRef
	}
	entry, err := e.acquireView(ctx, req.View)
	if err != nil {
		return api.NodeContext{}, err
	}
	defer e.releaseView(req.View, entry)
	if (mode == "as_known_at_anchor" && entry.descriptor.Anchor.Knowledge == nil) || (mode == "retrospective" && entry.mode != "audit_snapshot") {
		return api.NodeContext{}, api.ErrInvalidRequest
	}
	scope := nodePageScope(entry, "typed_context", struct {
		Ref              model.NodeRef
		Mode, Projection string
		Sections         []string
		Depth            int
	}{req.Ref, mode, req.Projection, sections, l.depth})
	after, err := readNodeToken(req.Continuation, scope)
	if err != nil {
		return api.NodeContext{}, err
	}
	c, err := e.nodeContextCatalog(ctx, entry)
	if err != nil {
		return api.NodeContext{}, e.nodeReadError(err)
	}
	// Domain subjects are inspected lazily from their registered root. Expand
	// only this request's subject and explicit references, not every event's
	// payload, and never enlarge an execution-knowledge View.
	if err = addRequestedDomainContext(ctx, &nodeQueryReader{source: e.index}, entry, &c, req.Ref); err != nil {
		return api.NodeContext{}, e.nodeReadError(err)
	}
	n, exists := c.nodes[req.Ref]
	if !exists {
		return api.NodeContext{}, ErrNodeNotFound
	}
	anchors, err := e.nodeDeclaredAnchors(ctx, entry, n)
	if err != nil {
		return api.NodeContext{}, e.nodeReadError(err)
	}
	out := api.NodeContext{Ref: req.Ref, Anchor: entry.descriptor.Anchor.Clone(), Mode: mode, Coverage: "bounded_explicit_relations", Anchors: anchors, Sections: map[string][]api.NodeSummary{}}
	if mode == "as_known_at_anchor" {
		out.Coverage = "execution_knowledge_at_anchor"
	}
	if mode == "retrospective" {
		out.Coverage = "retrospective_at_view_fence"
	}
	wanted := map[string]bool{}
	for _, s := range sections {
		wanted[s] = true
		out.Sections[s] = []api.NodeSummary{}
	}
	parents, children, err := nodeContextRelations(c)
	if err != nil {
		return api.NodeContext{}, err
	}
	up, upLimited := contextWalk(req.Ref, parents, l.depth)
	down, downLimited := contextWalk(req.Ref, children, l.depth)
	out.DepthLimited = upLimited || downLimited
	items := map[string]typedContextItem{}
	add := func(section string, ref model.NodeRef) {
		if !wanted[section] {
			return
		}
		p, visible := c.nodes[ref]
		if !visible {
			return
		}
		s := p.detail.Summary
		key := section + "/" + nodeSortKey(ref)
		items[key] = typedContextItem{key: key, section: section, summary: &s}
	}
	if wanted["node"] {
		add("node", req.Ref)
	}
	reached := map[model.NodeRef]int{req.Ref: 0}
	for ref, depth := range up {
		if ref != req.Ref {
			add("upstream", ref)
		}
		reached[ref] = depth
	}
	for ref, depth := range down {
		if ref != req.Ref {
			add("downstream", ref)
		}
		if old, exists := reached[ref]; !exists || depth < old {
			reached[ref] = depth
		}
	}
	missing := map[string]bool{}
	if wanted["upstream"] {
		for ref := range up {
			p := c.nodes[ref]
			for _, source := range p.detail.Sources {
				if source.Kind == model.NodeObject && source.Object == p.detail.Payload {
					continue
				}
				if _, visible := c.nodes[source]; !visible {
					missing["upstream"] = true
				}
			}
		}
	}
	hasInput := false
	// Partition-specific source expansion follows the record's declared links,
	// rather than going up to an Execution and down into unrelated siblings.
	for ref, depth := range reached {
		p := c.nodes[ref]
		section := typedNodePartition(p)
		if section == "" || !wanted[section] {
			continue
		}
		add(section, ref)
		if section == "inputs" {
			hasInput = true
		}
		if section != "inputs" && section != "artifacts" && section != "activityReceipts" {
			continue
		}
		type sourceStep struct {
			ref   model.NodeRef
			depth int
		}
		queue := []sourceStep{{ref, depth}}
		seen := map[model.NodeRef]bool{ref: true}
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			for _, source := range c.nodes[current.ref].detail.Sources {
				// The profile snapshot is a storage container, not an input.
				if source.Kind == model.NodeObject && source.Object == p.detail.Payload {
					continue
				}
				_, visible := c.nodes[source]
				if !visible {
					missing[section] = true
					continue
				}
				if source.Kind != model.NodeObject {
					continue
				}
				if current.depth >= l.depth {
					out.DepthLimited = true
					continue
				}
				add(section, source)
				if !seen[source] {
					seen[source] = true
					queue = append(queue, sourceStep{source, current.depth + 1})
				}
			}
		}
	}
	if wanted["inputs"] && !hasInput {
		out.Gaps = append(out.Gaps, api.NodeContextGap{Section: "inputs", Reason: "no_recorded_input"})
	}
	for _, section := range sections {
		if missing[section] {
			out.Gaps = append(out.Gaps, api.NodeContextGap{Section: section, Reason: "unavailable_in_view"})
		}
	}
	if wanted["stateDiff"] {
		changes, available, err := nodeRevisionChanges(c, n)
		if err != nil {
			return api.NodeContext{}, e.nodeReadError(err)
		}
		if !available {
			out.Gaps = append(out.Gaps, api.NodeContextGap{Section: "stateDiff", Reason: "no_state_revision"})
		}
		for _, change := range changes {
			v := change
			key := "stateDiff/" + hex.EncodeToString(v.Key)
			items[key] = typedContextItem{key: key, section: "stateDiff", change: &v}
		}
	}
	ordered := make([]typedContextItem, 0, len(items))
	for _, item := range items {
		ordered = append(ordered, item)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].key < ordered[j].key })
	if len(ordered) > maxNodeQueryRecords {
		return api.NodeContext{}, storage.ErrLimit
	}
	count := 0
	last := ""
	ids := []model.ObjectID{}
	for _, item := range ordered {
		if item.key <= after {
			continue
		}
		if count >= l.nodes || count >= l.edges {
			out.Truncated = true
			break
		}
		if item.summary != nil {
			out.Sections[item.section] = append(out.Sections[item.section], *item.summary)
		} else {
			out.StateChanges = append(out.StateChanges, *item.change)
		}
		// Include a continuation reservation before accepting this item. This
		// also bounds gaps/anchors, which repeat unchanged on subsequent pages.
		out.Continuation = writeNodeToken(scope, item.key)
		if jsonBytes(out) > l.bytes {
			if item.summary != nil {
				out.Sections[item.section] = out.Sections[item.section][:len(out.Sections[item.section])-1]
			} else {
				out.StateChanges = out.StateChanges[:len(out.StateChanges)-1]
			}
			if count == 0 {
				return api.NodeContext{}, storage.ErrLimit
			}
			out.Truncated = true
			break
		}
		last = item.key
		count++
		if item.summary != nil {
			ids = append(ids, nodeDisclosureIDs(c.nodes[item.summary.Ref])...)
		}
		if item.change != nil {
			ids = append(ids, item.change.Before, item.change.After)
		}
	}
	if out.Truncated {
		out.Continuation = writeNodeToken(scope, last)
	} else {
		out.Continuation = ""
	}
	if jsonBytes(out) > l.bytes {
		return api.NodeContext{}, storage.ErrLimit
	}
	if err = ctx.Err(); err != nil {
		return api.NodeContext{}, err
	}
	return finishNodeRead(ctx, e, entry, "get_node_context", out, c, ids)
}

// Both sides were decoded and authenticated against their actual StateRoot by
// addRevisionNodes. The baseline is the explicit StateParent, never live head.
func nodeRevisionChanges(c nodeCatalog, n nodeProjection) ([]api.NodeStateChange, bool, error) {
	if n.detail.Encoding != "graph/Revision/v1+json" {
		return nil, false, nil
	}
	var revision graph.Revision
	if json.Unmarshal(n.detail.Data, &revision) != nil {
		return nil, false, ErrInvalidStateData
	}
	var parent graph.Revision
	if revision.StateParent != "" {
		p, ok := c.nodes[model.NodeRef{Kind: model.NodeRevision, Revision: revision.StateParent}]
		if !ok || json.Unmarshal(p.detail.Data, &parent) != nil {
			return nil, false, ErrInvalidStateData
		}
	}
	out := []api.NodeStateChange{}
	i, j := 0, 0
	for i < len(parent.Entries) || j < len(revision.Entries) {
		var change api.NodeStateChange
		switch {
		case j == len(revision.Entries) || (i < len(parent.Entries) && bytes.Compare(parent.Entries[i].Key, revision.Entries[j].Key) < 0):
			change = api.NodeStateChange{Key: bytes.Clone(parent.Entries[i].Key), Before: parent.Entries[i].Value}
			i++
		case i == len(parent.Entries) || bytes.Compare(parent.Entries[i].Key, revision.Entries[j].Key) > 0:
			change = api.NodeStateChange{Key: bytes.Clone(revision.Entries[j].Key), After: revision.Entries[j].Value}
			j++
		default:
			change = api.NodeStateChange{Key: bytes.Clone(revision.Entries[j].Key), Before: parent.Entries[i].Value, After: revision.Entries[j].Value}
			i++
			j++
		}
		if change.Before != change.After {
			out = append(out, change)
		}
	}
	return out, true, nil
}
