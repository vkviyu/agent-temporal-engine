package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

var _ api.AgentRecordReader = (*Client)(nil)

func validateAgentStateRecords(p api.AgentStateRecordsPage) error {
	if p.Anchor.Validate() != nil || p.Anchor.Fence != p.Fence || p.Anchor.Cursor != nil || p.Anchor.Knowledge != nil ||
		!optionalID(p.RootSet) || !optionalID(p.LiveControlRoot) || !optionalID(p.Catalog) ||
		p.RecordCount > agentprofile.MaxStateRecords || p.RecordBytes > agentprofile.MaxStateBytes ||
		len(p.Records) > api.MaxAgentStatePageRecords || uint32(len(p.Records)) > p.RecordCount || len(p.Continuation) > 16384 {
		return protocol.ErrProtocol
	}
	if p.Found {
		if p.Catalog == "" || p.RootSet == "" || p.LiveControlRoot == "" {
			return protocol.ErrProtocol
		}
	} else if p.Catalog != "" || p.RecordCount != 0 || p.RecordBytes != 0 || len(p.Records) != 0 || p.Continuation != "" {
		return protocol.ErrProtocol
	}
	if (p.RecordCount == 0) != (p.RecordBytes == 0) ||
		(p.RecordCount > 0 && len(p.Records) == 0) ||
		(p.Continuation != "" && (len(p.Records) == 0 || uint32(len(p.Records)) >= p.RecordCount)) {
		return protocol.ErrProtocol
	}
	var total uint64
	for i, r := range p.Records {
		sum := sha256.Sum256(r.Raw)
		// A page can start in the middle of a duplicate multiset. Only full
		// DecodeRecordImage verifies ordinals starting at zero.
		if len(r.Key) != 40 || !bytes.Equal(r.Key[:32], sum[:]) || len(r.Raw) > agentprofile.MaxStateRecordBytes ||
			(i > 0 && bytes.Compare(p.Records[i-1].Key, r.Key) >= 0) ||
			agentprofile.ValidateStateRecord(r.Raw) != nil {
			return protocol.ErrProtocol
		}
		total += uint64(len(r.Raw))
	}
	if total > p.RecordBytes {
		return protocol.ErrProtocol
	}
	body, err := json.Marshal(p)
	if err != nil || len(body) > api.MaxAgentStatePageBytes {
		return protocol.ErrProtocol
	}
	return nil
}

// GetAgentStateRecords reads one page. Its cursor is reusable by the same
// authenticated owner on another connection, with the same anchor and limit.
func (c *Client) GetAgentStateRecords(ctx context.Context, r api.ReadAgentStateRecordsRequest) (api.AgentStateRecordsPage, error) {
	var out api.AgentStateRecordsPage
	if err := c.principal(r.Principal); err != nil {
		return out, err
	}
	if r.Limit < 0 || r.Limit > api.MaxAgentStatePageRecords || (r.Continuation != "" && r.Anchor == nil) {
		return out, api.ErrInvalidRequest
	}
	if err := c.call(ctx, "get_agent_state_records", protocol.ReadAgentStateRecordsRequest{
		Space: r.Space, Anchor: r.Anchor, Limit: r.Limit, Continuation: r.Continuation,
	}, &out, nil); err != nil {
		return api.AgentStateRecordsPage{}, err
	}
	fail := func() (api.AgentStateRecordsPage, error) {
		_ = c.Close()
		return api.AgentStateRecordsPage{}, protocol.ErrProtocol
	}
	limit := r.Limit
	if limit == 0 {
		limit = api.MaxAgentStatePageRecords
	}
	if out.Anchor.Space != r.Space || (r.Anchor != nil && !equalAnchor(*r.Anchor, out.Anchor)) || len(out.Records) > limit ||
		(r.Continuation != "" && (!out.Found || len(out.Records) == 0 || out.Continuation == r.Continuation)) {
		return fail()
	}
	for _, record := range out.Records {
		if err := ctx.Err(); err != nil {
			return api.AgentStateRecordsPage{}, err
		}
		if agentprofile.ValidateStateRecordScope(record.Raw, r.Space, c.identity.Principal) != nil {
			return fail()
		}
	}
	// A complete first page must already account for the declared catalog.
	if r.Continuation == "" && out.Continuation == "" {
		var total uint64
		for _, record := range out.Records {
			total += uint64(len(record.Raw))
		}
		if uint32(len(out.Records)) != out.RecordCount || total != out.RecordBytes {
			return fail()
		}
	}
	return out, nil
}

// ReadFullAgentState starts at the first page and validates the complete image.
// Aggregation still requires memory proportional to the full state.
// No partial Snapshot is returned on any failure.
func (c *Client) ReadFullAgentState(ctx context.Context, r api.ReadAgentStateRequest) (api.AgentStateResult, error) {
	req := api.ReadAgentStateRecordsRequest{Space: r.Space, Principal: r.Principal, Anchor: r.Anchor}
	first, err := c.GetAgentStateRecords(ctx, req)
	if err != nil {
		return api.AgentStateResult{}, err
	}
	fail := func() (api.AgentStateResult, error) {
		_ = c.Close()
		return api.AgentStateResult{}, protocol.ErrProtocol
	}
	page := first
	var records []agentprofile.StateRecord
	var total uint64
	seen := make(map[string]bool)
	for {
		if err := ctx.Err(); err != nil {
			return api.AgentStateResult{}, err
		}
		if page.Found != first.Found || !equalAnchor(page.Anchor, first.Anchor) || page.Fence != first.Fence ||
			page.RootSet != first.RootSet || page.LiveControlRoot != first.LiveControlRoot || page.Catalog != first.Catalog ||
			page.RecordCount != first.RecordCount || page.RecordBytes != first.RecordBytes {
			return fail()
		}
		for _, record := range page.Records {
			if err := ctx.Err(); err != nil {
				return api.AgentStateResult{}, err
			}
			if uint32(len(records)) >= first.RecordCount || uint64(len(record.Raw)) > first.RecordBytes-total ||
				(len(records) != 0 && bytes.Compare(records[len(records)-1].Key, record.Key) >= 0) {
				return fail()
			}
			records = append(records, record)
			total += uint64(len(record.Raw))
		}
		if page.Continuation == "" {
			break
		}
		if len(page.Records) == 0 || seen[page.Continuation] || uint32(len(records)) >= first.RecordCount || total >= first.RecordBytes {
			return fail()
		}
		seen[page.Continuation] = true
		req.Anchor, req.Continuation = &first.Anchor, page.Continuation
		page, err = c.GetAgentStateRecords(ctx, req)
		if err != nil {
			return api.AgentStateResult{}, err
		}
	}
	if uint32(len(records)) != first.RecordCount || total != first.RecordBytes {
		return fail()
	}
	image, err := agentprofile.DecodeRecordImage(records)
	if err != nil || image.CheckScope(r.Space, c.identity.Principal) != nil {
		return fail()
	}
	snapshot, err := image.Snapshot()
	if err != nil || agentprofile.ValidateStateSnapshot(snapshot) != nil {
		return fail()
	}
	if err := ctx.Err(); err != nil {
		return api.AgentStateResult{}, err
	}
	return api.AgentStateResult{Found: first.Found, Snapshot: snapshot, Anchor: first.Anchor,
		Fence: first.Fence, RootSet: first.RootSet, LiveControlRoot: first.LiveControlRoot}, nil
}
