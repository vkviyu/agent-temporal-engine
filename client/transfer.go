package client

import (
	"context"
	"reflect"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

var _ api.TransferCommands = (*Client)(nil)

func (c *Client) ExportTransfer(ctx context.Context, r api.ExportTransferRequest) (transfer.Manifest, error) {
	var out transfer.Manifest
	if err := c.principal(r.Principal); err != nil {
		return out, err
	}
	if err := c.call(ctx, "export_transfer", protocol.ExportTransferRequest{Space: r.Space, Anchor: r.Anchor, Roots: r.Roots, Omit: r.Omit, Policy: r.Policy}, &out, nil); err != nil {
		return out, err
	}
	if !validTransferExport(out, r, c.identity.Principal) {
		_ = c.Close()
		return transfer.Manifest{}, protocol.ErrProtocol
	}
	return out, nil
}
func (c *Client) ImportTransfer(ctx context.Context, r api.ImportTransferRequest) (api.CommitResult, error) {
	out := api.CommitResult{Code: api.CodeNotSubmitted, RequestID: r.Key.RequestID}
	if err := c.principal(r.Key.Principal); err != nil {
		return out, err
	}
	key := r.Key
	key.Principal = c.identity.Principal
	if err := key.Validate(); err != nil {
		return out, err
	}
	raw, err := r.Manifest.Canonical()
	if err != nil {
		return out, err
	}
	if r.ApprovedDigest != r.Manifest.ID || key.Space == r.Manifest.Source.Space {
		return out, api.ErrInvalidRequest
	}
	err = c.call(ctx, "import_transfer", protocol.ImportTransferRequest{Space: key.Space, RequestID: key.RequestID, Manifest: raw, ApprovedDigest: r.ApprovedDigest, ExpectedFence: r.ExpectedFence}, &out, &key)
	return out, err
}
func (c *Client) GetTransfer(ctx context.Context, r api.ReadTransferRequest) (api.TransferResult, error) {
	var out api.TransferResult
	if err := c.principal(r.Principal); err != nil {
		return out, err
	}
	if err := c.call(ctx, "get_transfer", protocol.ReadTransferRequest{Space: r.Space, ID: r.ID, Anchor: r.Anchor}, &out, nil); err != nil {
		return out, err
	}
	if !validTransferResult(out, r, c.identity.Principal) {
		_ = c.Close()
		return api.TransferResult{}, protocol.ErrProtocol
	}
	return out, nil
}

func validTransferExport(m transfer.Manifest, r api.ExportTransferRequest, principal model.PrincipalID) bool {
	if m.Validate() != nil || m.Source.Space != r.Space || m.Principal != principal || m.Policy != r.Policy || (r.Anchor != nil && !equalAnchor(m.Source, *r.Anchor)) || !emissionFenceBefore(m.Source.Fence, m.PolicyFence) {
		return false
	}
	roots := transfer.SortReferences(append([]transfer.Reference(nil), r.Roots...))
	if !reflect.DeepEqual(roots, m.Roots) {
		return false
	}
	omit := map[model.ObjectID]bool{}
	for _, id := range r.Omit {
		omit[id] = true
	}
	for _, entry := range m.Entries {
		if omit[entry.Source.ID] {
			return false
		}
	}
	return true
}
func validTransferResult(out api.TransferResult, r api.ReadTransferRequest, principal model.PrincipalID) bool {
	if out.Anchor.Validate() != nil || out.Anchor.Space != r.Space || out.Anchor.Fence != out.Fence || out.Anchor.Cursor != nil || out.Anchor.Knowledge != nil || !optionalID(out.RootSet) || (r.Anchor != nil && !equalAnchor(out.Anchor, *r.Anchor)) {
		return false
	}
	if !out.Found {
		return reflect.DeepEqual(out.Record, transfer.ImportRecord{})
	}
	record := out.Record
	if _, err := record.Canonical(); err != nil || record.Space != r.Space || record.Principal != principal || record.Manifest.ID != r.ID || out.RootSet == "" {
		return false
	}
	seen := map[model.ObjectID]bool{}
	for i, mapping := range record.Mappings {
		if !reflect.DeepEqual(mapping.Source, record.Manifest.Entries[i].Source) || seen[mapping.Target] {
			return false
		}
		seen[mapping.Target] = true
	}
	return true
}
