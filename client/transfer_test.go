package client

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

func transferManifestFixture(t *testing.T, space model.SpaceID) transfer.Manifest {
	t.Helper()
	source := model.SnapshotAnchor{Space: space, Fence: model.CommitFence{Store: "source-store", LSN: 1, CommitDigest: model.CommitDigest(strings.Repeat("a", 64))}}
	object := codec.Object{Type: "test.evidence", Version: 1, Space: space, Payload: []byte("original source bytes")}
	raw, err := codec.EncodeObject(object)
	if err != nil {
		t.Fatal(err)
	}
	id, err := codec.ObjectID(object)
	if err != nil {
		t.Fatal(err)
	}
	ref := transfer.Reference{Kind: "object", ID: id, Anchor: source}
	m := transfer.Manifest{Schema: 1, Source: source, Principal: "p", Policy: transfer.RedactionPolicy{ID: "explicit", Version: "1"}, PolicyFence: source.Fence, Roots: []transfer.Reference{ref}, Entries: []transfer.Entry{{Source: ref, Encoding: "object", Data: raw}}}
	m.ID = m.Digest()
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	return m
}
func transferResponseFixture(t *testing.T) api.TransferResult {
	m := transferManifestFixture(t, "origin")
	f := model.CommitFence{Store: "destination", LSN: 3, CommitDigest: model.CommitDigest(strings.Repeat("b", 64))}
	return api.TransferResult{Found: true, Anchor: model.SnapshotAnchor{Space: "s", Fence: f}, Fence: f, RootSet: model.ObjectID(strings.Repeat("c", 64)), Record: transfer.ImportRecord{Schema: 1, Space: "s", Principal: "p", Manifest: m, Trust: "caller-approved-foreign-evidence", Mappings: []transfer.Mapping{{Source: m.Entries[0].Source, Target: model.ObjectID(strings.Repeat("d", 64))}}}}
}
func TestTransferClientRejectsMalformedArchiveSuccess(t *testing.T) {
	for name, mutate := range map[string]func(*api.TransferResult){
		"different-fence":            func(r *api.TransferResult) { r.Fence.LSN++ },
		"substituted-history":        func(r *api.TransferResult) { r.Fence.LSN++; r.Anchor.Fence = r.Fence },
		"invalid-root":               func(r *api.TransferResult) { r.RootSet = "bad" },
		"missing-root":               func(r *api.TransferResult) { r.RootSet = "" },
		"foreign-space":              func(r *api.TransferResult) { r.Anchor.Space = "foreign" },
		"selector":                   func(r *api.TransferResult) { r.Anchor.Knowledge = &model.KnowledgeKey{Actor: "a", Execution: "e"} },
		"not-found-with-record":      func(r *api.TransferResult) { r.Found = false },
		"foreign-record-space":       func(r *api.TransferResult) { r.Record.Space = "foreign" },
		"foreign-record-owner":       func(r *api.TransferResult) { r.Record.Principal = "other" },
		"wrong-manifest":             func(r *api.TransferResult) { r.Record.Manifest.ID = model.ObjectID(strings.Repeat("e", 64)) },
		"modified-content":           func(r *api.TransferResult) { r.Record.Manifest.Entries[0].Data = []byte("replacement") },
		"invalid-mapping":            func(r *api.TransferResult) { r.Record.Mappings[0].Target = "bad" },
		"substituted-mapping-source": func(r *api.TransferResult) { r.Record.Mappings[0].Source.Anchor.Fence.LSN = 0 },
		"missing-mapping":            func(r *api.TransferResult) { r.Record.Mappings = nil },
		"promoted-trust":             func(r *api.TransferResult) { r.Record.Trust = "local-authority" },
	} {
		t.Run(name, func(t *testing.T) {
			r := transferResponseFixture(t)
			anchor := r.Anchor.Clone()
			id := r.Record.Manifest.ID
			mutate(&r)
			c := nodeResponseClient(t, r)
			if _, err := c.GetTransfer(context.Background(), api.ReadTransferRequest{Space: "s", ID: id, Anchor: &anchor}); !errors.Is(err, protocol.ErrProtocol) {
				t.Fatalf("accepted malformed archive response: %v", err)
			}
			if !c.closed.Load() {
				t.Fatal("invalid archive success left connection reusable")
			}
		})
	}
}
func TestTransferClientRejectsChangedExportContract(t *testing.T) {
	for name, mutate := range map[string]func(*transfer.Manifest){
		"owner":   func(m *transfer.Manifest) { m.Principal = "other"; m.ID = m.Digest() },
		"policy":  func(m *transfer.Manifest) { m.Policy.Version = "2"; m.ID = m.Digest() },
		"anchor":  func(m *transfer.Manifest) { m.Source.Fence.LSN++; m.PolicyFence = m.Source.Fence; m.ID = m.Digest() },
		"content": func(m *transfer.Manifest) { m.Entries[0].Data = []byte("unbound bytes"); m.ID = m.Digest() },
		"digest":  func(m *transfer.Manifest) { m.ID = model.ObjectID(strings.Repeat("f", 64)) },
	} {
		t.Run(name, func(t *testing.T) {
			m := transferManifestFixture(t, "s")
			request := api.ExportTransferRequest{Space: "s", Anchor: &m.Source, Roots: append([]transfer.Reference(nil), m.Roots...), Policy: m.Policy}
			source := m.Source.Clone()
			request.Anchor = &source
			mutate(&m)
			c := nodeResponseClient(t, m)
			if _, err := c.ExportTransfer(context.Background(), request); !errors.Is(err, protocol.ErrProtocol) {
				t.Fatalf("changed export contract accepted: %v", err)
			}
		})
	}
	// Whole-record omission cannot be undone by an otherwise valid manifest.
	m := transferManifestFixture(t, "s")
	c := nodeResponseClient(t, m)
	if _, err := c.ExportTransfer(context.Background(), api.ExportTransferRequest{Space: "s", Roots: m.Roots, Omit: []model.ObjectID{m.Entries[0].Source.ID}, Policy: m.Policy}); !errors.Is(err, protocol.ErrProtocol) {
		t.Fatalf("omitted bytes exported: %v", err)
	}
}
func TestTransferClientAcceptsBoundExportImportAndHistoricalAbsence(t *testing.T) {
	m := transferManifestFixture(t, "s")
	c := nodeResponseClient(t, m)
	got, err := c.ExportTransfer(context.Background(), api.ExportTransferRequest{Space: "s", Anchor: &m.Source, Roots: m.Roots, Policy: m.Policy})
	if err != nil || !reflect.DeepEqual(got, m) {
		t.Fatal(got, err)
	}
	for _, found := range []bool{true, false} {
		t.Run(map[bool]string{true: "record", false: "absent"}[found], func(t *testing.T) {
			r := transferResponseFixture(t)
			id := r.Record.Manifest.ID
			r.Found = found
			if !found {
				r.Record = transfer.ImportRecord{}
			}
			c := nodeResponseClient(t, r)
			if _, err := c.GetTransfer(context.Background(), api.ReadTransferRequest{Space: "s", ID: id, Anchor: &r.Anchor}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
