package service

import (
	"context"
	"reflect"
	"testing"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/client"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

func TestTransferEmbeddedSDKCrossSpaceApprovalAndRecovery(t *testing.T) {
	for _, transport := range []string{"pipe", "unix"} {
		t.Run(transport, func(t *testing.T) {
			ctx := context.Background()
			core, path := nativeAgentStore(t, true)
			credentials := Options{Credentials: map[string]Credential{"owner-token": {Principal: "owner", Spaces: []model.SpaceID{"s", "target"}}, "other-token": {Principal: "other", Spaces: []model.SpaceID{"s", "target"}}}}
			server, owner, other := conformanceClients(t, core, transport, credentials)
			capable := false
			for _, cap := range owner.Identity().Capabilities {
				capable = capable || cap == "transfer/archive-v1"
			}
			if !capable {
				t.Fatal("missing transfer archive capability")
			}
			key := func(space model.SpaceID, id string) model.RequestKey {
				return model.RequestKey{Space: space, Principal: "owner", RequestID: model.RequestID(id)}
			}
			evidenceAck, err := core.Mutate(ctx, api.MutateRequest{Key: key("s", "transfer-evidence"), Batch: api.StateBatch{Operations: []api.StateMutation{{Key: []byte("evidence"), Kind: api.MutationPut, Value: api.Value{Kind: api.ValueBytes, Data: []byte("private source evidence")}}}}})
			domainAck(t, evidenceAck, err)
			view, err := core.OpenView(ctx, api.OpenViewRequest{Space: "s", Principal: "owner"})
			if err != nil {
				t.Fatal(err)
			}
			entries, err := core.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref})
			if err != nil || len(entries.Items) != 1 {
				t.Fatal(entries, err)
			}
			if err := core.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}); err != nil {
				t.Fatal(err)
			}
			content := entries.Items[0].Value
			source := memory.Source{SourceID: content, Anchor: model.SnapshotAnchor{Space: "s", Fence: evidenceAck.Fence}}
			apply := func(id string, command memory.Command) api.CommitResult {
				t.Helper()
				ack, err := core.ApplyMemory(ctx, api.ApplyMemoryRequest{Key: key("s", id), Command: command})
				return domainAck(t, ack, err)
			}
			m := memory.Memory{Space: "s", Kind: memory.KindFact, Claim: []byte("portable verified fact"), Sources: []memory.Source{source}}
			apply("transfer-candidate", memory.Command{PutCandidate: &m})
			current := func(status memory.Status) model.ObjectID {
				t.Helper()
				state, err := core.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "s"})
				if err != nil {
					t.Fatal(err)
				}
				for _, m := range state.Snapshot.Memories {
					if m.Status == status {
						return m.ID
					}
				}
				t.Fatal("missing memory status", status)
				return ""
			}
			candidate := current(memory.StatusCandidate)
			apply("transfer-validation", memory.Command{Validate: &memory.ValidateCommand{ID: candidate, Validation: memory.Validation{StrategyVersion: 1, Passed: true, Evidence: []memory.Source{source}}}})
			validated := current(memory.StatusValidated)
			promotedAck := apply("transfer-promotion", memory.Command{Promote: &validated})
			promoted := current(memory.StatusPromoted)
			anchor := model.SnapshotAnchor{Space: "s", Fence: promotedAck.Fence}
			export := api.ExportTransferRequest{Space: "s", Principal: "owner", Anchor: &anchor, Roots: []transfer.Reference{{Kind: "memory", ID: promoted, Anchor: anchor}}, Omit: []model.ObjectID{content}, Policy: transfer.RedactionPolicy{ID: "explicit-source-omission", Version: "1"}}
			manifest, err := owner.ExportTransfer(ctx, export)
			if err != nil {
				t.Fatal(err)
			}
			local, err := core.ExportTransfer(ctx, export)
			if err != nil || !reflect.DeepEqual(local, manifest) {
				t.Fatalf("export SDK differs from embedded: %v", err)
			}
			if manifest.Validate() != nil || manifest.ID == "" {
				t.Fatal("invalid transfer manifest")
			}
			for _, entry := range manifest.Entries {
				if entry.Source.ID == content {
					t.Fatal("omitted source evidence was included")
				}
			}
			read := api.ReadTransferRequest{Space: "target", Principal: "owner", ID: manifest.ID}
			before, err := owner.GetTransfer(ctx, read)
			if err != nil || before.Found {
				t.Fatal(before, err)
			}
			request := api.ImportTransferRequest{Key: key("target", "archive-import"), Manifest: manifest, ApprovedDigest: manifest.ID}
			unapproved := request
			unapproved.ApprovedDigest = ""
			if out, err := owner.ImportTransfer(ctx, unapproved); err == nil || out.Code != api.CodeNotSubmitted {
				t.Fatalf("approval omission accepted: %+v %v", out, err)
			}
			importAck, err := owner.ImportTransfer(ctx, request)
			domainAck(t, importAck, err)
			assertAckEqual(t, importAck, func() (api.CommitResult, error) { return core.ImportTransfer(ctx, request) })
			changed := request
			changed.Manifest.Policy.Version = "2"
			changed.Manifest.ID = changed.Manifest.Digest()
			changed.ApprovedDigest = changed.Manifest.ID
			conflict, err := owner.ImportTransfer(ctx, changed)
			if err != nil || conflict.Code != api.CodeConflict || conflict.Conflict != api.ConflictIdempotency {
				t.Fatalf("changed manifest reused durable receipt: %+v %v", conflict, err)
			}
			assertAckEqual(t, conflict, func() (api.CommitResult, error) { return core.ImportTransfer(ctx, changed) })
			for _, run := range []func() error{
				func() error { r := export; r.Space = "forbidden"; _, err := owner.ExportTransfer(ctx, r); return err },
				func() error { r := read; r.Space = "forbidden"; _, err := owner.GetTransfer(ctx, r); return err },
				func() error {
					r := request
					r.Key.Space = "forbidden"
					_, err := owner.ImportTransfer(ctx, r)
					return err
				},
			} {
				if err := run(); err == nil {
					t.Fatal("unauthorized transfer space accepted")
				} else {
					remoteCode(t, err, api.ErrorUnauthorized)
				}
			}
			spoofedImport := request
			spoofedImport.Key.Principal = "other"
			if _, err := owner.ImportTransfer(ctx, spoofedImport); err != client.ErrPrincipal {
				t.Fatalf("SDK allowed import principal spoof: %v", err)
			}
			past := read
			past.Anchor = &before.Anchor
			check := func(store *ate.Store, c, foreign *client.Client) {
				t.Helper()
				assertReadEqual(t, func() (api.TransferResult, error) { return store.GetTransfer(ctx, read) }, func() (api.TransferResult, error) { return c.GetTransfer(ctx, read) })
				assertReadEqual(t, func() (api.TransferResult, error) { return store.GetTransfer(ctx, past) }, func() (api.TransferResult, error) { return c.GetTransfer(ctx, past) })
				got, err := c.GetTransfer(ctx, read)
				if err != nil || !got.Found || got.Record.Manifest.ID != manifest.ID || got.Record.Space != "target" || got.Record.Principal != "owner" || len(got.Record.Mappings) != len(manifest.Entries) {
					t.Fatalf("archive import record mismatch: %+v %v", got, err)
				}
				for i, mapping := range got.Record.Mappings {
					if mapping.Target == mapping.Source.ID || !reflect.DeepEqual(mapping.Source, manifest.Entries[i].Source) {
						t.Fatal("source identity/anchor was reused as destination authority")
					}
				}
				assertAckEqual(t, importAck, func() (api.CommitResult, error) { return c.ImportTransfer(ctx, request) })
				if hidden, err := foreign.GetTransfer(ctx, api.ReadTransferRequest{Space: "target", ID: manifest.ID}); err != nil || hidden.Found {
					t.Fatalf("foreign principal read archive: %+v %v", hidden, err)
				}
				spoof := read
				spoof.Principal = "other"
				if _, err := c.GetTransfer(ctx, spoof); err != client.ErrPrincipal {
					t.Fatalf("principal spoof accepted: %v", err)
				}
				state, err := store.GetAgentState(ctx, api.ReadAgentStateRequest{Space: "target", Principal: "owner"})
				if err != nil || len(state.Snapshot.Tasks)+len(state.Snapshot.Executions) != 0 {
					t.Fatal("foreign archive created executable authority", state, err)
				}
				mem, err := store.GetMemoryState(ctx, api.ReadMemoryStateRequest{Space: "target"})
				if err != nil || len(mem.Snapshot.Memories) != 0 {
					t.Fatal("foreign archive was silently promoted to local memory", mem, err)
				}
			}
			check(core, owner, other)
			_ = owner.Close()
			_ = other.Close()
			if err := server.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			if err := core.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, report, err := ate.OpenExisting(path, ate.Options{AgentProfile: true})
			if err != nil || !report.Verified {
				t.Fatal(report, err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			})
			_, owner, other = conformanceClients(t, reopened, transport, credentials)
			check(reopened, owner, other)
		})
	}
}
