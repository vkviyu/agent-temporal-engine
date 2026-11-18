package engine_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

type transferDisclosureOracle struct {
	sourceSpace   string
	targetSpace   string
	included      bool
	omitted       bool
	imported      bool
	sourceBlocked bool
}

func (o *transferDisclosureOracle) Export(omitted bool) {
	o.omitted = omitted
	o.included = !omitted
}

func (o *transferDisclosureOracle) Import(approved bool) {
	o.imported = approved && o.included
}

func (o *transferDisclosureOracle) WithdrawSource() {
	o.sourceBlocked = true
}

func (o *transferDisclosureOracle) CanExport() bool {
	return !o.sourceBlocked
}

func (o *transferDisclosureOracle) CanReadArchive(space, principal string) bool {
	return o.imported && space == o.targetSpace && principal == "host-principal"
}

func (o *transferDisclosureOracle) compareArchive(req api.ReadTransferRequest, got api.TransferResult, err error) error {
	allowed := o.CanReadArchive(string(req.Space), string(req.Principal))
	if err != nil || got.Found != allowed {
		return fmt.Errorf("archive space=%s principal=%s: found=%v want=%v err=%v", req.Space, req.Principal, got.Found, allowed, err)
	}
	if !allowed {
		if !reflect.DeepEqual(got.Record, transfer.ImportRecord{}) {
			return fmt.Errorf("denied archive leaked record: %+v", got.Record)
		}
		return nil
	}
	if got.Record.Space != req.Space || got.Record.Principal != req.Principal ||
		got.Record.Manifest.ID != req.ID || len(got.Record.Mappings) == 0 ||
		len(got.Record.Mappings) != len(got.Record.Manifest.Entries) {
		return fmt.Errorf("archive ownership or nonempty mapping mismatch: %+v", got.Record)
	}
	for _, mapping := range got.Record.Mappings {
		if mapping.Source.ID == "" || mapping.Target == "" || mapping.Source.ID == mapping.Target {
			return fmt.Errorf("archive did not remap source identity: %+v", mapping)
		}
	}
	return nil
}

func (o *transferDisclosureOracle) compareExport(got transfer.Manifest, err error, payload model.ObjectID) error {
	if !o.CanExport() {
		if (!errors.Is(err, memory.ErrHistoryAccessDenied) && !errors.Is(err, memory.ErrNotEligible)) ||
			!reflect.DeepEqual(got, transfer.Manifest{}) {
			return fmt.Errorf("withdrawn source must deny export without content: %+v %v", got, err)
		}
		return nil
	}
	if err != nil || got.ID == "" || len(got.Entries) == 0 {
		return fmt.Errorf("allowed export missing: %+v %v", got, err)
	}
	included, gap := false, false
	for _, entry := range got.Entries {
		included = included || entry.Source.ID == payload
	}
	for _, entry := range got.Gaps {
		gap = gap || entry.Source.ID == payload
	}
	if included != o.included || gap != o.omitted {
		return fmt.Errorf("payload disclosure included=%v gap=%v; want included=%v omitted=%v", included, gap, o.included, o.omitted)
	}
	return nil
}

func TestIndependentTransferDisclosurePreservesSourceTargetBoundaries(t *testing.T) {
	ctx := context.Background()
	s, export, payload := transferCoreFixture(t)
	want := &transferDisclosureOracle{
		sourceSpace: "host-space", targetSpace: "destination",
	}
	want.Export(false)
	manifest, err := s.e.ExportTransfer(ctx, export)
	if mismatch := want.compareExport(manifest, err, payload); mismatch != nil {
		t.Fatal(mismatch)
	}

	key := func(id string) model.RequestKey {
		request := agentKey(id)
		request.Space = model.SpaceID(want.targetSpace)
		return request
	}
	request := api.ImportTransferRequest{
		Key:      key("independent-transfer-import"),
		Manifest: manifest, ApprovedDigest: manifest.ID,
	}
	ack, err := s.e.ImportTransfer(ctx, request)
	if err != nil || ack.Code != api.CodeDurableAck {
		t.Fatalf("import=%+v %v", ack, err)
	}
	want.Import(true)
	read := api.ReadTransferRequest{
		Space: "destination", Principal: "host-principal", ID: manifest.ID,
	}
	record, err := s.e.GetTransfer(ctx, read)
	if mismatch := want.compareArchive(read, record, err); mismatch != nil {
		t.Fatal(mismatch)
	}
	check := func(t *testing.T, stage string) {
		t.Helper()
		for _, space := range []string{want.sourceSpace, want.targetSpace} {
			for _, principal := range []string{"host-principal", "other"} {
				req := api.ReadTransferRequest{Space: model.SpaceID(space), Principal: model.PrincipalID(principal), ID: manifest.ID}
				got, err := s.e.GetTransfer(ctx, req)
				if mismatch := want.compareArchive(req, got, err); mismatch != nil {
					t.Fatalf("%s: %v", stage, mismatch)
				}
				if want.CanReadArchive(space, principal) && !reflect.DeepEqual(got.Record, record.Record) {
					t.Fatalf("%s: destination archive changed", stage)
				}
			}
		}
		got, err := s.e.ExportTransfer(ctx, export)
		if mismatch := want.compareExport(got, err, payload); mismatch != nil {
			t.Fatalf("%s: %v", stage, mismatch)
		}
		if retry, err := s.e.ImportTransfer(ctx, request); err != nil || retry != ack {
			t.Fatalf("%s: import receipt changed: %+v %v", stage, retry, err)
		}
	}
	check(t, "before-withdrawal")

	redacted := export
	redacted.Omit = []model.ObjectID{payload}
	want.Export(true)
	redactedManifest, err := s.e.ExportTransfer(ctx, redacted)
	if mismatch := want.compareExport(redactedManifest, err, payload); mismatch != nil {
		t.Fatal(mismatch)
	}
	want.Export(false)

	checkpoint, err := s.e.CreateRecoveryCheckpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	applyMemory(t, s.e, "independent-transfer-withdraw", memory.Command{
		RetractSource: &memory.RetractSourceCommand{
			Source: payload,
			Retraction: memory.Retraction{
				Kind:   memory.RetractionPrivacyErasure,
				Reason: "source export withdrawal",
			},
		},
	})
	want.WithdrawSource()
	check(t, "after-withdrawal")
	independentDisclosureRecovery(t, s, checkpoint.Position.LSN, check)

	t.Run("negative-controls", func(t *testing.T) {
		for _, req := range []api.ReadTransferRequest{
			{Space: model.SpaceID(want.sourceSpace), Principal: "host-principal", ID: manifest.ID},
			{Space: model.SpaceID(want.targetSpace), Principal: "other", ID: manifest.ID},
		} {
			if err := want.compareArchive(req, record, nil); err == nil {
				t.Fatal("comparator accepted archive disclosure outside its owner")
			}
		}
		emptyMappings := record
		emptyMappings.Record.Mappings = nil
		if err := want.compareArchive(read, emptyMappings, nil); err == nil {
			t.Fatal("comparator accepted empty mappings")
		}
		if err := want.compareExport(manifest, nil, payload); err == nil {
			t.Fatal("comparator accepted export after withdrawal")
		}
	})
}
