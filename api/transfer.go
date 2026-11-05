package api

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

const MethodImportTransfer = "transfer.import/v1"

type ImportTransferCommand struct {
	Manifest       transfer.Manifest `json:"manifest"`
	ApprovedDigest model.ObjectID    `json:"approved_digest"`
}

func EncodeImportTransfer(c ImportTransferCommand) ([]byte, error) {
	if c.Manifest.Validate() != nil || c.ApprovedDigest != c.Manifest.ID {
		return nil, transfer.ErrInvalid
	}
	return json.Marshal(c)
}
func DecodeImportTransfer(raw []byte) (ImportTransferCommand, error) {
	var c ImportTransferCommand
	if len(raw) > MaxCommandBody {
		return c, transfer.ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, transfer.ErrInvalid
	}
	b, err := EncodeImportTransfer(c)
	if err != nil || !bytes.Equal(b, raw) {
		return ImportTransferCommand{}, transfer.ErrInvalid
	}
	return c, nil
}

type ExportTransferRequest struct {
	Space     model.SpaceID
	Principal model.PrincipalID
	Anchor    *model.SnapshotAnchor
	Roots     []transfer.Reference
	Omit      []model.ObjectID
	Policy    transfer.RedactionPolicy
}
type ImportTransferRequest struct {
	Key            model.RequestKey
	Manifest       transfer.Manifest
	ApprovedDigest model.ObjectID
	ExpectedFence  *model.CommitFence
}
type ReadTransferRequest struct {
	Space     model.SpaceID
	Principal model.PrincipalID
	ID        model.ObjectID
	Anchor    *model.SnapshotAnchor
}
type TransferResult struct {
	Found   bool
	Record  transfer.ImportRecord
	Anchor  model.SnapshotAnchor
	Fence   model.CommitFence
	RootSet model.ObjectID
}
type TransferCommands interface {
	ExportTransfer(context.Context, ExportTransferRequest) (transfer.Manifest, error)
	ImportTransfer(context.Context, ImportTransferRequest) (CommitResult, error)
	GetTransfer(context.Context, ReadTransferRequest) (TransferResult, error)
}
