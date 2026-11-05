package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const MethodApplyEmission = "agent.emission.apply/v1"

type EmissionSpec struct {
	ID               string                        `json:"id"`
	Execution        model.ExecutionID             `json:"execution"`
	Task             model.EntityID                `json:"task"`
	GoalEpoch        uint64                        `json:"goal_epoch"`
	Authority        agentprofile.AuthorityBinding `json:"authority"`
	AuthorityVersion uint64                        `json:"authority_version"`
	Content          model.ObjectID                `json:"content"`
	Anchor           model.SnapshotAnchor          `json:"anchor"`
	Offset           uint64                        `json:"offset"`
	Length           uint64                        `json:"length"`
	Destination      string                        `json:"destination"`
	// Projection follows ReadContent: empty selects payload; value_bytes
	// selects core byte-value Data. The requested range must fit in the projection.
	Projection string `json:"projection,omitempty"`
	// Schema 3 adds deterministic logical expiry and an optional exact external
	// Activity/Attempt binding. Query scope never invents an Execution authority.
	Query         string         `json:"query,omitempty"`
	Activity      string         `json:"activity,omitempty"`
	Attempt       string         `json:"attempt,omitempty"`
	RequestDigest model.ObjectID `json:"request_digest,omitempty"`
	ContentKind   string         `json:"content_kind,omitempty"` // empty/raw, packet, summary
	NotAfterLSN   uint64         `json:"not_after_lsn,omitempty"`
}

type EmissionCommand struct {
	// SchemaVersion is encoded in the binary envelope, not in its JSON body.
	// Clean V1 accepts schema 3 only.
	SchemaVersion  uint64                        `json:"-"`
	Issue          *EmissionSpec                 `json:"issue,omitempty"`
	ClaimID        string                        `json:"claim_id,omitempty"`
	ClaimVersion   uint64                        `json:"claim_version,omitempty"`
	ClaimAuthority agentprofile.AuthorityBinding `json:"claim_authority,omitempty"`
	RevokeID       string                        `json:"revoke_id,omitempty"`
	RevokeVersion  uint64                        `json:"revoke_version,omitempty"`
}

// EmissionProof records the exact policy cut and explicit dependency closure
// used to authorize an Issue. It is historical evidence, not a reusable grant
// under that old policy. Claim always checks the current policy again.
type EmissionProof struct {
	Space            model.SpaceID     `json:"space"`
	Principal        model.PrincipalID `json:"principal"`
	PolicyFence      model.CommitFence `json:"policy_fence"`
	PolicyRoot       model.ObjectID    `json:"policy_root"`
	Dependencies     []model.ObjectID  `json:"dependencies"`
	DependencyDigest model.ObjectID    `json:"dependency_digest"`
	Projection       string            `json:"projection"`
	ContentDigest    model.ObjectID    `json:"content_digest"`
	RangeDigest      model.ObjectID    `json:"range_digest"`
	TotalBytes       uint64            `json:"total_bytes"`
}

// EmissionDependencyDigest hashes a sorted, unique dependency set with framed
// identities and a domain tag. The empty set and noncanonical sets are invalid.
func EmissionDependencyDigest(ids []model.ObjectID) (model.ObjectID, error) {
	if len(ids) == 0 {
		return "", ErrInvalidRequest
	}
	h := sha256.New()
	h.Write([]byte("ATE-EMISSION-DEPENDENCIES\x00\x01"))
	var length [10]byte
	for i, id := range ids {
		if !model.ValidDigest(string(id)) || (i > 0 && ids[i-1] >= id) {
			return "", ErrInvalidRequest
		}
		n := binary.PutUvarint(length[:], uint64(len(id)))
		h.Write(length[:n])
		h.Write([]byte(id))
	}
	return model.ObjectID(hex.EncodeToString(h.Sum(nil))), nil
}

type EmissionRecord struct {
	Spec           EmissionSpec                  `json:"spec"`
	Status         string                        `json:"status"` // issued, claimed, revoked
	Version        uint64                        `json:"version"`
	ClaimAuthority agentprofile.AuthorityBinding `json:"claim_authority,omitempty"`
	Proof          *EmissionProof                `json:"proof,omitempty"`
}

type ApplyEmissionRequest struct {
	Key           model.RequestKey
	Command       EmissionCommand
	ExpectedFence *model.CommitFence
}
type ReadEmissionRequest struct {
	Space     model.SpaceID
	Principal model.PrincipalID
	ID        string
	Anchor    *model.SnapshotAnchor
}
type EmissionResult struct {
	Found   bool
	Record  EmissionRecord
	Anchor  model.SnapshotAnchor
	Fence   model.CommitFence
	RootSet model.ObjectID
}
type EmissionCommands interface {
	ApplyEmission(context.Context, ApplyEmissionRequest) (CommitResult, error)
	GetEmission(context.Context, ReadEmissionRequest) (EmissionResult, error)
}

func validateEmission(c EmissionCommand) error {
	version := emissionSchema(c)
	n := 0
	if c.Issue != nil {
		n++
	}
	if c.ClaimID != "" {
		n++
	}
	if c.RevokeID != "" {
		n++
	}
	if version != 3 || n != 1 || (c.ClaimID != "" && c.ClaimVersion == 0) || (c.RevokeID != "" && c.RevokeVersion == 0) {
		return ErrInvalidRequest
	}
	if c.Issue != nil {
		s := c.Issue
		queryScope := version >= 3 && s.Query != "" && s.Execution == "" && s.Task == "" && s.AuthorityVersion == 0 && s.Authority == (agentprofile.AuthorityBinding{})
		executionScope := s.Query == "" && s.Execution != "" && s.Task != "" && s.AuthorityVersion > 0 && s.Authority.Incarnation > 0
		if s.ID == "" || (!queryScope && !executionScope) || s.GoalEpoch == 0 || !model.ValidDigest(string(s.Content)) || s.Anchor.Validate() != nil || s.Offset > 1<<40 || s.Length == 0 || s.Length > 1<<30 || s.Destination == "" {
			return ErrInvalidRequest
		}
		if s.Projection != "" && s.Projection != "payload" && s.Projection != "value_bytes" {
			return ErrInvalidRequest
		}
		if s.ContentKind != "" && s.ContentKind != "raw" && s.ContentKind != "packet" && s.ContentKind != "summary" {
			return ErrInvalidRequest
		}
		if (s.ContentKind == "packet" || s.ContentKind == "summary") && s.Projection != "" && s.Projection != "value_bytes" {
			return ErrInvalidRequest
		}
		if (s.Activity != "" || s.Attempt != "" || s.RequestDigest != "") && (s.Activity == "" || s.Attempt == "" || !model.ValidDigest(string(s.RequestDigest))) {
			return ErrInvalidRequest
		}
		if queryScope && s.Activity == "" {
			return ErrInvalidRequest
		}
	}
	return nil
}

func EncodeEmissionCommand(c EmissionCommand) ([]byte, error) {
	v := emissionSchema(c)
	if v != 3 {
		return nil, ErrInvalidRequest
	}
	if err := validateEmission(c); err != nil {
		return nil, fmt.Errorf("%w: emission: %w", ErrInvalidRequest, err)
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	out := append([]byte("ATE-EMISSION\x00"), byte(v))
	var b [10]byte
	n := binary.PutUvarint(b[:], uint64(len(raw)))
	out = append(out, b[:n]...)
	return append(out, raw...), nil
}
func DecodeEmissionCommand(raw []byte) (EmissionCommand, error) {
	p := []byte("ATE-EMISSION\x00")
	if len(raw) < len(p)+2 || !bytes.Equal(raw[:len(p)], p) || raw[len(p)] != 3 {
		return EmissionCommand{}, ErrInvalidRequest
	}
	l, n := binary.Uvarint(raw[len(p)+1:])
	if n <= 0 || int(l) != len(raw)-len(p)-1-n {
		return EmissionCommand{}, ErrInvalidRequest
	}
	var c EmissionCommand
	d := json.NewDecoder(bytes.NewReader(raw[len(p)+1+n:]))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, ErrInvalidRequest
	}
	c.SchemaVersion = uint64(raw[len(p)])
	canonical, err := EncodeEmissionCommand(c)
	if err != nil || !bytes.Equal(canonical, raw) {
		return EmissionCommand{}, ErrInvalidRequest
	}
	return c, nil
}

func emissionSchema(c EmissionCommand) uint64 {
	if c.SchemaVersion != 0 {
		return c.SchemaVersion
	}
	if s := c.Issue; s != nil && (s.Query != "" || s.Activity != "" || s.Attempt != "" || s.RequestDigest != "" || s.ContentKind != "" || s.NotAfterLSN != 0) {
		return 3
	}
	return 3
}
