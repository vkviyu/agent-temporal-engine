package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var _ api.AgentRecordReader = (*Engine)(nil)

// This is output pagination: each page authenticates and materializes the
// complete catalog, including disclosure dependencies outside the output page.
func (e *Engine) GetAgentStateRecords(ctx context.Context, req api.ReadAgentStateRecordsRequest) (api.AgentStateRecordsPage, error) {
	if ctx.Value(readDeliveryKey{}) == nil {
		return embeddedReadDelivery(ctx, e, "get_agent_state_records", req.Principal, req, func(scoped context.Context) (api.AgentStateRecordsPage, error) {
			return e.GetAgentStateRecords(scoped, req)
		})
	}
	if err := validAgentScope(req.Space, req.Principal); err != nil {
		return api.AgentStateRecordsPage{}, err
	}
	if !e.options.AgentProfile {
		return api.AgentStateRecordsPage{}, ErrUnsupportedMethod
	}
	if err := e.publicGate(); err != nil {
		return api.AgentStateRecordsPage{}, err
	}
	if err := e.readReady(ctx); err != nil {
		return api.AgentStateRecordsPage{}, err
	}
	if req.Limit < 0 || req.Limit > api.MaxAgentStatePageRecords || (req.Continuation != "" && req.Anchor == nil) {
		return api.AgentStateRecordsPage{}, api.ErrInvalidRequest
	}
	limit := req.Limit
	if limit == 0 {
		limit = api.MaxAgentStatePageRecords
	}
	anchor := model.SnapshotAnchor{Space: req.Space, Fence: e.readPublication(ctx).Fence()}
	roots, rootID := e.readRoots(ctx), e.readRootSet(ctx)
	if req.Anchor != nil {
		anchor = req.Anchor.Clone()
		if anchor.Validate() != nil || anchor.Space != req.Space || anchor.Cursor != nil || anchor.Knowledge != nil {
			return api.AgentStateRecordsPage{}, model.ErrAnchorMismatch
		}
		var err error
		roots, rootID, err = e.rootsAtFence(ctx, anchor.Fence, false)
		if err != nil {
			return api.AgentStateRecordsPage{}, e.knowledgeReadFailure(ctx, err)
		}
	}
	id, found, err := state.Get(ctx, e.reader(ctx), systemSpace, roots.LiveControl, agentPartitionKey(req.Space, req.Principal))
	if err != nil {
		return api.AgentStateRecordsPage{}, e.readFailureAt(ctx, err)
	}
	out := api.AgentStateRecordsPage{Found: found, Anchor: anchor, Fence: anchor.Fence, RootSet: rootID, LiveControlRoot: roots.LiveControl, Records: []agentprofile.StateRecord{}}
	snapshot := agentprofile.New().Snapshot()
	var image agentprofile.RecordImage
	if found {
		obj, err := readObject(ctx, e.reader(ctx), id)
		if err != nil {
			return api.AgentStateRecordsPage{}, e.readFailureAt(ctx, err)
		}
		catalog, owner, err := loadAgentRecordCatalog(ctx, e.reader(ctx), obj)
		if err != nil {
			return api.AgentStateRecordsPage{}, e.readFailureAt(ctx, err)
		}
		if obj.Space != req.Space || owner != req.Principal {
			return api.AgentStateRecordsPage{}, e.readFailureAt(ctx, agentRecordReadFailure(ErrInvalidStateData))
		}
		image = catalog.image
		snapshot, err = image.Snapshot()
		if err != nil {
			return api.AgentStateRecordsPage{}, e.readFailureAt(ctx, err)
		}
		out.Catalog, out.RecordCount, out.RecordBytes = id, uint32(len(catalog.objects)), image.Size()
	}
	ids, err := e.checkAgentSnapshotDisclosure(ctx, roots, req.Space, req.Principal, snapshot, true)
	if err != nil {
		if errors.Is(err, memory.ErrHistoryAccessDenied) {
			return api.AgentStateRecordsPage{}, err
		}
		return api.AgentStateRecordsPage{}, e.knowledgeReadFailure(ctx, err)
	}
	if found {
		out, err = packAgentRecords(ctx, req, limit, out, image)
		if err != nil {
			return api.AgentStateRecordsPage{}, err
		}
	} else if req.Continuation != "" {
		return api.AgentStateRecordsPage{}, api.ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return api.AgentStateRecordsPage{}, err
	}
	if err := e.viewOutputGate(); err != nil {
		return api.AgentStateRecordsPage{}, err
	}
	return finishReadDelivery(ctx, e, "get_agent_state_records", req.Principal, out, anchor, "", "", ids)
}

func agentRecordsScope(req api.ReadAgentStateRecordsRequest, limit int, page api.AgentStateRecordsPage) string {
	raw, _ := json.Marshal(struct {
		Version      string
		Principal    model.PrincipalID
		Space        model.SpaceID
		Page         api.AgentStateRecordsPage
		Limit, Bytes int
	}{"agent/record-read-v1", req.Principal, req.Space, page, limit, api.MaxAgentStatePageBytes})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func packAgentRecords(ctx context.Context, req api.ReadAgentStateRecordsRequest, limit int, out api.AgentStateRecordsPage, image agentprofile.RecordImage) (api.AgentStateRecordsPage, error) {
	scope := agentRecordsScope(req, limit, out)
	after, err := readNodeToken(req.Continuation, scope)
	if err != nil {
		return api.AgentStateRecordsPage{}, err
	}
	keys, err := image.RecordKeys()
	if err != nil {
		return api.AgentStateRecordsPage{}, err
	}
	start := 0
	if after != "" {
		key, err := hex.DecodeString(after)
		if err != nil || len(key) != agentRecordKeyBytes || hex.EncodeToString(key) != after {
			return api.AgentStateRecordsPage{}, api.ErrInvalidRequest
		}
		i := sort.Search(len(keys), func(i int) bool { return bytes.Compare(keys[i], key) >= 0 })
		if i >= len(keys)-1 || !bytes.Equal(keys[i], key) {
			return api.AgentStateRecordsPage{}, api.ErrInvalidRequest
		}
		start = i + 1
	}
	out.Records = []agentprofile.StateRecord{}
	// Tokens have fixed-length digest/key fields. Reserve the largest token
	// once, then add each record's JSON length and comma in linear total work.
	reserve := out
	reserve.Continuation = writeNodeToken(scope, hex.EncodeToString(make([]byte, agentRecordKeyBytes)))
	budget := api.MaxAgentStatePageBytes - jsonBytes(reserve)
	for i := start; i < len(keys) && len(out.Records) < limit; i++ {
		if err := ctx.Err(); err != nil {
			return api.AgentStateRecordsPage{}, err
		}
		record, err := image.RecordAt(i)
		if err != nil {
			return api.AgentStateRecordsPage{}, err
		}
		size := jsonBytes(record)
		if len(out.Records) != 0 {
			size++
		}
		if size > budget {
			break
		}
		budget -= size
		out.Records = append(out.Records, record)
	}
	if start+len(out.Records) < len(keys) {
		if len(out.Records) == 0 {
			return api.AgentStateRecordsPage{}, storage.ErrLimit
		}
		out.Continuation = writeNodeToken(scope, hex.EncodeToString(out.Records[len(out.Records)-1].Key))
	}
	if jsonBytes(out) > api.MaxAgentStatePageBytes {
		return api.AgentStateRecordsPage{}, storage.ErrLimit
	}
	return out, nil
}
