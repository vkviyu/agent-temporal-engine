package engine

import (
	"encoding/json"
	"strconv"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type taskTraceTokenScope struct {
	Anchor    model.SnapshotAnchor
	Principal model.PrincipalID
	Subject   api.TraceSubject
	Session   string
	Limit     int
	Filter    any
}

func taskTraceTokenScopeDigest(req api.TaskTraceRequest, anchor model.SnapshotAnchor, filter any) (string, error) {
	raw, err := json.Marshal(taskTraceTokenScope{
		Anchor:    anchor,
		Principal: req.Principal,
		Subject:   req.Subject,
		Session:   req.Session,
		Limit:     req.Limit,
		Filter:    filter,
	})
	if err != nil {
		return "", err
	}
	return string(emissionDigest(raw)), nil
}

func readTaskTracePageStart(raw string, count int) (int, error) {
	start, err := strconv.Atoi(raw)
	if err != nil || start < 1 || start >= count {
		return 0, api.ErrInvalidRequest
	}
	return start, nil
}

func tracePage(req api.TaskTraceRequest, anchor model.SnapshotAnchor, filter any, count int) (start, end int, next string, err error) {
	scope, err := taskTraceTokenScopeDigest(req, anchor, filter)
	if err != nil {
		return 0, 0, "", err
	}
	if req.Continuation != "" {
		raw, err := readNodeToken(req.Continuation, scope)
		if err != nil {
			return 0, 0, "", err
		}
		start, err = readTaskTracePageStart(raw, count)
		if err != nil {
			return 0, 0, "", err
		}
	}
	size := req.Limit
	if size == 0 {
		size = defaultTaskTracePage
	}
	end = min(start+size, count)
	if end < count {
		next = writeNodeToken(scope, strconv.Itoa(end))
	}
	return start, end, next, nil
}

func tracePositionScope(req api.TaskTraceRequest, anchor model.SnapshotAnchor) (string, error) {
	return taskTraceTokenScopeDigest(req, anchor, "overview-position-v1")
}

func readTracePosition(req api.TaskTraceRequest, anchor model.SnapshotAnchor) (tracePosition, string, error) {
	scope, err := tracePositionScope(req, anchor)
	if err != nil {
		return tracePosition{}, "", err
	}
	if req.Continuation == "" {
		return tracePosition{}, scope, nil
	}
	raw, err := readNodeToken(req.Continuation, scope)
	if err != nil {
		return tracePosition{}, "", err
	}
	var position tracePosition
	if json.Unmarshal([]byte(raw), &position) != nil || position.ID == "" {
		return tracePosition{}, "", api.ErrInvalidRequest
	}
	return position, scope, nil
}
