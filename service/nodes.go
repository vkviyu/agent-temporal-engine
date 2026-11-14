package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/protocol"
)

func (ss *session) serveReadRequest(ctx context.Context, req protocol.Request) (bool, error) {
	var read func(context.Context, []byte) error
	switch req.Method {
	case "read_content":
		return true, ss.replyContent(ctx, req)
	case "open_view", "lookup_request":
		if _, ok := ss.server.backend.(api.ReadDeliveryPreparer); !ok {
			return true, ss.reply(ss.errResponse(req.CorrelationID, apiError(api.ErrorUnsupported)))
		}
		return true, ss.replyDispatchedRead(ctx, req)
	case "list_nodes":
		read = ss.listNodes
	case "get_node":
		read = ss.getNode
	case "get_node_context":
		read = ss.getNodeContext
	case "search_nodes":
		read = ss.searchNodes
	case "trace_nodes":
		read = ss.traceNodes
	case "get_state", "get_view_state", "get_view_entries", "resolve_publication", "snapshot", "export_transfer", "get_transfer",
		"get_agent_state", "get_query_intake", "get_query_progress", "get_knowledge", "get_summary", "get_memory", "get_annotations", "get_investigation", "get_emission",
		"get_task_trace", "get_task_trace_evidence",
		"get_graph_revision", "get_graph_branch", "diff_graph", "plan_graph_join", "trace_graph":
		return true, ss.replyDispatchedRead(ctx, req)
	default:
		return false, nil
	}
	return true, ss.replyReadDelivery(ctx, req, func(scoped context.Context) error {
		return read(scoped, req.Body)
	})
}

func (ss *session) replyDispatchedRead(ctx context.Context, request protocol.Request) error {
	return ss.replyReadDelivery(ctx, request, func(scoped context.Context) error {
		response := ss.dispatch(scoped, request)
		if response.Error != nil {
			return response.Error
		}
		return nil
	})
}

func (ss *session) replyReadDelivery(ctx context.Context, request protocol.Request, read func(context.Context) error) error {
	backend, ok := ss.server.backend.(api.ReadDeliveryPreparer)
	if !ok {
		return ss.reply(ss.errResponse(request.CorrelationID, apiError(api.ErrorUnsupported)))
	}
	digest := sha256.Sum256(request.Body)
	binding := api.ReadDeliveryBinding{
		Destination: ss.readDestination(request.CorrelationID),
		Method:      request.Method, RequestDigest: model.ObjectID(hex.EncodeToString(digest[:])),
	}
	grant, err := backend.PrepareReadDelivery(ctx, binding, read)
	if err != nil {
		return ss.reply(ss.errResponse(request.CorrelationID, err))
	}
	if err := ctx.Err(); err != nil {
		return ss.reply(ss.errResponse(request.CorrelationID, err))
	}
	body, err := grant.Consume(binding)
	if err != nil {
		return ss.reply(ss.errResponse(request.CorrelationID, err))
	}
	return ss.reply(protocol.Response{Version: protocol.Version, CorrelationID: request.CorrelationID, Body: json.RawMessage(body)})
}

func (ss *session) readDestination(correlation string) api.ReadDestination {
	return api.ReadDestination{Principal: ss.principal, Session: ss.id, Request: correlation}
}

func (ss *session) listNodes(ctx context.Context, raw []byte) error {
	var r api.ListNodesRequest
	if err := protocol.DecodeStrict(raw, &r); err != nil {
		return err
	}
	if _, owned := ss.views[r.View]; !owned {
		return ErrUnauthorized
	}
	b, ok := ss.server.backend.(api.BasicNodeReader)
	if !ok {
		return apiError(api.ErrorUnsupported)
	}
	_, err := b.ListNodes(ctx, r)
	return err
}
func (ss *session) getNode(ctx context.Context, raw []byte) error {
	var r api.GetNodeRequest
	if err := protocol.DecodeStrict(raw, &r); err != nil {
		return err
	}
	if _, owned := ss.views[r.View]; !owned {
		return ErrUnauthorized
	}
	b, ok := ss.server.backend.(api.BasicNodeReader)
	if !ok {
		return apiError(api.ErrorUnsupported)
	}
	_, err := b.GetNode(ctx, r)
	return err
}
func (ss *session) getNodeContext(ctx context.Context, raw []byte) error {
	var r api.NodeContextRequest
	if err := protocol.DecodeStrict(raw, &r); err != nil {
		return err
	}
	if _, owned := ss.views[r.View]; !owned {
		return ErrUnauthorized
	}
	b, ok := ss.server.backend.(api.BasicNodeReader)
	if !ok {
		return apiError(api.ErrorUnsupported)
	}
	_, err := b.GetNodeContext(ctx, r)
	return err
}

func (ss *session) searchNodes(ctx context.Context, raw []byte) error {
	var r api.SearchNodesRequest
	if err := protocol.DecodeStrict(raw, &r); err != nil {
		return err
	}
	if _, owned := ss.views[r.View]; !owned {
		return ErrUnauthorized
	}
	b, ok := ss.server.backend.(api.NodeExplorer)
	if !ok {
		return apiError(api.ErrorUnsupported)
	}
	_, err := b.SearchNodes(ctx, r)
	return err
}

func (ss *session) traceNodes(ctx context.Context, raw []byte) error {
	var r api.TraceRequest
	if err := protocol.DecodeStrict(raw, &r); err != nil {
		return err
	}
	if _, owned := ss.views[r.View]; !owned {
		return ErrUnauthorized
	}
	b, ok := ss.server.backend.(api.NodeExplorer)
	if !ok {
		return apiError(api.ErrorUnsupported)
	}
	_, err := b.Trace(ctx, r)
	return err
}

func (ss *session) replyContent(ctx context.Context, request protocol.Request) error {
	var read api.ReadContentRequest
	if err := protocol.DecodeStrict(request.Body, &read); err != nil {
		return ss.reply(ss.errResponse(request.CorrelationID, err))
	}
	if _, owned := ss.views[read.View]; !owned {
		return ss.reply(ss.errResponse(request.CorrelationID, ErrUnauthorized))
	}
	backend, ok := ss.server.backend.(api.ContentEmissionReader)
	if !ok {
		return ss.reply(ss.errResponse(request.CorrelationID, apiError(api.ErrorUnsupported)))
	}
	destination := ss.readDestination(request.CorrelationID)
	grant, err := backend.PrepareContentRead(ctx, read, destination)
	if err != nil {
		return ss.reply(ss.errResponse(request.CorrelationID, err))
	}
	if err := ctx.Err(); err != nil {
		return ss.reply(ss.errResponse(request.CorrelationID, err))
	}
	content, err := grant.Consume(destination, read)
	if err != nil {
		return ss.reply(ss.errResponse(request.CorrelationID, err))
	}
	return ss.reply(protocol.Response{Version: protocol.Version, CorrelationID: request.CorrelationID, Body: mustJSON(content)})
}
