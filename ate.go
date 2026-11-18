// Package ate is the experimental embedded entry point for the Agent Temporal
// Engine. Core state, receipts, historical views and opt-in static agent/v1
// transitions share one publisher. The agentstore package adds typed profile
// convenience methods over this same core, without owning another journal.
package ate

import (
	"context"
	"sync"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/engine"
	"github.com/vkviyu/agent-temporal-engine/observation"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

type Options = engine.Options
type AdmissionOptions = engine.AdmissionOptions
type BatchOptions = engine.BatchOptions
type ViewOptions = engine.ViewOptions

// Store owns its engine, owner lock and native directory handle. Create and
// OpenExisting require an existing private directory; Close is retryable.
type Store struct {
	mu              sync.Mutex
	core            *engine.Engine
	directory       *storage.DirectoryIO
	directoryClosed bool
}

func Create(path string, options Options) (*Store, error) {
	d, err := storage.OpenDirectory(path)
	if err != nil {
		options.Observation.Emit(observation.Event{Kind: observation.Create, Outcome: observation.Failure})
		return nil, err
	}
	core, err := engine.Create(d, options)
	if err != nil {
		return nil, storage.CloseOnError(err, d)
	}
	return &Store{core: core, directory: d}, nil
}

// OpenExisting returns physical diagnostics even on rejection. Only a nil
// error and non-nil Store mean both physical and semantic replay succeeded.
func OpenExisting(path string, options Options) (*Store, storage.RecoveryReport, error) {
	d, err := storage.OpenDirectory(path)
	if err != nil {
		options.Observation.Emit(observation.Event{Kind: observation.Open, Outcome: observation.Failure})
		return nil, storage.RecoveryReport{}, err
	}
	core, report, err := engine.OpenExisting(d, options)
	if err != nil {
		return nil, report, storage.CloseOnError(err, d)
	}
	return &Store{core: core, directory: d}, report, nil
}

func (s *Store) Commit(ctx context.Context, req api.CommitRequest) (api.CommitResult, error) {
	return s.core.Commit(ctx, req)
}
func (s *Store) Mutate(ctx context.Context, req api.MutateRequest) (api.CommitResult, error) {
	return s.core.Mutate(ctx, req)
}
func (s *Store) LookupRequest(ctx context.Context, req api.LookupRequestRequest) (api.LookupRequestResult, error) {
	return s.core.LookupRequest(ctx, req)
}
func (s *Store) GetState(ctx context.Context, req api.ReadStateRequest) (api.StateValueResult, error) {
	return s.core.GetState(ctx, req)
}
func (s *Store) Snapshot(ctx context.Context) (api.StateSnapshot, error) { return s.core.Snapshot(ctx) }

// CloseContext stops admission and read grants, drains accepted work, and then
// retires the engine and directory. Cancellation ends only this caller's wait;
// keep the Store and retry closure to finish releasing its resources.
func (s *Store) CloseContext(ctx context.Context) error {
	if s.core != nil {
		if err := s.core.CloseContext(ctx); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.directory == nil || s.directoryClosed {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.directory != nil && !s.directoryClosed {
		s.directoryClosed = true
		return s.directory.Close()
	}
	return nil
}

func (s *Store) Close() error { return s.CloseContext(context.Background()) }

func (s *Store) OpenView(ctx context.Context, req api.OpenViewRequest) (api.ViewDescriptor, error) {
	return s.core.OpenView(ctx, req)
}
func (s *Store) CloseView(ctx context.Context, req api.CloseViewRequest) error {
	return s.core.CloseView(ctx, req)
}
func (s *Store) GetViewState(ctx context.Context, req api.ReadViewStateRequest) (api.StateValueResult, error) {
	return s.core.GetViewState(ctx, req)
}
func (s *Store) GetViewEntries(ctx context.Context, req api.ReadViewEntriesRequest) (api.StateEntriesResult, error) {
	return s.core.GetViewEntries(ctx, req)
}
func (s *Store) ListNodes(ctx context.Context, req api.ListNodesRequest) (api.NodePage, error) {
	return s.core.ListNodes(ctx, req)
}
func (s *Store) GetNode(ctx context.Context, req api.GetNodeRequest) (api.NodeDetail, error) {
	return s.core.GetNode(ctx, req)
}
func (s *Store) GetNodeContext(ctx context.Context, req api.NodeContextRequest) (api.NodeContext, error) {
	return s.core.GetNodeContext(ctx, req)
}
func (s *Store) SearchNodes(ctx context.Context, req api.SearchNodesRequest) (api.NodeSearchResult, error) {
	return s.core.SearchNodes(ctx, req)
}
func (s *Store) Trace(ctx context.Context, req api.TraceRequest) (api.NodeTrace, error) {
	return s.core.Trace(ctx, req)
}
func (s *Store) ReadContent(ctx context.Context, req api.ReadContentRequest) (api.ContentRange, error) {
	return s.core.ReadContent(ctx, req)
}
func (s *Store) PrepareContentRead(ctx context.Context, req api.ReadContentRequest, destination api.ReadDestination) (api.ReadEmissionGrant, error) {
	return s.core.PrepareContentRead(ctx, req, destination)
}
func (s *Store) PrepareReadDelivery(ctx context.Context, binding api.ReadDeliveryBinding, read func(context.Context) error) (api.ReadDeliveryGrant, error) {
	return s.core.PrepareReadDelivery(ctx, binding, read)
}
func (s *Store) ResolvePublication(ctx context.Context, req api.ResolvePublicationRequest) (api.SourcePublication, error) {
	return s.core.ResolvePublication(ctx, req)
}
func (s *Store) ApplyAgent(ctx context.Context, req api.ApplyAgentRequest) (api.CommitResult, error) {
	return s.core.ApplyAgent(ctx, req)
}
func (s *Store) GetAgentState(ctx context.Context, req api.ReadAgentStateRequest) (api.AgentStateResult, error) {
	return s.core.GetAgentState(ctx, req)
}

var _ api.AgentRecordReader = (*Store)(nil)

func (s *Store) GetAgentStateRecords(ctx context.Context, req api.ReadAgentStateRecordsRequest) (api.AgentStateRecordsPage, error) {
	return s.core.GetAgentStateRecords(ctx, req)
}
func (s *Store) AgentProfileEnabled() bool   { return s.core.AgentProfileEnabled() }
func (s *Store) EventProfileEnabled() bool   { return s.core.EventProfileEnabled() }
func ClassifyError(err error) api.ErrorClass { return engine.ClassifyError(err) }

func (s *Store) ClassifyError(err error) api.ErrorClass { return engine.ClassifyError(err) }
