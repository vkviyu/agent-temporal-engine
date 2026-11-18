package engine

import (
	"errors"
	"fmt"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/investigation"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/state"
	"github.com/vkviyu/agent-temporal-engine/storage"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

func TestClassifyPrepareErrorPriority(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code api.ResultCode
	}{
		{"cas", errCAS, api.CodeConflict},
		{"wrapped-cas", fmt.Errorf("precondition: %w", errCAS), api.CodeConflict},
		{"cas-before-limit", errors.Join(errCAS, storage.ErrLimit), api.CodeConflict},
		{"cas-before-rejection", errors.Join(errCAS, knowledge.ErrInvalid), api.CodeConflict},
		{"limit-before-rejection", errors.Join(storage.ErrLimit, knowledge.ErrInvalid), api.CodeResourceLimit},
		{"selection-limit-before-rejection", knowledge.ErrContextSelectionLimit, api.CodeResourceLimit},
		{"limit-before-reservation", errors.Join(storage.ErrLimit, ErrActionReservation), api.CodeResourceLimit},
		{"wrapped-rejection", fmt.Errorf("input: %w", knowledge.ErrInvalid), api.CodeRejected},
		{"reservation", ErrActionReservation, api.CodeRejected},
		{"nil", nil, ""},
		{"unknown", errors.New("unexpected preparation failure"), ""},
		{"stored-decode-limit", errors.Join(ErrStoredObject, codec.ErrObjectLimit), ""},
		{"io-limit", errors.Join(ErrObjectIO, storage.ErrLimit), ""},
		{"stored-rejection", errors.Join(ErrStoredObject, knowledge.ErrInvalid), ""},
		{"io-rejection", errors.Join(ErrObjectIO, knowledge.ErrInvalid), ""},
		{"semantic-rejection", errors.Join(ErrSemanticCorruption, knowledge.ErrInvalid), ""},
		{"invalid-state", ErrInvalidStateData, ""},
		{"corruption", storage.ErrCorruption, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := api.CommitRequest{Method: api.MethodApplyKnowledge, RequestID: "classification"}
			got, responseErr, handled := classifyPrepareError(req, tc.err)
			want := api.CommitResult{}
			wantErr := tc.err
			if tc.code != "" {
				want.Code, want.RequestID = tc.code, req.RequestID
			}
			if tc.code == api.CodeConflict {
				want.Conflict = api.ConflictPrecondition
				wantErr = nil
			}
			if handled != (tc.code != "") || got != want || responseErr != wantErr {
				t.Fatalf("got (%+v, %v, %t), want (%+v, %v, %t)", got, responseErr, handled, want, wantErr, tc.code != "")
			}
		})
	}
}

func TestClassifyPrepareErrorDomains(t *testing.T) {
	for _, tc := range []struct {
		method string
		err    error
	}{
		{api.MethodApplyAgent, agentprofile.ErrInvalid},
		{api.MethodApplyWork, ErrWorkBoundary},
		{api.MethodApplyAnnotation, annotation.ErrInvalid},
		{api.MethodUpdateInvestigation, investigation.ErrInvalid},
		{api.MethodApplyTaskTrace, api.ErrInvalidRequest},
		{api.MethodApplySummary, knowledge.ErrInvalidSummary},
		{api.MethodImportTransfer, transfer.ErrInvalid},
		{api.MethodApplyEmission, ErrEmissionRejected},
		{api.MethodApplyKnowledge, knowledge.ErrInvalid},
		{api.MethodApplyGraph, graph.ErrCommandShape},
		{api.MethodApplyMemory, memory.ErrInvalidMemory},
	} {
		t.Run(tc.method, func(t *testing.T) {
			wrapped := fmt.Errorf("prepare: %w", tc.err)
			for _, method := range []string{tc.method, api.MethodMutateState, "unknown-method"} {
				req := api.CommitRequest{Method: method, RequestID: "domain"}
				got, responseErr, handled := classifyPrepareError(req, wrapped)
				want := api.CommitResult{}
				if method == tc.method {
					want = api.CommitResult{Code: api.CodeRejected, RequestID: req.RequestID}
				}
				if handled != (method == tc.method) || got != want || responseErr != wrapped {
					t.Fatalf("%s: got (%+v, %v, %t), want %+v with original error", method, got, responseErr, handled, want)
				}
			}
		})
	}
}

func TestClassifyPrepareErrorCommonFailures(t *testing.T) {
	methods := []string{
		api.MethodMutateState, api.MethodApplyAgent, api.MethodApplyWork,
		api.MethodApplyAnnotation, api.MethodUpdateInvestigation, api.MethodApplyTaskTrace,
		api.MethodApplySummary, api.MethodImportTransfer, api.MethodApplyEmission,
		api.MethodApplyKnowledge, api.MethodApplyGraph, api.MethodApplyMemory,
		"unknown-method",
	}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := api.CommitRequest{Method: method, RequestID: "common"}
			for _, limit := range []error{
				storage.ErrLimit, state.ErrLimit, codec.ErrObjectLimit, graph.ErrLimit,
				knowledge.ErrContextSelectionLimit, annotation.ErrLimit, investigation.ErrLimit,
			} {
				wrapped := fmt.Errorf("prepare: %w", limit)
				got, responseErr, handled := classifyPrepareError(req, wrapped)
				if !handled || got != (api.CommitResult{Code: api.CodeResourceLimit, RequestID: req.RequestID}) || responseErr != wrapped {
					t.Fatalf("resource limit %v: %+v %v %t", limit, got, responseErr, handled)
				}
				for _, fault := range []error{ErrStoredObject, ErrObjectIO} {
					failure := errors.Join(fault, wrapped)
					got, responseErr, handled = classifyPrepareError(req, failure)
					if handled || got != (api.CommitResult{}) || responseErr != failure {
						t.Fatalf("stored failure %v became request outcome: %+v %v %t", failure, got, responseErr, handled)
					}
				}
			}
			got, responseErr, handled := classifyPrepareError(req, ErrActionReservation)
			if !handled || got != (api.CommitResult{Code: api.CodeRejected, RequestID: req.RequestID}) || responseErr != ErrActionReservation {
				t.Fatalf("reservation: %+v %v %t", got, responseErr, handled)
			}
			for _, fault := range []error{ErrStoredObject, ErrObjectIO, ErrInvalidStateData, ErrSemanticCorruption, storage.ErrCorruption} {
				got, responseErr, handled := classifyPrepareError(req, fault)
				if handled || got != (api.CommitResult{}) || responseErr != fault {
					t.Fatalf("internal failure %v became request outcome: %+v %v %t", fault, got, responseErr, handled)
				}
			}
		})
	}
}
