package api

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/vkviyu/agent-temporal-engine/agentprofile"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const MethodApplyWork = "agent.work.apply/v1"
const MaxWorkSteps = 64

// WorkUnit is a bounded protected transition, not a second journal or a list of
// independently receipted requests. Every step must be a registered pure agent
// or knowledge/emission reducer. There is exactly one publication and one RequestKey.
// Source anchors still refer to previously published history, never an
// intermediate step or the unit's eventual commit digest. query_start and
// child_spawn adopt their explicit initial observations before checkpointing.
// Their initial checkpoint must have an empty knowledge position; the engine
// binds its resulting KnowledgeSource to this Work's exact producer and seed
// Revision. Root seeds include the published Query input and GoalKnowledge;
// child seeds include an explicit parent Import and the child's GoalKnowledge.
// message_consume adopts a new Observation matching the Message payload before
// writing the recipient continuation. The caller leaves its knowledge position
// empty so Work can bind it to the final recipient Revision in this publication.
type WorkUnit struct {
	Kind      string           `json:"kind"`
	Query     string           `json:"query,omitempty"`
	Intake    *QueryIntake     `json:"intake,omitempty"`
	Steps     []WorkStep       `json:"steps,omitempty"`
	Decision  *DecisionBinding `json:"decision,omitempty"`
	QueryTurn *QueryTurn       `json:"query_turn,omitempty"`
}

type WorkStep struct {
	Method string `json:"method"`
	Body   []byte `json:"body"`
}

// QueryIntake preserves the exact next request before a Task exists. Route and
// Configuration are immutable host decisions; recovery executes Next unchanged
// instead of asking a model to rediscover them or allocating a fresh key.
type QueryIntake struct {
	Query         agentprofile.Query `json:"query"`
	Route         string             `json:"route"`
	Configuration []byte             `json:"configuration"`
	Next          CommitRequest      `json:"next"`
}

type QueryIntakeState struct {
	Intake  QueryIntake `json:"intake"`
	Started bool        `json:"started"`
}

// DecisionBinding binds the complete invocation set to one accepted model
// result. Definitions are immutable published content, not mutable tool names.
type DecisionBinding struct {
	Activity    string               `json:"activity"`
	Result      string               `json:"result"`
	Invocations []DecisionInvocation `json:"invocations,omitempty"`
}
type DecisionInvocation struct {
	Invocation    string         `json:"invocation"`
	Definition    model.ObjectID `json:"definition"`
	Version       string         `json:"version"`
	DispatchClass string         `json:"dispatch_class"`
}

type ApplyWorkRequest struct {
	Key           model.RequestKey
	Unit          WorkUnit
	ExpectedFence *model.CommitFence
}
type ReadQueryIntakeRequest struct {
	Space     model.SpaceID
	Principal model.PrincipalID
	Query     string
	Anchor    *model.SnapshotAnchor
}
type QueryIntakeResult struct {
	Found  bool
	State  QueryIntakeState
	Anchor model.SnapshotAnchor
}
type WorkCommands interface {
	ApplyWork(context.Context, ApplyWorkRequest) (CommitResult, error)
	GetQueryIntake(context.Context, ReadQueryIntakeRequest) (QueryIntakeResult, error)
}

func AgentWorkStep(command agentprofile.Command) (WorkStep, error) {
	b, err := agentprofile.EncodeCommand(command)
	return WorkStep{Method: MethodApplyAgent, Body: b}, err
}
func KnowledgeWorkStep(command knowledge.Command) (WorkStep, error) {
	b, err := knowledge.EncodeCommand(command)
	return WorkStep{Method: MethodApplyKnowledge, Body: b}, err
}

func EmissionWorkStep(command EmissionCommand) (WorkStep, error) {
	b, err := EncodeEmissionCommand(command)
	return WorkStep{Method: MethodApplyEmission, Body: b}, err
}

func validateWorkUnit(u WorkUnit) error {
	queryTurn := u.Kind == "query_open" || u.Kind == "query_checkpoint" || u.Kind == "query_decision" || u.Kind == "query_tool" || u.Kind == "query_answer"
	if queryTurn != (u.QueryTurn != nil) {
		return ErrInvalidRequest
	}
	switch u.Kind {
	case "query_intake":
		if u.Intake == nil || len(u.Steps) != 0 || u.Decision != nil || u.Query != u.Intake.Query.ID || u.Query == "" {
			return ErrInvalidRequest
		}
		q := u.Intake
		if q.Route == "" || len(q.Route) > MaxIdentityBytes || len(q.Configuration) == 0 || q.Query.Session == "" || q.Query.Version != 1 || q.Query.Status != "open" || q.Next.Validate() != nil || q.Next.Method != MethodApplyWork {
			return ErrInvalidRequest
		}
		if _, err := agentprofile.EncodeCommand(agentprofile.Command{Lifecycle: &agentprofile.LifecycleCommand{PutQuery: &q.Query}}); err != nil {
			return err
		}
	case "query_start", "model_decision", "message_consume", "child_spawn", "checkpoint", "emission_dispatch":
		if u.Intake != nil || len(u.Steps) == 0 || len(u.Steps) > MaxWorkSteps {
			return ErrInvalidRequest
		}
		if u.Kind == "query_start" && u.Query == "" {
			return ErrInvalidRequest
		}
		if (u.Kind == "model_decision") != (u.Decision != nil) {
			return ErrInvalidRequest
		}
	case "query_open", "query_checkpoint", "query_decision", "query_tool", "query_answer":
		if u.Intake != nil || !queryText(u.Query) || len(u.Steps) == 0 || len(u.Steps) > MaxWorkSteps || (u.Kind == "query_decision") != (u.Decision != nil) || validateQueryTurn(u.QueryTurn, u.Kind) != nil {
			return ErrInvalidRequest
		}
	default:
		return ErrInvalidRequest
	}
	if u.Kind == "emission_dispatch" && (len(u.Steps) != 2 || u.Steps[0].Method != MethodApplyEmission || u.Steps[1].Method != MethodApplyAgent) {
		return ErrInvalidRequest
	}
	for _, step := range u.Steps {
		switch step.Method {
		case MethodApplyAgent:
			c, err := agentprofile.DecodeCommand(step.Body)
			if err != nil {
				return err
			}
			if c.Control == nil {
				return ErrInvalidRequest
			}
		case MethodApplyEmission:
			c, err := DecodeEmissionCommand(step.Body)
			if err != nil || u.Kind != "emission_dispatch" || c.ClaimID == "" {
				return ErrInvalidRequest
			}
		case MethodApplyKnowledge:
			if _, err := knowledge.DecodeCommand(step.Body); err != nil {
				return err
			}
		default:
			return ErrInvalidRequest
		}
	}
	return nil
}

// EncodeWorkUnit fixes the schema and JSON field order. Decode re-encodes the
// complete envelope; duplicate/unknown fields, alternate order and trailing
// bytes are rejected rather than creating multiple hashes for the same unit.
func EncodeWorkUnit(u WorkUnit) ([]byte, error) {
	if err := validateWorkUnit(u); err != nil {
		return nil, fmt.Errorf("%w: work unit: %w", ErrInvalidRequest, err)
	}
	raw, err := json.Marshal(u)
	if err != nil {
		return nil, err
	}
	if len(raw) > MaxCommandBody-16 {
		return nil, ErrInvalidRequest
	}
	out := append([]byte("ATE-WORK\x00"), 1)
	var lenbuf [10]byte
	n := binary.PutUvarint(lenbuf[:], uint64(len(raw)))
	out = append(out, lenbuf[:n]...)
	return append(out, raw...), nil
}
func DecodeWorkUnit(b []byte) (WorkUnit, error) {
	if len(b) > MaxCommandBody {
		return WorkUnit{}, ErrInvalidRequest
	}
	prefix := []byte("ATE-WORK\x00")
	if len(b) < len(prefix)+2 || !bytes.Equal(b[:len(prefix)], prefix) || b[len(prefix)] != 1 {
		return WorkUnit{}, ErrInvalidRequest
	}
	length, n := binary.Uvarint(b[len(prefix)+1:])
	if n <= 0 || length > MaxCommandBody || int(length) != len(b)-len(prefix)-1-n {
		return WorkUnit{}, ErrInvalidRequest
	}
	raw := b[len(prefix)+1+n:]
	var err error
	var u WorkUnit
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&u); err != nil {
		return WorkUnit{}, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	canonical, err := EncodeWorkUnit(u)
	if err != nil || !bytes.Equal(canonical, b) {
		return WorkUnit{}, ErrInvalidRequest
	}
	return u, nil
}
