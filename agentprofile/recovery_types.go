package agentprofile

import "github.com/vkviyu/agent-temporal-engine/model"

// AuthorityBinding is current execution authority, not a historical read
// capability. Deployment identities come from the trusted owner controller.
// Evidence, Compatibility and *Evidence fields in this schema are opaque
// attestation digests supplied by that controller. Their compatibility ObjectID
// type does not make them stored content references or validate the external
// assertion. Hosts retain the corresponding attestation artifacts separately.
type AuthorityBinding struct {
	Deployment  string
	OwnerEpoch  uint64
	Incarnation uint64
	Holder      string
}
type ExecutionAuthority struct {
	Execution model.ExecutionID
	Version   uint64
	Binding   AuthorityBinding
	Status    string
	Evidence  model.ObjectID
}
type ActivitySpec struct {
	Activity       string
	Execution      model.ExecutionID
	Query          string
	Role           string
	Step           uint64
	Invocation     string
	GoalEpoch      uint64
	InputFence     model.CommitFence
	ContextPacket  model.ObjectID // embedded Knowledge PacketID, not a raw core object
	RequestDigest  model.ObjectID // ExactRequestDigest of the complete stored request bytes
	Request        model.ObjectID // published request content at InputFence
	Recipient      string
	Effect         string // pure, idempotent, non_idempotent
	IdempotencyKey string
	ResourceScope  string
	Contract       EffectContract
}
type ActivityControl struct {
	Activity           string
	Version            uint64
	Incarnation        uint64
	AcceptedResult     string
	RetryAuthorization model.ObjectID
	RetryObservedAt    uint64 `json:",omitempty"` // lower bound for the authorized claim observation
}
type AttemptControl struct {
	Attempt          string
	Activity         string
	Sequence         uint64
	Authority        AuthorityBinding
	RequestDigest    model.ObjectID
	Status           string
	DispatchEvidence model.ObjectID
	ClaimUse         RetryUse
	DispatchUse      RetryUse
}
type ResultCapture struct {
	Result      string
	Quarantined bool
}
type ActivityAcceptance struct {
	Activity, Result string
	Version          uint64
	Authority        AuthorityBinding
	Evidence         model.ObjectID
}
type ActivitySettlement struct {
	Activity string
	Version  uint64
	Status   string
	Evidence model.ObjectID
}
type AttemptReconciliation struct {
	Attempt            string
	Version            uint64
	Outcome            string // occurred, no_effect, compensated, unresolved
	Evidence           model.ObjectID
	FutureEffectFenced bool
}
type QueryAnswer struct {
	Query, Slot, Kind string
	Version           uint64
	Body              []byte
	Final             bool
}
type WorkStart struct {
	Query, Slot  string
	Task         model.EntityID
	Execution    model.ExecutionID
	IntentDigest model.ObjectID `json:"intent_digest,omitempty"`
}
type TaskLink struct {
	Task            model.EntityID
	ParentExecution model.ExecutionID
	Invocation      string
	InheritedTask   model.EntityID
	InheritedEpoch  uint64
	Compatibility   model.ObjectID
	IntentDigest    model.ObjectID `json:"intent_digest,omitempty"`
	GoalObservation model.ObjectID `json:"goal_observation,omitempty"`
}
type GoalObservation struct {
	Execution   model.ExecutionID
	Epoch       uint64
	Evidence    model.ObjectID
	Observation model.ObjectID `json:"observation,omitempty"`
}
type TaskOutcome struct {
	Task                  model.EntityID
	Execution             model.ExecutionID
	GoalEpoch             uint64
	Result, Validation    model.ObjectID
	Sequence              uint64
	ValidationRequirement model.ObjectID `json:"validation_requirement,omitempty"`
}
type ActionIntent struct {
	Action       PendingAction
	Method       string
	ExactCommand []byte
	Authority    AuthorityBinding
	InputObjects []model.ObjectID
	// ExpectedFence binds the optional outer CommitRequest precondition in
	// Control schema 3. Nil is distinct from a supplied fence and is never
	// refreshed during recovery. Publishing the reservation itself advances
	// the global fence, so ordinary reserved mutations should usually use
	// StateBatch.ExpectedRoot for a domain-state precondition instead.
	ExpectedFence *model.CommitFence `json:"expected_fence,omitempty"`
}
type ActionResolution struct {
	Key           string
	Generation    uint64
	Outcome       string // applied, absent, unknown
	ObservedFence model.CommitFence
	Receipt       model.ObjectID
}
type ResumePlan struct {
	Execution                model.ExecutionID
	ExpectedIncarnation      uint64
	ExpectedAuthorityVersion uint64
	Next                     AuthorityBinding
	AdapterVersion           string
	Checkpoint               model.ObjectID // published host checkpoint content
	CompatibilityEvidence    model.ObjectID
	ResourceFenceEvidence    model.ObjectID
}
type ExecutionJoin struct {
	ID      string
	Parent  model.ExecutionID
	Members []model.ExecutionID
	Mode    string
	Quorum  uint64
	Version uint64
	Winners []model.ExecutionID
	Status  string
}

// RecoverySnapshot stores immutable decisions and current execution-control
// projections. The enclosing core command log remains the transition history.
type RecoverySnapshot struct {
	Authorities            []ExecutionAuthority    `json:"authorities,omitempty"`
	ActivitySpecs          []ActivitySpec          `json:"activity_specs,omitempty"`
	ActivityControls       []ActivityControl       `json:"activity_controls,omitempty"`
	AttemptControls        []AttemptControl        `json:"attempt_controls,omitempty"`
	Captures               []ResultCapture         `json:"captures,omitempty"`
	Acceptances            []ActivityAcceptance    `json:"acceptances,omitempty"`
	Settlements            []ActivitySettlement    `json:"settlements,omitempty"`
	Reconciliations        []AttemptReconciliation `json:"reconciliations,omitempty"`
	Answers                []QueryAnswer           `json:"answers,omitempty"`
	Starts                 []WorkStart             `json:"starts,omitempty"`
	TaskLinks              []TaskLink              `json:"task_links,omitempty"`
	Goals                  []GoalObservation       `json:"goals,omitempty"`
	Outcomes               []TaskOutcome           `json:"outcomes,omitempty"`
	Actions                []ActionIntent          `json:"actions,omitempty"`
	Resolutions            []ActionResolution      `json:"resolutions,omitempty"`
	Resumes                []ResumePlan            `json:"resumes,omitempty"`
	Joins                  []ExecutionJoin         `json:"joins,omitempty"`
	Imports                []ActivityAcceptance    `json:"imports,omitempty"`
	ValidationRequirements []ValidationRequirement `json:"validation_requirements,omitempty"`
	IdempotencyGuarantees  []IdempotencyGuarantee  `json:"idempotency_guarantees,omitempty"`
	RetryAuthorizations    []RetryAuthorization    `json:"retry_authorizations,omitempty"`
}

type AnswerQueryCommand struct {
	Query, Slot, Kind string
	ExpectedVersion   uint64
	Body              []byte
	Final             bool
}
type StartWorkCommand struct {
	Query, Slot          string
	ExpectedQueryVersion uint64
	Task                 Task
	Execution            Execution
}
type GrantAuthorityCommand struct {
	Authority       ExecutionAuthority
	ExpectedVersion uint64
}
type FenceAuthorityCommand struct {
	Execution       model.ExecutionID
	ExpectedVersion uint64
	Evidence        model.ObjectID
}
type CreateActivityCommand struct {
	Spec      ActivitySpec
	Authority AuthorityBinding
}
type ClaimAttemptCommand struct {
	Attempt   string
	Activity  string
	Sequence  uint64
	Authority AuthorityBinding
	Use       RetryUse
}
type DispatchAttemptCommand struct {
	Attempt   string
	Authority AuthorityBinding
	Evidence  model.ObjectID
	Use       RetryUse
}
type CaptureResultCommand struct{ Result ActivityResult }
type AcceptActivityCommand struct {
	Activity, Result string
	ExpectedVersion  uint64
	Authority        AuthorityBinding
	Evidence         model.ObjectID
}
type SettleActivityCommand struct {
	Activity        string
	ExpectedVersion uint64
	Status          string
	Evidence        model.ObjectID
}
type ReconcileAttemptCommand struct {
	Attempt            string
	ExpectedVersion    uint64
	Outcome            string
	Evidence           model.ObjectID
	FutureEffectFenced bool
}
type RetryActivityCommand struct {
	Activity        string
	ExpectedVersion uint64
	Authority       AuthorityBinding
	Use             RetryUse
}
type SaveContinuationCommand struct {
	Value           Continuation
	ExpectedVersion uint64
	Authority       AuthorityBinding
}
type ObserveGoalCommand struct {
	Execution   model.ExecutionID
	Epoch       uint64
	Evidence    model.ObjectID
	Authority   AuthorityBinding
	Observation model.ObjectID
}
type CompleteTaskCommand struct {
	Task               model.EntityID
	Execution          model.ExecutionID
	Epoch              uint64
	Result, Validation model.ObjectID
	Authority          AuthorityBinding
}
type CancelExecutionCommand struct {
	Execution           model.ExecutionID
	ExpectedIncarnation uint64
}
type SpawnChildCommand struct {
	Parent         model.ExecutionID
	Invocation     string
	Task           Task
	Execution      Execution
	InheritedEpoch uint64
	Compatibility  model.ObjectID
	Authority      AuthorityBinding
}
type PutActionCommand struct {
	Intent    ActionIntent
	Authority AuthorityBinding
}
type ResolveActionCommand struct{ Resolution ActionResolution }
type RebindActionCommand struct {
	Key                string
	ExpectedGeneration uint64
	NewRequest         model.RequestKey
	Method             string
	ExactCommand       []byte
	CommandDigest      model.ObjectID
	Authority          AuthorityBinding
	FenceEvidence      model.ObjectID
	ExpectedFence      *model.CommitFence
}
type BeginStepCommand struct {
	Step      Step
	Authority AuthorityBinding
}
type RecordInvocationCommand struct {
	Invocation Invocation
	Authority  AuthorityBinding
}
type FinishStepCommand struct {
	Execution       model.ExecutionID
	Sequence, Epoch uint64
	Authority       AuthorityBinding
}
type JoinExecutionsCommand struct {
	Join            ExecutionJoin
	ExpectedVersion uint64
	Authority       AuthorityBinding
	TimedOut        bool
}
type RebaseInheritedCommand struct {
	Execution            model.ExecutionID
	SourceTask           model.EntityID
	ExpectedEpoch, Epoch uint64
	Compatibility        model.ObjectID
	Authority            AuthorityBinding
	Observation          model.ObjectID
}

// CheckpointCommand publishes the next exact intent and its continuation in
// one transition. Neither half is installed if either precondition fails.
type CheckpointCommand struct {
	Intent          ActionIntent
	Continuation    Continuation
	ExpectedVersion uint64
	Authority       AuthorityBinding
}

type SendMessageCommand struct {
	Message   Message
	Authority AuthorityBinding
}

type ConsumeMessageCommand struct {
	Consumption MessageConsumption
	Authority   AuthorityBinding
}

type BindValidationCommand struct {
	Requirement     ValidationRequirement
	ExpectedVersion uint64
	Authority       AuthorityBinding
}

// ControlCommand is a closed, versioned control union. It records decisions;
// it never executes a model, tool, callback or external resource operation.
type ControlCommand struct {
	AnswerQuery      *AnswerQueryCommand
	StartWork        *StartWorkCommand
	GrantAuthority   *GrantAuthorityCommand
	FenceAuthority   *FenceAuthorityCommand
	Resume           *ResumePlan
	CreateActivity   *CreateActivityCommand
	ClaimAttempt     *ClaimAttemptCommand
	DispatchAttempt  *DispatchAttemptCommand
	CaptureResult    *CaptureResultCommand
	AcceptActivity   *AcceptActivityCommand
	SettleActivity   *SettleActivityCommand
	ReconcileAttempt *ReconcileAttemptCommand
	RetryActivity    *RetryActivityCommand
	SaveContinuation *SaveContinuationCommand
	ObserveGoal      *ObserveGoalCommand
	CompleteTask     *CompleteTaskCommand
	CancelExecution  *CancelExecutionCommand
	SpawnChild       *SpawnChildCommand
	PutAction        *PutActionCommand
	ResolveAction    *ResolveActionCommand
	RebindAction     *RebindActionCommand
	BeginStep        *BeginStepCommand
	RecordInvocation *RecordInvocationCommand
	FinishStep       *FinishStepCommand
	JoinExecutions   *JoinExecutionsCommand
	RebaseInherited  *RebaseInheritedCommand
	Checkpoint       *CheckpointCommand
	SendMessage      *SendMessageCommand
	ConsumeMessage   *ConsumeMessageCommand
	// ImportActivityResult explicitly accepts a quarantined prior-incarnation
	// result only after an occurred reconciliation; it never reruns its effect.
	ImportActivityResult    *AcceptActivityCommand
	BindValidation          *BindValidationCommand
	PutIdempotencyGuarantee *PutIdempotencyGuaranteeCommand
	AuthorizeRetry          *AuthorizeRetryCommand
}

// currentControlSchema is the only schema emitted and accepted by V1.
const currentControlSchema uint64 = 5

func (c ControlCommand) SchemaVersion() uint64 { return currentControlSchema }
