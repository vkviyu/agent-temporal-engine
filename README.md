# Agent Temporal Engine (ATE)

[English](README.md) | [简体中文](README.zh-CN.md)

ATE is a temporal engine for agents.

It gives an external Agent system a durable way to remember what happened, when it happened, what an agent knew at that point, which decisions depended on it, and where execution can continue after a failure.

ATE is intentionally not an Agent Loop. It does not decide how an agent thinks, call a model on its own, run tools, or manage a workspace. The host Agent system keeps those responsibilities. ATE records and validates the resulting timeline.

## Why a temporal engine?

Conversation history and vector memory answer only part of an agent's memory problem. A long-running, multi-agent task also needs to answer questions such as:

- What did this execution know before it made a decision?
- Which result came from which child Agent?
- Was a tool call planned, dispatched, retried, or left uncertain?
- Did the parent Agent actually consume the child's result?
- Can we inspect the task as it existed yesterday?
- Can we continue from an old state as a new branch without rewriting history?
- Can a memory be withdrawn from future use while preserving its audit trail?

ATE represents these facts as immutable, versioned, source-aware records. Time is part of the meaning of the record, rather than metadata added after the fact.

## A long-running multi-agent task

An external Agent system may run a task like this:

```text
Task: fix an intermittent order-service timeout
└── Root Execution
    ├── Code-investigation Execution
    ├── Regression-test Execution
    └── Connection-pool Execution
```

The children may finish in any order. Their model calls and tool calls may be retried. The root may consume only a selected summary from one child while keeping the other child's private findings isolated.

ATE records the timeline around that work:

```text
Task
  → Execution
  → Step
  → Activity / Attempt
  → Result
  → Observation
  → Knowledge Revision
  → Context Packet
  → next decision
```

The host runs the Agent Loop. ATE supplies the durable timeline, historical reads, explicit knowledge transfer, recovery position, and branchable state.

## Four timelines

ATE is easiest to understand as four connected timelines:

```text
Execution timeline
Task → Execution → Step → Activity → Attempt → Result

Knowledge timeline
Result / Message → Observation → Knowledge Revision → Context Packet

Domain timeline
Revision → Branch → Fork / Diff / Join

Recovery timeline
Pending Action → Authority → Continuation → Resume
```

They describe different facts:

- The execution timeline says what an Agent did.
- The knowledge timeline says what an Agent adopted and later used.
- The domain timeline says how state was explored and merged.
- The recovery timeline says where a long-running execution can safely continue.

They are connected by explicit references and events. A child finishing does not automatically mean that its parent has consumed its result. A Graph merge does not automatically merge a Git workspace. A memory stored in one execution is not automatically visible to another execution.

### Query-only and task-backed queries

A Query can complete in either of two explicit modes.

- A Query-only flow uses `QueryProgress` and `(Query, DecisionSequence)` for its controller state. It may record query-scoped model or tool Activities, but it does not create a synthetic `Task`, `Execution`, or `Step`. If it starts no Task, its final answer has no `TaskOutcome` requirement.
- A task-backed Query explicitly starts one or more Tasks. Each started Task must produce its own Execution and accepted `TaskOutcome` before the Query can publish a final answer. The final answer remains Query-scoped, but it cannot omit or invent the outcomes of the Tasks it started.

The distinction is semantic. A short conversation, routing decision, or read-only tool interaction does not gain an artificial execution history merely because it is durable. Work that crosses into a Task must declare that transition and then follow the Task and Execution lifecycle.

## Core concepts

| Concept                | Meaning                                                            |
| ---------------------- | ------------------------------------------------------------------ |
| `Task`                 | A durable goal instance                                            |
| `Execution`            | One actor carrying out a task, optionally under a parent execution |
| `Step`                 | One logical decision step in an execution                          |
| `Activity` / `Attempt` | One external operation and each dispatch attempt                   |
| `Observation`          | Content explicitly adopted into an execution's knowledge           |
| `Knowledge Revision`   | A version of one Actor/Execution knowledge stream                  |
| `ContextPacket`        | The exact historical context selected for a decision               |
| `SnapshotAnchor`       | A stable historical read position                                  |
| `Revision` / `Branch`  | A versioned domain state and its navigation reference              |
| `Continuation`         | A durable recovery position                                        |
| `Memory`               | A sourced, validated and reusable fact or experience               |

The core identity and time types are defined in [`model/`](model/). The public contracts are in [`api/`](api/).

## What ATE provides

ATE currently provides the core mechanisms for:

- immutable, content-addressed objects;
- historical snapshots and anchored reads;
- isolated knowledge per Actor/Execution;
- explicit cross-execution knowledge import;
- parallel child execution records and execution joins;
- model and tool Activity/Attempt lifecycle records;
- bounded retries and reconciliation of uncertain effects;
- durable continuations for recovery;
- branch, diff and explicit merge of domain state;
- sourced Memory and ExperiencePath lifecycle management;
- context selection and exact recorded model input;
- trace, node, content and provenance queries;
- local embedded and Unix service access;
- durable receipts, semantic replay and audit-preserving compaction.

The current implementation status, command examples, format boundaries and validation evidence are in [`docs/ATE_IMPLEMENTATION_STATUS.md`](docs/ATE_IMPLEMENTATION_STATUS.md).

## What ATE deliberately leaves to the host

The external host remains responsible for:

- the Agent Loop and model provider integration;
- interpreting model output;
- choosing and running tools;
- workspace, Git and file-system operations;
- application-specific scheduling and policies;
- external effect reconciliation;
- deciding which results should become observations or memories.

This separation lets ATE remain independent from GSEA, Eino, model providers and host application packages. The host integrates through the public API and the versioned `agent/v1` profile.

## Architecture at a glance

```text
External Agent system
  ├── model calls
  ├── tool execution
  ├── scheduling
  └── workspace operations
          │
          │ commands, results, observations and decisions
          ▼
ATE
  ├── immutable Objects and Events
  ├── copy-on-write Radix state
  ├── five authoritative roots
  ├── append-only Journal
  ├── historical Anchors and Views
  ├── Agent execution profile
  ├── Knowledge and Memory projections
  └── semantic replay and recovery
```

The five authoritative roots separate domain state, execution knowledge, branch references, live control state and command receipts. A single Coordinator validates candidate transitions and publishes them through one durable commit path.

## Reading guide

For Go SDK and Unix Socket methods, parameters, return values, and examples, see the [ATE API reference](docs/ATE_API_REFERENCE.md) (Chinese).

If you are new to the project, read in this order:

1. [`docs/AGENT_TEMPORAL_ENGINE_DESIGN.md`](docs/AGENT_TEMPORAL_ENGINE_DESIGN.md) — overall design and semantics.
2. [`docs/ATE_EXECUTION_PROFILE_WALKTHROUGH.md`](docs/ATE_EXECUTION_PROFILE_WALKTHROUGH.md) — one long-running multi-agent scenario.
3. [`api/`](api/) and [`model/`](model/) — public contracts and time-related identities.
4. [`engine/`](engine/), [`state/`](state/) and [`storage/`](storage/) — how the model is persisted and replayed.
5. [`agentprofile/`](agentprofile/), [`knowledge/`](knowledge/), [`graph/`](graph/) and [`memory/`](memory/) — the four timelines in code.
6. [`docs/ATE_IMPLEMENTATION_STATUS.md`](docs/ATE_IMPLEMENTATION_STATUS.md) — implementation details, CLI commands and current evidence.

## Quick start

The project targets Go 1.27.0:

```sh
go test ./...
go test -race ./...
go vet ./...
```

For state-store initialization, recovery, local service usage and format details, see [`docs/ATE_IMPLEMENTATION_STATUS.md`](docs/ATE_IMPLEMENTATION_STATUS.md).

## Project status

ATE is an experimental V1 implementation. The core state, Agent profile, Graph, Knowledge, Memory, query, recovery and local-service paths are implemented and covered by package and integration tests. Strict V1 acceptance remains open. Native evidence covers Darwin/arm64 APFS and Linux/arm64 ext4, including real ENOSPC and an orderly guest reboot; see [platform scope](docs/PLATFORM_SUPPORT.md). Device power-loss certification and broad external exactly-once guarantees are not claimed. Current gaps and verification are tracked in the [2026-11-06 cross-audit](docs/validation/V1_CROSS_AUDIT_2026-11-06.md).
