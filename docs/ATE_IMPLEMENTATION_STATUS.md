# Agent Temporal Engine (ATE) — Implementation Status

[English](../README.md) | [简体中文](ATE_IMPLEMENTATION_STATUS.zh-CN.md)

ATE is an independent, local-first temporal execution infrastructure for long-running and multi-agent systems.

It is intentionally independent from GSEA, Eino, model providers, and any host application's internal packages. Host applications integrate through the public API and the versioned `agent/v1` profile.

Current status: core state, static `agent/v1`, Graph, Memory, Knowledge, bounded Work units, Summary provenance, typed Validation, NodeContext projections and Emission grant/revoke share one coordinator, receipt authority, RootSet and P1 publisher. Domain commands carry canonical intent through admission; the coordinator checks receipts before running their reducers, and replay verifies the same transitions. The implementation includes invocation settlement and lifecycle records, owner-scoped Knowledge history, domain and knowledge Revision Cursors, explicit cross-execution Import, formal Event occurrences, bounded Node Search/Trace/Content, SourcePublicationRef, RecordedInput and AuditFull physical compaction. The reproducible V1 implementation and cross-audit addendum are recorded in `validation/ATE_V1_IMPLEMENTATION_ADDENDUM_2026-11-02.md`; external model-quality and platform-durability evidence remain explicitly scoped. All packages remain public, with no `internal/` directories or third-party dependencies.

A single coordinator processes FIFO commands against a private incremental staging view. Transactions may span segments, while individual frames remain whole and object addresses stay stable. One P1 publishes the whole batch; each transaction keeps its original receipt, roots and commit proof. Canceling after admission ends only that caller's wait; the owned command continues even if all waiters leave. Candidate-dependent rejections are withheld until publication too.

The root package provides `Create`, `OpenExisting`, `Mutate`, `Commit`, `ApplyAgent`, `GetAgentState`, `ApplyWork`, `GetQueryIntake`, `ApplyEmission`, `GetEmission`, `ApplySummary`, `GetSummary`, `ApplyGraph`, `GetGraphRevision`, `GetGraphBranch`, `ApplyMemory`, `GetMemoryState`, `ApplyKnowledge`, `GetKnowledgeState`, `ListNodes`, `SearchNodes`, `Trace`, `GetNode`, `GetNodeContext`, bounded `ReadContent`, `ResolvePublication`, historical Views, `LookupRequest`, `GetState`, `Snapshot`, `CompactAuditFull`, `ReclaimRetired`, `CloseContext` and retryable `Close`. Enable Agent commands/replay with `Options{AgentProfile: true}`. Graph updates `BranchRefs`; Memory, Knowledge and Summary update `ExecutionKnowledge`; Agent, Work and Emission state updates `LiveControl`. The root facades submit to the same core, while `graphstore`, `knowledge.Durable` and `memory.Durable` remain standalone adapters with separate store ownership. A process-local InFlight is not a durable admission token. Profile partitions remain bounded whole-snapshot projections. Unix responses use one-shot destination-bound delivery grants, and embedded callers consume the same grant immediately before returning data.

Knowledge reads require a Principal supplied by the trusted embedding host or authenticated local connection. A missing KnowledgeKey selects only that owner's partition; a supplied key selects one Actor/Execution and is checked against historical and current ownership. `execution_knowledge` Views expand adopted content only. Domain Cursors select a published Revision while retaining the chosen Fence's control state. Knowledge Cursors select the exact adopted Observation prefix for a Key; packets never advance that revision. Cross-execution reuse requires explicit `knowledge.ImportCommand` and preserves origin/source provenance. Observations may explicitly carry a typed `memory` or `experience_path` logical ID; the core resolves it through the historical Memory projection and current model dispatch rejects revoked versions. `RecordedInput` binds a packet to exact previously published model request bytes, readable through bounded `ReadContent`; metadata-only legacy packets cannot authorize model activity. `SourcePublicationRef` resolves a producer transaction to its own fence/root and never substitutes latest. Durable ActionIntent reservations bind a request key, method, exact body and current authority before a new core commit; published retries remain receipt-first. `storage.Maintain` remains conservative quarantine cleanup; `CompactAuditFull` replaces a verified physical layout while retaining the complete audit log and reader-pinned generations.

Execution primary decisions require a schema-4 ContextPacket with `Requirements`. The engine derives structural obligations at the packet's historical source cut and verifies their exact adopted content. The host supplies the versioned renderer and semantic candidate policy. `EffectContract` declares receiver capabilities, while a finite `IdempotencyGuarantee` and single-sequence `RetryAuthorization` govern retry of unresolved idempotent effects. The trusted adapter supplies timing and terminal evidence and enforces the receiver deadline or epoch fence.

Protected root starts, child spawns, message consumption and primary decision acceptance use bounded Work. Every model or tool Activity dispatch uses `emission_dispatch`, which publishes the exact grant claim and Attempt dispatch under one receipt. When the Activity binds a ContextPacket, its grant selects that same packet and authenticates the entire recorded request. The adapter sends after publication and reconciles external outcomes separately.

## Layout

- `ate.go`: embedded facade and native-resource ownership.
- `model/`: public identity, anchor and RootSet value types.
- `api/`: public command, query, error, and capability contracts.
- `engine/`: bounded admission, one FIFO coordinator, private staging, durable receipts, state planning and semantic replay.
- `state/`: immutable path-compressed radix, reader-defined persistence.
- `reference/`: concurrent in-memory receipt oracle; no domain execution or durability.
- `codec/`: bounded canonical primitives.
- `storage/`: frame/metadata codecs, rooted file IO, owner lock, segmented P1 publisher and physical recovery.
- `cmd/ate/`: binary entrypoint.
- `protocol/`, `service/`, `client/`: bounded local IPC and connection-affine SDK.
- `graph/`, `graphstore/`, `journalstore/`: deterministic graph algorithms and durable semantic-record adapters.
- `agentprofile/`: static typed transitions, canonical current-V1 command/snapshot codecs and durable recovery state machine.
- `agentstore/`: typed Agent convenience facade over core LiveControl and original receipt fences.
- `tests/architecture/`: public package dependency and no-`internal` checks.
- `testdata/journal/`: independently generated golden transaction.
- `docs/`: the design, implementation, execution-profile, and API-protocol documents.

## Build

```sh
go test ./...
go test -race ./...
go vet ./...
go run ./cmd/ate version
go run ./cmd/ate help
# Verified state core; DIR already exists and is empty for init
go run ./cmd/ate init-state DIR
go run ./cmd/ate recover-state DIR
# Required when this store contains agent.apply/v1 transactions
go run ./cmd/ate recover-state --agent-profile DIR
# Rewrite a closed store's physical layout, retaining its full audit history
go run ./cmd/ate compact-state --agent-profile DIR
# Local service: RUN_DIR already exists and is private; ATE_AUTH_TOKEN is set
go run ./cmd/ate serve --agent-profile --socket /absolute/private-run/ate.sock --space agent DIR
# DIR must already exist and be empty for init-journal
go run ./cmd/ate init-journal DIR
go run ./cmd/ate recover-journal DIR
# Explicitly preserve and remove an unpublished tail
go run ./cmd/ate recover-journal --quarantine-tail DIR
```

The module targets Go 1.27.0. AuditFull maintenance details and limitations are
specified in the [compaction registry](formats/compaction-experimental-1.md).

The binary also supports `check-journal --previous-digest HEX [--previous-lsn N] FILE` for an experimental **raw frame stream**. It reads without modifying files and reports `publication_verified: false`. It is not a store verification or repair command and does not follow CURRENT/manifest.

Semantic recovery uses `recover-state`; `recover-journal` remains a physical-only tool and reports semantic verification as false. Neither command silently promotes a complete tail transaction.

Format registries: [V1 manifest](formats/ate-v1-manifest-2026-11-02.md) records the accepted compatibility boundary; the underlying experimental registries remain linked for byte-level details. Native IO uses an explicit file/directory sync profile; this development milestone has no device/power-loss certification.

The current V1 candidate uses a clean format cut (Control 5, Invocation 2, Continuation opcode 8, Work 1, Emission 3 and Packet 4). The historical audit and scope decision retain their original findings and evidence boundaries. Strict V1 acceptance remains open. [V1 manifest](formats/ate-v1-manifest-2026-11-02.md) · [Independent audit](validation/V1_INDEPENDENT_CURRENT_AUDIT_2026-11-03.md) · [Strict V1 scope](validation/STRICT_V1_SCOPE_DECISION_2026-11-03.md)

Earlier stage validation covers [M6 continuation](validation/M6_2026-11-01.md), [M5](validation/M5_2026-10-31.md), [M4](validation/M4_2026-10-31.md), [M3](validation/M3_2026-10-31.md), [M2b](validation/M2B_2026-10-31.md), [M2a](validation/M2A_2026-10-31.md), [M1f](validation/M1F_2026-10-31.md), [M1e](validation/M1E_2026-10-31.md), [M1d](validation/M1D_2026-10-31.md), [M1c](validation/M1C_2026-10-31.md), [M1b](validation/M1B_2026-10-30.md) and [M0 / M1a](validation/M0_M1A_2026-10-30.md).
