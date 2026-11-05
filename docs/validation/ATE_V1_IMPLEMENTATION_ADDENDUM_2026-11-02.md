# ATE V1 implementation addendum — 2026-11-02

This addendum records work completed after the original static audit. The
original matrix remains intact as an audit baseline; this file is the
implementation evidence for the current tree. It is superseded for the final
strict-compatibility verdict by
`V1_DOCUMENT_CODE_CROSS_AUDIT_2026-11-02.md`.

Implemented mechanisms (closure of the full requirement is governed by the
cross-audit, rather than by this mechanism list):

- **G-02:** deterministic bounded Context selector, Packet schema 3, Summary
  provenance/codec/source-cut validation, typed Summary observations and
  historical NodeContext projections.
- **G-03:** persistent Emission grant issue/claim/revoke with immutable content
  range and destination, authority/goal/current-policy rechecks, historical
  reads and reopen/compaction coverage. Revocation prevents new claims; it
  cannot retract an external byte already delivered by a trusted adapter.
- **G-07:** current reuse and historical disclosure gates now traverse explicit
  Summary dependencies as well as Memory/ExperiencePath provenance. Factual
  correction and UseBan preserve audit bytes; privacy/ACL withdrawal denies
  disclosure. Arbitrary copied bytes and generic low-level diagnostics remain
  outside provenance-based detection.
- **G-04:** Query intake is a durable Work unit before Task creation. It stores
  route, configuration, exact next request and request reservation. A different
  RequestKey cannot start the same query.
- **G-05/G-06:** bounded Work units execute registered agent and knowledge
  reducers in one private candidate and publish one receipt. Model decision,
  message consumption, knowledge adoption, continuation, child seed and child
  authority are tested for all-or-nothing behavior and reopen recovery.
- **G-08:** a small independent map oracle compares Spawn/Cancel and Emission
  Issue/Claim/Revoke outcomes; existing independent radix/graph oracles remain.
  This is not yet a complete Profile reference model.
- **G-11:** typed ValidationRequirement/Envelope is bound to the current goal,
  artifact, command, environment, tool and policy. Wrong or stale evidence is
  rejected; legacy opaque evidence remains replay-only.
- **G-12:** NodeContext supports recorded input, known-at-anchor,
  evidence-neighborhood and retrospective modes with bounded sections,
  explicit anchors, state diffs, recovery receipts and gaps.

Evidence commands for the new work are:

```text
GOTOOLCHAIN=local GOCACHE=/tmp/ate-go-build go test ./engine -run 'TestWork|TestEmission|TestSummary|TestTaskCompletion|TestTypedNodeContext|TestIndependentControlOracle' -count=1
GOTOOLCHAIN=local GOCACHE=/tmp/ate-go-build go test ./... -count=1
GOTOOLCHAIN=local GOCACHE=/tmp/ate-go-build go test -race ./... -count=1
GOTOOLCHAIN=local GOCACHE=/tmp/ate-go-build go vet ./...
GOTOOLCHAIN=local GOCACHE=/tmp/ate-go-build go build ./...
```

The deterministic host scenarios remain mechanism/effect evidence, not a
claim about paid-provider success rates or cross-project transfer. Real-model
quality and native Linux/power-loss certification require external test
resources and remain explicitly outside what this repository can prove.

The final cross-audit found additional strict-compatibility gaps: complete
Invocation/DecisionRef binding, Continuation adapter/knowledge-position fields,
Emission dependency/policy binding, Profile Event attribution, and embedded to
Unix SDK conformance for the new Work/Summary/Emission APIs. Those findings are
recorded in the cross-audit report and prevent declaring all four documents
strictly satisfied until resolved or explicitly moved to a later version.

Follow-up fixes now include Invocation schema 2/Control schema 4 contracts,
Query-only DecisionSequence recovery, typed Continuation positions, Profile
Event schema 2 attribution, Emission schema 2/3 policy/dependency proofs, a
Work-bound exact dispatch grant, cross-Space read-only transfer archives,
independent profile/reference models, a frozen V1 format registry, and real
embedded/Unix SDK conformance. The Work pending-tool completion contradiction
and Emission response-validation gap were also fixed. The cross-audit records
which of these are main-path closure and which retain explicit compatibility,
external executor, semantic subject mapping, exhaustive reference-model, or model-effect limits.
