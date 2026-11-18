# TaskTrace metadata read costs

Measured on darwin/arm64, Apple M1, using three 100 ms samples per case:

```sh
go test ./engine -run '^$' \
  -bench 'BenchmarkTaskTrace(ScopeLookup|HistoricalChildren|SmallPage)$' \
  -benchmem -benchtime=100ms -count=3
```

The baseline is the working tree immediately before the scope reuse and
historical child-prefix changes. It includes the earlier TaskTrace graph and
pagination refactoring. Numbers below are medians; allocation byte counts are
rounded where noted.

| Operation                                   |       Before |    After | Allocated bytes before → after |
| ------------------------------------------- | -----------: | -------: | -----------------------------: |
| Scope membership, 1 key                     |     120.8 ns | 82.39 ns |                          0 → 0 |
| Scope membership, 128 keys                  |     4,975 ns | 84.33 ns |                     19,112 → 0 |
| Scope membership, 1,024 keys                |    41,680 ns | 87.14 ns |                    163,920 → 0 |
| Historical first 32 children out of 1,000   |     8,033 ns | 378.3 ns |                 98,304 → 3,200 |
| Historical first 32 children out of 100,000 | 1,142,472 ns | 417.7 ns |             ~9,601,000 → 3,200 |
| First 32 indexed events out of 1,000        |     5,253 ns | 5,253 ns |                13,824 → 13,824 |
| First 32 indexed events out of 100,000      |     5,538 ns | 5,469 ns |                13,824 → 13,824 |

## Changes and invariants

Scope membership previously constructed a selected-key map on every lookup
while holding the Engine mutex. The read request now constructs that immutable
map once and shares it across its graph traversal. The optimized membership
benchmark excludes this one-time construction; it measures repeated lookups.
The full space/principal/kind/ID key and per-call historical LSN check remain.

Child enumeration previously copied the entire current child list before
finding its historical boundary. It now locates the boundary under the mutex
and copies only the visible prefix. The returned slice still owns its storage.
Concurrent index appends cannot change returned entries or expose later LSNs.

Regression tests cover scope isolation, fence inclusion/exclusion, cancellation,
duplicate scope keys, caller key-slice mutation, empty historical prefixes,
owned result slices, and concurrent child appends across capacity growth.

## Limits of the measurements

These are synthetic in-memory metadata benchmarks. They exclude journal IO,
event decoding, authorization, dependency disclosure, and API serialization.
They establish reduced repeated allocation and copy cost, not an end-to-end
API latency guarantee.

The small-page case uses one stream without a milestone filter. It does not
measure sparse milestones, many overlapping streams, or deep continuation
positions. The page merge algorithm was not changed.

At this measurement stage, causal evidence recomputed the reachable-path
intersection before each offset page. The later bounded reuse described below
removes that repetition on cache hits; cold reads retain the traversal ceiling.
Scope memory is proportional to the request's key count; child-copy memory is
proportional to the visible prefix.

## Follow-up: causal evidence event loading

The next change removes event-body loading from the forward descendant walk.
That walk needs only the installed child index. The backward ancestor walk
still loads bodies to discover parents; all in-scope result nodes are explicitly
materialized before node projection and dependency disclosure. An external
source remains the responsibility of the boundary reader.

This also fixes an omitted source event: the backward walk deliberately stops
at `From`, so an in-scope `From` could previously be returned without its event
being cached. Its operation and payload dependencies are now available to the
existing node and disclosure logic.

```sh
go test ./engine -run '^$' -bench '^BenchmarkTaskTraceEvidenceBranches$' \
  -benchmem -benchtime=100ms -count=3
```

The synthetic graph contains `from -> middle -> to` and optional side branches
from `from` that cannot reach `to`. Medians from three samples on the same M1:

| Side branches |     Time before → after | Bytes/op before → after | Events loaded before → after |
| ------------- | ----------------------: | ----------------------: | ---------------------------: |
| 0             |     955.1 ns → 1,095 ns |             872 → 1,160 |                        2 → 3 |
| 2,048         | 614,630 ns → 259,308 ns |     ~910,421 → ~319,748 |                    2,050 → 3 |

The extra load in the zero-branch case is the required source event. This
benchmark uses in-memory event objects and a request cache cleared between
iterations; it does not model disk or canonical event decoding costs.

Traversal sets, child-index enumeration, sorting, and offset pagination remain.
Side branches still consume metadata traversal work and count against the
existing ceiling. Ancestors of `to` may still require event bodies even if
they ultimately do not intersect `from`'s descendants. Unrelated descendant
bodies are no longer read or validated by this evidence request.

Added regressions verify skipped side bodies, source projection and payload
disclosure dependencies, multiple causal paths, external boundary sources,
unreachable destinations, relevant body/metadata errors, cancellation, and
the traversal ceiling. Validation passed:

```sh
go test ./...
go test -race ./engine ./tests/host -run 'Test.*(TaskTrace|Task)' -count=1
go vet ./...
git diff --check
```

## Long multi-Agent histories: shared ancestry and repeated pages

Consider several Agents working from the same request and exchanging results
over thousands of recorded steps. An overview should reuse their shared history;
reading the next evidence page should reuse the already established path while
still honoring a source withdrawn since the previous page.

Overview now discovers the ancestor DAG once per request, stopping at explicit
or built-in milestones and foreign scope boundaries. A topological pass
propagates two 64-bit page-membership masks through that graph. Work is bounded
by inspected links (65,536), page nodes (128) and output edges (512), rather than
charging the same ancestor chain separately for every page node. Cycles fail
closed. A regression with 128 targets and 1,024 shared intermediate ancestors
performs 1,025 metadata lookups and preserves every disclosure dependency.
Thirty deterministic DAG cases compare edges and dependencies against an
independent per-target traversal, including joins and stopping points.

Causal evidence uses a disposable per-Engine cache of complete sorted path IDs
and deduplicated historical dependency IDs. Its key binds anchor, principal,
subject, session, endpoints, historical root set and trace-edit record. Page
size remains bound to the public continuation token, but does not duplicate a
cached path. Values are immutable and shared; a hit does not clone the full
path. The retained cache is limited to 32 entries and a conservative 16 MiB
accounting budget, using least-recently-used eviction. Active readers can retain
an evicted value until their request ends. Concurrent cold requests may compute
the same path independently; cache locks never enclose traversal or IO.

The cache holds no contexts, readers, pins, event bodies or authorization
results. Each request resolves its historical scope, loads current-page events,
and re-expands/checks the complete dependency set against the current read cut.
Privacy erasure or ACL revocation of a terminal validation outside the current
page still rejects delivery. Shutdown and writer/read fencing stop new cache
insertions. Compaction preserves these immutable logical IDs; reopen starts cold.
Eviction, oversized entries and restart fall back to computation with the same
tokens. Cross-scope source nodes retain their boundary projection on cold and
cached pages, including after compaction and reopen.

Commands, run on darwin/arm64 Apple M1, three 200 ms samples:

```sh
go test ./engine -run '^$' \
  -bench 'Benchmark(TaskTraceOverviewSharedAncestors|TaskTraceEvidenceRepeatedPages|AuditEventInstallation)$' \
  -benchmem -benchtime=200ms -count=3
```

Medians:

| Scenario                                               |      Time/op |  Bytes/op | Events materialized/op |
| ------------------------------------------------------ | -----------: | --------: | ---------------------: |
| Overview: 32 targets, 1,024 shared ancestors           |   408,640 ns |   756,198 |                      — |
| Overview: 128 targets, 1,024 shared ancestors          |   430,338 ns |   808,500 |                      — |
| Evidence: 16 chains, 2,050 events, recompute each page | 3,148,378 ns | 2,560,104 |                  2,050 |
| Same evidence: cached path, pages of 32                |     7,100 ns |    10,164 |                  31.54 |

Evidence measurements include path preparation/cache lookup and page event
materialization in an in-memory fixture. The recompute case clears the cache
each iteration; it is a cold-path comparison, not a historical implementation
benchmark. The last page is partial, hence the average below 32 events.
Measurements exclude historical ledger loading, journal IO, current policy
closure/checks, serialization and transport. In particular, each real page
still checks full-path dependencies: API cost is not constant in path length.
The first page still computes the complete bounded intersection; this is
cross-page reuse, not an incremental first-page traversal protocol.

## Audit verifier and batch staging

Audit compaction still replays the canonical history and compares roots,
publication and object counts. Its private verifier now omits the four
TaskTrace query indexes. Batch staging omits them too. Both retain semantic
events, stream heads, aliases, generic event metadata and historical proofs.
The live publication owner and normal reopen continue building all indexes.

| Synthetic event installation      | Indexed time/op | Audit time/op | Indexed bytes/op | Audit bytes/op |
| --------------------------------- | --------------: | ------------: | ---------------: | -------------: |
| 256 transactions / 512 events     |      363,593 ns |    176,411 ns |          928,153 |        389,299 |
| 4,096 transactions / 8,192 events |    7,085,608 ns |  3,178,115 ns |       15,836,489 |      6,227,310 |

Each operation constructs an Engine and installs prebuilt core/profile events
across 16 causal chains. Allocation reductions are about 58% and 61% in this
event-installation component. These are allocated bytes, not measured peak RSS,
and the numbers do not include command validation, semantic replay or physical
compaction IO. A real-journal regression separately verifies identical semantic
state before/after compaction, no query-index allocation in the verifier, index
reconstruction on reopen and unchanged receipts.

Validation includes:

```sh
go test ./...
go test -race ./engine ./tests/host \
  -run 'Test.*(TaskTrace|TraceEvidenceCache|AuditVerifier|AuditStaging|AuditInstall|LongTask)' -count=1
go test -race ./engine -run '^TestTaskTraceCacheIntegrationExternalSourceBoundary$' -count=1
go vet ./...
git diff --check
```
