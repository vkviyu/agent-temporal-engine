# Agent state growth and snapshot reuse

> Historical pre-record baseline. The body below preserves the snapshot-era
> implementation, limits and measurements; its “current” statements and remaining
> storage work describe that earlier tree. Current preparation uses bounded
> record roots and does not retain the snapshot representation. See
> [Agent record storage and checkpoint measurements](AGENT_RECORD_STORAGE_2026-11-14.md)
> and the latest [preparation follow-up](AGENT_RECORD_STORAGE_PREPARATION_2026-11-15.md).
> The follow-up measured full-state record preparation before the legacy control
> was removed. Full-state CPU costs remain; those short preparation samples
> exclude publication/fsync, measure cumulative allocation rather than peak RSS,
> and do not rerun or revise checkpoint measurements.

Long multi-Agent tasks accumulate execution state under a Space/Principal
partition. A small new command can therefore process megabytes of existing
history. The record format removes repeated durable payload writes while
retaining full semantic validation. It does not remove the partition's finite
capacity.

## Behavior and compatibility

`agentprofile.RecordImage` owns validated records and normalized state. Record
objects undergo complete schema authentication and cross-record validation.
Restoring an image creates an independent Ledger and rebuilds derived slots.
Returned records and snapshots are owned copies.

The Engine loader authenticates a profile once instead of decoding it twice.
Agent preparation frames the command-produced canonical bytes without taking
another full snapshot and encoding it again.

Each Work preparation has a private single-slot record catalog cache. Every load first
resolves the requested root's profile object ID and matches space and owner.
Only the latest catalog is retained, and each use restores an independent Ledger.
An unabsorbed candidate cannot replace the state identified by the current root.
No intermediate persistent record objects are removed.

Tests compare cached and uncached Work preparation by ordered object IDs, kinds
and encoded bytes, transactions, roots, receipts and event projections. They
include interleaved knowledge writes, recorded-operations replay, historical
roots, canceled/rejected candidates and retry isolation. Separate tests compare
image framing against independently assembled legacy framing and reject
noncanonical or mismatched stored input.

TaskTrace caches a minimal immutable profile projection keyed by space,
principal and profile object ID. It keeps scope relationships and disclosure
dependency IDs, not payloads, Ledgers, readers, pins or authorization results.
Historical roots are rebound for each request. The cache has independent limits
of 32 entries and a conservative 16 MiB accounting budget; active requests may
retain evicted entries until completion. Oversized projections remain readable
without retention. Concurrent cold requests may build duplicate projections.

Current authorization remains per delivery. Public-API regression cases cover
privacy erasure and ACL revocation after warming Activity, Attempt,
ActivityAcceptance, ActivityResult and QueryAnswer projections, then repeat after
compaction and reopen. They include transitive packet sources, final-answer
witnesses and tool definitions. Denial returns an entirely empty result.
QueryAnswer outcome dependencies and its input evidence are not independently
isolated from all other references in these fixtures; projection unit tests
also check retained fields.

## Computation measurements

Measured on darwin/arm64, Apple M1. Times below are medians of three samples.
Allocated bytes are cumulative bytes per operation, not peak resident memory.

The profile fixture contains task/execution/step/invocation groups, 2 KiB
invocation arguments/results and accepted model-result provenance.
`ApplyCommand` before/after measurements use one-second samples:

| Groups | Canonical snapshot bytes |     Before |      After | Allocated MB before → after |
| ------ | -----------------------: | ---------: | ---------: | --------------------------: |
| 16     |                   83,745 |   1.740 ms |   1.259 ms |               3.630 → 2.489 |
| 256    |                1,336,789 |  31.122 ms |  22.717 ms |             66.984 → 45.112 |
| 1,024  |                5,346,518 | 186.491 ms | 130.944 ms |           273.955 → 184.305 |

Full raw `RestoreSnapshot` has no demonstrated improvement: at 1,024 groups its
median changed from 126.782 ms to 133.792 ms, with essentially unchanged
allocation. Authentication still processes the full snapshot.

Current-tree component comparisons at 1,024 groups:

| Operation                                 | Repeated processing | Image path |
| ----------------------------------------- | ------------------: | ---------: |
| Decode plus restore, modeling loader work |          256.384 ms | 132.625 ms |
| Command plus another snapshot encode      |          193.646 ms | 134.028 ms |

Restoring an already authenticated image costs 3.012 ms in this fixture;
copying its canonical bytes costs 0.223 ms. These measurements exclude Engine
root lookup, object framing, IO, publication and authorization.

Work preparation uses four sequential Agent reductions, optionally with an
interleaved knowledge reduction. Each iteration starts with a fresh cache.
Three 300 ms samples:

| History groups | Work              | Without reuse | With reuse |
| -------------- | ----------------- | ------------: | ---------: |
| 16             | Agent             |       6.48 ms |    3.14 ms |
| 16             | Agent + knowledge |       7.67 ms |    4.31 ms |
| 128            | Agent             |      35.50 ms |   15.37 ms |
| 128            | Agent + knowledge |      42.52 ms |   21.39 ms |

Allocated bytes fall approximately 44–53%. The fixture includes boundary
validation, reduction, object absorption and final transaction/receipt
construction. It excludes fixture setup, admission, journal publication/fsync
and event generation. Event-enabled equivalence is tested separately.

For TaskTrace, a fixture with 1,024 tasks/queries and 4,096 executions measures
the following components:

| Operation                                                | Task scope | Query scope |
| -------------------------------------------------------- | ---------: | ----------: |
| Raw restore, snapshot, scope and provenance              |   17.91 ms |    16.88 ms |
| Projection construction from an already decoded snapshot |    1.49 ms |     1.51 ms |
| Cached scope and provenance lookup                       |     593 ns |     88.5 ns |

Projection construction excludes raw decoding, so its row is not total cold
request cost. The warm row excludes historical root authentication, event
loading, policy checks and delivery. Actual reader-count tests establish one
profile read on a miss, none on a hit or unrelated root update, and a new read
after a profile change.

## Capacity and recovery

| Layer                                                       | Limit          | Over-limit behavior                                |
| ----------------------------------------------------------- | -------------- | -------------------------------------------------- |
| Core snapshot object payload, including envelope            | 8 MiB          | `storage.ErrLimit`; Engine returns `ResourceLimit` |
| Complete standalone profile snapshot                        | 32 MiB         | `agentprofile.ErrInvalid`                          |
| Individual encoded byte string, including lifecycle section | 16 MiB         | `codec.ErrLengthOverflow`                          |
| Each bounded record collection                              | 65,536 records | `agentprofile.ErrInvalid`                          |

These limits compose: the first encountered limit wins. Engine capacity is less
than 8 MiB of canonical profile bytes because the owner and envelope occupy
space. Serialized lifecycle byte fields also include JSON/base64 overhead.
Counts apply per collection, not as one aggregate record budget.

The encoder now checks the final aggregate size after lifecycle data and all
section counts. Previously it could return a snapshot exceeding its own 32 MiB
decoder limit. Tests exercise limit minus one, exact limit and limit plus one,
and prove command rejection leaves ledger bytes and derived slots unchanged.

Engine tests independently exercise 8,388,607, 8,388,608 and 8,388,609-byte
core payloads. A valid sub-KiB command that grows a near-limit profile beyond
capacity returns `ResourceLimit` without a receipt, consumed LSN, changed roots,
installed events, journal writes or fencing. A subsequent smaller write works.

A native-directory integration fixture uses one task, one execution, one step,
12 approximately 512 KiB query inputs and 16 committed transactions. It verifies
exact profile bytes, historical reads, receipts, roots, fences and events before
and after reopen and AuditFull compaction. One isolated run observed:

| Measurement              |           Result |
| ------------------------ | ---------------: |
| Final canonical profile  |  8,388,196 bytes |
| Final core payload       |  8,388,217 bytes |
| Published journal        | 69,301,376 bytes |
| Reopen before compaction |           1.19 s |
| AuditFull compaction     |           1.50 s |
| Compaction bytes copied  | 69,301,280 bytes |
| Reopen after compaction  |           1.30 s |

These are single-run integration observations with OS caches uncontrolled,
not high-record-count throughput or cold-disk latency claims.

## Remaining storage work

Full snapshots still grow with retained records in a partition. Each successful
command still serializes the full new state, and Work retains its intermediate
persistent objects for replay compatibility. AuditFull preserves canonical
history; it neither prunes profile records nor releases profile capacity.

The near-limit fixture demonstrates that compaction is not a capacity remedy.
Supporting substantially longer histories in one partition requires a separate,
versioned storage design: bounded record/chunk objects with a persistent indexed
root, plus an explicit checkpoint/replay strategy. That design must specify old
journal recovery, historical anchors, disclosure dependencies and transaction
versioning before changing emitted bytes. This patch intentionally leaves that
format decision open; it establishes the baseline and removes redundant work.

## Reproduction and validation

```sh
go test ./agentprofile -run '^$' \
  -bench 'BenchmarkSnapshot(Dedup|Image)$' -benchmem -benchtime=1s -count=3
go test ./engine -run '^$' \
  -bench '^BenchmarkWorkAgentImagePreparation$' -benchmem -benchtime=300ms -count=3
go test ./engine -run '^$' \
  -bench '^BenchmarkTaskTraceProfile$' -benchmem -benchtime=200ms -count=3
go test ./engine -run '^TestAgentRecord' -count=1 -v
go test ./...
go test -race ./agentprofile ./engine ./tests/host
go test -race ./engine -run '^TestTaskTrace(Profile|CacheIntegration|HistoricalEvidence)' -count=1
go vet ./...
git diff --check
```

The before profile timings were collected before the image implementation;
the current tree reproduces the after timings and side-by-side component/Work
comparisons. Full-suite, race and static checks passed; the subsequently added
disclosure matrix passed its targeted normal and race runs.
