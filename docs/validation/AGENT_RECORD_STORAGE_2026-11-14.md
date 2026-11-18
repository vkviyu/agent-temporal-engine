# Agent record persistence and authenticated checkpoint measurements

Measured on 2026-11-15 against the current ATE working tree, based on commit
`1161b28`. The report name follows the requested validation date. This experiment
adds only the benchmark/test file and this report; it changes no production code.
Host: Apple M1, darwin/arm64, Go 1.27.0. Native storage uses `testing.TB.TempDir`
and `storage.OpenDirectory`, whose durability profile is `FileSyncExperimental`.

Record persistence substantially reduces the journal bytes for these updates.
It still prepares the complete in-memory Agent state. Reduced durable bytes do
not establish reduced CPU time, allocation, or resident memory. **Record
preparation was slower in every measured case (1.84–2.86x)**. Authenticated
checkpoint reopen was faster for these histories with all reconciliation costs
included.

Preparation was subsequently optimized without changing persisted bytes; see
[the follow-up measurements](AGENT_RECORD_STORAGE_PREPARATION_2026-11-15.md).
The original measurements below remain a baseline, including their CPU costs.

## Scope and reproduction

The original implementation benchmark was a one-off working-tree experiment
and has since been removed with the legacy comparison path. The byte and
checkpoint figures below remain historical measurements; they are not a
currently runnable compatibility benchmark.

Run the integration measurements and the bounded benchmarks separately, without
another test/benchmark process competing for CPU:

```sh
go -C /Users/zhaozhiqiang/work/agent-temporal-engine test ./agentprofile ./engine -run 'TestAgentRecord|TestWorkAgentRecord' -count=1
```

The integration test collects three full and three checkpoint-assisted reopen
samples per history size, alternating order. Each size reuses exactly the same
published directory, checkpoint, eight-transaction suffix, publication and
semantic state. It does not rebuild the history between modes or recertify the
checkpoint between samples. The Go benchmarks perform three measured iterations
per case, plus the framework's one-iteration calibration. Benchmark `ns/op` is
an arithmetic mean; the integration test separately prints the three-sample
median. There is no adaptive seconds-long benchmark loop.

OS caches are uncontrolled. “Full” means `DisableRecoveryCheckpoints: true`,
not cold disk; “warm” means consuming the authenticated recovery checkpoint,
not reusing an open Engine. Store/transaction identities are generated normally.
Their effect on content-addressed radix topology can change native byte totals
slightly between independent executions. The component fixture has fixed
identities and deterministic byte totals.

## Persistence comparison

The component cases reuse `workImageFixture` with 16 or 128 groups. Each group
has a task, execution, open step, and query with 768 input bytes and 896 answer
bytes. These are fully authenticated state fixtures seeded in a private index,
not publicly published histories. The native history below uses admissible open
queries with empty answers.

The small update rebases one task's goal. Work contains four sequential Agent
reductions: grant authority and checkpoint an action/continuation for each of
two executions. Events are enabled. The record case starts from the equivalent
record representation, so migration/setup cost is excluded from steady updates.
Both paths retain full state loading, validation, reduction, encoding, radix
updates, intermediate Work objects, event construction and final receipts.

The historical comparison path used a monolithic snapshot oracle. That oracle
is no longer part of the implementation or test suite. The record figures are
kept to document the format decision, not to promise runtime support for the
removed representation.

| Groups | Operation         | Initial legacy object, B | Legacy framed transaction, B | Record framed transaction, B | Legacy / record object count |
| -----: | ----------------- | -----------------------: | ---------------------------: | ---------------------------: | ---------------------------: |
|     16 | Small goal update |                   43,638 |                       47,120 |                        7,744 |                       9 / 12 |
|     16 | Four-step Work    |                   43,638 |                      194,912 |                       36,704 |                      28 / 52 |
|    128 | Small goal update |                  348,281 |                      351,760 |                       19,688 |                       9 / 15 |
|    128 | Four-step Work    |                  348,281 |                    1,413,472 |                       83,128 |                      28 / 62 |

“Framed” is the length returned by the real `storage.EncodeTransaction`, including
all object/event records, framing, metadata and receipt/commit frames. These
component transactions are not fsynced. More small objects can be written even
when the total byte count falls.

The native integration fixture also measures actual `PublishedEnd` growth across
public `Engine.Commit` calls, including admission, preparation and durable ACK.
It asserts that growth equals the corresponding framed modern plan. The legacy
reference uses the same logical pre-state privately converted to a monolithic
snapshot and fully prepared/encoded; it is not a second native legacy store.
All byte figures exclude manifest/CURRENT rewrites, checkpoint files, filesystem
metadata and device write amplification.

|       Native run | Operation         | Actual record journal growth, B | Legacy framed reference, B | Public commit wall time |
| ---------------: | ----------------- | ------------------------------: | -------------------------: | ----------------------: |
| 128 transactions | Small goal update |                           9,360 |                     31,920 |               26.256 ms |
| 128 transactions | Four-step Work    |                          38,968 |                    119,304 |               27.232 ms |
| 256 transactions | Small goal update |                           9,064 |                     31,920 |               25.958 ms |
| 256 transactions | Four-step Work    |                          38,848 |                    119,304 |               27.894 ms |

Both writes occur near the beginning of each run: Work at LSN 66 and the goal
update at LSN 67. The two rows per operation are independent stores with the same
logical workload, not updates made against 128/256-transaction final states.
The commit durations are single observations, include fsync/publication, and have
no measured legacy public-latency comparator. They are not throughput medians.

## Native history and reopen method

Each history has 16 distinct worker actors, 16 tasks, 16 executions and 16 open
steps in one host-owned Space/Principal profile partition. Setup publishes a
source value and four public commands per worker, then the four-step Work as one
transaction. A small goal update follows; new recorded queries grow the history
to 128 or 256 transactions. Final state includes two pending actions, two
continuations, two authority records and their recovery action intents. All
commands use real published input fences and ordinary validation constraints.

Checkpoint certification runs at transaction 120 or 248, followed by eight
ordinary query transactions. Certification performs full physical and semantic
verification; it is reported separately and is not charged to reopen latency.
The fixture retains its canonical audit history and does not compact it.

The measured reopen operation is the sum of the actual `OpenExisting` and
`Close` durations. Correctness reads/assertions between those calls are excluded.
The benchmark uses timer stop/start for the same boundary. `OpenExisting` includes
checkpoint decoding/authentication, **projection reconciliation against the
certified immutable transaction stream**, the ordinary complete physical scan,
suffix semantic replay, query-index reconstruction, publication-witness sync
and coordinator startup. `Close` retires the coordinator, journal and owner lock.
No decoder-only or reconciliation-free timing is presented as reopen latency.

`CheckpointTransactions` counts certified reducer transitions skipped.
`ScannedTransactions` counts the main physical audit scan and remains the full
history length in both modes. The checkpoint path additionally reconciles its
prefix; the reported physical count is not a count of all internal scan passes.

Every reopen compares the entire encoded generic checkpoint projection against
the published baseline, including roots, history, object locations, envelopes,
events, heads and receipt hints. Public reads also compare current/historical
Agent state, TaskTrace output and every request receipt. A returned query payload
is mutated and reread to verify ownership. Each integration iteration explicitly
reacquires/releases the native owner lock after closing the Engine.

Setup engines close before measurement. Every measured engine closes in its
iteration, including assertion-failure cleanup; directory handles close after
all engines. Benchmarks retain no plans/engines in global sinks or per-iteration
cleanup lists.

## Reopen results

These are the successful final integration command's observations, rounded only
for display. Each row used three samples on one unchanged published history.

| Transactions | Final queries | Published journal, B | Indexed objects | Checkpoint certification |
| -----------: | ------------: | -------------------: | --------------: | -----------------------: |
|          128 |            77 |            1,408,480 |           2,047 |               697.995 ms |
|          256 |           205 |            3,661,744 |           4,171 |             3,545.301 ms |

| Transactions | Mode       | Open + close samples, ms, in collection order |    Median, ms | CheckpointTransactions | ScannedTransactions |
| -----------: | ---------- | --------------------------------------------- | ------------: | ---------------------: | ------------------: |
|          128 | Full       | 792.857, 758.110, 777.713                     |   **777.713** |                      0 |                 128 |
|          128 | Checkpoint | 151.387, 146.035, 150.012                     |   **150.012** |                    120 |                 128 |
|          256 | Full       | 3,949.317, 3,889.516, 3,981.494               | **3,949.317** |                      0 |                 256 |
|          256 | Checkpoint | 368.421, 325.977, 342.556                     |   **342.556** |                    248 |                 256 |

The measured full/warm median ratios are 5.184x and 11.529x. The larger case also
has more query records per replayed state; this is not evidence of a universal
speedup factor or a constant-time checkpoint open. Both paths still read the
physical history, checkpoint reconciliation has its own cost, and certification
itself costs approximately a full replay in this experiment.

## CPU and allocation results

The bounded benchmark command passed in 40.967 seconds. Go reported eight
available benchmark CPUs (`-8`). These are three-iteration arithmetic means,
not statistical confidence intervals or medians of multiple benchmark runs.
Small differences should not be interpreted as stable regressions/improvements.

Preparation measures complete private `prepareAgent` / `prepareWork` with
events enabled. It excludes public admission, transaction framing, publication
and fsync; the separately reported framed bytes are computed outside its timer.
Work always starts at the same root with a fresh per-Work image slot.

| Groups | Operation         | Legacy, ms/op | Record, ms/op | Legacy allocated B/op | Record allocated B/op | Legacy / record allocations/op |
| -----: | ----------------- | ------------: | ------------: | --------------------: | --------------------: | -----------------------------: |
|     16 | Small goal update |         2.229 |         4.977 |             3,214,208 |             4,527,925 |                 5,880 / 36,992 |
|     16 | Four-step Work    |         3.257 |         5.984 |             8,743,048 |             7,908,245 |                15,943 / 56,322 |
|    128 | Small goal update |         9.110 |        26.039 |            31,799,384 |            35,962,128 |               39,972 / 289,040 |
|    128 | Four-step Work    |        17.870 |        41.156 |            67,227,634 |            55,329,912 |               80,692 / 361,983 |

Record state memory preparation remains full: loading authenticates the record
set, restore reconstructs an independent Ledger, command application builds the
next full image, and persistence compares record keys against the existing
radix. The Work image slot avoids some repeated loads but does not turn the
reducer into a changed-record-only computation. This is a durable-byte reduction,
not a demonstrated CPU optimization. Work allocates fewer cumulative bytes in
these cases, while allocation counts rise substantially.

The allocation benchmark also measures complete `OpenExisting` plus `Close`.
Assertions/public comparison reads are outside the timer and allocation window,
as is fixture construction/certification. These histories are new native stores
from the benchmark invocation; each size still shares one store between modes.

| Transactions | Mode       | Mean, ms/op | Allocated B/op | Allocations/op | Checkpoint / scanned transactions |
| -----------: | ---------- | ----------: | -------------: | -------------: | --------------------------------: |
|          128 | Full       |     820.451 |    865,011,381 |      8,171,484 |                           0 / 128 |
|          128 | Checkpoint |     156.197 |    148,486,141 |      1,245,266 |                         120 / 128 |
|          256 | Full       |   3,992.704 |  4,415,887,328 |     34,776,326 |                           0 / 256 |
|          256 | Checkpoint |     332.051 |    356,561,706 |      2,681,746 |                         248 / 256 |

**Allocated B/op is cumulative Go allocation, not retained heap or peak RSS.**
No peak RSS measurement or claim is made. Checkpoint reopen still allocates a
substantial amount of memory and performs work proportional to retained history.

## Final validation

All checks passed:

- New persistence reference and native integration tests: 34.533 seconds.
- Both benchmark families, three measured iterations each: 40.967 seconds.
- Repository-wide tests: pass, including Engine (178.211 seconds), storage,
  Agent profile, acceptance, architecture and host tests. Only the already
  collected `TestStateEvolutionCheckpointMeasurements` was skipped in this
  broader invocation to avoid another set of expensive timing samples.
- Targeted race checks: pass, 71.972 seconds. These cover both persistence sizes,
  the 128-transaction native reopen history, independent Work restores and
  failure isolation. Race-instrumented timings are not used in the result tables.
- `go vet ./...`: pass.
- Whitespace checks for the working diff and both new untracked files: pass.

Exact additional validation commands:

```sh
go -C /Users/zhaozhiqiang/work/agent-temporal-engine test ./... -count=1
go -C /Users/zhaozhiqiang/work/agent-temporal-engine test -race ./engine -run 'TestWorkAgent(Image|Record)' -count=1
go -C /Users/zhaozhiqiang/work/agent-temporal-engine vet ./...
git -C /Users/zhaozhiqiang/work/agent-temporal-engine diff --check
git diff --no-index --check /dev/null /Users/zhaozhiqiang/work/agent-temporal-engine/docs/validation/AGENT_RECORD_STORAGE_2026-11-14.md
```

For the two `--no-index` checks, status 1 with no diagnostics indicates that the
new file differs from the empty input; neither produced a whitespace diagnostic.
The validation runs finished before handoff. No test/benchmark Engine, owner
lock, directory handle or background command is intentionally left running.
