# Agent record preparation: redundant authentication and copying

Measured on 2026-11-15 in the ATE working tree based on commit `1161b28`.
Host: Apple M1, darwin/arm64, Go 1.27.0; benchmark CPU suffix `-8`.

Removing redundant authentication and payload copies reduced median record
preparation time by **19–33%** in these four historical cases. The comparison
control and benchmark have since been removed with the legacy representation.
Allocated bytes fell by 24–36%, and allocation counts by 25–49%. This does not
make preparation proportional to the changed records.

The existing report at
[AGENT_RECORD_STORAGE_2026-11-14.md](AGENT_RECORD_STORAGE_2026-11-14.md)
retains its original measurements. This follow-up measures preparation only;
it does not rerun or revise checkpoint measurements.

## Changes and trust boundaries

Implementation is confined to
[record_image.go](../../agentprofile/record_image.go)
and
[agent_record_storage.go](../../engine/agent_record_storage.go).

- Full image loading checks the child object envelope, scope, references and
  byte totals, then authenticates record schemas once through
  `DecodeRecordImage`. Previously `readAgentRecord` authenticated every child
  first, and `DecodeRecordImage` repeated that work. Standalone
  `readAgentRecord` / registry validation still authenticates the child.
- `EncodeRecordImage` uses a private generated-data path. It retains its freshly
  allocated, bounded, sorted and hashed record buffers without hashing and
  copying them again. Typed decoding, normalization, binary child decoder
  checks, allocation preflight, complete cross-record validation and sorting
  remain. The closed JSON schema is guarded against custom codecs.
- For freshly marshaled JSON only, a byte scan enforces the same 65,536-token
  and depth-32 budgets without allocating `Decoder.Token` values. Typed JSON
  decoding still creates owned state. The redundant canonical re-encode is
  omitted, except for replacement-character escape cases where the exact
  comparison is retained. Public input still uses the original token scanner,
  unknown-field rejection and exact canonical re-encode.
- `RecordImage.RecordKeys()` copies keys without copying payloads.
  `RecordImage.RecordAt(index)` returns an owned record. Storage copies a
  payload only after its key misses the old content index. Both accessors
  return owned memory; no callback exposes the private image buffers.

No persistent format, canonical framing, resource cap or mutable Ledger sharing
was changed in the measured implementation. No dirty-record reducer was
introduced.

## Reproduction and interpretation

The following command was used for the historical comparison, with three
samples and five measured iterations per sample:

```sh
go -C /Users/zhaozhiqiang/work/agent-temporal-engine test ./agentprofile ./engine -run 'TestAgentRecord|TestWorkAgentRecord' -count=1
```

The old benchmark file has been removed. The figures below are historical
measurements of full private preparation and are not currently reproducible as
a legacy-versus-record comparison.

Each group has a task, execution, open step and a query with 768 input bytes
and 896 answer bytes. Initial legacy objects are 43,638 B for 16 groups and
348,281 B for 128 groups. The record cases start from equivalent record roots.

The former “Legacy” column measured the image-aware preparation path that
preceded the record-only cutover. That path and its frozen pre-record oracle
are historical measurements, not current ATE behavior.

Each reported sample is Go's arithmetic mean of five iterations. Tables use
the median of three samples, independently for each metric. These short runs
do not establish confidence intervals. No other test or benchmark was launched
by this agent during either benchmark. Host scheduling, caches and GC remain
uncontrolled. The after-run 128-group historical-control small-update sample of
37.137 ms is an outlier and is retained below; its median is 9.599 ms.
Historical control medians changed between runs despite unchanged control code.

## Preparation results

All times are milliseconds per operation.

| Groups | Operation      | Record before median | Record after median | Time reduction | Legacy after median | Record / legacy after |
| -----: | -------------- | -------------------: | ------------------: | -------------: | ------------------: | --------------------: |
|     16 | Small update   |                3.183 |               2.180 |          31.5% |               1.757 |                 1.24x |
|     16 | Four-step Work |                5.885 |               4.751 |          19.3% |               2.916 |                 1.63x |
|    128 | Small update   |               26.194 |              17.642 |          32.6% |               9.599 |                 1.84x |
|    128 | Four-step Work |               41.266 |              32.120 |          22.2% |              17.972 |                 1.79x |

All time samples, in collection order:

| Groups | Operation    | Format | Before samples, ms/op           | After samples, ms/op            |
| -----: | ------------ | ------ | ------------------------------- | ------------------------------- |
|     16 | Small update | Legacy | 1.776683, 1.655917, 1.381658    | 2.082642, 1.756575, 1.466450    |
|     16 | Small update | Record | 3.282200, 3.183392, 3.059683    | 2.287208, 2.114400, 2.180425    |
|     16 | Work         | Legacy | 3.146825, 2.847500, 3.058650    | 2.928067, 2.916167, 2.871558    |
|     16 | Work         | Record | 5.885225, 5.844525, 6.024833    | 4.751383, 4.830108, 4.700167    |
|    128 | Small update | Legacy | 8.616408, 9.023050, 8.961883    | 37.137250, 9.599308, 8.927333   |
|    128 | Small update | Record | 26.323292, 25.894683, 26.194233 | 17.642000, 17.585025, 19.187667 |
|    128 | Work         | Legacy | 18.826025, 18.103500, 18.093558 | 18.785400, 17.608850, 17.971583 |
|    128 | Work         | Record | 42.213242, 40.957967, 41.265892 | 32.120392, 32.546775, 32.112258 |

Median allocations:

| Groups | Operation    | Format | Before B/op | After B/op | Before allocs/op | After allocs/op |
| -----: | ------------ | ------ | ----------: | ---------: | ---------------: | --------------: |
|     16 | Small update | Legacy |   3,133,284 |  3,101,193 |            5,865 |           5,852 |
|     16 | Small update | Record |   4,530,732 |  2,904,454 |           36,997 |          19,447 |
|     16 | Work         | Legacy |   8,820,920 |  8,631,699 |           15,946 |          15,896 |
|     16 | Work         | Record |   7,903,758 |  6,039,764 |           56,315 |          42,289 |
|    128 | Small update | Legacy |  30,471,627 | 30,472,160 |           39,943 |          39,946 |
|    128 | Small update | Record |  35,983,846 | 22,914,910 |          289,073 |         148,675 |
|    128 | Work         | Legacy |  67,945,126 | 67,632,624 |           80,710 |          80,685 |
|    128 | Work         | Record |  55,323,827 | 40,775,614 |          361,976 |         257,790 |

B/op is cumulative allocation, not retained heap or peak RSS. Although record
allocated bytes are now lower than legacy in all four cases, allocation counts
remain substantially higher.

## Persistent bytes

Framed bytes and object counts were identical before and after:

| Groups | Operation    | Legacy framed B/op | Record framed B/op | Legacy objects/op | Record objects/op |
| -----: | ------------ | -----------------: | -----------------: | ----------------: | ----------------: |
|     16 | Small update |             47,120 |              7,744 |                 9 |                12 |
|     16 | Work         |            194,912 |             36,704 |                28 |                52 |
|    128 | Small update |            351,760 |             19,688 |                 9 |                15 |
|    128 | Work         |          1,413,472 |             83,128 |                28 |                62 |

The following historical full-transaction SHA-256 values cover ordered object
bytes, roots, events, receipts and commit metadata. The comparison test has
been removed with the pre-record representation:

| Groups | Operation    | SHA-256                                                            |
| -----: | ------------ | ------------------------------------------------------------------ |
|     16 | Small update | `86775be01355e9807d3ec58fa80d97975bf647943bdffb0c9b8cb5c54fb56286` |
|     16 | Work         | `4a7b2f4ebde981c92c9e0247247b736f4af9e638617cf692fec337bea8e8d6f9` |
|    128 | Small update | `4a6ebbe3857523fe106a5b24eec2aa9b9300b5119cea228e001316d215e34126` |
|    128 | Work         | `f1d4fbb2b5927885af0e2a6037a04846b9b5247954105b00c343e7b695fdeff3` |

## Correctness validation

Tests were added or extended in
[record_image_test.go](../../agentprofile/record_image_test.go)
and
[agent_record_storage_test.go](../../engine/agent_record_storage_test.go).

- Generated encoding is compared with the original encode-to-public-decode
  algorithm for nested recovery, nil/empty collections, escaping, invalid UTF-8,
  binary decoder bounds and broken relationships.
- Public malformed-record tests supply matching content digests so unknown
  fields, duplicate fields, whitespace, trailing data and bad versions reach
  schema authentication rather than failing only at key verification.
- Generated token/depth counting is compared with the public scanner at both
  boundaries. A typed recovery record that passes allocation preflight but
  exceeds the token budget still returns `ErrStateLimit`.
- Input mutation, returned key/raw mutation, and mutation of a restored Ledger
  leave the image and another restored Ledger unchanged.
- Correctly hashed stored graphs with unknown-field or allocation-budget
  violations fail as `ErrStoredObject`; they are not resource rejections.
- Existing large-state growth, compaction/reopen, historical reads, receipts,
  unchanged-leaf reuse, atomic public resource rejection, record replay and
  mixed Work tests pass.

Commands and final outcomes:

```sh
go -C /Users/zhaozhiqiang/work/agent-temporal-engine test ./agentprofile ./engine -run 'TestRecord|TestStateSnapshot|TestApplyCommandRecords|TestAgentRecord|TestWorkAgentImage' -count=1
go -C /Users/zhaozhiqiang/work/agent-temporal-engine test -race ./agentprofile ./engine -run '^Test(RecordImageGeneratedMatchesPublicDecoder|RecordGeneratedJSONBudgetMatchesPublic|AgentRecordStoredLimitRemainsCorruption|WorkAgentImageRestoresIndependentLedgers|WorkAgentImageFailureIsolation)$' -count=1
```

The focused and race commands above are the current record-only validation
commands. The before/after timings in this report are historical measurements;
no current legacy comparison or checkpoint timing is claimed here.

Preparation still loads and validates the complete bounded record set, restores
independent Ledgers, clones candidate state, encodes/hashes all records and scans
the old radix keys. JSON decoding, reflection preflight, semantic validation,
normalization and sorting still incur full-state costs. These measurements
support removing duplicate work within the current format; they do not establish
incremental in-memory preparation or parity with legacy CPU time.
