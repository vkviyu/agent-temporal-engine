# Local native scale sample — 2026-11-03

This is a fixed small sample on Darwin/arm64, Apple M1, Go 1.27.0. It uses real private temporary directories and the native file/directory sync implementation. The process ran alongside development work; CPU, filesystem cache and device state were not isolated. These measurements are local evidence, not general throughput, device durability or large-store capacity claims.

The raw, unedited Go JSON output is [native-scale-2026-11-03.jsonl](native-scale-2026-11-03.jsonl). Its SHA-256 is `8bc7071d30f7f3a5023eeabef35c413c6bdf242bf87d8e5ee94296d70d07f413`. The parsed numeric summary is [native-scale-2026-11-03-summary.json](native-scale-2026-11-03-summary.json).

Each engine sample first seeds 128 or 1024 distinct 1 KiB keys in one transaction, then measures 64 acknowledged updates to a 64-key hot set with eight bounded writers. Seeding is excluded. `MaxCount=16` actually grouped eight transactions per CURRENT replacement here, bounded by the eight writers; it is not evidence of full 16-member batches. Three fresh-store samples were run per configuration. The native meter delegates every call to DirectoryIO and counts successful file Sync/SyncDir returns, written bytes and CURRENT replacements; it does not trace kernel syscalls or replace IO.

| Seed keys | Max batch | ACK/s median (range) | p50 ACK ms | p95 ACK ms | File / dir sync per ACK | Written bytes per ACK | Reopen ms | AuditFull ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 128 | 1 | 31.36 (23.44–33.86) | 258.95 | 308.96 | 3.000 / 2.000 | 6483 | 46.91 | 117.61 |
| 128 | 16 | 209.85 (180.81–227.85) | 36.20 | 48.08 | 0.375 / 0.250 | 6159 | 39.93 | 118.69 |
| 1024 | 1 | 44.98 (25.09–48.60) | 170.96 | 209.94 | 3.000 / 2.000 | 7243 | 81.92 | 129.97 |
| 1024 | 16 | 357.76 (351.85–361.75) | 22.04 | 24.90 | 0.375 / 0.250 | 6917 | 76.74 | 129.05 |

Latency columns are medians of the three per-run quantiles, each based on only 64 updates; they are not tail-service guarantees. ACK latency includes bounded writer queueing. Reopen verifies all 65 transactions, exact semantic roots and a historical receipt. AuditFull verifies and copies the full retained audit prefix, then checks unchanged logical roots. It does not measure selective garbage collection or an unbounded execution history.

The physical reader-pin experiment uses the public physical Journal API with 16 opaque 1 KiB records and a 1 KiB segment target. It is separate from engine View leases and semantic replay. In every sample all 33 old segments remained pinned through compaction, old bytes remained identical, reclaim before release reported pending segments, and release allowed complete retirement. The configured delay is an extra hold *after* compaction, not total pin lifetime.

| Extra hold after compaction | Pin acquire µs median | Compaction ms median | Release + reclaim ms median |
| ---: | ---: | ---: | ---: |
| 0 ms | 4.29 | 58.88 | 16.96 |
| 10 ms | 3.83 | 79.93 | 28.94 |

The pin timings vary with filesystem work and the shared host. The two hold configurations establish retention behavior and show measured costs; they do not isolate a causal latency penalty from a 10 ms hold. Source retirement is a separate measured operation, and the requested hold is intentionally not included in its duration.

Reproduce the exact fixed sample:

```sh
GOCACHE=/tmp/ate-gocache GOPATH=/tmp/ate-gopath GOTOOLCHAIN=local \
  go test -json ./tests/benchmarks -run '^$' -bench '^BenchmarkNativeScale/' \
  -benchtime=1x -count=3 -timeout=240s
```

Go `-count=3` reruns the benchmark independently, so each row’s local Trial index is zero; timestamps and log order distinguish the three runs. Standard benchmark ns/op and allocation fields describe one 64-update trial (pin lanes describe pin acquisition only). The custom commit/ACK, reopen, compaction and release metrics carry their stated units.

Remaining evidence gaps include multi-process contention, different devices/filesystems, Linux native runs, power loss, long histories, quota saturation, large Profile partitions, provider latency, and realistic host workloads. No passing threshold was chosen from these observed numbers.
