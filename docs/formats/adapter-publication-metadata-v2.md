# Independent adapter publication metadata v2

`journalstore` and `graphstore` are trusted embedding adapters over the physical
Journal. Their transaction metadata is separate from the core Engine's five-root
state format and from domain record payloads.

## Why two counters are required

An append advances both the logical transaction LSN and the physical publication
Generation. AuditFull compaction advances only Generation; it preserves the
logical position, transaction bytes and committed history.

Each adapter transaction therefore records both its logical LSN and the physical
Generation of its original publication. For example:

| Operation          | Logical LSN | Physical Generation |
| ------------------ | ----------- | ------------------- |
| First append       | 1           | 1                   |
| Compaction         | 1           | 2                   |
| Second append      | 2           | 3                   |
| Another compaction | 2           | 4                   |

Replaying the second transaction must validate LSN 2, while retrying its request
must still return original Generation 3. Neither the current Generation 4 nor
the logical LSN 2 can replace that original receipt field.

## Encodings

| Adapter        | Transaction field | Metadata encoding                                                                                    |
| -------------- | ----------------- | ---------------------------------------------------------------------------------------------------- |
| `journalstore` | `RefUpdates`      | Literal `ATE-JOURNALSTORE-2\x00`, big-endian uint64 LSN, big-endian uint64 Generation                |
| `graphstore`   | `ControlUpdates`  | Literal `ATE-GRAPHSTORE\x00`, canonical `codec.Uint(2)`, `codec.Uint(LSN)`, `codec.Uint(Generation)` |

The other transaction metadata field must be empty. `journalstore` accepts the
exact fixed length. `graphstore` rejects trailing bytes and enforces its 128-byte
metadata bound. These encodings do not change Graph revision bytes, branch
command schema 1, request identities or the physical Journal framing.

## Validation and recovery

- LSN is nonzero and equals the enclosing transaction's LSN. The Journal verifies
  the contiguous logical sequence and digest chain.
- Generation is at least LSN and strictly increases between adapter transactions.
  Gaps are legal because layout-only publications do not add transactions.
- New writes reject overflow of either counter before adding one.
- Recovered receipts retain the encoded original Generation. Request retries
  return the original result before checking current mutation preconditions.
- Unsupported adapter metadata is rejected by semantic replay before tail repair.

The adapters retain their own encoding and error contracts; this metadata is not
a second publication authority. CURRENT and the physical Journal still determine
the selected durable history.

## Development format cutover

Both adapters now write and read only v2 metadata. V1 metadata is explicitly
unsupported; there is no runtime migration or fallback. This also affects
standalone domain stores that persist through `journalstore`, even when their
domain payload schema itself has not changed.

Existing development directories are not deleted or rewritten automatically.
Callers needing a fresh development store should create a new directory.

Implementation and regressions:

- [journalstore codec and replay](../../journalstore/store.go)
- [journalstore compaction/reopen regressions](../../journalstore/correctness_test.go)
- [graphstore codec and replay](../../graphstore/store.go)
- [graphstore compaction/reopen regressions](../../graphstore/correctness_test.go)
- [graphstore branch metadata rejection](../../graphstore/join_regression_test.go)
