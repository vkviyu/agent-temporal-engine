# Agent record state v1

This is the only persisted Agent profile state format. It replaces the
experimental single-object profile representation and stores semantic records,
not byte chunks of a serialized snapshot. Command bytes, request digests and
the public `Snapshot` result remain separate contracts.

The canonical record schema is registered as `agent.record-state` in
`formats/registry.json` and checked against `agentprofile.RecordStateVersion`.
The retired `agent.snapshot` family is not supported.

## Object graph and wire layout

The existing system LiveControl key (`agent/v1`, space, principal, each encoded
as a codec string) points to one record root.
All new objects have core object version 1 and the profile's user space.

`ate.agent.record-root` payload:

1. Core schema 1 and flags 0, as canonical unsigned varints.
2. Owner, as a codec string.
3. Optional record radix root, using the core optional object-ID encoding.
4. Record count, as a canonical unsigned varint.
5. Sum of canonical record byte lengths, as a canonical unsigned varint.

The root's reference set is exactly the nonempty radix root. Count, byte sum
and radix emptiness must agree. The root is independently versioned by its
object type/version; it is not snapshot version 3.

The persistent radix uses the profile's user space. A leaf key is the 32 raw
SHA-256 bytes of its canonical record followed by an eight-byte big-endian
duplicate ordinal, starting at zero. The ordinal preserves valid multisets
where the existing validator permits identical records; it does not relax any
logical uniqueness checks. Leaf values are content-addressed record object IDs.
Using content keys avoids renumbering records when an identity is inserted.
Replacing a record deletes its old key and inserts its new key.

`ate.agent.record` payload:

1. Core schema 1 and flags 0.
2. Owner, as a codec string.
3. Canonical record, as a codec byte string.

The canonical record starts with ASCII `ATE-AGENT-RECORD` and NUL, then
canonical unsigned varints for record schema (1) and the following tag.
The remaining bytes are the body; trailing bytes are forbidden.

| Tag | Record body                                   |
| --- | --------------------------------------------- |
| 0   | Exactly `{}`; recovery-presence marker        |
| 1   | `EncodeTask` binary record                    |
| 2   | `EncodeExecution` binary record               |
| 3   | `EncodeStep` binary record                    |
| 4   | `EncodeInvocation` binary record              |
| 5   | `EncodePendingAction` binary record           |
| 6   | Query                                         |
| 7   | Activity                                      |
| 8   | Attempt                                       |
| 9   | ActivityResult                                |
| 10  | Message                                       |
| 11  | MessageConsumption                            |
| 12  | Continuation                                  |
| 13  | ExecutionAuthority                            |
| 14  | ActivitySpec                                  |
| 15  | ActivityControl                               |
| 16  | AttemptControl                                |
| 17  | ResultCapture                                 |
| 18  | ActivityAcceptance                            |
| 19  | ActivitySettlement                            |
| 20  | AttemptReconciliation                         |
| 21  | QueryAnswer                                   |
| 22  | WorkStart                                     |
| 23  | TaskLink                                      |
| 24  | GoalObservation                               |
| 25  | TaskOutcome                                   |
| 26  | ActionIntent                                  |
| 27  | ActionResolution                              |
| 28  | ResumePlan                                    |
| 29  | ExecutionJoin                                 |
| 30  | ActivityAcceptance, in the Imports collection |
| 31  | ValidationRequirement                         |
| 32  | IdempotencyGuarantee                          |
| 33  | RetryAuthorization                            |
| 34  | OutputChunk                                   |

Tags 6–34 use one typed JSON struct, with the field names, declaration order,
JSON tags and nested types declared in the agentprofile v1 schema. Encoding is
Go `encoding/json.Marshal`: compact JSON, HTML escaping, base64 byte slices,
and declared `omitempty` behavior. Nil and nonnil empty fields retain that
codec's distinction after record normalization: in particular, an
empty `ActivityResult.OutputChunks` is nil and encodes as `null`, keeping
record IDs stable after normalization.
Embedded RawMessage fields normalize through marshal and
decode. Unknown fields, duplicate fields, whitespace, alternative field order,
alternative escaping/numbers, and trailing content fail exact re-encoding.
The tag-to-type registry is closed and explicit. No input-selected Go type,
interface-valued object, or generic reflection deserializer is accepted.
Changing a persisted type's fields requires a new record schema/version.

## Validation and limits

The reader authenticates object hashes, owner and space on every record, radix
keys and duplicate ordinals, root count and byte sum, and canonical encoding.
It then validates the entire reconstructed profile with the existing semantic
and cross-record checks. Recovery presence is explicit, including empty
recovery state. Child recovery records without that marker are rejected.
The first five record types use their existing binary decoders, including
their field limits and normalization. The resulting snapshot uses canonical
record ordering before restoring independent Ledger indexes and owned data.

Hard format bounds are:

- 4 MiB per canonical record, including its domain and tag.
- 16,384 records across all collections, including the presence marker.
- 256 MiB summed canonical record bytes per profile.
- JSON depth 32 and 65,536 tokens per record before typed slice allocation.
- A conservative allocation preflight before marshaling each record; extremely
  nested or heavily escaped records can be rejected below the byte ceiling.
- Radix scans: 65,536 nodes, 16,384 entries, 4 MiB retained entry data and 32 MiB
  read node bytes. Radix updates retain the existing per-batch protections.

Command, object, transaction, publication and total-object protections also
apply. These limits deliberately do not promise that every combination of
individually maximal records can fit in a single command or migration.
`RecordImage` and `ApplyCommandRecords` are the state image APIs. They do not
materialize a monolithic lifecycle or snapshot envelope.

Record-state budget failures return `agentprofile.ErrStateLimit`, separately
from malformed-state errors. Engine admission maps this sentinel to
`ResourceLimit` without publishing roots, a receipt or an LSN. Stored record
budget violations carry `ErrStoredObject`; they remain fail-closed corruption,
not command resource rejections.

An API Snapshot is an in-memory view of the record-backed state. Facades should
use `ValidateStateSnapshot(Snapshot) error` or
`RestoreStateSnapshot(Snapshot) (*Ledger, error)`. These helpers apply the
record limits and full child-decoder and cross-record checks. They must not be
used to relax the selected storage format's write limits. Scope and
transport-frame checks remain the facade's responsibility.

## Updates, replay and ownership

Preparation always produces a record root. Unchanged content keys reuse their
existing record objects. Only changed
keys enter `state.Apply`, which reuses unchanged radix subtrees. Mutations are
sorted and batched deterministically at a bounded radix batch size.
The transaction builder deduplicates records and radix nodes against durable
objects and prior Work substeps.

Replay regenerates the record plan from the durable state and compares final
operations and ordered transaction records exactly. No representation
selection context is needed by recovery checkpoints.

Work retains one private immutable image at a time, keyed by resolved profile
object ID, space and principal. Restores own their mutable state. Intermediate
record roots and radix nodes can remain in Work transactions, subject to
existing Work/transaction/object limits and builder deduplication.

Task trace loading and public Agent reads load the record root and then use the
same disclosure checks. The record graph references
the structural objects. Schema-specific external references, source fences
and logical Knowledge PacketIDs retain their existing semantics; raw object
references alone are not a sufficient selective-sweeping policy. AuditFull
compaction retains and verifies canonical history.

## Remaining performance work

This implementation bounds and reduces durable delta bytes; it does not yet
make reducer CPU or peak memory proportional to the changed records.
Preparation still clones, encodes and validates the complete bounded profile.
Loading authenticates its records, and storing compares the old radix entry
set. Cross-record validation and lifecycle sorting can still be expensive.
Incremental validated indexes, dirty-record tracking and lazy projections are
separate work. They must preserve validation, disclosure and exact replay
semantics rather than bypassing them.
