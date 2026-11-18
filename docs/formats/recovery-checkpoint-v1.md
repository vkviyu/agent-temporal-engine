# Recovery checkpoint v1

`Engine.CreateRecoveryCheckpoint(ctx)` and the `ate.Store` facade method are
explicit maintenance operations.
Opening a store consumes a valid selected checkpoint by default.
`engine.Options.DisableRecoveryCheckpoints` forces complete semantic replay.
`RecoveryReport.CheckpointTransactions` reports how many reducer transitions
were skipped; `ScannedTransactions` still reports the entire physical history.

The operation changes publication generation, not the logical commit fence,
root set, transaction bytes, or logical object addresses. All audit history is
retained. Checkpoints do not authorize any garbage collection.

## Authority and certification

The only publication authority remains `CURRENT`. Its existing SHA-256 binding
selects an immutable manifest. Manifest schema 4 adds a checkpoint descriptor
containing:

1. Store ID and checkpoint ID.
2. Semantic certification/encoding schema.
3. Exact checkpoint byte size and SHA-256 over the entire file.
4. SHA-256 over the logical frame stream from offset 96 through the exclusive
   source published end. Segment headers and seal footers are excluded.
5. Source manifest ID, SHA-256, generation, logical published end, LSN, commit
   digest, and post-root-set digest.

The referenced source manifest authenticates the exact physical layout and
prefix against which certification ran. The logical prefix digest binds those
same transaction bytes across subsequent physical compaction. Source store,
generation, fence, root and logical end must agree with that source manifest.
The source must precede the selecting manifest and cannot exceed its tip.
At the source LSN, the ordinary physical transaction scan also checks the
checkpoint's commit digest, root and logical end.

Recovery does not select checkpoints by directory scan, a “latest” pointer, or
filename-based promotion. An unselected file never allows replay to be skipped.
Every later ordinary publication carries the selected descriptor forward;
AuditFull compaction carries it forward unchanged.

These are the same integrity and trusted-writer assumptions as CURRENT and the
journal, not a cryptographic signature against an attacker able to rewrite
CURRENT and all of its digest dependencies.

## Publication protocol

The Engine maintenance writer excludes command publication and compaction.
Storage holds its own sole writer continuously across verification, encoding
and installation.

1. Run the complete `VerifyPublished` physical scan and semantic replay into a
   fresh private audit verifier, even if this Engine opened from a checkpoint.
   No checkpoint is used to certify another checkpoint.
2. Check the verifier's roots, fence and object count against the active
   publication, and authenticate the on-disk identity, source manifest and CURRENT
   against the captured writer state.
3. Hash the exact logical source prefix, allocate immutable random file IDs,
   and honor cancellation before staging.
4. Create the checkpoint directory if necessary; sync the store directory.
5. Stream the projection into an exclusively created checkpoint file, sync
   the file, close it, then sync the checkpoint directory.
6. Publish a new schema-4 manifest using the existing immutable manifest
   write/sync/install/directory-sync protocol.
7. Write and sync a temporary CURRENT, atomically replace CURRENT, then sync
   the store directory. Only now install the new in-memory publication.

After staging starts, an IO or encoding failure fences the writer and reports
an uncertain maintenance publication. Failed handle closure retains ownership
through the existing retryable cleanup machinery. Reopen follows only the
durable CURRENT, so a crash selects either the earlier complete authority or
the new complete authority. Checkpoint publication itself retains unselected
staged files; it does not garbage-collect loose checkpoints or manifests.
Separate, explicitly requested R07 metadata maintenance can reclaim authenticated
unselected manifests outside the proof closure, but never checkpoint streams.

### R07 metadata maintenance boundary

`storage.Maintain(ctx, d, storage.RetentionPolicy{})` defaults to inspection under
exclusive ownership. `storage.RetentionPolicy{DropPublicationStaging: true}`
explicitly requests publication metadata removal; no automatic cleanup of user
stores is added. Existing checkpoint and manifest encodings remain unchanged.

Maintenance preserves the selected checkpoint reference and the transitive
`RecoveryCheckpoint.Source`/`LayoutSource` manifest proofs, including sources
referenced by valid obsolete/staged manifests and pending compaction intents.
Selected and retained checkpoint paths are references, possibly to absent files,
not verification of stream contents. Metadata validation is not semantic replay
or checkpoint certification; maintenance never reads or removes checkpoint
streams or segments and does not consume a checkpoint to skip replay.

A loose checkpoint stream or staging entry without a selected/proof reference
is `UnknownMetadata`. It sets `PublicationReclamationBlocked` and appears in
`ReclamationBlockers`, blocking all publication metadata removal, not just that
file. Removing an obsolete manifest can leave its checkpoint loose on a later
pass; the stream is preserved and that later pass blocks rather than silently
deleting it. Obsolete source manifests remain pinned for the current pass and
can require subsequent explicit passes before becoming reclaimable.

Before unblocked reclamation, maintenance syncs manifests and then the store
root. This may stabilize an observed CURRENT replacement left unsynced by an
interrupted publisher, without rewriting its bytes or selecting a candidate.
It is not recovery or evidence that the checkpoint projection is valid. See the
[R07 store maintenance contract](store-experimental-1.md#r07publication-metadata-的显式维护)
for sorted report categories, scan budgets, directory-sync retries and retryable
`CleanupError` file → owner ownership.

## Manifest encoding

Schemas 1, 2 and 3 retain their exact previous encoding when no checkpoint is
selected. Schema 4 uses the existing `ATEMAN01` envelope and checksum:

- Version 4 and flags 0.
- The same common fields through `PostRootSet` as schemas 1–3.
- A segment descriptor count and descriptors; zero denotes the legacy single
  segment layout.
- A layout-source presence integer, exactly 0 or 1. If 1, the existing
  `LayoutSource` and retired descriptor list follow.
- The checkpoint descriptor fields above, in that order. IDs use the existing
  16-byte ID encoding; unsigned integers use canonical varints; digests use
  the existing 32-byte digest encoding.

The existing `MaxManifestBytes` limit applies. A checkpoint requires nonzero
schema, size and digests. Checkpoint size is capped at 1 GiB. Unknown manifest
schemas fail closed; an unknown checkpoint schema can fall back to full replay.

## Engine projection encoding

The checkpoint is a sequence of records. Each record has a four-byte unsigned
little-endian length followed by that many canonical JSON bytes. Each payload
is at most 1 MiB. There is no compression, unrestricted graph deserialization,
or allocation based on an unchecked record length.

Canonical JSON is the exact `encoding/json.Marshal` encoding of the v1 Go DTOs
in `engine/checkpoint_codec.go`: field declaration order, no insignificant
whitespace, normal Go string escaping, base64 for byte slices, and decimal
integer arrays for fixed byte arrays. The decoder re-encodes each decoded DTO
and requires byte equality, rejecting duplicate or unknown fields and alternate
spellings. A complete file digest is verified before parsing and again over the
bytes consumed. Trailing bytes are rejected.

Records appear in this order:

1. Header: magic `ATE-RECOVERY-CHECKPOINT`, schema 1, agent/event profile flags,
   source publication, all five roots, root-set object ID and section counts.
2. Object catalogue entries, strictly ordered by object ID. Each contains the
   logical record payload offset and length. No physical segment identity is
   embedded. Locations must lie within the certified source prefix.
3. Historical envelopes in contiguous LSN order, including genesis. Each
   contains transaction ID, fence, root-set object ID, space and principal.
   Transaction and historical-fence indexes are reconstructed from these.
4. Events in original publication order, including their metadata, operation
   and ordinal. Event objects are loaded through authenticated logical
   locations to reconstruct immutable event blocks, TaskTrace subject indexes,
   causal-child indexes and reverse metadata indexes.
5. Event heads and fact aliases, strictly ordered by raw key bytes. Keys are
   byte slices because canonical namespace keys can contain non-UTF-8 bytes.
6. Receipt hints, strictly ordered by their 32 raw bytes.

Section counts are bounded by normalized engine object and transaction limits.
The encoder streams records rather than materializing the entire checkpoint.
Sorting metadata and the restored engine maps still require memory proportional
to the retained catalogue; the format does not promise constant-memory Engine
recovery.

The projection does not serialize agent snapshots, agent records, domain
reducers, or reducer-specific Go values. Their immutable objects remain in the
journal and are referenced by roots and logical locations. This permits the
same generic checkpoint to cover different supported state representations.

Locks, coordinator/admission state, private staging overlays, views, IO handles,
reader pins, cancellation/fencing state, read caches, and TaskTrace evidence or
profile caches are omitted.

## Opening and fallback

Open first authenticates CURRENT, identity and the selected manifest and opens
its physical layout. For an enabled selected checkpoint it authenticates the
source manifest, the logical prefix, the checkpoint size/digest, supported
schema/profile combination and the complete bounded projection.

The common header's magic, descriptor/header schema agreement and source
bindings are validated before unsupported-epoch or profile fallback. A schema-1
descriptor with a schema-2 payload is corruption. A known schema's complete
projection is validated in its recorded profile before a profile mismatch can
select full replay.

Projection validation scans the certified logical transaction prefix without
executing reducers. Every object ID and exact record location must match the
catalogue, with no omissions or extras. Every historical transaction envelope
and genesis proof must match; every receipt must match its transaction and be
present in the final receipt root. The complete receipt hint set is compared.

For EventProfile stores, every certified transaction must have exactly one
core event, after its profile facts. Profile-event membership and ordinal order,
command/operations bindings, stream sequence and predecessor, and causal-parent
history are checked. Stream heads and fact heads are rebuilt from event bodies.
Branch-position and imported-observation aliases are derived from the recorded
commands, registry epoch and historical post-roots. The complete reconstructed
head map must equal the serialized map, including aliases.

Missing checkpoint files, unknown checkpoint schemas and incompatible profile
flags select complete semantic replay from genesis. A profile flag mismatch
does not enable a disabled profile: historical replay must still accept every
transaction under the requested options. Any partial decoded projection stays
private and is discarded before fallback. Resource cleanup failure is never
hidden as a fallback.

Checkpoint corruption or invalid source bindings fail closed. Operators can
explicitly request full replay with `DisableRecoveryCheckpoints`; that path
does not use checkpoint data to skip anything. It still authenticates CURRENT,
the selected physical layout, all historical transaction bytes and all semantic
transitions.

When a checkpoint is accepted, the physical scanner still validates every
historical frame, transaction digest chain, transaction ID, segment header,
footer and selected prefix. Only reducer execution through the certified LSN
is skipped. Every later transaction goes through normal semantic replay.
Nothing is exposed until both physical validation and suffix replay succeed.
After those checks (or complete replay when fallback is selected), recovery
verifies compaction sources and successfully collects and validates ordinary
orphans (`collectOrphans`). If any compaction intents or selected-manifest retired
segment descriptors exist, it then stabilizes CURRENT with `SyncDir(".")` before
reconciling compaction intents or removing authenticated retired segments.
Recovery applies the tail policy after that cleanup. Only after these stages
succeed does the separate final publication-witness sync permit Ready. Unknown
intent paths fail earlier during metadata loading, before prefix verification
or layout cleanup; orphan-collection failure also stops before stabilization.

An interrupted compaction can leave a visible CURRENT replacement whose root
directory sync has not completed. Stabilizing it before cleanup prevents a crash
after `TailReject` refusal from restoring an old CURRENT whose source segments
were already deleted. The manifest file and its directory were synced before
CURRENT replacement, so this barrier only syncs the root. Checkpoint acceptance
does not replace this barrier or the final Ready witness.

`TailReject` preserves unpublished active-tail bytes and ordinary orphan segments
and returns `storage.ErrRecoveryRequired` if either remains at the policy check;
Open is not read-only. After verification, either policy may remove authenticated
redundant compaction candidates and retired segments, or preserve malformed bytes
from recognized intent paths in synced quarantine evidence before removing those
intents. Checkpoint acceptance does not bypass these checks or authorize cleanup
on its own. The stabilization barrier applies under either policy, including
when recognized intents contain malformed bytes. A plain active-tail `TailReject`
open with no compaction intents or retired descriptors still performs no
directory sync. `TailQuarantine` additionally permits active-tail/orphan repair
after preserving and syncing its evidence.

Stabilization failure returns `storage.ErrRecoveryIncomplete` with the underlying
IO error before any layout cleanup or tail refusal; `Verified=true`,
`PublicationSynced=false`, and cleanup lists are empty. Layout cleanup failures
can also return `storage.ErrRecoveryIncomplete` before tail refusal.
Reports may be partial: cleanup lists do not prove later directory
syncs succeeded, and `TailDisposition` may not yet be `preserved`. Failed closes
retain retryable `CleanupError` handles and their causes, with file → owner
retirement order; finish cleanup before closing the caller's DurableIO or
reopening. `Verified` becomes true only after prefix, replay and compaction
source verification; it is not true on every error. `PublicationSynced` becomes
true only when the final Ready witness sync succeeds. Successful stabilization
leaves it false, as do subsequent tail refusal or a failed final witness sync.
False does not exclude earlier directory syncs, including root stabilization
and the sync needed to preserve malformed intent evidence. A report without a
successfully opened store does not grant Ready.

## Compatibility and limits

Schema 1 is also a semantic certification compatibility epoch. A change that
invalidates earlier certification or alters the generic index projection must
bump `recoveryCheckpointSchema`; the new binary then performs full historical
replay for older checkpoints. Adding a supported immutable state representation
does not itself require embedding that representation in this format.

Checkpoint creation intentionally costs a full physical and semantic replay.
Warm open accelerates historical reducer work, not physical audit scanning.
It also reads/hashes the logical checkpoint prefix and the checkpoint file,
scans the certified logical transactions to reconcile the projection, and
rebuilds event query indexes. This extra validation pass does not execute old
reducers. The API is available on `engine.Engine` and `ate.Store`; automatic
checkpoint scheduling is not added.

Validation includes before/after fault injection at every storage checkpoint
publication mutation, exact historical fallback transaction comparisons,
source/fence/root/prefix and checkpoint corruption rejection before tail repair,
warm suffix/full replay projection equivalence, profile gates, historical
receipts and views, five-root state, empty genesis, and compaction compatibility.
