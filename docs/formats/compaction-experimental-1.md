# AuditFull compaction experimental 1

## Scope and entry points

`storage.Journal.CompactAuditFull` replaces only physical layout. The core
`Engine.CompactAuditFull` first reconstructs a private projection with the same
typed replay used during Open and checks the five roots and object catalog.
The public Store forwards it; `ate compact-state [--agent-profile]
[--event-profile] DIR` obtains exclusive ownership and uses this semantic path.
`storage.Maintain` is separate: it defaults to metadata inspection and only
performs quarantine or authenticated unselected publication metadata cleanup
when explicitly requested. It does not remove segments or checkpoint streams.

All canonical TX frames, order, TxID, LSN, transaction digests, StateOps,
receipts and logical payload addresses are copied unchanged. AuditFull retains
independent control transactions and failed facts as well as graph ancestors.
Compaction currently packs the complete bounded ledger into one segment; it
does not promise a reduced authoritative history or a resumable index checkpoint.
Memory usage for copying is bounded by a 64 KiB buffer. Semantic replay still
has the configured object/profile limits and full-log recovery cost.

## Maintenance lifecycle

The sole Journal publication owner enters Maintenance with a fixed publication.
New physical writes and reader pins return ErrMaintenance. Existing readers
continue on the verified source layout. A Close request is sticky and waits
for the maintenance owner; completing maintenance cannot reopen a closing store.
The core serializes this operation with its command coordinator; new commands
remain subject to the existing bounded admission queue.

Before mutating storage, verify the source's full physical and canonical chain.
After the first layout mutation, finish installation or fence with an unknown
physical publication outcome. Cancellation before staging changes no files;
cancellation does not abandon a half-published CURRENT.

## Intent and manifest schema

The canonical staging intent uses magic `ATECMP01`, metadata SHA-256 envelope,
schema=1 and flags=0. Payload order:

1. Store ID and intent ID (the existing 16-byte metadata ID encoding).
2. LayoutSource: ManifestID, ManifestDigest, generation, logical PublishedEnd,
   Position.LSN, Position.Digest and PostRootSet.
3. Nonempty bounded array of newly allocated SegmentIDs.

The intent is synced in `compactions/INTENT.<id>` before any candidate segment
is created, followed by directory synchronization. Recovery validates its
source anchor against CURRENT's authenticated canonical ledger before using
it to remove redundant candidates. Malformed bytes at a recognized
`compactions/INTENT.<valid-id>` path are preserved as quarantine evidence after
verification; those bytes cannot authorize candidate removal. Unknown intent
names or invalid IDs fail during metadata loading, before prefix verification
or layout cleanup. Intents never select authoritative logical history.

Manifest schema=3 retains the common schema1 fields, then encodes the optional
current segment catalog (zero means the single initial-header segment), one
LayoutSource and a nonempty bounded array of RetiredSegments. A retired
descriptor must belong to the source's authenticated layout and cannot name a
current segment. Source generation is strictly smaller than the new generation;
logical floor, Position and roots cannot move backwards. Schema1/2 bytes remain
unchanged. The independent Python vector is in
`testdata/compaction-manifest-schema3.hex` (544 bytes; full SHA-256
`1322f4b5cb1c00cfa971202f84a959603de776672745e3a414b3a36f6431123b`).

Candidate files are fully synced and independently scanned before the same
manifest/CURRENT P1 installer publishes the replacement. The in-memory layout
is swapped only after CURRENT's directory sync succeeds. Generation advances
once; logical CommitFence and RootSet remain identical.

## Reader retirement and recovery

`PinnedReader` captures the publication and physical segment handles under the
same owner mutex used by compaction. It cannot read future transaction bytes.
After installation, old handles move to the retirement set. `ReclaimRetired`
closes and removes only unpinned old segments, then syncs their directory.
Errors retain retryable obligations; reopening repeats retirement from the
authenticated manifest. Normal publication carries pending retirement
descriptors forward while readers still pin them.

Recovery always follows CURRENT. Before CURRENT changes, the old complete
layout remains authoritative and candidate segments are staging. After CURRENT
changes, the new complete layout is authoritative and source descriptors
authorize retirement. Never fall back to a different manifest or delete an old
segment to make room for copying.

`TailReject` refuses repair of unpublished active-tail bytes and ordinary orphan
segments; it does not make Open read-only. Recovery preserves this order:
authenticate metadata and intent paths; verify the full published prefix,
replay if supplied, and compaction sources; successfully collect and validate
ordinary orphans (`collectOrphans`); if any compaction intents or selected-manifest
retired segment descriptors exist, stabilize CURRENT with `SyncDir(".")`;
reconcile compaction intents; remove authenticated retired segments; apply the
tail policy; sync the final publication witness before returning Ready.

The stabilization barrier precedes all layout cleanup under either tail policy,
including quarantine of malformed bytes at recognized intent paths. An interrupted
compaction can leave live CURRENT selecting the new layout while durable CURRENT
still selects the old one. Without this barrier, cleanup followed by `TailReject`
could durably delete old segments, then a crash could restore old CURRENT and
leave its selected layout corrupt. The publication protocol already synced the
manifest file and its directory before replacing CURRENT, so this barrier only
syncs the store root. Verification or orphan-collection failure stops before it.
A plain active-tail `TailReject` open with no compaction intents or retired
descriptors still performs no directory sync.

Consequently, either tail policy may remove authenticated redundant candidates
and retired segments before any tail refusal. Recognized intents with malformed
bytes are copied to `quarantine/COMPACTION.*`, with the evidence file and name
synced before closing and removing the intent. A valid intent never authorizes
removing a currently selected segment. Ordinary orphan segments and active-tail
bytes remain untouched under `TailReject`, which returns
`storage.ErrRecoveryRequired` if either remains after layout cleanup. CURRENT,
the selected manifest, published transaction bytes, logical fence and roots
remain unchanged by this cleanup.

Stabilization failure returns `storage.ErrRecoveryIncomplete`, retaining the
underlying IO error, before any layout cleanup or tail-policy check. At that
point `Verified=true`, `PublicationSynced=false`, and cleanup lists are empty.
Cleanup failures may also return `storage.ErrRecoveryIncomplete` before the
tail-policy check. The report records only progress reached; cleanup lists do
not certify subsequent directory syncs, and `TailDisposition` may not yet be
`preserved`. Failed resource closes retain retryable `CleanupError` handles
and the original cause; files must close before the owner lock is released.
Complete that cleanup before closing the caller's DurableIO or reopening.
`Verified` is set only after prefix, supplied replay and source verification;
earlier errors leave it false. `PublicationSynced` becomes true only when the final
Ready witness sync succeeds. Successful stabilization does not set it: tail
refusal or a failed final witness sync still leaves it false. False does not
exclude earlier `SyncDir` calls for stabilization, intent quarantine or cleanup,
and the report alone does not grant Ready.

### R07 publication metadata retention

After releasing the Store/Journal owner, callers can explicitly request
`storage.Maintain(ctx, d, storage.RetentionPolicy{DropPublicationStaging: true})`.
The zero-value policy inspects only; `DropQuarantine` is an independent opt-in.
No background GC or automatic cleanup/deletion of user store directories is
added, and the intent and manifest encodings above remain unchanged.

Maintenance can remove authenticated obsolete final manifests as well as
manifest staging and temporary CURRENT files, but only outside the selected
metadata and transitive proof closure. It retains `LayoutSource` and checkpoint
source manifests, valid pending compaction intents and their sources. Even a
valid obsolete/staged manifest scheduled for removal pins its sources for the
entire pass, so obsolete chains may require later explicit passes. Maintenance
never consumes an intent or deletes active, retired or candidate segments;
segment retirement remains the separate verified recovery/`ReclaimRetired` path.

Unknown metadata, including loose checkpoint streams or staging without a
selected/proof reference, is preserved and blocks **all** publication removal
through `PublicationReclamationBlocked` and sorted `ReclamationBlockers`.
Metadata/source-descriptor validation is not a physical history scan or semantic
replay and cannot substitute for the verification required by compaction.

Before unblocked reclamation, manifests and then the store root are synced.
This can stabilize a CURRENT replacement observed after an interrupted
publisher's unsynced rename, but never rewrites CURRENT or retained proof bytes.
The [R07 store maintenance contract](store-experimental-1.md#r07publication-metadata-的显式维护)
defines the sorted inventory, 16,384-entry/64 MiB aggregate scan budgets and
nonpaginated `DurableIO.List` caveat, post-removal directory-sync retries, and
`CleanupError` retries that retain file → owner close order.

## Evidence and limits

Tests enumerate before/after errors at every modeled durable mutation; cover
source corruption, cancellation, reader pin/release, concurrent append,
Close/Maintenance interleaving, repeated compaction, reopen, five-root history,
exact model input and original receipts. R08 regressions start with a real
compaction whose CURRENT replacement is visible but whose root sync failed,
then exercise `TailReject` with an active tail, an ordinary orphan, or both.
They check stabilization before deletion and preservation of the selected
history across crash/reopen, along with stabilization faults, orphan validation
before the barrier, and the separate final Ready witness. They use an explicit
filesystem sync fault model and native process-local tests; they do not certify
hardware power failure, dishonest same-user filesystem modifiers, selective
history erasure, or no-pause online compaction.
