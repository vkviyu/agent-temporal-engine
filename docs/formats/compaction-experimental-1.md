# AuditFull compaction experimental 1

## Scope and entry points

`storage.Journal.CompactAuditFull` replaces only physical layout. The core
`Engine.CompactAuditFull` first reconstructs a private projection with the same
typed replay used during Open and checks the five roots and object catalog.
The public Store forwards it; `ate compact-state [--agent-profile]
[--event-profile] DIR` obtains exclusive ownership and uses this semantic path.
`storage.Maintain` remains the separate quarantine cleanup API.

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
it to remove redundant candidates. Invalid intent bytes are preserved as
quarantine evidence. Intents never select authoritative logical history.

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

## Evidence and limits

Tests enumerate before/after errors at every modeled durable mutation; cover
source corruption, cancellation, reader pin/release, concurrent append,
Close/Maintenance interleaving, repeated compaction, reopen, five-root history,
exact model input and original receipts. They use an explicit filesystem sync
fault model and native process-local tests; they do not certify hardware power
failure, dishonest same-user filesystem modifiers, selective history erasure,
or no-pause online compaction.
