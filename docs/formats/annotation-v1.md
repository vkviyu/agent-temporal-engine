# Annotation V1

`annotation` is a public domain package. `ApplyAnnotation` (`annotation.apply/v1`;
local method `apply_annotation`) accepts one host-approved immutable `Record`.
`GetAnnotations` (`get_annotations`) returns the authenticated owner's append-only
records at a publication Anchor. These commands are available in the embedded
facade and Unix service/SDK through `annotation/v1`.

The reviewed dependency direction is `annotation -> model`, with API, engine and
transports allowed to consume the domain. Annotation imports no engine, storage,
transport, host application or external package. The architecture guard records
this policy alongside the existing graph/knowledge/memory domains.

Records live in an owner partition of `RootSet.ExecutionKnowledge`, under a
SHA-256 commitment of length-prefixed Space and Principal. An annotation changes
no original state, branch head, execution control, required context or audit fact.
The ID commits to Target, Label, Text, Reason, Hide, Source, Evidence and Retracts.
Accepted labels are duplicate, superseded, irrelevant, retracted, title and note.
The reason is mandatory. Target and evidence must be factual NodeRefs; another
Annotation is not evidence. A title is sourced derived text, never a new fact.

`Source` is an exact published SnapshotAnchor. Target and every Evidence ref must
be visible at that cut, under its Cursor/Knowledge selectors and current owner
checks. Unknown objects, foreign executions, unpublished cuts and unadopted
knowledge are rejected. Current disclosure restrictions are checked on new
acceptance and every read, including historical records and annotation-derived
event/command text after a source key is removed. Accepted RequestKey retries
still return the original receipt before checking today's policy.

A withdrawal is a new Record with `Retracts` naming an accepted, active annotation
in the same owner partition. Its Target must match and its Source must already
contain the earlier acceptance. Label/Text/Hide are empty on a withdrawal; Source,
Evidence and Reason remain committed. One withdrawal cannot cancel independent
judgments or another withdrawal. Historical Anchors retain their original labels.

Ordinary state/audit Views apply active owner Hide judgments only in ListNodes
and SearchNodes at their fixed publication cut. `IncludeHidden=true` restores the
complete authorized navigation corpus, and is bound into pagination tokens.
Other principals' Views are unaffected. Execution-knowledge Views always select
members through explicit adoption and their Knowledge Cursor; owner navigation
annotations do not implicitly change that knowledge. GetNode, Trace and context
expansion preserve original facts, causal links and mandatory constraints.
Annotation details are inspectable as `annotation/Annotation/<ID>` logical entities
in ordinary Views, with current provenance checks. They do not become adopted
knowledge merely because a caller knows the ID.

Record bytes are `ATE-ANNOTATION\0v1\0` followed by the exact Go struct-field-order
JSON of Record. IDs are SHA-256 of the same prefix and JSON with ID empty.
Snapshot bytes use `ATE-ANNOTATIONS\0v1\0` and exact JSON of the ordered Records.
Decode re-encodes and compares bytes, rejecting alternate spelling, duplicate keys,
unknown fields, trailing bytes and edited identities. Future version prefixes are
unsupported; there is no legacy decoder. The independent Python-built record
vector is `annotation/testdata/record-v1.hex`.

Bounds are 1,024 records per owner, 64 KiB per canonical JSON record, 64 evidence
refs per record, 8 KiB reasons, 32 KiB text and the existing 1 MiB state-value cap.
The event registry records Annotation.Apply and Annotation.Retract facts at the
annotation ID; withdrawals retain their acceptance causal edge. Replay and
AuditFull compaction run the same reducer and retain original records and bytes.
