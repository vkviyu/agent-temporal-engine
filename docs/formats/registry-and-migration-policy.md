# Machine-readable compatibility registry

`formats/registry.json` is the release artifact for the supported format/version
inventory. The public `formats` package embeds it and provides owned copies,
strict parsing, decode/replay/write policy lookup, and a fail-closed migration
planner. It has no dependency on the engine, service, model or a reducer.

The document is canonical indented JSON, schema 1. Families and capability IDs
are strictly ordered; versions and method sets are strictly increasing. Unknown
fields, duplicate keys, duplicate methods/families, trailing data, noncanonical
JSON and unknown registry versions are rejected. `selector=opcode` denotes an
operation selector, not a format schema number (notably Continuation opcodes
7/8). Domain strings use the visible `\0` notation for a NUL domain suffix.

The optional family `layout` names the field contract within the version and
domain. Current Control 5 includes effect capabilities and finite retry records.
Continuation opcode 8 and Work 1 carry producer-bound knowledge seed semantics.
Packet 4 adds the Requirements manifest. Control 1–4, Packet 1–3 and Continuation
opcode 7 are retired pre-release shapes.

The outer Agent command remains schema 1 and retains standalone operations for
unmanaged state. Agent snapshots 1 and 2, Observation 1–4 and core Event 1 and 2
remain registered formats. Their current canonical layout and per-operation
validation still govern admission and replay.

Each family version distinguishes:

- **decode**: the corresponding canonical format decoder accepts this version;
- **replay**: historical bytes may be preserved and replayed with their recorded
  semantics;
- **write**: the version is eligible for new writes. This is only a format-level
  statement. Authority, per-operation field validation, profile enablement,
  receipts and runtime admission remain owned by production code.

A registered version never makes arbitrary bytes of that version valid. The
registry cannot authorize access or substitute for the real decoder. Unknown
versions are rejected; a format-specific unsupported error remains appropriate.

## Compatibility and migration rules

Supported historical bytes, content identities, original RequestKeys and receipts
are preserved. Opening an existing store or AuditFull compaction is not a
semantic migration. Re-encoding an Invocation under schema 2 cannot manufacture
the accepted-result/tool contract missing from a schema-1 record; similarly,
typed Continuation metadata cannot be inferred during replay.

`PlanMigration` therefore returns an identity-preserving plan only for the same
supported family/version. A different registered destination version returns
`ErrMigrationRequired`; an unknown source/destination returns `ErrUnsupported`.
It performs no filesystem writes, drops no journal records, and never silently
rehashes historical objects. There is no general semantic store migrator in this
release. An explicit future exporter/importer must preserve the original store,
create destination identities and record a source→destination mapping, policy,
source space, omissions and schema provenance before it can be accepted.

Malformed experimental schemas are not additional supported versions. They
require their original implementation to export an explicit recovery artifact;
adding an exception to a canonical decoder would conceal a compatibility error.

## Executable compatibility corpus

`formats/testdata/compatibility.json` contains independently framed fixed bytes
for the object envelope, Branch v1, core Event v2, attributed Agent Event v1,
local hello request v1, Continuation opcode 8 with KnowledgeSource, Control 5
SaveContinuation and finite retry records, and Packet 4 with Requirements.
Event vectors include complete object envelopes and explicit references, not
only an unchecked event payload. Tests invoke the actual public decoders and
encoders, assert byte-for-byte preservation and reject every truncated prefix
and a trailing byte. Existing package-specific golden corpora continue to own
their richer semantic examples, including Knowledge Observation/Packet/Summary.

`formats/testdata/hello-full.json` freezes the exact native enabled-server hello
identity/capability representation. Its test opens a real Store and Unix socket
and compares the service output, rather than testing a fake capability list.
Public codec schema constants are also checked against the registry.
The complete dispatch method inventory is compared to the production service's
static wire routes, including Query-only `get_query_progress`; mutation remains
the existing `commit` transport with the canonical Work envelope.

Core Event v1 remains a live format for non-profile commands. Agent/Work
transactions use core Event v2 with Profile registry 1 and typed Agent Event v1.
Historical core Event v1 is not automatically expanded into inferred Agent
events. These distinct selection rules are part of the compatibility boundary.

## Release status

The registry uses `release: v1.0.0` and `status: frozen` for the current byte
inventory. Strict V1 release acceptance remains open. Its gates include the
final format corpus, admission, replay of supported histories, the four-document
cross-audit and scoped native platform and real-model evidence.

The Continuation source vectors are generated independently by
`agentprofile/testdata/generate_continuation_source.py`. The actual public
Agent decoder checks them via the registered lifecycle/control wrapper, and
all truncations are rejected. `formats/testdata/generate_control_packet.py`
independently produces the Effect, RetryUse and Packet Requirements fixtures.
