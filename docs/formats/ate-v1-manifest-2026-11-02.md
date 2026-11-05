# ATE V1 format and support manifest — updated 2026-11-04

This manifest records the current clean V1 candidate format boundary. Opening or
compacting a supported journal preserves its recorded bytes and identities. Readers
reject unregistered versions before publishing or repairing a store. A future
migration requires an explicit offline export and a new request that keeps the old
receipt addressable.

| Area | V1 versions | Admission policy |
| --- | --- | --- |
| Core object, StateOps, Receipt, RootSet, Event | object v1; event v1/v2 | canonical bytes only; event v1 is the no-profile core occurrence and v2 carries typed profile facts |
| Agent control command | outer schema 1, Control schema 5 | Control schemas 1–4 reject. Schema 5 binds typed Continuation positions, Invocation contracts, immutable effect capabilities, finite guarantees and scoped retry authorization |
| Tool definition | schema 1 | `ATE-TOOL-DEFINITION` binds name, version, schema encoding, dispatch mode/class and the complete effect contract. External and delegate modes are checked at their respective Activity/Spawn boundaries |
| Invocation record | schema 2 | accepted primary result, immutable tool definition/version and dispatch class are mandatory for typed/model decisions |
| Continuation lifecycle record | opcode 8 | typed owner/Step/direct-or-producer-bound knowledge/adapter/next-action position is mandatory; a true pre-seed empty position cannot erase known knowledge |
| Agent ValidationRequirement/Envelope | control opcode 31, current schema | immutable task/goal/artifact/command/environment binding is required for current completion and replay |
| Knowledge Observation | schemas 1–4 | schemas 1/2 remain readable; typed direct/imported records use 3/4 and include `PayloadKind` |
| Knowledge Packet | schema 4 | Schemas 1–3 reject. The packet binds optional exact RecordedInput, Selection and Requirements. Execution primary decisions require the verified structural manifest |
| Annotation record/snapshot | schema 1 | owner-partitioned sourced judgments and append-only withdrawals; navigation Hide never changes facts or execution knowledge |
| Knowledge Summary | schema 1 | source cursor/frontier, exact generation input/output and validation provenance are mandatory |
| Agent Work unit | schema 1 | One RequestKey and receipt for the bounded registered reducers. Root and child seed, authority and producer-bound continuation publish atomically. Bound Emission Claim and external Attempt dispatch also publish together |
| Emission grant | schema 3 | schema 1/2 are rejected; Issue binds projection/exact range digests, dependency closure/digest, policy cut and exact Activity/Attempt dispatch when applicable; Claim rechecks current policy. All model/tool dispatch uses atomic Claim + Dispatch; packet-bound Activities grant the same packet |
| Local protocol | v1 | strict length-bounded JSON; unknown methods and fields reject |

The machine registry marks the listed formats `frozen`. Strict V1 acceptance remains open. A
different format requires explicit export/import or a new release. Unknown versions are
rejected before publication.

The compatibility corpus includes the fixed Control 5 and Packet 4 bytes in
`formats/testdata/compatibility.json`, the package golden tests, the generated
Observation vectors, the Summary vector under `knowledge/testdata`, and the
reopen/compaction tests for Work, Validation and Emission. A malformed old
experimental Observation schema 4 is not silently rehashed: it needs an
explicit extraction/migration program because rewriting it would change a
historical identity.

The supported runtime evidence is intentionally scoped. Darwin/arm64 has
native create/open, fault-injection and compaction runs. Linux/amd64 has a
successful cross-compile; native Linux durability is not certified by this
manifest. Other operating systems remain unsupported by the native directory
backend. No test here is a power-loss certification or a promise that an
external network can recall bytes already sent after a grant is revoked.

Control 5 retains the `Continuation.Position.KnowledgeSource` layout from the
producer-bound pre-release change. The source is mutually exclusive with the
direct KnowledgeFence/Cursor/Key triple. Current readers reject the incomplete
pre-release position and retired Control and Packet versions. The exact binary
layouts, source authentication and seed rules are in the format specifications.
The machine registry's layout labels and independently encoded compatibility
vectors identify the current shape. [Agent formats](agent-core-experimental-1.md) · [Domain formats](domain-core-experimental-1.md)
