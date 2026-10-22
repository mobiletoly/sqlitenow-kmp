# Dependency planning contract candidate

`dependency-planning.json` records local persistence and public dependency-planning requirements.
It is deliberately outside current HTTP/wire fixtures: `localContractCandidates` in
CLASSIFICATIONS.json names its two consumers and exact limited executed coverage.

The paired Go/JVM tests execute state discriminator validation, full typed base copy witnesses (local edit/requeue preserve B,
settlement rebases B, snapshot preserves frozen B), hard row/decoded-byte boundary
arithmetic (including row-first error precedence), exact guard equality, six bounded rekey relationship/order
witnesses and runtime-owner checks.
`componentDecodedBytes` is an already-measured complete standalone envelope in these arithmetic
vectors; these checks do not prove runtime encoding, graph discovery, SCC computation or limits.
Rekey witnesses derive edges only from declared server/desired FK facts and check one-row acyclic
selection using persisted ordinals after a JSON round trip. Each is a closed single-table example:
`serverLive` exhaustively lists known-live keys; other witness keys are known unpublished, never unknown.
Before deriving edges, both consumers require every nonempty `serverParent` fact to have a live
owner and a live target in that list. The dependent-child control therefore declares both P and C live.
Their history descriptions and coalescing
notes are not inputs to selection; SQLite capture, actual restart and pruning remain unexecuted.
The `runtimeScenarios` entries are reviewable Given/Expected assertions with explicit future owners,
not executable runtime traces. Every entry remains `executed: false` in this contract candidate.

The state ownership and lifecycle invariants are in
[oversqlite-dependency-planning.md](../../docs-engineering/oversqlite-dependency-planning.md). Runtime tests must later assert
business payloads, full A/B/L, both relationship views, dirty/outbox identity, source counters,
checkpoint and server state, including reopen. Do not promote scenario ownership validation into
a lifecycle pass. Manifest checking covers this directory as well as the rest of the corpus.

## Logical persisted format (normative local contract; no production DDL)

This logical field/index inventory is part of the dependency-planning contract. Field packing may use the
existing SQL builders; omission, alternate discriminators or additional authority objects require
contract review. All identity strings use exact canonical encoding, with no delimiter-based ambiguity.
The database contract/attachment owner supplies active database/service/account/generation identity;
per-record provenance records its origin explicitly so source rotation cannot relabel historical B.

| Owner | Added/replaced logical fields | Keys, checks and indexes |
| --- | --- | --- |
| Existing `_sync_attachment_state` | Add `attachment_epoch`, a distinct durable positive signed 64-bit attachment incarnation revision, initialized to 1. | Increment atomically on successful incarnation-changing attach/detach, account/source transitions and explicit replacement/reset (including repeated binding values). Preserve on idempotent attachment, reopen, ordinary push/pull/checkpoint and rollback. Overflow fails before mutation. Include in exact plan guard and retained-layout census; preserve historical B and frozen outbox replay semantics. See the engineering contract's attachment incarnation guard. |
| `_sync_row_state` | Keep schema/table/key and row_version; replace deleted flag with `server_kind`; add nullable `server_payload`, required `server_provenance`, boolean `relationships_known`, nonnegative `server_revision`. | Existing identity PK. `live` requires complete payload/version>0; `deleted` payload NULL/version>0; absent/unpublished/unknown payload NULL/version=0 sentinel (not authoritative version). known iff kind!=unknown. Index `(relationships_known,table_name,key_json)` for exceptional unknown coverage. |
| `_sync_dirty_rows` | Keep op, base_row_version, dirty_ordinal and current payload slot; current `payload` now contains only full desired typed object or NULL for deletion. Add required `base_kind`, nullable `base_payload`, required `base_provenance`. Remove pair-valued payload interpretation. | Identity PK and stable `(dirty_ordinal,schema_name,table_name,key_json)` paging index. Present B requires complete payload and positive base_row_version; deleted B requires NULL and positive version; absent/unavailable B use NULL/0 sentinel with explicit kind and provenance. Op/existence/payload consistency checked. |
| `_sync_outbox_rows` | Add exactly the same three B fields; retain base_row_version, local_payload, wire_payload, row ordinal and frozen request identity. | Existing bundle/ordinal and identity uniqueness. Frozen B independent of any newer dirty B. Existing local_payload is full desired typed object or NULL for delete. |
| `_sync_relationships` | `view`, child schema/table/key, `fk_column`, parent schema/table/key, `owner_revision`. | PK `(view,child_schema,child_table,child_key,fk_column)`; inverse index `(view,parent_schema,parent_table,parent_key,child_schema,child_table,child_key,fk_column)`. FK column belongs to pinned admitted shape. Server view references live A revision; desired view references dirty revision; NULL FKs have no edge. Delete edges with their owner in the same writer transaction. |
| `_sync_conflict_state` | Add nonnegative `baseline_revision`; retain dirty_revision and existing identity fields. | Baseline revision increments for A/server-index changes, dirty revision for L/B/desired-index changes; checked counter overflow fails transaction rather than wrapping. |
| `_sync_push_plan` | singleton, `unit`, serialized exact `guard`, phase, seed/cleanup cursors, current DFS root/frame reference, next SCC/diagnostic ordinal, selected row/byte counts, entries/bytes charges and intrinsic-capacity detail when needed. | One active plan per database. Guard equality under writer lock; phase is clear, discover, scc, pack or ready. Counts nonnegative checked int64. Charge metadata itself. |
| `_sync_push_nodes` | plan unit, schema/table/key, owning intent revision, discovery status and next adjacency cursor, DFS index/lowlink/parent/on-stack/stack ordinal, component ID, ready/blocked state and bounded reason, diagnostic ordinal, encoded charge. | PK `(unit,schema,table,key)`. Indexed frontier/status, DFS stack ordinal, component/diagnostic ordinal and ready scheduling. No whole graph sort or payload column; metadata copies charged. |
| `_sync_push_edges` | plan unit, dependent node identity, prerequisite node identity, traversal ordinal, encoded charge. | Unique directed endpoints; outgoing and incoming indexed traversal. Dedup lookup charged even when insert suppressed. Graph rows refer only to the same guarded plan. |

Provenance is a canonical typed metadata object: database_id, service_id, account/scope (explicit
unbound marker before initial attach), generation_id/fingerprint (explicit unadmitted marker before
admission), local_version/physical_shape, origin_source_id, origin (`never_published`, `pull`,
`settlement`, `snapshot`, `explicit_conflict`), optional validated bundle sequence and authoritative
version. Source/generation binding must be explicit before publication. A snapshot exclusion records
its snapshot sequence; a conflict response without sequence records its version/context without
inventing a scope sequence. For missing/corrupt evidence use unavailable/unknown handling, not a
valid-looking provenance object. B preserves its origin even when current A changes.

A/B payload objects contain every admitted column keyed by canonical column name with existing
`storage`/`value` tags. NULL has an explicit null tag; text/blob/numeric values follow existing capture
encodings and admitted codecs. Desired deletion uses the explicit operation plus SQL NULL, not a
partial old-row payload. The exact logical representation is identical across clients even if their
SQL binders differ. Readiness/capture checks enforce complete columns and discriminator coherence.

The new capture/revision triggers and both relationship indexes are part of the retained-object census.
A broken server/desired index-owner invariant is corruption and fails the affected operation before
freeze/apply; it is never silently interpreted as known-empty or repaired by scanning business tables.
Baseline changes, capture and revision/index maintenance share their writer's atomic transaction.

## Rekey representation

There is deliberately no old-key/new-key link or rekey discriminator in this inventory. Capture
atomically writes independent per-key dirty intents. Planner edges derive from FK facts only;
`_sync_push_edges` never gains a synthetic rename edge. Equal durable facts have equal planning
results regardless of local edit history. Freeze/requeue and B retention follow each identity's
existing owners. A never-published intermediate key can be pruned only with authoritative evidence
and no frozen ambiguity; pruning cannot strand a rename link because none exists. Published or
ambiguous intermediate keys remain ordinary work/replay. See the engineering contract's rekey
lifecycle for the explicit lack of remote cross-key atomicity and insertion-before-deletion guarantee.

## A3-02 physical accounting and preservation inventory

`local-apply/a3-workspace.json` declares the exact added column inventory separately
for common owners and Go-only persisted push staging. Both paired consumers pin
this inventory and execute all local arithmetic, work, reservation-transfer, page
and guard cases. `local-apply/a3-pending.json` executes the bounded pending sample,
precedence and complete unchanged-state vectors. These are pure contracts; their
runtime obligations remain unexecuted until separately recorded native evidence.

The existing `_sync_operation_state` singleton owns `workspace_context` (TEXT,
non-null empty default), `reserved_entries`, `reserved_bytes`,
`snapshot_stage_entries`, `snapshot_stage_bytes`, `snapshot_cleanup_ordinal`
(nonnegative checked INTEGER, zero defaults). Go additionally owns
`push_stage_entries`, `push_stage_bytes`, `push_cleanup_ordinal` in that singleton.
Kotlin committed push input is transient and must reserve its complete independent
representations; it does not acquire a new persisted push-stage authority.

The apply plan adds `operation_kind` (pull/settlement/snapshot/conflict),
`preserve_schema`, `preserve_table`, `preserve_key` (non-null TEXT, empty defaults),
`atomic_work` (nonnegative checked INTEGER, zero default), and
`preservation_complete` (INTEGER boolean, zero default). Apply rows add `role`
(intended/survivor/exception) and `owner_revision` (nonnegative checked INTEGER,
zero default); add `_sync_apply_rows_role_ordinal_idx(role,ordinal)`. Existing
identity uniqueness and ordinal indexes stay authoritative. SQL NULL payload
means explicit absence. Exception rows preserve live deviations and must not seed
whole-backlog dependency expansion. No extra payload copy of primary immutable L
or historical B is introduced. The dirty identity PK provides the preservation
keyset `(schema_name,table_name,key_json)`; the cursor is stored after completed work.

Snapshot stage rows, and Go push stage rows, add `charge` (nonnegative checked
INTEGER, zero default). A retained stage row costs one entry and exactly
`64 + 6 * UTF8(identity_strings) + UTF8(payload)` bytes: identity strings are all
stored textual non-payload fields in schema order, including the snapshot ID or
push operation; absent payload contributes zero bytes. Checked multiplication and
addition precede allocation/SQL binding. Owner totals change atomically with stage
rows; bounded deletion subtracts the stored charge and retains its cleanup cursor.
No payload-table SUM is used for an allocation decision. Reopen reconciliation
checks rows in charged, cancellable keyset pages and rejects inconsistent totals.

The full reservation binding is the ordered inventory in the fixture. Temporary
reservations shared by coordinated handles carry that exact operation/context
binding in the existing operation owner. Writer-serialized admission adds every
simultaneous owner before checking limits, entries before bytes. A transfer changes
ownership without charging the same stored representation twice; release occurs
once only after the representation is gone. Cancellation retains the reservation
through resource/job/transaction cleanup. Only acquisition of the existing
exclusive database lifetime lease after the previous owner ended can reclaim
orphaned transient reservations; opening another coordinated client, a writer
lock, a new invocation or elapsed time cannot do so. Retained stage and planner
charges survive reclamation. Supported identities and exclusions are the accepted
SE03 boundaries recorded in the governing ownership clarification.

Limits use checked nonnegative signed 64-bit arithmetic. Page admission is at most
128 rows with a 64 KiB soft byte cap; one larger row requires admission of its full
peak copies first. Ready atomic work is persisted before business mutation. Work
stops retain it; intrinsic capacity failure cannot be labeled resumable. Full guard
changes invalidate derived work while retaining original input and intent.

Stage maintenance uses the exact insert/update/delete trigger inventory declared
in `physicalInventory.stageMaintenance`. Each trigger changes the row's computed
stored charge and its existing operation-owner totals in the same writer statement.
Checked INTEGER constraints reject overflow and underflow; retained-object census
checks the trigger bodies and rejects missing or tampered maintenance. These are
derived stage accounting, not another persistence owner or business relationship
index. Bounded cleanup still owns its work and cursor.

`workspace_context` is empty TEXT without reservations, otherwise a JSON array in
allocation order. Every record has exactly `token`, `unit`, `context` (lowercase
64-hex strings), `entries`, and `bytes` (nonnegative signed int64). The token names
the live allocation; context binds the complete guarded operation. Sum of record
counts must equal the reserved singleton counters. A record persists through every
page/transaction which retains a live copy; transfer never credits independent
copies as shared. A matching token can release once after cleanup, with cancellation
masked only for that cleanup. Fresh lifetime acquisition may clear the complete
abandoned transient array after layout validation; creation of an internal client
or an operation gate cannot clear it.


## SQLiteNow complete-context retained format (normative)

The logical A/B provenance described above resolves through the SQLiteNow-only
`complete-context-v1` physical layout in `provenance-storage.json`. Go retains its
existing physical layout. The wire, request hashes, typed payloads and logical
A/B meanings do not change. This format has no migration or compatibility reader.
Opening an old retained layout must report explicit incompatibility/recreation
required while preserving the database. `local_version` remains the application
representation version; the retained format has its own identity.

The fixture contains exact context/state DDL and the shared observation constraint.
Production owner columns are `server_provenance` / `base_provenance`, now compact
objects with exactly `context_id` (positive integer), `origin` and nullable
`bundle_sequence`. They contain no authoritative version; existing `row_version`
and `base_row_version` own that evidence. `server_context_id` / `base_context_id`
are STORED generated INTEGER references with ON DELETE RESTRICT and separate
indexes on each of `_sync_row_state`, `_sync_dirty_rows`, `_sync_outbox_rows`.
The unchanged owner kind/payload/version constraints preserve live/deleted and
absent/unpublished/unknown or present/deleted/absent/unavailable discriminators.
Observation objects reject unknown/duplicate members on affected reads.

The ten context columns are a complete immutable identity. Text uses exact BINARY
equality and canonical existing physical-shape encoding; the compound UNIQUE
constraint compares every captured field, without digest-only equality. A context
is interned once at admission/writer transitions. Triggers read the active integer
root and copy compact historical observations. Context UPDATE is forbidden by a
retained immutable trigger. Its active pointer is a root, distinct from attachment
epochs: equal context reuse never validates work from an old attachment epoch.

Snapshot installation receives an explicit validated target context for included
and excluded/absence observations: target admitted account/generation/source,
current physical representation. It installs those references before atomically
publishing attachment/source state. It never interns the old current source as
the target. Pending/frozen work still refuses replacement before staging; no
frozen request is requeued or retargeted by normalization.

Anonymous-to-attached and unadmitted-to-admitted binding may atomically replace
references only in A and dirty B owned by those existing transitions. Their new
immutable contexts preserve each captured source. Frozen B stays independent.
Current owners describe current expectations; affected historical reads resolve
their captured immutable identity and validate eligibility separately. Missing,
dangling, corrupt or mismatched evidence follows existing integrity/unavailable
outcomes; unknown B is never treated as absent.

Direct roots are A, dirty B, frozen B and the active pointer. Push plans retain
row identities/revisions, not serialized HistoricalBase objects. Upload journals
retain wire request/binding/outcome bytes; those are not local context references.
Apply rows retain typed payloads or snapshot references, not HistoricalBase.
Transient DirtySnapshotRow/DirtyRowCapture, PushOutboundSnapshot and outbox rows
retain compact references while their dirty/frozen durable owner remains rooted;
freeze/requeue/settlement transfers roots in the existing owning transaction.
A reference must not survive both its writer and its last durable root. No opaque
serialized local-base owner has been found, so no extra retention relation is added.

Cleanup examines at most 32 keyset candidates per slice, ordered by context_id
above cleanup_cursor. For every candidate, at most four explicit indexed root
probes plus at most four SQLite FK enforcement probes execute. Deletion and cursor
advance share one writer transaction. The cursor advances even when every context
is referenced and wraps to zero after exhaustion. Cancellation rolls back the
slice; restart resumes the persisted cursor. Concurrent root changes serialize
under the writer; FK restrictions independently reject dangling deletes. Cleanup
never scans all owners or relies only on the number of contexts deleted.

Logical retained provenance bytes count context text/integer records once, compact
observation bytes, generated integer references and pointer/cursor/format state.
Payload bytes and physical database/WAL/index bytes are separate measurements.
Primary A/B, frozen outbox and primary edge exemptions remain unchanged. SQL
primary identity interning does not create a charged retained derived copy.
Actual host copies/decoded objects and SQL/host derived temporary representations
retain their existing owning charge formulas and live reservations. Prefer no host
context decoding in row loops; any bounded writer-scoped validation memo must be
charged for its live reference entries and must never retain an uncharged complete
context copy. No process-global cache, work-weight or numeric-limit change is allowed.
