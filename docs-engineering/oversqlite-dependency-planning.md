# Oversqlite authoritative state and dependency planning

Status: **design contract; this reference does not attest implementation or qualification.**
This document defines the local state, ownership, lifecycle, resource bounds and public results for
FK-safe push dependency planning in the Go and Kotlin clients. The contract is self-contained;
temporary planning identifiers and review provenance belong in specifications and archived evidence.
The [logical persisted format](../oversqlite-contracts/local-planning/README.md) is its field/index
inventory, and [dependency-planning.json](../oversqlite-contracts/local-planning/dependency-planning.json)
is its shared fixture family. HTTP schemas remain defined by the paired
[OpenAPI document](../../go-oversync/swagger/two_way_sync.yaml); this contract changes no HTTP field,
protocol version, hash domain or admitted FK topology.

Notation: **A** is current confirmed server state, **B** is an intent's historical base,
**L** is current local desired state, and **S** is newly validated server state. These are data-state
symbols, not project phases. An SCC is a strongly connected component of directed prerequisites;
all its operations are indivisible in one request.

## Authority and representation

Use the existing row-state, capture, outbox, application and opening owners. There are two distinct
facts, named **A** and **B** here (internal notation, not new runtime public payload types):

- **A** is the latest validated, durably installed server observation for one identity. It includes
  the complete typed payload when live, existence, authoritative row version when supplied, and
  observation provenance. Its relationships drive server-visible dependency lookup. It is not a
  claim to know remote writes after that observation; ordinary server conflict/FK validation remains.
- **B** is the full typed historical base on which a particular pending local intent began. Every
  dirty intent and frozen outbox row retains B independently of A and desired L. A subsequent local
  edit, failed push, deferred conflict or fresh observation cannot silently replace B with A/S.
  Local operations before admission retain their local contract identity and are bound atomically
  to the admitted generation; they do not invent a server observation.

Identity is `(database_id, service_id, account/scope, generation_id, generation_fingerprint,
canonical schema/table/key)`. Source ID, attachment epoch, observation kind, bundle sequence (when
provided), and authoritative version bind provenance and restart guards. LocalVersion/physical-shape
identity binds the local typed representation. Never compare row versions across generations or
accounts. A source rotation alone does not change a historical B's origin. Bind the new active source
separately; the existing explicit recovery transition remains the only authority to retarget outbox.

Represent A with explicit `live`, `deleted`, `absent`, `unpublished`, or `unknown` kind, not SQL NULL
as a union discriminator. `live` requires a complete typed payload and positive row version;
`deleted` requires a positive tombstone version and no payload; `absent` has no fabricated row version
and requires authoritative absence evidence (including a complete snapshot's exclusion at its
sequence). `unpublished` identifies a locally introduced identity with no accepted server observation:
it has no server edges, but does not prove global server absence or preclude an insertion collision.
`unknown` has no usable relationship evidence. Missing metadata for a previously published row is
integrity failure, never a never-published insertion. Persist explicit absence only for identities
that need it (pending/outbox/recovery references); a full snapshot need not enumerate the key universe.

Relationship knowledge is separately `known` or `unknown`. For known live A, enumerate every admitted
outgoing FK: a typed NULL produces no edge; a table with zero FKs is known-empty. Deleted/absent/
unpublished have known-empty outgoing edges. Unknown is never treated as empty or as every possible
edge. Completeness is tied to the full payload, exact admitted FK shape and its revision. A locally
edited or deleted row retains its server edges until a validated authoritative transition changes A.
A table without outgoing FKs can still have incoming references.

B is explicit `present`, `deleted`, `absent`, or `unavailable`, with the same typed payload and
provenance rules. A never-published insertion has `absent` B with origin `never_published`; authoritative
absence has its own origin. Retain tombstone versions for reinsertion and ordinary concurrency checks.
Every admitted persisted column is captured, including non-FK columns, nulls, exact int64/decimal
values, blobs, JSON-valued columns and generated/non-writable values under the admitted schema.
Use the existing storage-tagged capture representation and codec validation, with canonical typed
column equality; do not round values through floating point, omit columns as an implicit default,
or infer full B from an FK subset, SQLite OLD after prior local edits, or current server S. Generated
columns are retained evidence, not synthesized writable fields. Identity/key columns are not merged.

## Persistence and atomic ownership

### Attachment incarnation guard

The attachment epoch is a distinct durable positive signed 64-bit revision in the existing
`_sync_attachment_state` owner, initialized to 1. It identifies an attachment incarnation, not
the current source/account values and not the in-memory Kotlin opening/publication epoch.
Successful attach/detach, account changes and source rotation increment it atomically when they
change the attachment incarnation. Explicit attachment replacement/reset increments it even
when the resulting binding values repeat. Idempotent attachment operations, reopen, ordinary
push/pull and checkpoint updates preserve it. Failed or rolled-back transitions preserve it;
overflow fails before mutation, without wrapping or resetting. Generation/shape and baseline/dirty
revision guards remain separate. The epoch never retargets historical B provenance or ambiguous
outbox evidence. Saved push plans bind this epoch in addition to source/account; frozen outbox
replay retains its existing priority. This field participates in the coordinated breaking retained
layout census, with no migration or implicit reset.

This clarification was agreed by the owner on 2026-09-27 during A2-03 implementation.

The internal layout has no migration or compatibility reader:

| State owner | Required state and justification |
| --- | --- |
| `_sync_row_state` | The authoritative row record holds A's kind, full typed payload, provenance and relationship completeness/revision. Retain the existing version/deletion semantics where applicable; enforce discriminator/version/payload consistency. One current A per identity, not an append-only history archive. |
| `_sync_dirty_rows` | Store explicit desired existence/payload and B kind/payload/provenance; polymorphic `[current,base]` payloads are invalid. Preserve first B through all local updates. The existing dirty ordinal/revision owns intent ordering and invalidation. |
| `_sync_outbox_rows` | Move/copy the exact selected intent's B with its frozen L and existing wire payload/base version. B survives removal from dirty rows, process restart, rejected requeue and ambiguity. Do not alter wire hash domains to include local metadata. |
| `_sync_relationships` | One indexed projection with `view=server` or `view=desired`, child identity, admitted FK-column identity, typed parent identity and owner revision. Server edges derive only from A; desired edges derive only from current dirty L. This is a maintained index of those owners, not independently editable authority. Outbox first replay means no separate outbox graph/index is needed. Parent-first and child-first indexes include view and context. |
| Existing `_sync_conflict_state` revision owner | Own the authoritative-baseline and dirty revisions. Writers increment the appropriate revision in the same transaction as A/B/L/edge changes, including trigger-driven changes and changes with unchanged row counts. Attachment/generation guards stay with existing owners. |
| `_sync_push_plan`, `_sync_push_nodes`, `_sync_push_edges` | One derived, bounded plan descriptor, keyed nodes with durable DFS/SCC/frontier/candidate state, and directed prerequisite edges. Reuse nodes for frames/membership/diagnostic ordinals; do not add a separate graph engine, scheduler, coordinator or copy of all business payloads. Existing push preparation owns these tables. |

The exact logical fields, discriminators and index inventory are fixed in the paired
[logical persisted format](../oversqlite-contracts/local-planning/README.md) (normative local format, no production DDL).
The physical implementation may factor columns/SQL helpers within these owners, but must not create
another authority, remove any specified invariant, or add an unreviewed persistence object. The new
layout census includes all named tables, columns, indexes, constraints, revision triggers and capture
trigger bodies, including removal of superseded capture SQL. Old version-only baseline layouts and partial/tampered
new layouts fail with the existing format/admission error **before configuration writes, DDL, trigger
replacement, seed adoption, opening-journal mutation or application callbacks**. Perform the existing
read-only retained-object inspection before WAL/other configuration changes, and recheck under the
reserved writer gate before mutation. No implicit reset, best-effort repair, old-capture interpretation
or automatic edge reconstruction on ordinary open. A crash in a valid layout must resume; it must
not be mistaken for old format. An explicit supported fresh database initializes all objects together.

A/B are ordinary per-record storage, exempt from temporary workspace accounting, not unlimited historical archives.
Authoritative server edges and current desired edges are bounded by admitted FK arity times their
owning records and belong to that ordinary storage. Every temporary copy, graph edge/frame or retained
diagnostic plan is charged workspace. Do not exempt a staging copy by giving it a baseline name.

All authoritative transitions update A and server edges before leaving their existing atomic apply
transaction; never derive A from business values after application triggers or preserved L restoration.
Decode validated wire S into the local typed representation directly. Verify final business payloads
separately with the existing survivor-preserving apply mechanisms. A version-skipped row must retain its newer A, B and edges. Equal
version with inconsistent authoritative payload is integrity failure, not permission to overwrite it.

First local capture copies B from valid A (or explicit unpublished absence), before later local writes
can run. A never changes from ordinary local DML. Local UPDATE/DELETE/rekey replaces desired edges
atomically with dirty L; it never replaces server edges. Edits while an outbox exists create a newer
dirty intent with a full base from the then-current A; the frozen outbox's B remains independent.
Publication/settlement atomically installs committed S as A, and rebases **remaining desired intent
for affected keys** to full S, even when local L is restored and the authoritative business write is
skipped. Clear B only when its owning intent is satisfied/disposed or moved intact to outbox.
Unrelated dirty rows preserve B. Rollback/cancellation preserves all pre-transaction A/B/L/edges.

A validated conflict observation may advance A without changing B only in the reached explicit
resolution transaction; mere transport receipt/defer does not install A. KeepLocal/KeepMerged installs
S as A and as B of the remaining explicit retry intent; AcceptServer removes the selected intent only
under existing explicit authorization/guards. Requeue without authoritative rebase restores original
B, including when newer L wins; do not recreate B from requeued local payload. Missing historical B
returns `resolution_base_unavailable` whenever a transition requires it; it is not repaired from S.

Snapshot installation initializes full A/edges for each included row before ready publication; its
complete exclusion proves absence only in that snapshot context. Ordinary clean replacement clears
old-context A/edges atomically. Preserved-outbox recovery keeps frozen B and wire bytes/base versions
exact, even when the snapshot supplies newer A; reapply frozen L without overwriting snapshot A.
Only the already-authorized source-recovery retarget changes source/sequence. Do not silently rebase
an ambiguous frozen request. Any later proven non-commit replan uses existing resolution/requeue
rules. New-layout restart retains this distinction. Generation replacement installs target A atomically
with existing publication; old-generation B/outbox must be drained or explicitly resolved by the
existing opening workflow, never translated heuristically. No internal-layout migration is introduced.

Local DELETE retains A and B until settled. Confirmed deletion drops outgoing server edges but retains
its tombstone A under existing row-state retention; incoming child edges disappear only with each
child's authoritative effect. Absence metadata can be reclaimed only with no dirty/outbox/conflict/
recovery references and no plan depending on it. Invalidation/cleanup must precede that reclamation.
No-op pruning requires proven equality to A, or unpublished insert-then-delete with no outbox ambiguity;
clear only that intent/B/desired edges under its revision guard, keep A and all other captures.
Detach/source/account cleanup follows existing lifecycle authorization, clearing plans and edge rows
with their owners; no cross-account observation reuse. Uninstall explicitly removes the new objects.

## Directed discovery and selection

An edge `X -> Y` means **X requires Y**, so Y must already be authoritative or be sent in the same
or an earlier accepted batch. A row is one desired logical operation per canonical identity. Build
prerequisites with indexed server/desired relationships, not initial-page membership:

1. A child inserting/reparenting to a parent not live in A depends on that parent's pending insertion
   or permitted resurrection. An existing live parent does not pull its unrelated scalar edit into
   the child's component. A missing/unknown required parent gives the safe pending result, not a
   fabricated parent/null. A self-reference to the same pending insertion is a self-edge.
2. A parent delete/rekey-old depends on every server-visible child's pending delete or reparent away,
   including UPDATE captured after the actual `max_push_rows + 1` lookahead. Follow the child's own
   prerequisites, including a newly inserted Q. A clean server child without a safe explicit desired
   disposition cannot be invented as a delete. Current local desired references that still require
   the disappearing parent also block the destructive candidate; they are not ignored because A
   had a different parent. Local FK action-generated intents are ordinary captured operations.
3. A rekey captures delete-old and upsert-new as independent per-identity intents. It creates no
   synthetic old-delete -> new-insert prerequisite. Only actual server/desired FK relationships add
   edges: if server child C points to P and desired C points to new Q, delete P requires C's reparent,
   and C's reparent requires Q's insertion. Without that relationship, deleting P and inserting Q
   are independent, even when both came from one local key update. Never infer a pairing from payload
   similarity, ordinals, adjacent capture events or transaction history. No table-level union or
   unconditional reverse dependency. Mutual delete/cyclic insert prerequisites produce SCCs;
   acyclic chains are splittable.

Discover a root's complete relevant prerequisite graph through indexed keyset pages. An initial dirty
page is a scheduling seed only. Unknown coverage cannot be disproved by scanning only the first page:
retain indexed unknown-row markers/completeness in row-state, and conservatively block destructive
roots in the applicable admitted FK tables until evidence is available. Do not merge their unrelated
operations into one group. Such uncertainty is exceptional; fresh supported lifecycle paths must
populate complete A. Later remote writes remain subject to ordinary server validation.

Collapse SCCs with an iterative durable traversal; no recursive host graph or unbounded recursive SQL.
Order the resulting prerequisite DAG deterministically by ready component's earliest dirty ordinal,
then canonical schema/table/key (bytewise canonical encoding). Canonical wire order/hash rules inside
a frozen request remain those of the existing compact transport; traversal order never rewrites committed history. A selected
candidate is prerequisite-closed against A plus earlier committed components. Request packing may
combine ready components; it must not convert a reachable acyclic closure into an indivisible unit.

Keep independent work sendable. A root with unknown evidence or an oversized SCC is marked blocked
in the guarded plan; scan subsequent roots with a saved keyset cursor within the work budget. The
one-bundle primitive sends a valid available batch first. If none is available, return the earliest
blocked result; at work exhaustion return `sync_work_limit` with saved progress, not a guessed
oversize. A high-level call can commit independent batches then report the remaining blocker with
confirmed progress. Do not report convergence or pull while dirty/outbox work remains. Preserve the
single existing 64-attempt budget, including local preparation/no-op attempts and nested recovery;
no planner-owned drain loop or second counter. The no-progress detector recognizes durable traversal
progress; a typed work/capacity result terminates the invocation without an automatic retry spin.

### Rekey capture, coalescing and partial progress

The local UPDATE transaction atomically records the old identity's deletion and the new identity's
upsert through the existing capture owner. Each key keeps its own full B and latest desired L; no
old/new association is persisted. This is per-record synchronization, not a remote atomic-rename API.
A planner treats observationally identical persisted states identically after reopen, regardless of
whether they arose from one key update or an unrelated delete and insert.

For a known-live P -> unpublished Q with no dependent relationships, both operations are ready.
The existing earliest-ordinal rule may choose delete P before insert Q under a one-row hard limit.
A crash between accepted batches may therefore leave neither key present remotely. An insertion
conflict or validation failure after that deletion preserves Q's pending intent, reports the confirmed
deleted prefix and follows ordinary error/conflict handling. It neither restores P automatically nor
claims that the local UPDATE was a remotely atomic rename. Actual FK prerequisites still block any
unsafe removal; the planner never deletes a clean dependent merely to send a rekey.

Coalesce by identity and authoritative evidence only:

- For P -> Q -> R before any publication, where P is known live and Q/R were unpublished, retain
  delete P and upsert R. Q's unpublished insert-then-delete is a safe no-op only under the existing
  no-ambiguous-outbox guard. No P/R pairing or historical chain needs to survive pruning.
- If Q was already published, its later deletion is real work with Q's own B and server relationships;
  if Q is frozen in an ambiguous request, retain/replay it exactly before planning later operations.
  Never prune Q merely because it was an intermediate key in local edit history.
- A return to an earlier key is an ordinary upsert of that identity with its own B. Apply the same
  complete equality/absence no-op rules; do not transfer B or row versions between identities.
- Freeze moves each selected operation's B/L into the existing outbox independently. An unfrozen
  counterpart remains dirty. Requeue restores that key's original B and latest desired L under the
  existing proven-non-commit rules. Neither freeze, settlement nor requeue invents or preserves a
  rename association. A newer dirty edit cannot rewrite ambiguous frozen bytes.
- Settlement and cleanup remove/rebase only the affected per-key owners and their derived edges.
  Pruning a transient unpublished key drops its desired projection; its actual capture/prune revisions
  invalidate a plan normally. Restart rebuilds prerequisites from durable A/L/index state and the exact
  guard, or replays the frozen outbox first. It never depends on an in-memory key-update event.

## Bounds, restart and local public results

Effective soft rows/bytes are the existing local targets capped by discovery hard limits. A whole
ready SCC may exceed soft targets only as the first component of an otherwise empty batch and only
within hard row, complete decoded request, and expanded individual-row limits. Stop packing a batch
before the next component crosses a soft/hard bound; send it in a later batch if it fits alone.
Count the exact complete compact UTF-8 JSON document (source-bundle ID digit width, initialization
ID when present, fixed hash, group boundaries, escaped payloads, punctuation); compressed size is
irrelevant. Preserve `push_row_exceeds_admitted_limit` for a too-large single expanded row and
`invalid_admitted_contract` for a legal single row whose complete envelope cannot fit, under the retained transport contract.
Only a proven multi-row SCC exceeding a hard request bound is a dependency-group error. Row-first
precedence if both dimensions exceed; never label a partially discovered closure an SCC.

Use the shared workspace configuration and accounting described below, not separate planner allowances.
Charge descriptor/node/edge/DFS-SCC frame/member/candidate metadata and every simultaneously resident
key/payload/encoded-body copy before allocation. Every indexed lookup, duplicate lookup, edge visit,
comparison, SCC step, staging/cleanup write and repeated replan is work. Reserve exact encoded lengths
or safe checked bounds before materialization. Keyset pages visit at most 128 entries and at most
64 KiB of encoded working content; one larger admitted entry may use a single-entry page only after
reserving its full size against remaining aggregate capacity. Each row may still have large fanout:
persist its next edge cursor. Check cancellation each page and within 256 charged steps. Overflow
is a typed capacity/admission failure before allocation. Account retained survivor-apply workspace together
with planner state.

Persist one plan with phases `clear`, `discover`, `scc`, `pack`, `ready`, and bounded diagnostic state.
Its guard binds database/service/account, attachment epoch/source, generation/fingerprint/local shape,
registration order/FK shape, dirty revision, baseline revision, checkpoint, source next-ID, outbox
identity, and admitted hard limits. Own scalar cursors, node DFS lowlink/stack state, SCC membership,
blocked-root state and charges transactionally. Read/write bounded pages under short reserved writer
transactions, release before HTTP/callback. After reopen, resume only the exact guard. Local writes,
requeue/prune, A changes or source/layout/limit changes invalidate it. Conservative invalidation on an
unrelated local write is permitted by this contract; this makes no unconditional convergence promise amid local
writes. Freeze rechecks the guard at the writer lock; stale candidates send nothing and consume no ID.

Stale plan cleanup is indexed, paged, charged, cancellable and restartable in `clear`; free a charge
exactly once with the deleted record. Do not allocate a replacement beside uncharged obsolete state.
After successful freeze, the outbox is authority for exact replay; the derived plan is invalid and
cleaned in bounded steps. Completion, explicit diagnostic release, detach and replacement use the same
cleanup owner. Release never alters primary intent/outbox/A/B. No time-based eviction can erase replay
evidence. Lowering workspace limits below retained use fails before mutation; read/release remains
available. A work-only interruption retains useful traversal; intrinsic capacity returns stable
non-resumable detail until data/limits change, rather than restarting identical work endlessly.

These **contract shapes** apply to both clients and map through Swift's existing facade/error boundary.
Numeric counters are nonnegative signed 64-bit values in both languages;
limits/page sizes are positive and checked before narrowing. No new HTTP fields/endpoints:

| Go / Kotlin name | Exact semantic fields |
| --- | --- |
| `PushDependencyGroupExceedsAdmittedLimitError` / `PushDependencyGroupExceedsAdmittedLimitException` | `reason=push_dependency_group_exceeds_admitted_limit`, `dimension=rows|decoded_bytes`, `limit`, `actual` (exact, complete SCC), `componentRows`, `componentDecodedBytes` (complete standalone envelope at guarded next ID), `diagnosticId` (opaque 64 lowercase hex), `sample` (bounded record refs), `hasMore`, `progress`. Use this shape for both dimensions; no separate Kotlin row-only exception or compatibility alias. |
| `FKReconciliationRequiredError` / `FKReconciliationRequiredException` | `reason=fk_reconciliation_required`, `detail=relationship_unknown|dependency_unavailable|historical_repair_required`, `unit` (64-hex guarded plan identity), bounded `sample`, `hasMore`, `progress`. No busy retry, no claim of completed reconciliation. This pending result is a prerequisite for safe dependency planning. |
| `ResolutionBaseUnavailableError` / `ResolutionBaseUnavailableException` | `reason=resolution_base_unavailable`, `unit`, bounded `sample`, `hasMore`, `progress`. Missing/corrupt full B for a required transition; no inference from current S. This base-error prerequisite does not define automatic merging. |
| Existing `SyncWorkLimitError/Exception`, `SyncWorkspaceLimitError/Exception` | Preserve the existing fields/reasons described below; extend `unit` to the guarded push plan and attach the same `progress`. Entry/byte limits remain hard; work becomes a soft invocation target under the snapshot-capacity amendment. |
| `SyncProgress` (both) | `dirtyRows`, `outboundRows`, `committedBundles`, measured at the final guarded local read; committedBundles counts unique confirmed bundles in this public invocation, not HTTP attempts. Earlier committed prefix is never rolled back or hidden by a later planner error. |
| `PushDependencyDiagnosticPage` (both) | `diagnosticId`, `identities`, optional `nextCursor`; entries ordered by canonical schema/table/key, no payload. |
| `Client.ReadPushDependencyDiagnostic(ctx,id,cursor,pageSize)` / `readPushDependencyDiagnostic(id,cursor,pageSize)` | Local paged read; default 64 entries, maximum 128, soft target 64 KiB encoded identities. A larger single identity uses a one-entry page only within the admitted expanded-row maximum and reserved aggregate workspace; otherwise return typed workspace bytes capacity with its bounded hash. Never truncate a key or allocate it unreserved. |
| `Client.ReleasePushDependencyDiagnostic(ctx,id)` / `releasePushDependencyDiagnostic(id)` | Local idempotent release of that retained plan's diagnostic; paged charged cleanup may return sync_work_limit. Does not clear intent. |
| `StalePushDependencyDiagnosticError` / `StalePushDependencyDiagnosticException` | `reason=stale_push_dependency_diagnostic`, bounded `diagnosticId`; missing, released, mismatched revision/account/source or malformed/cross-context cursor. No fallback to another plan. |

Go fields use exported UpperCamelCase; Kotlin fields use lowerCamelCase. Record refs carry schema,
table and canonical typed key; returned samples total at most 16 refs/4096 encoded bytes, and may be
empty when one identity is larger. `hasMore` is true when refs were omitted. Error messages expose
only bounded metadata, never captured payload or raw SQL. Only the SCC error needs paged membership;
other errors' omitted refs are rediscovered on a later guarded attempt, not a hidden unbounded list.
Diagnostic IDs/cursors are opaque context/revision-bound tokens; cursor carries a bounded last ordinal
and plan binding, rejects tampering, and cannot cross accounts or revisions. Retain membership in the
same charged node rows, no second report journal. If capacity prevents complete SCC discovery, return
workspace/work failure rather than claim exact SCC counts or incomplete paged membership as complete.

Pre-freeze failures leave business values, A/B/dirty intent, source next-ID, checkpoint and outbox
unchanged except authorized independent prior commits or separately committed safe no-op pruning.
They may commit charged traversal/cleanup and diagnostic metadata. Cancellation/deadline wins over a
newly constructed incomplete error; current page rolls back, earlier staged pages survive. If outbox
already exists, replay takes precedence over planning/diagnostic staleness. Never replace ambiguous
wire bytes, clear its B, reserve another ID, or block its settlement behind irrelevant stale-plan work.

### Shared workspace configuration and failures

Go options are `MaxSyncWorkspaceEntries`, `MaxSyncWorkspaceBytes` and
`MaxSyncWorkUnitsPerInvocation`; Kotlin/Swift use `maxSyncWorkspaceEntries`,
`maxSyncWorkspaceBytes` and `maxSyncWorkUnitsPerInvocation`. Current defaults are 131,072 entries,
268,435,456 bytes (256 MiB) and a soft target of 1,000,000 work units. Values are positive; omitted zero-valued
Go options select defaults. Limits aggregate per database across all simultaneously retained
working state. Primary business, dirty/outbox and ordinary per-record baseline storage is exempt;
every temporary copy is charged. Existing snapshot and wire limits also apply; the strictest wins.

`SyncWorkLimitError` / `SyncWorkLimitException` carries `unit`, `limit`, `used`, reason
`sync_work_limit` and the progress defined above. This is internal bounded-slice/compatibility evidence. Ordinary public operations resume local work inside the library; customers do not retry a scheduling yield.
`SyncWorkspaceLimitError` / `SyncWorkspaceLimitException` additionally carries `dimension`,
`reserved`, `resumable` and reason `sync_workspace_limit`. Current dimensions are `entries` and `bytes`.
The bounded unit hash identifies the guarded operation. Neither incomplete result consumes a source
sequence or checkpoint. Preparation yields only at a durable boundary. An otherwise admissible first
ready atomic operation runs even when it crosses the work target; crossing alone does not roll it back.
After completion, later units may cross internal slice boundaries with retained progress. Never split a committed bundle to fit
the work target. Stale cleanup does not refund work already spent during the invocation.

## Synchronization ownership

The existing push preparation owner performs guarded paged discovery, durable SCC traversal,
ready-component packing and a final writer recheck before the existing freeze/hash/send/replay path.
Ordinary preparation adds no network call. Frozen outbox replay takes precedence over a new plan.
The public one-bundle primitive and single shared 64-attempt high-level loop own drainage; pull
runs only after local dirty/outbox work is drained. There is no planner coordinator or retry wrapper.

## Storage rationale

The layout adds four persistent objects to the existing owners: one relationship projection and
three bounded plan objects.
They are justified by indexed inverse lookup, paged SCC discovery and crash resume. The existing
row-state record owns current full A; dirty and outbox own historical B. This deliberately stores full
payloads twice while intent exists; A may advance while B must not. There is no separate history log,
B repository, diagnostic journal, generic graph engine or scheduler. Desired and server edge views
are derived from different existing facts and cannot be edited as independent truth.

`_sync_apply_edges` belongs to survivor application and describes current business relationships,
including clean/dirty/local relationships absent from A. Its scan per apply and workspace charges
remain part of that owner. The planner never reads it as server authority; its distinct purpose
does not create a competing dependency authority.

## Complete baseline lifecycle matrix

“A” and “B” have the meanings defined above. All updates below include owner revision and edge-index changes.
“Atomic apply” means the existing reserved SQLite writer/apply-mode transaction with rollback,
FK enforcement and survivor/final-payload checks; no HTTP or user callback inside it. Contract-only
checks of the fixtures do not execute these transitions.

| Transition | Concrete Go owner | Concrete Kotlin owner | Transaction invariant / contract witness |
| --- | --- | --- | --- |
| Fresh runtime before application access | `prepareRetainedContractWithFinalize`, `initializeDatabaseInTx`, `installClientRuntime` | `SyncRuntimeInitializer`, `inspectRetainedRuntime`, `OversqliteOpening.publishReady` | Read-only census before configuration, recheck under writer; all new objects initialized before ready. Old/partial objects reject before any mutation/callback. `format-rejection`. |
| Preexisting local rows / anonymous capture | `capturePreexistingAnonymousRowsInTx`, `captureAnonymousTableInTx` in lifecycle_state.go | `SyncRuntimeInitializer.adoptExistingRowsForTable` | Existing initial-bind writer transaction creates L, explicit absent B and unpublished A, desired edges; never calls local rows server-live. Admission remaps namespace and binds full contract together. `attach-unpublished`. |
| Attach initialized-local / initialized-empty | `connectLocked`, `persistConnectedLifecycleState`, `installAdmittedContract` | `OversqliteClientLifecycleCoordinator.attach`/`persistConnectedLifecycleState`, `OversqliteContractGateway` | Connect lease/binding transaction records context; initialization is not publication. Preserve unpublished B and exact outbox on resume. Stale initialization cleanup cannot discard B while intent survives. `attach-unpublished`. |
| Attach remote-authoritative | `beginRemoteReplaceLocked`, `finalizeRemoteReplaceLocked`, `applyStagedSnapshotLocked` | lifecycle coordinator `beginRemoteReplace`/`finalizeRemoteReplace`, download `applyStagedSnapshot` | Explicit existing remote-replace lifecycle authorizes installation; stage before writer, atomically install A/edges and publish binding. No window for user writes before full base exists. `snapshot-clean`. |
| First/subsequent UPDATE, INSERT, DELETE, rekey; second connection | `triggerRenderData`, generated managed capture triggers | `createTriggersForTable` in OversqliteTriggers.kt, managed capture triggers | Same application DML transaction copies full B once from A, preserves B on later edits, updates L/desired edges/revision; A unchanged. Rekey keeps separate old/new B (new absent unless authoritative evidence exists), without pairing state; apply per-key coalescing and partial-progress rules above. `local-reparent`, `typed-base`, `rekey`. |
| No-op prune | `collectDirtyRowsForPushInTx`, `freezePushOutboundSnapshotInTx` | StageStore `preparePush`, PushWorkflow `persistPreparedOutboxSnapshot` | Writer verifies complete equality/explicit never-published delete, revisions and no ambiguous outbox; clear only matching dirty B and desired edges. A/server edges remain. `noop`. |
| Freeze | `ensurePushOutboundSnapshot`, `freezePushOutboundSnapshotInTx` | PushWorkflow `ensurePushOutboundSnapshot`/`persistPreparedOutboxSnapshot` | Guarded writer transaction, exact preflight before outbox; transfer B intact with frozen L, remove dirty/desired edges for selected rows, retain A/server edges. No ID advance on planning refusal. `freeze-reopen`. |
| Local edit during frozen request | Same capture triggers; `loadDirtyUploadStateInTx` | Same capture triggers; SyncStateStore `loadDirtyUploadState` | Separate local writer captures new B from current A without touching frozen B/L/hash; dirty revision invalidates derived plan. `edit-during-outbox`. |
| Publish/committed settlement including normalized/extra effects | `applyStagedPushBundleLocked`, `applyPushSettlementEffectsInTx`, `restorePushSettlementIntentInTx`, `finalizeStagedPushBundleInTx` | PushReplay `applyCommittedBundle`, `buildReplayPlans`, `OversqliteBundleApplier.applyAuthoritativeRow` | One atomic apply installs S as full A even when preserving L; B of remaining affected intent becomes S. Unrelated B stays. Verify survivors, clear settled outbox, advance source and checkpoint atomically. `settlement-rebase`, `extra-effect`. |
| Lost response / crash before or after local settlement | `stageAuthoritativePushRows`, `validateAuthoritativePushAgainstOutbox`, settlement above | PushWorkflow `loadPersistedOutboxSnapshot`/`commitPushOutboundSnapshot`, PushReplay above | Frozen B/bytes survive. Exact committed replay installs same A once, never double advances counters; transaction rollback exposes old complete state. `freeze-reopen`, `settlement-rebase`. |
| Rejection / restore / requeue | `restoreOutboundSnapshotToDirtyRows`, `requeueSnapshotRowsInTx`, `requeueDirtyIntentInTx` | PushConflict `restoreOutboundSnapshotToDirtyRows`, SyncStateStore `requeueSnapshotRows`/`requeueDirtyIntent` | Proven non-commit transaction restores original B and latest desired L (newer dirty wins); no implicit rebase and no server-edge change. Frozen B retained until move completes. `requeue-newer`. |
| Ordinary pull, update/delete and version skip | `applyPulledBundleLocked`, `applyPulledRows`, `applyBundleRowAuthoritativelyInTxUsing` | DownloadWorkflow `applyPulledBundle`, BundleApplier `applyAuthoritativeRow` | Clean gate and original hash/order validation retained. Atomic apply updates A/edges for accepted newer rows; skipped newer local version retains its A; checkpoint advances with completed bundle only. `pull-version-skip`. |
| Dirty/base preservation around application-trigger effects | `restoreTriggerAffectedDirtyIntentInTx`, current-business survivor stage | PushReplay `restoreDirtyBusinessRows`, current-business survivors | Captured authoritative wire S, not trigger-modified L, supplies A. Restore all protected L/B and desired edges consistently; failed payload/FK verification rolls back. `extra-effect`. |
| Confirmed server deletion vs local deletion | Authoritative apply and settlement; capture triggers | BundleApplier/SyncStateStore and capture triggers | Local delete keeps A/server relationships; confirmed tombstone updates A/version and drops outgoing edges; each child effect owns its edge removal. B retains prior full row until intent ends. `local-reparent`, `tombstone`. |
| Clean snapshot/rebuild/checkpoint-history recovery | `rebuildFromSnapshotWithOptionsLocked`, `installStagedSnapshotRowsInTx`, `finalizeSnapshotApplyStateInTx` | DownloadWorkflow `rebuildFromSnapshot`/`applyStagedSnapshot`, SyncStateStore `installAuthoritativeBaseline` | Download validates complete stage; final writer guard checks dirty/outbox/lifecycle. All A/edges replaced with snapshot view; no stale edges for excluded rows. Full A present before later local write. `snapshot-clean`. |
| Snapshot preserving prepared outbox / source recovery | `reapplyPreparedOutboxIntentLocallyInTx`, `retargetPreparedOutboxInTx`, snapshot finalizer | DownloadWorkflow `replayPreparedOutboxIntent`, OutboxStateStore `rebindBundleSource` | Snapshot gives A; frozen historical B/wire remains exact. Restore L without making it A. Rebind source only under existing authorized recovery proof. New source sequence semantics unchanged. `snapshot-outbox`. |
| Generation opening/ready publication | `Opening.resumeTargetSnapshot`, `publishReadyClient`, existing upgrade finalizer | GenerationOpeningWorkflow `rebuildAndPublishGeneration`/`publishCompletedGeneration` | Existing supported app-generation workflow owns transition; old intent drained/resolved before target full A ready. Old internal sync layout still rejected, never migrated. `generation-publication`. |
| Reached explicit conflict defer/retry | `preserveConflict`, `guardConflictDecision`, `restoreOutboundSnapshotToDirtyRows` | PushWorkflow `retryConflict`/`resolveConflict`, ConflictStateStore guards | Receipt/defer retains A/B/L/outbox. Retry changes no base merely by receipt. Unknown dependency/missing B returns typed pending/base failure with original intent. `conflict-pending`. |
| Reached AcceptServer/KeepLocal/KeepMerged | `resolvePushConflictAcceptServer`, `applyConflictResolutionPlan`, `applyConflictStateAction` | PushWorkflow `resolveConflict`, PushConflict `applyConflictResolutionPlan` | Existing explicit decision + revision guard + atomic safe apply; install validated S as A, remove selected B for acceptance or set retry B=S. Preserve unrelated B/newer captures. Missing closure/repair blocks before mutation. Automatic conflict policies and resolution reports are separate work. `conflict-rebase`. |
| Plan restart, stale cleanup, diagnostics | Existing push preparation / `beginImmediateTx` | StageStore/PushWorkflow + `TransactionMode.IMMEDIATE` | Short bounded staging transactions; exact guard resumes; stale plan clears with charged pages. Diagnostic cursor bound to same plan/context; release never clears A/B/outbox. `resume-cleanup`, `diagnostics`. |
| Detach/source/account/uninstall | `detachLocked`, `UninstallSync`, existing recovery/source-state owners | lifecycle coordinator `detach`/`executeDetach`/`clearFullLocalSyncState`, SourceStateStore/AttachmentStateStore (no existing standalone Kotlin uninstall API) | Existing pending-work gate and account authorization retained. Clear plan/index with owning records, retain source-origin B while referenced; no wrong-account reuse. Explicit uninstall removes new objects. `detach-cleanup`. |

All rows are baseline-lifecycle obligations, including snapshots and reached conflict paths.
Required guards include exclusive operations, dirty revision, immutable outbox identity, source/generation/attachment,
opening mutation authority, snapshot final gate, FK enforcement, cancellation, and payload verification.
Go preconfiguration inspection belongs to `prepareRetainedContractWithFinalize`; there is no second
opening protocol.


## Aggregate ordinary-operation workspace and safe pending outcomes

This section extends the shared contract to settlement, pull, snapshot/recovery and reached explicit
conflict application. It is a design contract, not a statement of current implementation. The
existing options, public result names and signed int64 fields above remain the public surface.
No new endpoint, transport field, protocol/hash domain, policy, report or supported topology is added.

### One accounting owner and exact capacity semantics

Storage limits are per local database within the supported lifetime-ownership domain defined below,
including concurrent handles and all retained derived state;
work is per outer public invocation. Push, pull, sync, rebuild/recovery, explicit resolution and
sync-then-detach enter the existing exclusive operation owner. Internal calls, retry/recovery rounds,
replay, cleanup and detach rounds inherit the same work meter and existing push-attempt allowance.
An independent later invocation gets a new work allowance, never a new storage allowance. Short
reserved-writer transactions serialize admission and reconcile retained totals; host-only counters
cannot authorize another handle to over-admit. No transaction spans HTTP or a resolver callback.

#### Supported database identity and reservation lifetime

A3 retains the existing same-runtime lifecycle-ownership boundary in the [SE03 implementation
request](../specs/sqlitenow-schema-evolution-se03-implementation-request.md#ordered-implementation-with-checkpoints),
Ordered implementation with checkpoints, item 2. Hard-link aliases, mixed-runtime
ownership and hostile-filesystem behavior remain outside the completion contract. This clarification
adds no lifecycle lease, persistence owner, ownership protocol or supported filesystem topology.
Go's existing stronger device/inode exclusion behavior is preserved.

The database identity is the one admitted by the existing lifetime owner: Go's physical-file
identity, Kotlin native canonical-path identity (including aliases that canonicalize to the same
path), or the persistent browser worker's database identity and lifecycle Web Lock. Node retains
transient per-connection identity and has no persistent restart claim. All admitted handles and
derived stages for the same supported identity share one retained/reserved storage budget. A second
independent managed owner is refused while that identity's lifetime lease is held. Internal clients
sharing a coordinated opening's lifetime owner do not receive independent storage allowances.
Another SQLite connection acquiring the writer does not acquire admission or reclamation authority.
Independent business-writer connections remain subject to the existing revision guards and final
writer revalidation; this ownership clarification does not exclude their writes from preservation.

Reservation admission, transfer and release use short writer transactions within the existing
operation/stage owners. Reservations crossing a transaction/page boundary retain their charge and
exact operation/context binding there; a client-local counter alone is not the database ledger.
Transient charges may be released only after their last charged copy is gone. Cancellation retains
ownership until active work, rollback and cleanup have completed; it is not itself abandonment proof.

A later opening may reclaim an orphan transient reservation only after successful reacquisition
of the existing exclusive database lifetime lease proves that the preceding lifetime owner has
ended. A new Client, operation gate, writer transaction, timeout or TTL does not supply that proof.
Multiple internal clients under one still-live opening owner must share the reservation state and
cannot reclaim each other's live charges. Retained committed input and derived stage rows remain
charged independently; reclamation of orphan transient charges never clears those rows, primary
intent, historical B, frozen outbox/replay identity or checkpoints. Existing format-before-mutation,
retained-limit admission and charged cleanup rules continue to apply.

Before production changes, the local-format inventory and paired consumers must record the exact
reservation fields, lifetime/operation binding, transfer/release and reacquisition proof within
the existing owners. Focused runtime evidence must cover second-handle refusal or shared-owner
accounting as applicable, supported canonical aliases, cancellation with live buffers and physical
process/worker death followed by reacquisition and reclamation. The isolated hard-link mechanism
witness is excluded-topology evidence, not fulfillment of these supported runtime cases. Stop for
a further contract amendment if this representation requires a new owner or ownership protocol,
or if the existing lifetime lease cannot establish exclusion for a supported identity.

For entries and bytes, `used` is already retained plus currently live temporary reservations across
all owners; `reserved` in a refused request is the additional requested amount, not already included
in `used`. Release transient reservations when the last charged copy is released; transfer a
reservation to a persisted copy atomically, without a double credit or uncharged interval. Count
simultaneous decoded/encoded/local-form representations separately when they are separate copies.
Use checked arithmetic before addition, multiplication, encoding, narrowing or allocation. Report
entries before bytes when both fail. Overflow returns typed capacity with bounded unit identity;
never wrap a counter, allocate first, or serialize an entire value just to measure its size.

| Working state | Owning facility and aggregate charge |
| --- | --- |
| Push traversal, SCC/frontier and retained diagnostics | Existing push-plan/node/edge owners, descriptor/cursors and temporary pages; keep exact A2 accounting and include other owners in admission. |
| Survivor rows/current-business edges and preservation exceptions | Existing apply-plan/rows/edges, descriptor/cursors, every stored copy and simultaneous page. They are derived from guarded current state, never A/B authority. |
| Validated committed-push stage | Existing `_sync_push_stage`; charge staged effects, their identity/metadata and every decoded effect/decision/map/page copy. Primary frozen outbox remains exempt, its copies do not. |
| Snapshot download/install stage, including generation opening | Existing `_sync_snapshot_stage` and existing snapshot/opening progress owner; charge each row, metadata and concurrent fetch/decode/apply buffers. Enforce existing snapshot totals and transport limits too. |
| Pull bundle, explicit conflict decision and preservation buffers | Existing download/conflict/apply owners; charge complete retained validated input, decisions, protected copies and pages even without a new durable table. Wire limits alone do not exempt these allocations. |
| Primary business, current A/server edges, dirty L/B/desired edges, frozen outbox/B | Ordinary storage exemptions remain. Direct indexed reads can avoid a redundant preservation copy; every host/staging copy and every comparison/visit is still charged. |

A row identity plus its payload and fixed metadata is one entry; an independently retained duplicate
representation is another entry. A descriptor is one entry with its encoded metadata/cursors. Existing
planner combined node/frame representations earn sharing credit only where storage is actually shared.
Charge canonical encoded keys/payloads and an explicit conservative metadata bound, not SQL file size
or RSS. Native heap/RSS is separately measured, since encoded quotas do not measure allocator overhead.
The implementation must document each reservation formula next to its owner and fixture witness.

Each indexed row/edge visit, failed/duplicate lookup, comparison, conversion, stage mutation, cleanup
step and restore/recheck pass consumes work. A bounded conservative upper bound may reserve several
steps together; it must cover actual visits and cannot hide a full scan, sort or recursive closure as
one unit. Explain the query plan for keyset paths. Include business-table clear, baseline/edge updates,
FK/survivor verification, snapshot finalization, and application-trigger-induced restoration visits in
final-apply preflight. Check cancellation each page and within 256 charged steps, including the final
transaction. Arbitrary database/trigger execution must not become an unbounded nominal step: preserve
existing statement cancellation and stop qualification if its work bound cannot be demonstrated.

Host page cap is 128 entries with a 64 KiB encoded soft target and the remaining aggregate capacity.
One larger admitted row uses a one-entry page only after reserving its full copies and metadata;
existing expanded-row limits still apply. Do not allocate a complete key/payload page before checking
its byte lengths. A complete atomic wire unit can exceed a page; page its durable input/decisions
without splitting its commit. No new public page-size option is introduced.

The internal `sync_work_limit` signal retains useful safe staging/cleanup progress and is consumed by the same public operation. `sync_workspace_limit` with dimension
`entries` or `bytes` stops before the refused allocation, freeze or destructive apply. `resumable=true`
means retained charged progress can advance without changing intent or limits (for example bounded
obsolete-state cleanup can release capacity); it does not promise automatic retry. Intrinsic entry or
byte capacity remains non-resumable until explicit data/limit action. The historical
`atomic_apply_work` preflight is removed. `used` is approximate work charged this invocation and may
exceed the target after a ready atomic unit. A prepared ready unit runs on a fresh invocation despite
its size; if earlier durable preparation exhausted the target, retain the ready plan and dispatch it
on the next internal slice of the same public operation. Preparation must not repeat merely to exhaust the same target again.

At open, validate format before any mutation, then validate aggregate retained charges with checked
arithmetic under the existing admission rules. Lower limits below retained use reject before changes;
read/release facilities remain available under their existing ownership. Stale cleanup is keyset-paged,
charged, cancellable and restartable, and frees each charge exactly once. Preserve required committed
input/outbox evidence even when space is full. Cleanup of irrelevant plans must not block exact replay;
replay may still return a genuine aggregate capacity result, never ignore the quota. Cancellation
rolls back the current transaction, retains earlier safe stage pages and preserves earlier committed
bundles. Release has no authority to delete primary intent or rewrite checkpoints.

### Preservation without another complete backlog copy

Use primary dirty/outbox L and B directly while apply mode prevents managed recapture, with exact
revision/outbox guards. Save only affected desired overrides, supported pre-apply live deviations,
and clean/version-skipped survivors that lack an immutable primary source. A primary reference must
remain valid throughout its use: compare/rebase affected keys in the existing atomic transition, and
stage their old expected values before changing that owner. Never infer B from a restored business row.
No complete host list/map of dirty keys or payloads, no whole-backlog TEMP payload mirror, and no new
persistent copy of the full backlog per settlement is permitted.

Application triggers can reach dirty rows outside the FK closure. The safety check may therefore
visit the full dirty backlog in indexed pages on a settlement. Charge those visits and comparisons,
including repeated restoration passes; do not claim constant total work because host memory is bounded.
Avoid retaining/serializing the complete backlog as preservation input. Prefer exact indexed stored
comparisons where the existing typed representation permits them; any necessary streamed host
comparison is separately counted as read/materialization work and released before the next page.
Report saved payload copies, streamed payload reads, maximum simultaneously live rows/bytes, database
visits and restore passes separately. A small settlement must not have retained-copy growth with an
unrelated backlog; its unavoidable verification work may grow with that backlog. The existing finite
restoration bound (at most 64 passes) remains a failure ceiling, not a fresh work allowance per pass.
Reserve a sound complete atomic bound, including worst-case permitted passes, before the first
business write. An oversized atomic unit returns capacity before mutation instead of retrying forever.
If reducing that bound requires a changed semantic guarantee, stop for contract amendment.

Use the shared final-state apply owner for pull, settlement, reached conflict and snapshot replacement.
A clean full snapshot defines replacement of the admitted scope, so rows excluded by it are not
survivors; included rows and explicitly preserved intent are. Validate the complete original snapshot
and final lifecycle gate before clearing anything. Protect included final payloads from FK/application
trigger effects during install and verify after preserved outbox L is reapplied. Snapshot A comes from
validated snapshot S; frozen B and request bytes remain historical. Preserve the existing proven source
recovery exception for retargeting, never retarget an ambiguous request on a mere snapshot observation.
Checkpoint/source/publication changes and stage release commit with the complete verified installation.

### Reconciliation-required result and precedence

Use the existing `FKReconciliationRequiredError` / `FKReconciliationRequiredException`, with
`reason=fk_reconciliation_required`, `detail`, `unit`, `sample`, `hasMore`, and `progress` from the public
shapes above. Extend the guarded unit hash to pull, settlement, snapshot/recovery or explicit conflict:
64 lowercase hex binding operation/input identity and its current local guards, with no raw payload.
A repeated identical guarded blocker returns the same unit; a relevant revision/context change changes
it. Samples retain exact canonical typed keys, at most 16 refs and 4096 encoded bytes, omit oversize
identities with `hasMore=true`, and reserve the sample's temporary storage. No hidden full result list,
new diagnostic journal or paging API is needed for this result.

| Detail | Decision boundary |
| --- | --- |
| `relationship_unknown` | A valid explicit unknown relationship marker prevents proving the required transition. Missing/corrupt mandatory fresh-layout metadata remains integrity/format failure, not a normal unknown marker. |
| `dependency_unavailable` | A validated selected result needs a concrete parent/dependent disposition not available in trustworthy local/validated input evidence. Do not invent a parent, deletion, NULL or clean-child policy decision. |
| `historical_repair_required` | A validated historical unit is inconsistent with concrete newer installed evidence and needs per-identity repair. Preserve the original unit and checkpoint. A bare FK failure supplies no such witness. |

Honor cancellation/deadline first when constructing a new pending result. Retain existing admission,
authorization/source/generation, proof/hash/order, dirty/outbox and stale-decision error precedence;
a malformed input, local CHECK/unique failure, or unexplained FK failure is not reconciliation-required.
Replay a frozen ambiguous request before considering a new dependency decision. Missing historical B
required by a transition remains `resolution_base_unavailable`. Capacity/work exhaustion prevents
claiming complete dependency discovery; return its typed limit instead of a guessed blocker.

The result terminates this invocation. Existing wrappers must not busy-retry it, invoke a full-scope
snapshot to evade it, detach, claim convergence, or mark an explicit decision complete. Known clean
ordinary snapshot recovery remains allowed under its existing authority; this rule prohibits using
snapshot replacement as a substitute for missing targeted dependency evidence. No Stage B network
operation is added. A3 preserves the original decision, A/B/L, frozen bytes/hash/source ID, and current
checkpoint, apart from earlier independently committed bundles and useful charged stage progress.
Independent planner work may already have committed; preserve that prefix in `progress`.

Measure `dirtyRows`/`outboundRows` at the final reserved local read on every affected public exit,
including direct pull/rebuild/resolution and recovery entered from sync-then-detach. `committedBundles`
counts unique confirmed push bundles in the outer invocation, not HTTP attempts, snapshot rows or
prior invocations; a confirmed remote commit counts even if local settlement is pending. Do not
silently substitute zero when progress capture fails or replace cancellation with a new limit result.
Preserve the underlying typed cause if a progress-read failure must be surfaced.

Swift continues using `SQLiteNowError.fkReconciliationRequired`, `.syncWorkLimit` and
`.syncWorkspaceLimit` with existing `SQLiteNowFKReconciliationRequired`, `SQLiteNowSyncWorkLimit`,
`SQLiteNowSyncWorkspaceLimit` and `SQLiteNowSyncPendingProgress`. Extend the existing runtime
`localPlanningJson` bridge for all reached operations; no generated API or alternate facade is needed.
Preserve exact Int64 counters above 2^53, the closed detail/dimension values, sample limits and boolean
resumability. Invalid local bridge data fails closed as protocol error. Caller-owned task cancellation
must remain Swift `CancellationError`, keep the operation lease until rollback/cleanup is finished,
and never become a retryable pending result. Test real Swift calls as well as JSON mapping.


## Library-owned work slices — 2026-10-13

This owner-authorized amendment supersedes public work-yield behavior in earlier sections. Ordinary push, pull, sync, rebuild, attachment/recovery, conflict resolution and local lifecycle operations complete their existing semantic unit inside the library. Push handles one logical bundle; sync drains successive bundles before pulling. The existing work-limit field and its 1,000,000 default now select an internal computation slice, not a customer-visible completion cap. No new SDK operation timeout is introduced: caller cancellation/deadlines and existing convergence guards remain authoritative. Entry/byte, wire, snapshot and dependency-group hard limits remain unchanged.

Continuation resumes the current local phase after a useful safe checkpoint and successful cleanup, under the same outer operation owner. It preserves exact frozen request, snapshot/replacement, pull ceiling, retry budgets, attempt/round allowances and accumulated uniquely confirmed effects. Slice accounting and the first-ready-atomic allowance may reset only at a safe boundary. Every positive target must allow a bounded useful step or admissible ready atomic unit; setup must not starve progress. A slice that yields without actual progress fails explicitly rather than spinning. Mixed work/cleanup failures, cancellation and actionable errors retain precedence. Scheduling yields do not escape ordinary public APIs or enter transport backoff. Historical work-error types remain available; internal bounded-step tests may observe them. No local format or HTTP change is authorized by this amendment.

The scoped qualification profile is the paired work-slice completion program, not a routine-maintenance exemption or full protocol/release acceptance. Fresh evidence requires paired corpus/boundary consumers; Go owning-client and nested-module tests with Go 1.25.12 and focused race instrumentation; KMP common/JVM comprehensive coverage; affected Android/JVM/iOS/macOS/JS Node/browser/Wasm and Swift cancellation/mapping; isolated live multi-batch, replay/restart, snapshot and cancellation; default-budget 100-user/50-concurrency complex scenario with verification; and current 1,000/10,000-row resource evidence. Add a synthetic 14-table/3,399-row witness for the historical DayTempo shape. Do not schedule retired 50,000-row replay campaigns. Existing per-command deadlines, failure diagnosis, candidate freezes and evidence invalidation rules remain.

Implementation ledger: Go `.artifacts/work-slice-completion-20261013/ledger.txt`, with immutable paired baselines/preimages and a current execution ledger. Specification and corpus changes alone do not assert runtime qualification. Delivery remains separately authorized.


### Seed-stage unpublished-delete pruning

During initial root seeding, a delete already validated as unpublished/absent/deleted may be pruned without discarding unchanged roots seeded earlier in that same guarded traversal. Charge its validation and the additional guard refresh. Under the same writer transaction, advance the seed cursor and rebind the existing guard after only this SDK-owned deletion; dependency/edge traversal has not begun and the deleted key was never inserted as a root. Per-node intent revisions remain their capture revisions. Any external relevant revision change still takes the normal bounded invalidation/cleanup path. No new persistent field, phase discriminator or wire behavior is introduced. Required witnesses compare seed visits for 125 live roots plus 10 unpublished deletes, retain malformed-base refusal and second-writer invalidation, and execute the mixed-CRUD complex workload.

Bounded planning transaction refinement (2026-10-13): public push preparation may run at most 32 existing planning steps in one writer transaction. Work admission, guards, capacity checks, cursor persistence and cancellation checks remain per step. A normal work refusal after successful steps commits their already saved private frontier only after cleanup succeeds; other failures roll back the entire group. Tiny targets still admit one useful step. Transaction-owned schema metadata may be reused within that writer snapshot and is released before continuation. Raw bounded-kernel tests keep one-step transaction entry points. No configured target, accounting weight, hard quota, public operation unit or stored representation changes.

Writer-scoped planning admission snapshots (2026-10-13): while a bounded push-planning writer transaction owns the database, operation/reservation/snapshot and apply-owner scalar charges are immutable. They may be read once and reused for that transaction. Every planning allocation still adds its current push-plan charge and requested temporary charge and applies the unchanged hard limits; no retained push workspace is discounted. The finite scalar snapshot is charged as 2 entries and 256 bytes, released before the writer is released, and never reused across a transaction or work slice. No payloads or live SQL handles are retained. Guard checks and per-step work charges remain unchanged. Raw kernels and non-planning operations retain uncached admission. Required structural witnesses compare metadata SQL reads, steps, charged work, hard refusals and cleanup, separately from timing.

Writer-scoped planning guard snapshots (2026-10-14): a guard hash, source-bundle ordinal, initialization identity and their previously admitted context-size bounds may be reused only inside the same bounded planning writer transaction. Every step still checks its plan guard and current workspace against those bounds and receives the unchanged full work charge. The cache is charged as 4 entries/8192 bytes in addition to the scalar-owner snapshot; initialization identities over 1024 UTF8 bytes or insufficient optional cache capacity use fresh reads instead. SDK-owned no-op deletion invalidates the cached guard before rebinding. Other guard inputs cannot change under this writer; external changes are read at the next writer and final freeze gate. Release before continuation; no persistence or wire change. Required witnesses retain identical planning steps/work and demonstrate fewer guard reconstructions plus external-write, no-op, hard-limit and cancellation safety.

Decision-copy reservation continuation correction (2026-10-14): capture the operation-owned buffer reservation base once before replay continuation. Each slice admits its complete simultaneous decision-copy workspace relative to that same base, never relative to a previous slice's temporary charge. The decision allowance may conservatively retain its maximum for the one outer operation; it is reused, not accumulated, and the existing once-only outer release remains mandatory. Statement pools remain slice-owned and close/release before progress commit. Retained apply inputs stay fully charged. A paired125-row/8-table/max-push40/default-and-target1 fixture must complete without growing reservation entries as slice count increases. No weights, quotas, public signatures or stored layout change.
