# Oversqlite KMP V1 Pruning And Source Recovery

## Status

Draft.

Upstream server/runtime spec:
[`../../go-oversync/specs/server-postgres-sync-schema-v1.md`](../../go-oversync/specs/server-postgres-sync-schema-v1.md)

Upstream source/auth spec:
[`../../go-oversync/specs/source-identity-and-auth-contract.md`](../../go-oversync/specs/source-identity-and-auth-contract.md)

OpenAPI spec:
[`../../go-oversync/swagger/two_way_sync.yaml`](../../go-oversync/swagger/two_way_sync.yaml)

## Summary

This spec defines the KMP oversqlite client changes required by the v1 server/PostgreSQL sync
redesign, especially bounded retained history, pruning, stricter per-source bundle sequencing, and
the final source-identity/auth handoff contract.

It exists separately because the server/Postgres target schema lives in `go-oversync`, while the
KMP client runtime lives in `sqlitenow-kmp`. Go oversqlite client work that is implemented inside
`go-oversync` remains part of the upstream spec/checklist there. This document is only for the KMP
client/runtime work.

## Scope

- KMP oversqlite remote API handling for push create, pull, committed bundle replay, and snapshot
  rebuild
- KMP sync request construction where the final upstream contract moved source identity from request
  bodies to authenticated transport metadata
- KMP local source-state handling and source-rotation behavior
- KMP local outbox / pending-intent behavior when upload recovery requires rebuild plus source
  rotation
- KMP public recovery contract when the server refuses a stale or out-of-order source tuple

## Goals

- align KMP oversqlite with the final v1 server protocol and pruning contract
- keep fail-closed behavior and never silently drop local unsynced intent
- make `sourceId` a fully oversqlite-managed internal identity
- treat `sourceId` as the current sync writer identity, not as auth/session identity
- distinguish retained-history pull recovery from upload-side stale-source recovery
- make destructive rebuild plus source rotation explicit when it is required by the server contract

## Priority Order

When two implementation choices conflict, choose in this order:

1. correctness, durability, and acknowledged-no-loss semantics
2. runtime behavior and operational clarity
3. storage / implementation simplicity

## Non-Goals

- redefining the server/PostgreSQL schema or retention rules
- redefining the Go oversqlite client work tracked in `go-oversync`
- preserving backward compatibility with older draft push-create protocol assumptions if the final
  v1 protocol is cleaner without them
- modeling host-app IAM, JWT/session, or device-principal semantics inside the KMP sync contract
- requiring host apps to generate, persist, or rotate `sourceId`

## Audited Current Behavior

The current KMP runtime already has several useful building blocks:

- local source sequencing uses `_sync_source_state.next_source_bundle_id`
- frozen push outbox snapshots reuse one logical `source_bundle_id` until authoritative replay
  succeeds
- authoritative replay advances the local next-source-bundle floor to
  `source_bundle_id + 1`
- pull-side `history_pruned` is already treated as rebuild-from-snapshot with the current source
- local sync metadata already persists current source state and per-source sequencing in
  `_sync_attachment_state.current_source_id` and `_sync_source_state`

The current KMP runtime also has important gaps relative to the upstream pruning design:

- `createPushSession()` only accepts `status = "staging"` or `status = "already_committed"`
- create-time `history_pruned`, create-time `source_sequence_out_of_order`, and commit-time
  `source_sequence_changed` are not decoded as structured upload-side recovery signals
- committed-bundle replay below the retained floor after remote commit currently is not treated as
  a distinct already-committed recovery path
- sync request construction still reflects older body-level `source_id` assumptions rather than the
  final `Oversync-Source-ID` transport contract
- rebuild is currently blocked while `_sync_outbox_*` still contains staged outbound rows

That last point matters because create-time stale-source failure happens after the client has
already frozen local dirty state into `_sync_outbox_*`.

## Upstream Contract This Client Must Honor

The upstream server/runtime spec now makes these rules load-bearing:

- for one `(user_id, source_id)`, successful new commits use a contiguous
  `source_bundle_id` sequence starting at `1`
- the next new committed bundle for a source must use
  `source_bundle_id = max_committed_source_bundle_id + 1`
- a client must never reset numbering for an existing `source_id`; destructive reset, destructive
  rebuild, reinstall, or replacement of the local install/data set that wants to restart numbering
  at `1` must first rotate to a fresh `source_id`
- `CreatePushSession` may reject a stale already-committed source tuple whose exact retained replay
  history has been pruned
- `CreatePushSession` may reject an out-of-order new source tuple with a dedicated source-sequence
  error
- `CommitPushSession` may reject an already staged upload if source-sequence revalidation fails
- pull-side retained-history gaps and create-time stale-source tuple failures are different
  situations, even if both currently use `history_pruned`
- committed-bundle replay below the retained floor for an already accepted remote bundle is also a
  different case from create-time stale-source failure
- authenticated `/sync/*` requests carry the caller's current source identity as sync transport
  metadata via the `Oversync-Source-ID` header
- host authentication and sync source identity are separate concerns; KMP must not model `sourceId`
  as auth/session identity
- `POST /sync/connect` no longer sends `source_id` in the request body
- `POST /sync/push-sessions` no longer sends `source_id` in the request body
- the source header and the resulting runtime `Actor.SourceID` are the authoritative request-level
  source identity for connect and push create

## Required KMP Decisions

### 1. Source Identity Is Fully Oversqlite-Managed

KMP must treat `sourceId` as a fully oversqlite-managed internal identity.

In this document, `sourceId` means the current source identity for one local runtime/source stream.
It is usually stable during ordinary operation, but it may be explicitly rotated on the same
install during source recovery or after successful destructive detach. It is not defined as a
hardware device identifier or stable auth/session principal.

If an app/backend also has a separate `deviceId`, that is a separate concept. In particular,
`sourceId` may be replaced during explicit source recovery, while a product-level `deviceId` may
remain stable across that transition.

Consequences:

- oversqlite generates the initial `sourceId`
- oversqlite persists the current `sourceId` in its own sync metadata
- oversqlite generates a fresh replacement `sourceId` when source recovery or successful
  destructive detach requires rotation
- host app code does not generate, persist, or rotate `sourceId`
- `sourceId` persistence lives in the same durability boundary as source sequencing and
  source-recovery state
- source rotation remains an internal runtime mechanism, not a caller-driven public lifecycle step

For v1, the source-id persistence model should be internal:

- `_sync_attachment_state.current_source_id` stores the currently active source stream
- `_sync_source_state` stores per-source bundle sequencing and replacement lineage
- first bootstrap generates and persists an initial `sourceId` if none is bound yet
- same-install source rotation updates those metadata tables as part of the same oversqlite-managed
  recovery flow

For v1, internally generated source ids should use UUID text:

- generated source ids must be non-empty
- generated source ids must remain opaque to callers even though UUID is the chosen implementation
  format
- generated replacement source ids must be fresh for recovery purposes
- generated replacement source ids must not equal the currently active source stream
- generated replacement source ids must not collide with any already-persisted local source stream

The host app still indirectly controls persistence by choosing where the local SQLite database
lives. It must not be required to mirror `sourceId` into separate host storage.

### 2. Public API Must Stop Exposing Manual Source Lifecycle

Because `sourceId` is fully oversqlite-managed in v1, the public API must stop exposing
caller-driven source lifecycle controls.

The target public contract is:

- app code does not call `open(sourceId)` with a caller-provided source value
- app code does not call `rotateSource(newSourceId)`
- app code does not pass a new source id into rotated rebuild APIs
- source rotation remains an internal runtime mechanism used by oversqlite recovery

The exact replacement public API shape is still an implementation decision, but the required
direction is:

- local startup should not require a source-id parameter
- `open()` should return only success/failure; pre-attach lifecycle diagnostics belong on
  `SourceInfo`, `attach(...)`, and later authenticated operations rather than an extra startup
  status surface
- source-rotation recovery should not require a caller-provided replacement source id
- rebuild/recovery should still require an explicit public call rather than running automatically
- rebuild/recovery should remain an attached/authenticated operation because snapshot recovery still
  depends on remote authority
- public recovery surfaces should report that source-rotation recovery is required or in progress
  without asking the app to manage source identity directly

Read-only observability is still allowed:

- KMP must expose a read-only `SourceInfo`-style diagnostic surface
- that surface is for debug/diagnostic use only and must be documented that way in KDoc
- `sourceId` remains opaque even when exposed for diagnostics
- callers must not treat the exposed source id as a lifecycle control, persistence contract, or
  semantically meaningful identifier format

At minimum, `SourceInfo` should expose:

- current source id
- whether any durable rebuild gate is active
- whether durable source-recovery-required mode is active
- the durable source-recovery reason when source-recovery-required mode is active

The intended `SourceInfo` contract is:

- `rebuildRequired` covers any durable rebuild gate, including generic rebuild-required and
  source-recovery-required state
- `sourceRecoveryRequired` specifically means the current source stream is blocked and rotated
  recovery is required before ordinary sync may resume
- `sourceRecoveryReason` is populated only when `sourceRecoveryRequired = true`
- `SourceInfo` should be available after successful local startup and does not require a fresh
  attached/authenticated session just to inspect persisted diagnostic state

### 3. Source Identity Is Sync Transport Metadata, Not Auth Identity

KMP must align with the final upstream auth boundary:

- the server authenticates the user separately from sync source identity
- KMP sends the current `sourceId` on authenticated `/sync/*` requests using
  `Oversync-Source-ID`
- KMP must not rely on JWT claims or other auth-token internals to define `sourceId`
- KMP must treat `Oversync-Source-ID` as authoritative request metadata for connect and push-create
  source identity

For the KMP client, this means:

- changing `sourceId` for recovery is a sync operation, not an auth/session rotation
- same-install source rotation must be possible without redefining `sourceId` as a stable
  device/session principal
- request construction for `connect` and `createPushSession` must follow the source-header contract
  rather than older body-level `source_id` assumptions
- the current source value comes from oversqlite local metadata, not from host app input

### 4. Pull-Side And Push-Create `history_pruned` Must Diverge

Pull-side retained-history pruning may continue to recover through keep-source rebuild:

- `pullToStable()` may catch pull-side `history_pruned`
- the client may rebuild from snapshot with the current source id in that case

Create-time stale-source pruning is different:

- it does not mean the client only missed retained bundle history
- it means the requested `(source_id, source_bundle_id)` tuple is stale relative to the durable
  per-source committed watermark
- keep-source rebuild alone does not repair that numbering mismatch

Therefore the KMP client must not treat create-time stale-source pruning as the same recovery path
as pull-side pruning.

Committed-remote replay pruning is different again:

- the server already accepted the source tuple and committed one authoritative bundle
- the client only lacks retained committed-bundle row replay
- source rotation is not required

Therefore the KMP client must treat committed-remote replay-pruned recovery as keep-source
recovery, not as stale-source recovery.

### 5. Upload Recovery Must Be Structured

The remote API layer must decode the final upstream upload-side recovery failures as structured
errors, not as generic HTTP/runtime failures.

The upstream wire contract is:

- `POST /sync/push-sessions` returns `409 history_pruned` for stale source tuples whose exact
  retained replay history was pruned
- `POST /sync/push-sessions` returns `409 source_sequence_out_of_order` for out-of-order new source
  tuples
- `POST /sync/push-sessions/{push_id}/commit` returns `409 source_sequence_changed` when commit-time
  source-sequence revalidation fails after staging

KMP must decode those create-time and commit-time outcomes as distinct structured recovery signals.

The push workflow must then treat upload-side stale-source / source-sequence failure as a dedicated
recovery-needed condition, not as:

- a generic upload transport failure
- a valid `PushSessionCreateResponse` status
- a signal to retry the same source tuple
- a signal to run keep-source rebuild automatically

### 6. Out-Of-Order Source Sequence Must Also Be Structured

The upstream server spec requires strict per-source sequencing and rejects gaps.

KMP must therefore treat an out-of-order new source tuple as a structured fail-closed condition.
That is separate from:

- retained duplicate replay
- ordinary `already_committed`
- structured `push_conflict`

KMP must route both `source_sequence_out_of_order` and `source_sequence_changed` into the same
stale-source / source-sequence recovery bucket as create-time stale-tuple `history_pruned`, not
generic retry.

## Required KMP Recovery Contract

### Pull-Side Retained-History Pruning

The current pull-side recovery remains valid:

- pull returns retained-history `history_pruned`
- KMP may rebuild from snapshot while keeping the current source

### Durable Rebuild-Required Without Source Recovery

KMP may also have a durable explicit-rebuild-required state without durable
source-recovery-required mode, for example after an interrupted or failed rebuild that already set
the local rebuild gate.

In that case, KMP must:

- preserve that rebuild-required state durably across restart
- continue to fail closed on ordinary sync until explicit `rebuild()` succeeds
- recover with keep-source rebuild semantics

### Committed-Remote Replay Pruned Below The Floor

If the client already knows remote commit succeeded and later committed-bundle replay falls below
the retained floor:

1. keep the current source id
2. rebuild from snapshot while keeping the current source
3. preserve the fact that the source tuple is already committed
4. advance local source sequencing past that committed tuple
5. clear committed-remote outbox state only after the rebuild-based recovery succeeds

This is not a source-rotation case.

### Create-Time Stale-Source Or Source-Sequence Failure

The supported v1 recovery is:

1. preserve local unsynced intent safely
2. rebuild from snapshot
3. rotate to a fresh oversqlite-generated `sourceId`
4. resume uploads under the new source with `source_bundle_id = 1`

This must be explicit in the KMP public contract.

This recovery is not automatic. Oversqlite must fail closed and require an explicit rebuild/recovery
call from app code before the source stream may continue.

`rebuild()` remains an attached/authenticated operation:

- successful local startup alone is not sufficient
- oversqlite must have current attached/authenticated scope context before rebuild executes
- if rebuild-required or source-recovery-required state survives restart, app code must reattach
  before invoking explicit rebuild/recovery

The rotated `sourceId` must be fresh for source-recovery purposes. Reusing an existing source stream
is not valid recovery.

The library must not:

- silently reuse the stale source tuple
- silently discard `_sync_outbox_*` content
- silently downgrade to keep-source rebuild

### Persisted Source-Recovery Mode

Once KMP observes create-time `history_pruned`, create-time `source_sequence_out_of_order`, or
commit-time `source_sequence_changed` for the frozen outbox bundle, it must durably enter a
source-recovery-required mode.

While that mode is active:

- `pushPending()`, `pullToStable()`, and any combined sync entry point must fail closed with the
  same structured source-recovery-needed outcome
- keep-source rebuild must also fail closed; it does not repair the source numbering mismatch
- only oversqlite-managed rotated rebuild may clear the recovery-required state
- process restart must not silently forget that recovery is required

The public recovery entry point should remain explicit:

- app code must call `rebuild()` (or an equivalently explicit public recovery API)
- oversqlite decides internally whether that call performs keep-source rebuild or
  rebuild-plus-rotate recovery
- if persisted state says source rotation is required, the explicit rebuild call must run the
  preserve-intent plus rotated-rebuild flow rather than a keep-source rebuild
- source-recovery-required mode must not be cleared by any ordinary sync call

The durable state does not need one exact storage shape, but it must preserve enough information to
fail closed and resume recovery correctly after restart. At minimum that means the implementation
must durably preserve:

- that source recovery is required, not only generic rebuild
- which current source stream was rejected
- which frozen outbound intent bundle triggered the condition
- which recovery reason was observed (`history_pruned`, `source_sequence_out_of_order`, or
  `source_sequence_changed`)
- whether the unsynced local intent still lives in `_sync_outbox_*` or has already been copied to a
  separate preservation form

## Local Intent Preservation Requirement

This is the most important KMP-specific implementation rule.

When create-time stale-source or out-of-order source failure happens:

- local dirty rows have already been frozen into `_sync_outbox_*`
- rebuild is currently blocked while `_sync_outbox_*` is populated
- a naive destructive rebuild would otherwise lose the only durable metadata describing unsynced
  local intent

Therefore KMP must implement one explicit preservation path for this case.

Target behavior:

- `_sync_outbox_*` in this state is treated as local unsynced intent, not as pending authoritative
  remote replay
- before destructive rebuild plus source rotation proceeds, the client must preserve or requeue
  that intent in a form that survives the rebuild
- after snapshot apply under the fresh internally generated `sourceId`, the preserved local intent must be restored as
  replayable local dirty state under the new source stream
- only after that restoration may the old outbox state be discarded permanently

Restart-safe phase boundaries are part of this guarantee:

- before KMP durably records an alternate preserved-intent representation, `_sync_outbox_*` remains
  the authoritative durable copy and must not be cleared
- once KMP durably records that alternate preserved-intent representation, restart must be able to
  resume from that point without guessing from business tables
- rotated rebuild must not clear the persisted source-recovery-required mode until snapshot apply
  and local-intent restoration have both succeeded durably
- if recovery fails mid-flight, restart must resume the same recovery flow rather than silently
  resuming normal sync on the rejected source stream

This spec intentionally requires the guarantee, but does not yet force one exact local-storage
shape for the preserved intent. The implementation may:

- requeue `_sync_outbox_*` back into `_sync_dirty_rows` before rebuild
- persist a dedicated recovery staging area and restore from it after rebuild
- or use another design that preserves unsynced local intent with the same fail-closed guarantee

What is not allowed:

- clearing `_sync_outbox_*` before the client has another durable representation of the same local
  intent
- running rebuild while assuming the app can reconstruct the local dirty set from business tables
  alone

## Required Public-Facing KMP Behavior

KMP must make the difference between these cases visible to callers:

- ordinary pull-side `history_pruned`
- create-time stale-source tuple rejection after pruning
- create-time out-of-order source sequence rejection
- commit-time `source_sequence_changed` after staging

At a minimum, the public contract must preserve the following recovery classification:

| Situation | Meaning | Required recovery | Source rotation required |
| --- | --- | --- | --- |
| pull-side `history_pruned` | retained pull history is no longer available | keep-source snapshot rebuild | no |
| create-time `history_pruned` | requested `(source_id, source_bundle_id)` is stale for the current source stream | preserve local intent, rebuild from snapshot, rotate source | yes |
| create-time `source_sequence_out_of_order` | requested next `source_bundle_id` is out of order for the current source stream | preserve local intent, rebuild from snapshot, rotate source | yes |
| commit-time `source_sequence_changed` | staged upload was later rejected by source-sequence revalidation | preserve local intent, rebuild from snapshot, rotate source | yes |
| committed-remote replay below retained floor | remote commit already succeeded, but retained replay rows are gone before local fetch | keep-source snapshot rebuild, preserve committed tuple semantics, advance local source floor after successful recovery | no |

For the source-recovery-required cases, the public behavior must clearly communicate:

- syncing cannot continue with the current source stream
- app code must invoke explicit rebuild/recovery before the next upload attempt
- oversqlite will perform rebuild-plus-rotate internally when persisted recovery state requires it
- once that requirement is recorded locally, subsequent sync calls continue returning the same
  recovery-needed outcome until rotated rebuild succeeds

Committed-remote replay below the retained floor is different:

- it must not surface as a source-rotation requirement
- it may recover through keep-source rebuild inside the push recovery flow
- callers must not be told to provide a new `sourceId` for this case

Whether KMP exposes that as:

- one structured exception type with reason codes
- multiple structured exception types
- or a public recovery result object

is an implementation decision. But the surfaced contract must be specific enough that app code can
distinguish keep-source rebuild from internally managed source-rotation recovery.

## Implementation Consequences

The KMP work implied by this spec includes at least:

- remote API decoding for create-time stale-source, create-time out-of-order, commit-time
  `source_sequence_changed`, and committed-remote replay-pruned `history_pruned`
- sync request construction updates so authenticated `/sync/*` requests carry
  `Oversync-Source-ID`, while `connect` and `push-sessions` no longer rely on body-level
  `source_id`
- internal source-id generation, persistence, and rotation inside oversqlite sync metadata rather
  than host app storage
- push workflow branching for the new structured recovery path
- persistent source-recovery-required state that survives restart until rotated rebuild succeeds
- local outbox-intent preservation across rebuild-plus-rotate recovery
- public API / error-surface updates so apps know when source-rotation recovery is required without
  passing a fresh source id themselves
- public rebuild API changes so rebuild remains explicit while mode selection becomes internal
- docs updates for source lifecycle, pruning recovery, and rebuild-plus-rotate usage
- tests for:
  - pull-side retained-history pruning with keep-source rebuild
  - create-time stale-source rejection after pruning
  - create-time out-of-order source sequence rejection
  - commit-time `source_sequence_changed` rejection after staging
  - committed-remote replay below retained floor with keep-source rebuild and local source-floor
    advance
  - preservation of unsynced local intent across rebuild-plus-rotate recovery
  - restart safety while the preservation/rebuild/restore flow is in progress
  - restart safety for persisted source-recovery-required state across `pushPending()`,
    `pullToStable()`, and rotated rebuild

## Checklist

## Implementation Phases

Work should proceed in this order:

1. public API and internal source ownership
2. request-contract and remote API alignment
3. durable rebuild/source-recovery state and rebuild routing
4. local intent preservation and rebuild-plus-rotate execution
5. diagnostics surface
6. docs cleanup and verification

The intent of this sequencing is:

- land the host-facing API and internal ownership model before deeper recovery changes
- align the wire contract before recovery logic depends on it
- establish durable state/routing before implementing destructive preservation/rebuild flows
- add `SourceInfo` after the underlying recovery state exists
- finish with docs/tests once the runtime contract is stable
### Phase 1: Public API And Internal Source Ownership

- [x] change public startup API from `open(sourceId)` to `open()`
- [x] make `open()` return success/failure only rather than an extra pre-attach status surface
- [x] remove public manual source lifecycle APIs such as `rotateSource(...)`
- [x] remove public rebuild mode selection and caller-provided replacement source ids
- [x] keep explicit `rebuild()` as the public recovery entry point
- [x] require `rebuild()` to remain an attached/authenticated operation
- [x] update public recovery/error surfaces so callers are told to run explicit `rebuild()` rather
      than provide a fresh source id
- [x] add required public read-only `SourceInfo`
- [x] document `SourceInfo` as debug/diagnostic only in KDoc
- [x] document that exposed `sourceId` values are opaque and not a control surface

- [x] generate the initial source id internally on first bootstrap
- [x] use UUID text for internally generated source ids in v1
- [x] require generated source ids to be non-empty
- [x] require generated replacement source ids to differ from the currently active source stream
- [x] require generated replacement source ids not to collide with any already-persisted local source
      stream
- [x] keep source-id format opaque to callers even though UUID is the chosen implementation
- [x] persist the active source in `_sync_attachment_state.current_source_id`
- [x] persist per-source sequencing and replacement lineage in `_sync_source_state`

### Phase 2: Request Contract And Remote API

- [x] send `Oversync-Source-ID` on authenticated `/sync/*` requests using the current internal
      source identity
- [x] stop sending body-level `source_id` for `connect`
- [x] stop sending body-level `source_id` for `push-sessions`
- [x] keep validating response-side `source_id` metadata where the wire contract still returns it
- [x] decode create-time `history_pruned` as structured stale-source recovery
- [x] decode create-time `source_sequence_out_of_order` as structured source-recovery-required
- [x] decode commit-time `source_sequence_changed` as structured source-recovery-required
- [x] decode committed-remote replay-pruned recovery separately from create-time stale-source
      failures

### Phase 3: Durable Recovery State And Rebuild Routing

- [x] preserve generic rebuild-required state durably across restart
- [x] preserve source-recovery-required state durably across restart
- [x] distinguish generic rebuild-required from source-recovery-required in local state
- [x] persist enough source-recovery state to resume recovery without guessing
- [x] fail closed on ordinary sync while generic rebuild-required is active
- [x] fail closed on ordinary sync while source-recovery-required is active
- [x] ensure ordinary sync calls do not clear source-recovery-required state

- [x] make `rebuild()` choose mode internally
- [x] run keep-source rebuild for ordinary rebuild-required state
- [x] run keep-source rebuild for pull-side `history_pruned`
- [x] run keep-source rebuild for committed-remote replay below retained floor
- [x] run rebuild-plus-rotate internally for create-time stale-source `history_pruned`
- [x] run rebuild-plus-rotate internally for create-time `source_sequence_out_of_order`
- [x] run rebuild-plus-rotate internally for commit-time `source_sequence_changed`
- [x] require successful attached/authenticated context before executing `rebuild()`
- [x] require app code to reattach after restart before invoking explicit rebuild when a rebuild gate
      survived restart

### Phase 4: Local Intent Preservation And Rebuild-Plus-Rotate Execution

- [x] treat frozen `_sync_outbox_*` as durable unsynced intent during source-recovery-required
      flows
- [x] implement one explicit preservation path before destructive rebuild-plus-rotate proceeds
- [x] prohibit clearing `_sync_outbox_*` before another durable preserved-intent representation
      exists
- [x] restore preserved intent under the fresh rotated source stream after snapshot apply
- [x] clear old outbox state only after restoration succeeds durably
- [x] ensure recovery can resume safely after crash/restart in each preservation/rebuild/restore
      phase

### Phase 5: Diagnostics Surface

- [x] define `SourceInfo.currentSourceId`
- [x] define `SourceInfo.rebuildRequired` as any durable rebuild gate, including
      source-recovery-required state
- [x] define `SourceInfo.sourceRecoveryRequired`
- [x] define `SourceInfo.sourceRecoveryReason`, populated only when
      `sourceRecoveryRequired = true`
- [x] make `SourceInfo` available after successful local startup without requiring a fresh attached
      session

### Phase 6: Docs Cleanup And Verification

- [x] update public docs and KDoc to remove caller-owned source lifecycle wording
- [x] update docs to describe source identity as internally managed sync writer identity
- [x] update docs to describe `rebuild()` as the explicit recovery entry point
- [x] update docs to describe `SourceInfo` as debug-only diagnostics
- [x] remove references to public `RebuildMode`, manual source rotation, and caller-provided source
      ids from KMP docs and examples
- [x] keep docs target-state only; do not document historical API shapes, migration notes, or old
      caller-owned source lifecycle examples
- [x] remove legacy caller-owned source-lifecycle code paths, obsolete errors/results, and dead
      state once the managed-source contract is fully in place
- [x] remove any temporary tolerance for transitional request/response handling that is no longer
      part of the final public contract

- [x] add tests for pull-side `history_pruned` with keep-source rebuild
- [x] add tests for generic rebuild-required surviving restart and resolving via explicit `rebuild()`
- [x] add tests for create-time stale-source rejection after pruning
- [x] add tests for create-time out-of-order source sequence rejection
- [x] add tests for commit-time `source_sequence_changed` rejection after staging
- [x] add tests for committed-remote replay below retained floor with keep-source rebuild and local
      source-floor advance
- [x] add tests for UUID-based internal source generation and non-reuse on rotation
- [x] add tests for required `SourceInfo` fields and availability after local startup
- [x] add tests for `Oversync-Source-ID` request behavior and removal of body-level `source_id`
- [x] add tests for preservation of unsynced local intent across rebuild-plus-rotate recovery
- [x] add tests for restart safety while preservation/rebuild/restore is in progress
- [x] add tests for fail-closed source-recovery-required state across `pushPending()`,
      `pullToStable()`, `sync()`, and explicit `rebuild()`

## Relationship To The Upstream Spec

This KMP spec depends on the upstream server/runtime decisions in:

- [`../../go-oversync/specs/server-postgres-sync-schema-v1.md`](../../go-oversync/specs/server-postgres-sync-schema-v1.md)
- [`../../go-oversync/specs/source-identity-and-auth-contract.md`](../../go-oversync/specs/source-identity-and-auth-contract.md)
- [`../../go-oversync/swagger/two_way_sync.yaml`](../../go-oversync/swagger/two_way_sync.yaml)

If the upstream wire contract changes, update this document rather than silently adapting KMP
behavior ad hoc.

Go oversqlite work that belongs to the same rollout remains tracked in the upstream
`go-oversync` spec and checklist. This document is only for the KMP-specific client/runtime work.
