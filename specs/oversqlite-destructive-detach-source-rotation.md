# Oversqlite Destructive Detach Source Rotation

## Status

Draft.

Related KMP spec:
[`oversqlite-v1-pruning-and-source-recovery.md`](./oversqlite-v1-pruning-and-source-recovery.md)

Relevant upstream server implementation references:
[`../../go-oversync/oversync/schema_init.go`](../../go-oversync/oversync/schema_init.go)
[`../../go-oversync/oversync/push_sessions.go`](../../go-oversync/oversync/push_sessions.go)
[`../../go-oversync/oversync/connect.go`](../../go-oversync/oversync/connect.go)

## Summary

This spec defines one lifecycle change in the KMP oversqlite client:

- successful destructive `detach()` must rotate to a fresh internal `sourceId`

This applies because successful detach already clears the local managed sync dataset. After that
cleanup, the next attach is not a continuation of the previous local source stream. It is a fresh
local sync incarnation and must not reuse the retired source stream's local bundle numbering.

This spec assumes oversqlite is still unreleased on both the KMP and server sides. Breaking changes
to pre-release lifecycle semantics are acceptable here, and no compatibility-preserving local
migration path is required.

## Problem

Current KMP behavior has a mismatch:

- successful detach deletes all managed local business content and clears dirty/outbox state
- successful detach still preserves the previous `current_source_id`
- local next-bundle sequencing is currently tracked only by local `sourceId`
- the real server keys committed source sequencing by `(user_id, source_id)`

That mismatch is exposed when one app install signs out one account and signs in another account:

1. user A attaches and commits bundle `1` on source `S`
2. successful detach wipes local managed data but keeps source `S`
3. user B attaches on the same app install
4. KMP attempts to continue local source sequencing for `S`
5. the server expects bundle `1` for `(user B, source S)` and rejects the reused numbering as
   `source_sequence_out_of_order`

The issue is therefore in the client lifecycle model, not in the server.

## Audited Current Behavior

In the current KMP runtime:

- `detach()` clears managed local tables, snapshot stage, dirty rows, and outbox state
- `detach()` persists anonymous lifecycle state with the same `current_source_id`
- public docs and KDoc currently describe detach as preserving source identity

In the current server runtime:

- `sync.source_state` is keyed by `(user_pk, source_id)`
- the next expected `source_bundle_id` is `1` when no row exists yet for that `(user, source)`
- `connect` already supports the same user reconnecting through a different source id
- no server-side sign-out lifecycle exists; the server only sees authenticated `user_id` and
  `Oversync-Source-ID`

## Scope

- successful destructive `detach()`
- successful destructive `syncThenDetach()`
- local metadata behavior for `_sync_attachment_state.current_source_id`
- local metadata behavior for `_sync_source_state`
- user-facing docs, engineering docs, and KDoc that describe detach/source semantics
- tests that currently assume source continuity after destructive detach

## Non-Goals

- changing the upstream server sequencing contract
- exposing caller-managed source lifecycle controls
- changing source-recovery-required rebuild-plus-rotate semantics
- changing blocked-detach behavior
- redefining `sourceId` as a product/backend `deviceId`
- preserving backward compatibility with the current pre-release detach/source contract
- introducing local metadata migration logic just to preserve the old pre-release source identity
  behavior

## Decision

### 1. Successful Destructive Detach Retires The Current Source

When `detach()` succeeds through the destructive managed-table cleanup path, oversqlite must:

1. clear managed local sync state
2. generate a fresh opaque internal `sourceId`
3. ensure local source-state metadata exists for that fresh source with next bundle id `1`
4. persist anonymous attachment state bound to that fresh source
5. return `DetachOutcome.DETACHED`

After successful destructive detach, the anonymous database is therefore already bound to a fresh
source stream before the next attach occurs.

### 2. The Old Source Is No Longer The Current Local Sync Incarnation

For lifecycle purposes, the source active before destructive detach is retired. The next attach on
the same app install must behave like the first use of a fresh local source stream, even if:

- the same user signs back in
- a different user signs in
- remote state already exists and attach resolves as `USED_REMOTE_STATE`

This keeps local lifecycle meaning aligned with the actual destructive sign-out behavior.

### 3. Source Rotation Is Conditional On Real Destructive Success

Source rotation must happen only when destructive detach actually succeeds.

The following paths must preserve the pre-existing source:

- `DetachOutcome.BLOCKED_UNSYNCED_DATA`
- detach failures that roll back mid-transaction
- any other detach exit that does not perform destructive managed-table cleanup

This preserves fail-closed behavior and avoids silently changing source identity when sign-out did
not actually complete.

For clarity, `detach()` while canceling a pending `remote_replace` must preserve the current
source. That path is a lifecycle cancel and does not perform the destructive managed-table cleanup
that justifies retiring the existing source stream.

### 4. `syncThenDetach()` Inherits The Same Rule

`syncThenDetach()` must rotate source only when its final detach outcome is `DETACHED`.

If `syncThenDetach()` ends in `BLOCKED_UNSYNCED_DATA`, the current source must remain unchanged.

### 5. Source Rotation Must Be Durable Across Restart

Successful destructive detach must durably persist the fresh source id in local metadata before the
operation returns success.

After `close()` plus a later `open()`, the local runtime must continue using the post-detach fresh
source rather than recreating or reverting to the retired one.

### 6. No Server Change Is Required

This change is fully compatible with the current server contract:

- the server persists source sequencing per `(user_pk, source_id)`
- the server treats a new `(user_id, source_id)` tuple as starting at bundle `1`
- the server already supports a remote-authoritative attach on a different source id for the same
  user

Therefore the corrective action belongs in KMP lifecycle handling and documentation.

Because the library and server are both still unreleased, KMP should prefer the cleaner lifecycle
contract over compatibility with the current pre-release detach behavior.

## Design Notes

### Why Not Keep Source Stable And Track Local Sequencing Per `(user_id, source_id)`?

That alternative would also satisfy the server contract, but it is not the preferred direction for
this product behavior.

Successful detach already destroys the local managed sync dataset. Treating the post-detach state as
a fresh local source incarnation is simpler and aligns better with the user's mental model:

- sign-out completed
- local synced data is gone
- the next sign-in starts from a fresh local sync writer stream

This keeps `sourceId` closer to "current local sync incarnation" than "stable install identity".

It is also the better pre-release choice because it simplifies the lifecycle model instead of
adding compatibility complexity for semantics that have not shipped yet.

### Source Lineage In Local Metadata

Implementation should keep local source-lineage observability coherent. The expected direction is:

- create a new `_sync_source_state` row for the fresh source with next bundle id `1`
- mark the previous source's `replaced_by_source_id` with the fresh source when the detach rotation
  succeeds

This lineage is local runtime metadata only. It does not imply any server-side migration step.

## Work Checklist

### Implementation

- [x] Update
      `library/src/commonMain/kotlin/dev/goquick/sqlitenow/oversqlite/DefaultOversqliteClient.kt`
      so the successful destructive `detach()` path generates a fresh internal `sourceId` after
      managed-table cleanup and persists anonymous state bound to that fresh source.
- [x] Update the same file so successful `syncThenDetach()` inherits the rotated-source result only
      when the final detach outcome is `DETACHED`.
- [x] Update
      `library/src/commonMain/kotlin/dev/goquick/sqlitenow/oversqlite/OversqliteSourceStateStore.kt`
      usage so the fresh post-detach source is initialized with `next_source_bundle_id = 1`.
- [x] Record local lineage for successful destructive detach by setting the previous source row's
      `replaced_by_source_id` to the fresh source.
- [x] Preserve the old source id for all non-destructive paths:
      `DetachOutcome.BLOCKED_UNSYNCED_DATA`, mid-detach rollback/failure, and any path that exits
      before destructive cleanup succeeds.
- [x] Preserve the old source id for `detach()` while canceling a pending `remote_replace`.
- [x] Persist the fresh post-detach source transactionally with successful destructive cleanup so
      it survives `close()` plus later `open()`.
- [x] Do not add compatibility-only metadata migration logic for the superseded pre-release
      behavior.

### Tests

- [x] Restore the currently failing JVM lane to green again in the environment that reproduced this
      issue, not just in isolated targeted tests.
- [x] Update JVM/common lifecycle tests that currently assume source continuity after successful
      destructive detach.
- [x] Add or update a contract test proving successful destructive `detach()` rotates
      `_sync_attachment_state.current_source_id`.
- [x] Add or update a contract test proving successful `syncThenDetach()` rotates
      `_sync_attachment_state.current_source_id`.
- [x] Add or keep a test proving `DetachOutcome.BLOCKED_UNSYNCED_DATA` preserves the existing
      source id.
- [x] Add or keep a test proving mid-detach rollback/failure preserves the existing source id.
- [x] Add or keep a test proving `detach()` during pending `remote_replace` preserves the existing
      source id.
- [x] Add or update a test proving successful destructive detach rotates source durably across
      `close()` plus later `open()`.
- [x] Update real-server smoke tests so same-install account switching uses the fresh post-detach
      source rather than reusing the retired one.
- [x] Make the currently failing cases pass again:
      `BundleMultiDeviceContractTest.sameUserDifferentDevices_convergeAcrossPushAndPull`,
      `RealServerSmokeTest.sameInstall_canAlternateUsersAndSeeCorrectRemoteDataAgainstRealServer`,
      `RealServerSmokeTest.sameInstall_heavyMultiChunkAlternationAgainstRealServer`, and
      `RealServerSmokeTest.sameInstall_localSeedThenRemoteAuthoritativeRestore_preservesSourceContinuityAgainstRealServer`.
- [x] Update tests that hard-code old detach wording or assume same-source continuation after the
      managed-table wipe.

### Documentation

- [x] Update
      `library/src/commonMain/kotlin/dev/goquick/sqlitenow/oversqlite/OversqliteClient.kt` KDoc so
      `detach()` and `syncThenDetach()` describe fresh-source rebinding on successful destructive
      detach.
- [x] Update `docs/sync/core-concepts.md` so the detach section states that successful destructive
      detach rotates source, while blocked detach does not.
- [x] Update `docs/sync/sync-operations.md` so the lifecycle reference reflects detach and
      `syncThenDetach()` source-rotation behavior.
- [x] Update `docs/sync/open-connect-rebuild.md` so source lifecycle documentation mentions
      destructive-detach rotation in addition to rebuild rotation.
- [x] Update `docs/sync/getting-started.md` so detach examples and lifecycle expectations stop
      implying stable source identity across sign-out.
- [x] Update `docs/sync/reactive-updates.md` so it continues to describe managed-table clearing on
      detach without implying source continuity.
- [x] Update `docs-engineering/sqlitenow_flow.md` so the startup/identity model and runtime flow
      mention successful destructive detach as a second internal source-rotation point.
- [x] Update `specs/oversqlite-v1-pruning-and-source-recovery.md` so it notes that source rotation
      now happens both for source recovery and for successful destructive detach.

### Verification

- [x] Run `./gradlew :library:jvmTest` and confirm the lane is green again.
- [x] Run targeted lifecycle-related tests if additional focused commands are helpful during
      iteration.
- [x] If common/shared test coverage changes materially, run `./gradlew :library:jsTest`.
- [x] When a compatible `nethttp_server` is available at the configured base URL, confirm the
      real-server smoke coverage in `:library:jvmTest` is green again rather than only skipped.
- [x] If this work changes real-server smoke expectations further, rerun the relevant opt-in
      real-server lane against `nethttp_server`.
- [x] Before closing the work, confirm the acceptance criteria below from passing tests and updated
      docs/KDoc.

## Acceptance Criteria

- after successful destructive `detach()`, `sourceInfo().currentSourceId` reports a fresh source id
- after successful destructive `detach()`, the next upload on the next attached account uses source
  bundle numbering from `1` for that fresh source
- same-install sign-out followed by sign-in no longer triggers `source_sequence_out_of_order`
  purely because KMP reused a retired local source stream
- blocked detach and rolled-back detach continue to preserve the previous source id
- `detach()` while canceling a pending `remote_replace` preserves the previous source id
- after successful destructive `detach()`, the fresh source id survives `close()` plus later
  `open()`
- `./gradlew :library:jvmTest` is green again in the reproducing environment
- when a compatible real server is available, the currently failing same-install real-server smoke
  cases are green again, not left red or papered over by changing the test lane
- the implementation does not carry compatibility/migration machinery solely for the superseded
  pre-release detach contract
