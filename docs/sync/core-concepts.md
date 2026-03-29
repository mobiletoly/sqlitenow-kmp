---
layout: doc
title: Sync Core Concepts
permalink: /sync/core-concepts/
parent: Sync
---

# Sync Core Concepts

Oversqlite requires one app-owned install `sourceId`. App code passes that value into
`open(sourceId)` and reuses the same exact string for auth whenever the server binds auth to source
identity.

## Three Different Identities

### Install Source Identity

This is the stable install-scoped `sourceId` that the app owns.

Oversqlite treats it as an opaque string and compares it exactly as provided. It does not trim,
normalize, case-fold, or reinterpret the value.

Rules:

- reuse the same `sourceId` for the lifetime of one local install / local data set
- reuse it across app launches, sign-out/sign-in, and account switching on the same install
- use different `sourceId` values for different installs/devices, even for the same account
- if local app data is wiped and the local database is recreated, generating a new `sourceId` is
  correct

### Attached Account Identity

This is the authenticated `userId` you pass to `attach(userId)`.

Account attachment decides which remote sync scope is currently active for the local database.

### Auth Identity

This is whatever your backend uses in tokens or sessions.

If your server binds auth to source identity, the same exact app-owned `sourceId` must be used both
for auth and for `open(sourceId)`.

## Lifecycle Model

Oversqlite lifecycle is:

1. `open(sourceId)`
2. `attach(userId)` when an authenticated session exists
3. normal sync operations
4. `detach()` or `syncThenDetach()` when leaving the attached account

### `open(sourceId)`

`open(sourceId)` is local-only.

It validates sync metadata, creates control tables, binds or validates the durable local
`sourceId`, and repairs interrupted local lifecycle state. It never talks to the server and never
attaches an account.

Call it on every launch before any lifecycle-aware sync operation, using the same app-owned install
`sourceId`.

### `attach(userId)`

`attach(userId)` is the account-binding step. It may:

- resume the same attached account
- use authoritative remote state
- authorize a first local seed
- start an authoritative empty scope
- return `RetryLater`

Call it whenever an authenticated session exists, not only on the first login gesture.

### `detach()`

`detach()` safely removes the current attached scope from the local database.

It is fail-closed: if attached pending sync data still exists, it returns
`DetachOutcome.BLOCKED_UNSYNCED_DATA` and makes no destructive local changes.

`detach()` does not change the install `sourceId`.

### `syncThenDetach()`

`syncThenDetach()` is bounded convenience sugar. It runs `sync()` and then tries `detach()`. If new
local writes arrive during the previous round, it may retry a small number of times. It never loops
forever, and it returns the final blocked outcome explicitly if detach still cannot proceed.

## Authority States

For the currently attached account, Oversqlite reports:

- `PENDING_LOCAL_SEED`
- `AUTHORITATIVE_EMPTY`
- `AUTHORITATIVE_MATERIALIZED`

These are scope/materialization states, not authentication states.

## Rebuild And Recovery

### `rebuild(RebuildMode.KEEP_SOURCE)`

Replaces local managed tables from the current authoritative remote snapshot while preserving the
same `sourceId`.

Use this when local managed state must be rebuilt, but the current source identity is still valid.

### `rebuild(RebuildMode.ROTATE_SOURCE, newSourceId)`

Rebuilds local managed tables from the remote snapshot and rotates to a fresh caller-provided
`newSourceId`.

Use this when recovery must abandon the current local source identity.

### `rotateSource(newSourceId)`

`rotateSource(newSourceId)` is the advanced API. It rotates the current source identity without
discarding pending local edits.

This is not normal login/logout lifecycle. Reach for it only when the current source identity must
be replaced as part of explicit recovery.

## `SourceBindingMismatchException`

This means the app called `open(sourceId)` with a different value than the one already durably
bound to the local database.

Default app behavior:

- stop sync
- treat the condition as a local identity/storage bug or wrong DB reuse
- use an explicit app-owned reset path that recreates the local Oversqlite database or local sync
  state

Only advanced apps that already persist and trust the original install `sourceId` outside the
Oversqlite database should attempt recovery by reopening with that exact original value.

Do not:

- silently generate a fresh `sourceId` and retry
- silently rebind an existing local database to another `sourceId`
- treat this as a transient network error

## What `open(sourceId)` Does Not Mean

`open(sourceId)` does not mean:

- the account is attached
- the server session is connected
- the local DB was synchronized
- remote state was rebuilt

`OpenState.ReadyAttached(userId)` means durable local attachment metadata exists. You should still
call `attach(userId)` when an authenticated session is present so the client can resume connected
sync operations.

## Reliable Source Continuity

Because source identity is install-scoped, the local install can:

- detach from user A
- attach user B
- detach again
- reattach user A later

while keeping the same source identity across those transitions.

The same user on two different devices must still use two different `sourceId` values.
