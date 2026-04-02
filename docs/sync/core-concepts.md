---
layout: doc
title: Sync Core Concepts
permalink: /sync/core-concepts/
parent: Sync
---

# Sync Core Concepts

Oversqlite manages sync source identity internally. App code owns authentication and account
attachment, but it does not generate, persist, or rotate `sourceId`.

## Three Different Identities

### Sync Writer Identity

Oversqlite maintains one current `sourceId` for the local runtime.

Important properties:

- it is the sync writer identity used on the wire
- it is opaque and debug-only from app code
- it lives in oversqlite metadata inside the local database
- it may rotate during explicit recovery on the same install

If your app/backend also has a `deviceId`, treat that as a separate concept. A product-level
`deviceId` may remain stable while oversqlite rotates the current sync writer identity.

### Attached Account Identity

This is the authenticated `userId` passed to `attach(userId)`.

Account attachment decides which remote account scope is currently active for the local database.

### Auth Identity

This is whatever your backend uses in tokens or sessions.

Oversqlite does not own auth. The server authenticates the request separately, and oversqlite sends
the current sync writer identity as sync transport metadata.

## Lifecycle Model

The oversqlite lifecycle is:

1. `open()`
2. `attach(userId)` whenever an authenticated session exists
3. normal sync operations
4. `detach()` or `syncThenDetach()` when leaving the attached account

### `open()`

`open()` is local-only.

It validates managed-table configuration, creates or repairs lifecycle metadata, installs local
triggers, restores or creates the current internal `sourceId`, and captures pre-existing managed
rows once when bootstrap policy allows it.

It never talks to the server and never attaches an account.

### `attach(userId)`

`attach(userId)` is the authenticated lifecycle step. It may:

- resume the same attached account
- use authoritative remote state
- authorize a first local seed upload
- start an authoritative empty scope
- return `RetryLater`

Call it whenever an authenticated session exists, not only on the first sign-in gesture.

### `detach()`

`detach()` safely removes the current attached account scope from the local database.

It is fail-closed: if attached pending sync data still exists, it returns
`DetachOutcome.BLOCKED_UNSYNCED_DATA` and makes no destructive local changes.

`detach()` does not change the internally managed source identity.

### `syncThenDetach()`

`syncThenDetach()` is bounded convenience sugar. It runs `sync()` and then attempts `detach()`. If
new local writes arrive during the previous round, it may retry a small number of times. It never
loops forever, and it returns the final blocked outcome explicitly if detach still cannot proceed.

## Authority States

For the currently attached account, oversqlite reports:

- `PENDING_LOCAL_SEED`
- `AUTHORITATIVE_EMPTY`
- `AUTHORITATIVE_MATERIALIZED`

These are scope/materialization states, not authentication states.

## Rebuild And Recovery

`rebuild()` is the explicit recovery entry point.

Important rules:

- it remains an attached/authenticated operation
- it rebuilds local managed tables from the authoritative remote snapshot
- oversqlite chooses the internal mode
- app code does not supply a replacement `sourceId`

In ordinary rebuild-required cases, `rebuild()` keeps the current source.

In source-recovery-required cases, `rebuild()` preserves frozen unsynced intent, rebuilds from the
snapshot, rotates to a fresh internally generated source, and restores the frozen intent under that
fresh source stream.

## Diagnostics

`sourceInfo()` exposes read-only source diagnostics.

Use it for logging, support tooling, or debug UI only.

Important rules:

- `SourceInfo.currentSourceId` is opaque
- callers must not persist it externally
- callers must not infer lifecycle meaning from its format
- callers must not treat it as a control surface

## What `open()` Does Not Mean

`open()` does not mean:

- the account is attached
- the server session is connected
- the local DB was synchronized
- remote state was rebuilt

Even when durable local attachment metadata already exists, you should still call `attach(userId)`
when an authenticated session is present so the client can resume connected sync operations.
