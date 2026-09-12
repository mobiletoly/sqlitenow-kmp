---
layout: doc
title: Sync Core Concepts
permalink: /sync/core-concepts/
parent: Sync
parent_url: /sync/
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
- it also rotates after successful destructive `detach()` / `syncThenDetach()`

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

1. `openForSync(...).advance()`
2. `attach(userId)` whenever an authenticated session exists
3. normal sync operations
4. `detach()` or `syncThenDetach()` when leaving the attached account

The public `DefaultOversqliteClient` constructors remain available for source compatibility, but
they do not grant coordinated ownership and cannot initialize a client over an ordinary-open
database. Handwritten consumers must reserve and publish through `openForSync`; only that path can
issue the internal ownership capability used by the runtime.

### Coordinated opening

Fresh and unchanged coordinated opening is local-only. Generation changes may perform the
explicitly journaled remote drain, discovery, replacement, and rebuild sequence.

See [Synchronized schema evolution]({{ site.baseurl }}/sync/schema-evolution/) for the directed upgrade plan, supported additive table/column/FK changes, namespace checks, rollback behavior, and returning-device rules.

It validates managed-table configuration, creates or repairs lifecycle metadata, installs local
triggers, restores or creates the current internal `sourceId`, and captures pre-existing managed
rows once when bootstrap policy allows it.

It never attaches an account. Before `advance()`, inspection does not prepare or mutate target
storage.

For KMP JS/Wasm browsers, coordinated opening uses the database's
packaged worker and deterministic direct-OPFS target; it does not inject a
snapshot backend. A one-time byte-level legacy import, when needed, completes
before generated schema migrations and before this Oversqlite lifecycle opens
its ten control tables. Retained legacy bytes and migration/health markers are
local storage evidence, not a replacement source identity or a remote rebuild.
JS Node uses the same default provider with transient in-memory storage.

### `attach(userId)`

`attach(userId)` is the authenticated lifecycle step. It may:

- resume the same attached account
- use authoritative remote state
- authorize a first local seed upload
- start an authoritative empty scope
- return `RetryLater`

Call it whenever an authenticated session exists, not only on the first sign-in gesture.

Each opened client discovers and admits the selected generation before it publishes an attached
result. A new attachment then connects using that exact contract. The client validates the complete
managed schema, pending values, server limits, and watch availability, then stores the contract
atomically. A same-user durable resume performs discovery again for the new client lifetime and
does not add ordinary-operation discovery preflights.

### Discovered Generation Contract

The v1 discovery response identifies one immutable generation, including its fingerprint, tables,
columns, visible keys and foreign keys. Server schemas and wire types come from that descriptor;
SQLite metadata determines compatible local storage. Missing or extra managed columns, mismatched
keys, invalid values and changed generation identities fail before upload or remote application.

The discovery response is the only admitted contract. There is no separate capabilities preview.

### Automatic Downloads

Automatic downloads are an optional attached-session worker. They are not part of `open()` or
`attach(userId)` and must be started explicitly by app code.

The worker only downloads. It may poll `pullToStable()` or, when the admitted discovery contract
sets `watch_enabled`, use `/sync/watch` as a metadata-only wake-up stream. The watch
payload is never applied directly; `pullToStable()` remains the authoritative remote-data path.

### `detach()`

`detach()` safely removes the current attached account scope from the local database.

It is fail-closed: if attached pending sync data still exists, it returns
`DetachOutcome.BLOCKED_UNSYNCED_DATA` and makes no destructive local changes.

If destructive cleanup succeeds, `detach()` clears managed local state and immediately rebinds the
anonymous database to a fresh internally generated `sourceId`.

If detach is blocked, rolls back, or only cancels a pending `remote_replace`, the existing source
id is preserved.

### `syncThenDetach()`

`syncThenDetach()` runs at most three sync/detach rounds with one 64-attempt push allowance. If
new local writes arrive during a round, it can drain them within that bound. A blocked result
includes its reason and pending row count; the last sync report is absent when no round finished.
When the final detach succeeds destructively, it rotates to a fresh internal source exactly like
plain `detach()`.

## Authority States

For the currently attached account, oversqlite reports:

- `PENDING_LOCAL_SEED`
- `AUTHORITATIVE_EMPTY`
- `AUTHORITATIVE_MATERIALIZED`

These are scope/materialization states, not authentication states.

## Conflict Resolution

When concurrent local and remote changes overlap, the configured resolver receives the local,
server, and common-base values for the affected row. It may defer the decision, accept the server
value, keep the local value, or return a merged value. A deferred result keeps the conflict pending
without publishing a false success. Accepted and merged outcomes are applied transactionally and
advance sync state only after the resulting row is durable; invalid decisions fail without
discarding the pending local intent.

Delete-versus-update conflicts use the same outcome model. Resolvers must not infer visibility of
fields absent from their generated schema, and mixed-generation peers preserve such hidden fields
when applying a valid local or merged outcome.

## Rebuild And Recovery

`rebuild()` is the optional explicit recovery entry point. Pull-side `history_pruned` and
`checkpoint_ahead` conditions durably set the rebuild gate before snapshot work begins, and normal
`sync()` or `pullToStable()` automatically resumes checkpoint recovery after interruption.

Important rules:

- it remains an attached/authenticated operation
- it rebuilds local managed tables from the authoritative remote snapshot
- oversqlite chooses the internal mode
- app code does not supply a replacement `sourceId`

In ordinary checkpoint-recovery cases, recovery keeps the current source. Pending offline work is
frozen and uploaded first when safe; an unresolved upload returns a typed actionable blocker while
preserving the app row, outbox, checkpoint, and rebuild gate. The checkpoint advances only in the
final atomic snapshot apply.

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

## What ready publication does not mean

Coordinated ready publication does not mean:

- the account is attached
- the server session is connected
- the local DB was synchronized
- remote state was rebuilt

Even when durable local attachment metadata already exists, you should still call `attach(userId)`
when an authenticated session is present so the client can resume connected sync operations.
