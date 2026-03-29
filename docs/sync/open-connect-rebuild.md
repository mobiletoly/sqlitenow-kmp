---
layout: doc
title: Open, Attach, and Rebuild
permalink: /sync/open-connect-rebuild/
parent: Sync
---

# Open, Attach, and Rebuild

These three concepts do different jobs:

- `open(sourceId)`: prepare local runtime state for one app-owned install identity
- `attach(userId)`: attach or resume an authenticated account scope
- `rebuild(...)`: replace local managed state from the authoritative remote snapshot

## `open(sourceId)`

`open(sourceId)` is local-only. It:

- validates sync-managed table metadata
- installs local triggers and guards
- repairs interrupted lifecycle metadata
- binds or validates the durable local `sourceId`
- captures pre-existing managed rows once when bootstrap policy allows it

It never talks to the server.

```kotlin
when (val state = client.open(sourceId).getOrThrow()) {
    OpenState.ReadyAnonymous -> Unit
    is OpenState.ReadyAttached -> Unit
    is OpenState.AttachRecoveryRequired -> Unit
}
```

`sourceId` is app-owned. Reuse the same install-scoped value across launches, sign-out/sign-in,
and account switching on one install. Different installs/devices must use different values.

## `attach(userId)`

`attach(userId)` is the authenticated lifecycle step.

```kotlin
when (val result = client.attach(userId).getOrThrow()) {
    is AttachResult.Connected -> {
        when (result.outcome) {
            AttachOutcome.RESUMED_ATTACHED_STATE -> Unit
            AttachOutcome.USED_REMOTE_STATE -> Unit
            AttachOutcome.SEEDED_FROM_LOCAL -> Unit
            AttachOutcome.STARTED_EMPTY -> Unit
        }
    }
    is AttachResult.RetryLater -> Unit
}
```

Important points:

- call it whenever an authenticated session exists
- `OpenState.ReadyAttached(userId)` is not a substitute for `attach(userId)`
- `RetryLater` is a normal lifecycle response, not an auth error

## `rebuild(RebuildMode.KEEP_SOURCE)`

Use this when local managed state must be replaced from the authoritative remote snapshot but the
current source identity should remain valid.

```kotlin
val report = client.rebuild(
    mode = RebuildMode.KEEP_SOURCE,
    newSourceId = null,
).getOrThrow()
```

## `rebuild(RebuildMode.ROTATE_SOURCE, newSourceId)`

Use this when recovery should also rotate to a fresh caller-provided source identity.

```kotlin
val report = client.rebuild(
    mode = RebuildMode.ROTATE_SOURCE,
    newSourceId = "install-source-recovery-2",
).getOrThrow()
```

## `rotateSource(newSourceId)`

`rotateSource(newSourceId)` rotates the current source identity without discarding pending local
edits.

```kotlin
val rotation = client.rotateSource("install-source-recovery-3").getOrThrow()
println("new source = ${rotation.sourceId}")
```

This is an advanced recovery API, not standard login/logout lifecycle.

## Source Binding Mismatch

If `open(sourceId)` throws `SourceBindingMismatchException`, the provided `sourceId` does not match
the local database already on disk.

Default handling:

- stop sync
- do not silently generate a new `sourceId`
- do not silently rebind the existing database
- run an explicit app-owned reset path that recreates the local Oversqlite database or local sync
  state

Only apps that already persist and trust the original install `sourceId` outside Oversqlite should
retry with a different value, and only if that value is the original one.

## Typical App Flow

```kotlin
client.open(sourceId).getOrThrow()

if (currentUserId != null) {
    when (val attach = client.attach(currentUserId).getOrThrow()) {
        is AttachResult.Connected -> client.sync().getOrThrow()
        is AttachResult.RetryLater -> Unit
    }
}
```

## Typical Logout Flow

```kotlin
val result = client.syncThenDetach().getOrThrow()
if (!result.isSuccess()) {
    // Ask the UI to stop producing new writes or keep the user attached.
}
```
