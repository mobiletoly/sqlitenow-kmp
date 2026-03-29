---
layout: doc
title: Getting Started with Sync
permalink: /sync/getting-started/
parent: Sync
---

# Getting Started with Sync

This is the current Oversqlite lifecycle:

1. Create a client
2. Load your app-owned install `sourceId`
3. Call `open(sourceId)` on every app launch
4. Call `attach(userId)` whenever an authenticated session exists
5. Use `sync()`, `pushPending()`, `pullToStable()`, `rebuild(...)`, or `syncThenDetach()` from
   there

## Prerequisites

You need:

- SQLiteNow already set up in your Kotlin Multiplatform module
- a compatible Oversqlite server
- a Ktor `HttpClient` that already knows how to authenticate requests

## Step 1: Add Ktor Dependencies

```kotlin
commonMain.dependencies {
    implementation("io.ktor:ktor-client-core:3.4.1")
    implementation("io.ktor:ktor-client-content-negotiation:3.4.1")
    implementation("io.ktor:ktor-client-auth:3.4.1")
    implementation("io.ktor:ktor-serialization-kotlinx-json:3.4.1")
}

androidMain.dependencies {
    implementation("io.ktor:ktor-client-okhttp:3.4.1")
}

iosMain.dependencies {
    implementation("io.ktor:ktor-client-darwin:3.4.1")
}
```

## Step 2: Mark Sync-Managed Tables

Use `enableSync=true` on every table that should participate in Oversqlite sync.

```sql
-- @@{ enableSync=true }
CREATE TABLE person (
    id TEXT PRIMARY KEY NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE
);

-- @@{ enableSync=true }
CREATE TABLE note (
    id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),
    person_id TEXT REFERENCES person(id) DEFERRABLE INITIALLY DEFERRED,
    title TEXT NOT NULL,
    content TEXT
);
```

### Key Requirements

Sync-managed tables must expose exactly one visible primary key column and it must hold UUID data:

- `TEXT` with canonical UUID strings
- `BLOB` with UUID bytes

Unsupported for sync-managed tables:

- `INTEGER PRIMARY KEY`
- composite keys

For absolute timestamps in sync-managed data, prefer RFC3339/ISO-8601 text with an explicit zone
such as `2026-03-24T18:42:11Z`, and map it to `kotlin.time.Instant` with adapters. Avoid naive
SQLite-style text such as `2026-03-24 18:42:11` when the value represents a real instant in time.
Oversqlite replay treats only explicit RFC3339 instants semantically; offset-free timestamp text is
treated as ordinary payload text.

## Step 3: Create an Authenticated `HttpClient`

Oversqlite does not own auth. Build a normal authenticated Ktor client:

```kotlin
fun createSyncHttpClient(
    baseUrl: String,
    getToken: suspend () -> String?,
    refreshToken: suspend () -> String?
): HttpClient {
    return HttpClient {
        install(ContentNegotiation) {
            json(Json {
                ignoreUnknownKeys = true
            })
        }
        install(Auth) {
            bearer {
                loadTokens {
                    getToken()?.let { BearerTokens(it, "") }
                }
                refreshTokens {
                    refreshToken()?.let { BearerTokens(it, "") }
                }
            }
        }
        defaultRequest {
            url(baseUrl)
        }
    }
}
```

If your backend binds auth to source identity, use the same exact app-owned install `sourceId`
value for auth and for `open(sourceId)`.

## Step 4: Create the Client

```kotlin
val client = db.newOversqliteClient(
    schema = "myapp",
    httpClient = httpClient,
    resolver = ServerWinsResolver,
)
```

The client is lifecycle-neutral until `open(sourceId)` runs.

## Step 5: Persist One Install `sourceId`

Recommended app pattern:

- generate a stable install `sourceId` the first time the app launches
- persist it in durable app storage
- reuse it across app launches, sign-out/sign-in, and account switching on the same install
- generate a different `sourceId` on a different device/install

If local app data is wiped and the local database is recreated, generating a new `sourceId` is
correct. The cost is loss of unsynced local-only state from the wiped database.

## Step 6: Open on Every Launch

```kotlin
when (val state = client.open(sourceId).getOrThrow()) {
    OpenState.ReadyAnonymous -> {
        // No local account is attached yet.
    }
    is OpenState.ReadyAttached -> {
        // Local attachment metadata exists for state.scope.
        // If the app still has an authenticated session, call attach(state.scope).
    }
    is OpenState.AttachRecoveryRequired -> {
        // A remote-authoritative replacement was interrupted.
        // Call attach(state.targetScope) when auth for that user is available.
    }
}
```

`open(sourceId)` is local-only and safe to run repeatedly.

## Step 7: Attach the Authenticated User

```kotlin
when (val attach = client.attach(currentUserId).getOrThrow()) {
    is AttachResult.Connected -> {
        when (attach.outcome) {
            AttachOutcome.RESUMED_ATTACHED_STATE -> Unit
            AttachOutcome.USED_REMOTE_STATE -> Unit
            AttachOutcome.SEEDED_FROM_LOCAL -> Unit
            AttachOutcome.STARTED_EMPTY -> Unit
        }
    }
    is AttachResult.RetryLater -> {
        // Retry after attach.retryAfterSeconds.
    }
}
```

Call `attach(userId)` whenever an authenticated session exists. It is not a one-time setup method.

## Step 8: Run Sync Operations

Normal interactive sync:

```kotlin
client.sync().getOrThrow()
```

Upload-only:

```kotlin
client.pushPending().getOrThrow()
```

Pull-only:

```kotlin
client.pullToStable().getOrThrow()
```

## Step 9: Detach Safely

If you want a direct detach:

```kotlin
when (client.detach().getOrThrow()) {
    DetachOutcome.DETACHED -> Unit
    DetachOutcome.BLOCKED_UNSYNCED_DATA -> {
        // Keep the user attached or sync first.
    }
}
```

If you want bounded best-effort flushing first:

```kotlin
val result = client.syncThenDetach().getOrThrow()
if (!result.isSuccess()) {
    // result.detach == BLOCKED_UNSYNCED_DATA
    // result.remainingPendingRowCount tells you what was left.
}
```

## Step 10: Rebuild or Rotate Only for Recovery

Rebuild from authoritative remote state:

```kotlin
client.rebuild(
    mode = RebuildMode.KEEP_SOURCE,
    newSourceId = null,
).getOrThrow()
```

Rebuild and rotate to a fresh source stream:

```kotlin
client.rebuild(
    mode = RebuildMode.ROTATE_SOURCE,
    newSourceId = "install-source-recovery-2",
).getOrThrow()
```

Advanced source-only rotation:

```kotlin
client.rotateSource("install-source-recovery-3").getOrThrow()
```

Normal lifecycle should use `open(sourceId)`, `attach()`, sync operations, and `detach()`. Rebuild
and rotation are recovery tools.

## `SourceBindingMismatchException`

If `open(sourceId)` throws `SourceBindingMismatchException`, the local database is already bound to
a different source identity.

Default handling:

- stop sync
- do not silently generate a new `sourceId`
- do not silently rebind the existing database
- run an explicit app-owned reset path that recreates the local Oversqlite database or local sync
  state

Only apps that already persist and trust the original install `sourceId` outside Oversqlite should
retry with a different value, and only if that value is the original one.
