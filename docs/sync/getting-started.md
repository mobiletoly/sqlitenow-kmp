---
layout: doc
title: Getting Started with Sync
permalink: /sync/getting-started/
parent: Sync
---

# Getting Started with Sync

This is the current oversqlite lifecycle:

1. Create a client
2. Call `open()` on every app launch
3. Call `attach(userId)` whenever an authenticated session exists
4. Use `sync()`, `pushPending()`, `pullToStable()`, `rebuild()`, or `syncThenDetach()`

## Prerequisites

You need:

- SQLiteNow already set up in your Kotlin Multiplatform module
- a compatible oversqlite server
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

Use `enableSync=true` on every table that should participate in oversqlite sync.

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

Sync-managed tables must expose exactly one visible primary-key column and it must hold UUID data:

- `TEXT` with canonical UUID strings
- `BLOB` with UUID bytes

Unsupported for sync-managed tables:

- `INTEGER PRIMARY KEY`
- composite keys

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

## Step 4: Create the Client

```kotlin
val client = db.newOversqliteClient(
    schema = "myapp",
    httpClient = httpClient,
    resolver = ServerWinsResolver,
)
```

The client is lifecycle-neutral until `open()` runs.

## Step 5: Open on Every Launch

```kotlin
client.open().getOrThrow()
```

`open()` is local-only and safe to run repeatedly.

## Step 6: Attach the Authenticated User

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

## Step 7: Run Sync Operations

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

## Step 8: Detach Safely

Direct detach:

```kotlin
when (client.detach().getOrThrow()) {
    DetachOutcome.DETACHED -> {
        // Managed local sync state was cleared and oversqlite rebound to a fresh source id.
    }
    DetachOutcome.BLOCKED_UNSYNCED_DATA -> {
        // Keep the user attached or sync first.
    }
}
```

Bounded best-effort flushing first:

```kotlin
val result = client.syncThenDetach().getOrThrow()
if (!result.isSuccess()) {
    // result.detach == BLOCKED_UNSYNCED_DATA
    // result.remainingPendingRowCount tells you what was left.
}
```

On successful destructive detach, the next attach starts from a fresh oversqlite source stream even
on the same install.

## Step 9: Rebuild Explicitly When Recovery Requires It

```kotlin
client.rebuild().getOrThrow()
```

`rebuild()` is the explicit recovery entry point. Oversqlite decides internally whether that
rebuild keeps the current source or performs rebuild-plus-rotate recovery.

## Step 10: Inspect Debug Diagnostics When Needed

```kotlin
val info = client.sourceInfo().getOrThrow()
println(info.currentSourceId)
println(info.rebuildRequired)
println(info.sourceRecoveryRequired)
```

`SourceInfo` is for diagnostics only. `currentSourceId` is opaque and must not be treated as an
app-owned control surface.
