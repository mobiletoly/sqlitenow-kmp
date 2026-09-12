---
layout: doc
title: Getting Started with Sync
permalink: /kmp/sync/getting-started/
parent: KMP Sync
parent_url: /kmp/sync/
---

# Getting Started with Sync

This is the current oversqlite lifecycle:

1. Reserve the database with `openForSync(...)`
2. Call `advance()` and retain its exact ready client
3. Call `attach(userId)` whenever an authenticated session exists
4. Use `sync()`, `pushPending()`, `pullToStable()`, `rebuild()`, or `syncThenDetach()`

## Prerequisites

You need:

- SQLiteNow already set up in your Kotlin Multiplatform module
- `dev.goquick.sqlitenow:oversqlite` added to the module that owns the generated database
- a compatible oversqlite server
- a Ktor `HttpClient` that already knows how to authenticate requests

Web apps do not inject a separate SQLite provider. Generated `openForSync(...)` reserves the
database; after `advance()` reaches ready, consume that opening's exact `readyOrNull()` database and
client pair. The pair uses the packaged direct-OPFS worker in supported JS/Wasm browsers and the
transient worker in JS Node. Browser capability and COOP/COEP/CSP requirements are
described in the [KMP getting-started guide]({{ site.baseurl }}/kmp/getting-started/).

## Step 1: Add Runtime Dependencies

Sync-enabled databases use two SQLiteNow runtime artifacts:

- `dev.goquick.sqlitenow:core`
- `dev.goquick.sqlitenow:oversqlite`

Replace `X.Y.Z` with the latest SQLiteNow release version.

```kotlin
commonMain.dependencies {
    implementation("dev.goquick.sqlitenow:core:X.Y.Z")
    implementation("dev.goquick.sqlitenow:oversqlite:X.Y.Z")
}
```

## Step 2: Add Ktor Dependencies

```kotlin
commonMain.dependencies {
    implementation("io.ktor:ktor-client-core:3.5.1")
    implementation("io.ktor:ktor-client-content-negotiation:3.5.1")
    implementation("io.ktor:ktor-client-auth:3.5.1")
    implementation("io.ktor:ktor-serialization-kotlinx-json:3.5.1")
}

androidMain.dependencies {
    implementation("io.ktor:ktor-client-okhttp:3.5.1")
}

iosMain.dependencies {
    implementation("io.ktor:ktor-client-darwin:3.5.1")
}
```

## Step 3: Enable Oversqlite Bridge Generation

The plugin only generates `buildOversqliteConfig(...)` and `newOversqliteClient(...)` when the
database DSL opts in explicitly:

```kotlin
sqliteNow {
    databases {
        create("AppDatabase") {
            packageName = "com.example.app.db"
            oversqlite = true
        }
    }
}
```

`oversqlite = true` enables oversqlite bridge code generation for that database. Table-level
`enableSync=true` still decides which tables participate in sync.

The generated `buildOversqliteConfig(...)` and `newOversqliteClient(...)` functions use the
generated `enableSync` table list directly. Numeric wire handling is automatic from SQLite affinity;
there is no per-column numeric metadata or `syncTables` override required for numeric values.
Generation with `oversqlite=false` is unchanged.

## Step 4: Mark Sync-Managed Tables

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

Sync-managed tables must expose exactly one visible primary-key column. The discovered server contract determines its wire type:

- `text`: `TEXT` containing an ordinary text key
- `uuid`: `TEXT` containing canonical UUID strings, or `BLOB` containing 16 UUID bytes

Unsupported for sync-managed tables:

- `INTEGER PRIMARY KEY`
- composite keys

## Step 5: Create an Authenticated `HttpClient`

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

## Step 6: Coordinate Opening and Retrieve the Ready Client

```kotlin
val config = db.buildOversqliteConfig(
    generationId = 1L,
    databaseId = "AppDatabase",
    serviceId = "myapp-sync",
)
val opening = db.openForSync(
    config = config,
    httpClient = httpClient,
    resolver = ServerWinsResolver,
)
opening.advance()
val ready = checkNotNull(opening.readyOrNull())
val database = ready.database
val client = ready.client
```

Select the server's generation and keep `databaseId` and `serviceId` stable across launches.
Fresh and unchanged opening works offline; first attachment
discovers and validates the generation and pending values before installing its contract and
connecting. Later opens use the retained contract. The SDK derives server schemas and conversions
from discovery and supplies source and generation headers.

Do not access application tables through the opening while it is pending. Database reads and writes become available only through the exact ready database/client pair.

## Step 7: Inspect and Advance Explicit Upgrades

```kotlin
val status = opening.status()
// Supply an explicit upgrade plan when status reports a version or generation edge.
opening.advance()
```

Do not call the ordinary database `open()` first. Coordinated opening owns inspection, migration,
publication, cancellation, and shutdown.

## Step 8: Attach the Authenticated User

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

## Step 9: Run Sync Operations

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

## Step 10: Optional Automatic Downloads

Automatic downloads are default-off. Start the worker only after `openForSync(...).advance()` has
published the ready pair and `attach(userId)` has completed.

```kotlin
val automaticDownloads = coroutineScope.launch {
    client.runAutomaticDownloads(
        db.buildOversqliteAutomaticDownloadConfig(
            bundleChangeWatchMode = BundleChangeWatchMode.AUTO,
        ),
    )
}

// Later, when this client/session should stop background downloads:
automaticDownloads.cancelAndJoin()
```

Watch support is an optional latency optimization. When the admitted discovery contract sets
`watch_enabled`, the worker uses `/sync/watch` only as a wake-up hint and still
downloads authoritative data through `pullToStable()`. If watch is unavailable, the worker falls
back to polling.

## Step 11: Detach Safely

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

## Step 12: Resume Checkpoint Recovery Automatically

```kotlin
client.rebuild().getOrThrow()
```

Ordinary `sync()` and `pullToStable()` automatically resume durable checkpoint recovery caused by
retained history or a checkpoint ahead of the server. `rebuild()` remains an optional explicit
control. Source-identity recovery still requires explicit `rebuild()`; Oversqlite decides
internally whether recovery keeps the current source or performs rebuild-plus-rotate recovery.

## Step 13: Inspect Debug Diagnostics When Needed

```kotlin
val info = client.sourceInfo().getOrThrow()
println(info.currentSourceId)
println(info.rebuildRequired)
println(info.sourceRecoveryRequired)
```

`SourceInfo` is for diagnostics only. `currentSourceId` is opaque and must not be treated as an
app-owned control surface.
