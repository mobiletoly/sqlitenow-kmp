---
layout: doc
title: Getting Started with Sync
permalink: /sync/getting-started/
parent: Sync
---

# Getting Started with Sync

This guide will walk you through enabling synchronization in your SQLiteNow application. In just a few steps, you'll have multi-device sync working with automatic conflict resolution and offline support.

## Prerequisites

Before you begin, make sure you have:

- SQLiteNow set up in your Kotlin Multiplatform project
- A basic understanding of SQLiteNow's code generation
- Access to a sync server (or willingness to set one up)


## Step 1: Add Ktor Dependencies

Add the required Ktor dependencies to your `build.gradle.kts`:

```kotlin
commonMain.dependencies {
    // Ktor for HTTP communication
    implementation("io.ktor:ktor-client-core:3.4.1")
    implementation("io.ktor:ktor-client-content-negotiation:3.4.1")
    implementation("io.ktor:ktor-serialization-kotlinx-json:3.4.1")
    implementation("io.ktor:ktor-client-auth:3.4.1")
}

androidMain.dependencies {
    implementation("io.ktor:ktor-client-okhttp:3.4.1")
}

iosMain.dependencies {
    implementation("io.ktor:ktor-client-darwin:3.4.1")
}
```


## Step 2: Enable Sync on Tables

First, decide which tables should be synchronized across devices. Add the `enableSync=true`
annotation to enable sync tracking:

```sql
-- Enable sync for this table
-- @@{ enableSync=true }
CREATE TABLE person (
    id TEXT PRIMARY KEY NOT NULL,  -- TEXT with UUID string
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);

-- Alternative: BLOB primary key with UUID bytes
-- @@{ enableSync=true }
CREATE TABLE note (
    id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),  -- BLOB with UUID bytes
    title TEXT NOT NULL,
    content TEXT,
    person_id TEXT REFERENCES person(id) DEFERRABLE INITIALLY DEFERRED,
    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
);
```

### Critical Primary Key Requirements

**MANDATORY: Primary keys MUST contain UUID data in one of these formats:**

**Option 1: TEXT type with UUID strings**
```sql
CREATE TABLE users (
    id TEXT PRIMARY KEY NOT NULL,  -- Will contain UUID strings like "550e8400-e29b-41d4-a716-446655440000"
    name TEXT NOT NULL
);
```

**Option 2: BLOB type with UUID bytes**
```sql
CREATE TABLE users (
    id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),  -- Contains 16-byte UUID data
    name TEXT NOT NULL
) WITHOUT ROWID;  -- Recommended for BLOB primary keys
```

**Why UUIDs are required:**

- **Global uniqueness**: UUIDs prevent conflicts when merging data from multiple devices
- **Offline creation**: Devices can create records offline without server coordination
- **Conflict resolution**: The sync system relies on globally unique identifiers
- **Cross-device consistency**: Same record has same ID across all devices

**Choosing between TEXT and BLOB:**

- **TEXT**: Human-readable, easier debugging, slightly larger storage (36 bytes vs 16 bytes)
- **BLOB**: More compact storage, better performance, requires `WITHOUT ROWID` for optimal
  performance
- **Foreign keys**: Must match the type of the referenced primary key

### Recommended Foreign Key Mode For Sync-Managed Tables

When a sync-managed table references another sync-managed table, prefer:

```sql
person_id TEXT REFERENCES person(id) DEFERRABLE INITIALLY DEFERRED
```

Why:

- oversqlite apply already defers foreign-key checks while remote bundles are replayed
- `INITIALLY DEFERRED` keeps your normal application writes closer to that behavior
- local transactions that insert related rows in multiple steps are less likely to fail

Use `INITIALLY IMMEDIATE` only if you deliberately want ordinary non-sync writes to fail earlier
when they temporarily violate FK ordering inside a transaction.


### Custom Primary Key Column Names

If your table uses a different column name for the primary key, specify it with `syncKeyColumnName`:

```sql
-- Custom primary key column name with TEXT
-- @@{ enableSync=true, syncKeyColumnName=user_uuid }
CREATE TABLE users (
    user_uuid TEXT PRIMARY KEY NOT NULL,  -- TEXT with UUID strings
    name TEXT NOT NULL,
    email TEXT UNIQUE
);

-- Custom primary key column name with BLOB
-- @@{ enableSync=true, syncKeyColumnName=user_id }
CREATE TABLE orders (
    user_id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),  -- BLOB with UUID bytes
    user_uuid TEXT NOT NULL,  -- Foreign key - type must match referenced table
    product_code TEXT NOT NULL
) WITHOUT ROWID;
```

**Important Notes:**
- Only tables with `enableSync=true` will be synchronized
- Tables without this annotation remain local-only
- You can mix sync-enabled and local-only tables in the same database
- Sync-enabled tables must expose exactly one visible sync key column, and it must be the local SQLite `PRIMARY KEY`
- **Primary keys must contain UUID data as either TEXT strings or BLOB bytes**
- `INTEGER` and `BIGINT` primary keys are not supported for sync-enabled tables
- Use `syncKeyColumnName` annotation for custom primary key column names
- The system can auto-detect primary key columns if not explicitly specified
- **Foreign keys must match the type of the referenced primary key** (TEXT or BLOB)
- **For sync-managed foreign keys, prefer `DEFERRABLE INITIALLY DEFERRED`**
- Sync-enabled local tables must not include the reserved server column `_sync_scope_id`

### UUID Generation in Your App

When inserting records, you must generate UUID values for primary keys. The approach depends on whether you're using TEXT or BLOB primary keys:

**For TEXT primary keys (UUID strings):**
```kotlin
@OptIn(ExperimentalUuidApi::class)
suspend fun createPerson(name: String, email: String) {
    val personId = Uuid.random().toString()  // Generate UUID string

    database.personQueries.insert(
        id = personId,  // TEXT primary key with UUID string
        first_name = name.split(" ").first(),
        last_name = name.split(" ").last(),
        email = email,
        created_at = System.currentTimeMillis()
    )
}
```

**For BLOB primary keys (UUID bytes):**
```kotlin
@OptIn(ExperimentalUuidApi::class)
suspend fun createNote(title: String, content: String) {
    val noteId = Uuid.random().toByteArray()  // Generate UUID bytes

    database.noteQueries.insert(
        id = noteId,  // BLOB primary key with UUID bytes
        title = title,
        content = content,
        updated_at = System.currentTimeMillis()
    )
}

// Alternative: Let SQLite generate the BLOB UUID automatically
suspend fun createNoteWithAutoId(title: String, content: String) {
    database.noteQueries.insert(
        // id will be auto-generated by DEFAULT (randomblob(16))
        title = title,
        content = content,
        updated_at = System.currentTimeMillis()
    )
}
```

**Alternative UUID libraries:**
- **Kotlin Multiplatform**: `kotlin.uuid.Uuid` (recommended)
- **Java/Android**: `java.util.UUID.randomUUID().toString()`
- **Third-party**: `com.benasher44:uuid` for KMP projects

## Step 3: Configure Authentication

Create an authenticated HttpClient (ktor) with JWT token management:

```kotlin
fun createSyncHttpClient(
    baseUrl: String,
    getToken: suspend () -> String?,
    refreshToken: suspend () -> String?
): HttpClient {
    return HttpClient {
        install(ContentNegotiation) {
            json(Json { ignoreUnknownKeys = true })
        }
        
        install(Auth) {
            bearer {
                loadTokens {
                    val token = getToken()
                    if (token != null) {
                        BearerTokens(accessToken = token, refreshToken = null)
                    } else {
                        null
                    }
                }
                
                refreshTokens {
                    val newToken = refreshToken()
                    if (newToken != null) {
                        BearerTokens(accessToken = newToken, refreshToken = null)
                    } else {
                        null
                    }
                }
            }
        }
        
        defaultRequest {
            url(baseUrl)
        }
    }
}
```

## Step 4: Implement Device ID Management

Create a persistent device ID that survives app restarts:

```kotlin
class DeviceIdManager {
    private var cachedDeviceId: String? = null
    
    fun getDeviceId(): String {
        return cachedDeviceId ?: run {
            val deviceId = loadOrCreateDeviceId()
            cachedDeviceId = deviceId
            deviceId
        }
    }
    
    private fun loadOrCreateDeviceId(): String {
        // Try to load existing device ID from storage
        val existingId = loadDeviceIdFromStorage()
        if (existingId != null) {
            return existingId
        }
        
        // Generate new device ID
        val newDeviceId = "device-${generateUUID()}"
        saveDeviceIdToStorage(newDeviceId)
        return newDeviceId
    }
    
    // Platform-specific implementations
    private fun loadDeviceIdFromStorage(): String? {
        // Android: SharedPreferences
        // iOS: UserDefaults
        // Implement based on your platform
        TODO("Implement platform-specific storage")
    }
    
    private fun saveDeviceIdToStorage(deviceId: String) {
        // Android: SharedPreferences
        // iOS: UserDefaults
        // Implement based on your platform
        TODO("Implement platform-specific storage")
    }
    
    private fun generateUUID(): String {
        // Use platform-specific UUID generation
        TODO("Implement UUID generation")
    }
}
```

## Step 5: Create Sync Client

Now create your sync client using the generated database method:

```kotlin
class SyncManager(
    private val database: YourDatabase, // Your generated database class
    private val deviceIdManager: DeviceIdManager,
    private val authManager: AuthManager // Your authentication system
) {
    private var syncClient: OversqliteClient? = null
    
    suspend fun initializeSync(): Result<Unit> {
        return try {
            val httpClient = createSyncHttpClient(
                baseUrl = "https://api.yourapp.com",
                getToken = { authManager.getAccessToken() },
                refreshToken = { authManager.refreshAccessToken() }
            )
            
            syncClient = database.newOversqliteClient(
                schema = "yourapp", // Your app's schema name
                httpClient = httpClient,
                resolver = ServerWinsResolver
            )
            
            Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
    
    suspend fun setupNewDevice(userId: String): Result<Unit> {
        val client = syncClient ?: return Result.failure(Exception("Sync not initialized"))
        val deviceId = deviceIdManager.getDeviceId()
        
        return try {
            // Bootstrap this device for the user
            val bootstrapResult = client.bootstrap(userId = userId, sourceId = deviceId)
            if (bootstrapResult.isFailure) {
                return Result.failure(bootstrapResult.exceptionOrNull() ?: Exception("Bootstrap failed"))
            }
            
            client.hydrate().getOrThrow()
            
            Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
    
    suspend fun restoreExistingDevice(userId: String): Result<Unit> {
        val client = syncClient ?: return Result.failure(Exception("Sync not initialized"))
        val deviceId = deviceIdManager.getDeviceId()
        
        return try {
            client.bootstrap(userId = userId, sourceId = deviceId).getOrThrow()
            client.pullToStable().getOrThrow()
            Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    suspend fun performSync(): Result<Unit> {
        val client = syncClient ?: return Result.failure(Exception("Sync not initialized"))

        return try {
            client.sync().getOrThrow()
            Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
}
```

Resolver choice is made when the sync client is created:

```kotlin
val serverWins = database.newOversqliteClient(
    schema = "yourapp",
    httpClient = httpClient,
    resolver = ServerWinsResolver,
)

val clientWins = database.newOversqliteClient(
    schema = "yourapp",
    httpClient = httpClient,
    resolver = ClientWinsResolver,
)

val custom = database.newOversqliteClient(
    schema = "yourapp",
    httpClient = httpClient,
    resolver = Resolver { conflict ->
        when (conflict.table) {
            "notes" -> MergeResult.KeepLocal
            "profiles" -> MergeResult.KeepMerged(mergeProfile(conflict.serverRow, conflict.localPayload))
            else -> MergeResult.AcceptServer
        }
    },
)
```

`resolver` is the client-wide policy hook. `KeepLocal` and `KeepMerged(...)` are returned later for
individual conflicts; they are not standalone constructor options.

## Step 6: Integrate with Your App

Finally, integrate sync into your application lifecycle:

```kotlin
class App {
    private val syncManager = SyncManager(database, deviceIdManager, authManager)
    
    suspend fun onUserSignIn(userId: String) {
        // Initialize sync system
        syncManager.initializeSync().getOrThrow()
        
        // Choose the correct restore path for this device
        if (hasExistingManagedData()) {
            syncManager.restoreExistingDevice(userId).getOrThrow()
        } else {
            syncManager.setupNewDevice(userId).getOrThrow()
        }
        
        // Start periodic sync
        startPeriodicSync()
    }
    
    private fun startPeriodicSync() {
        // Start background sync every 30 seconds
        // Implement using your preferred coroutine/background task mechanism
        launch {
            while (isActive) {
                delay(30_000) // 30 seconds
                try {
                    val result = syncManager.performSync()
                    if (result.isSuccess) {
                        println("Sync completed")
                    } else {
                        println("Sync failed: ${result.exceptionOrNull()?.message}")
                    }
                } catch (e: Exception) {
                    println("Sync error: ${e.message}")
                }
            }
        }
    }
}
```

## Next Steps

Congratulations! You now have basic sync functionality working. Here's what to explore next:

1. **[Core Concepts]({{ site.baseurl }}/sync/core-concepts/)** - Understand user/device identity best practices
2. **[Bootstrap & Hydration]({{ site.baseurl }}/sync/bootstrap-hydration/)** - Deep dive into device setup
3. **[Sync Operations]({{ site.baseurl }}/sync/sync-operations/)** - Learn about conflict resolution strategies
4. **[Server Setup]({{ site.baseurl }}/sync/server-setup/)** - Set up your own sync server

## Common Issues

### "Bootstrap failed" Error
- Ensure your server is running and accessible
- Check that your JWT token is valid
- Verify the user ID and device ID are properly formatted

### "Primary key constraint failed" or Sync Errors
- **Check primary key types**: Ensure all sync table primary keys are `TEXT` or `BLOB`, not `INTEGER`
- **Verify UUID generation**: Make sure you're generating proper UUID data (strings for TEXT, bytes for BLOB)
- **Foreign key consistency**: Foreign keys referencing sync tables must match the referenced primary key type (TEXT or BLOB)
- **Custom primary keys**: Use `syncKeyColumnName` annotation if your primary key isn't named "id"
- **BLOB primary keys**: Use `WITHOUT ROWID` for optimal performance with BLOB primary keys

### "No changes to upload"
- Make sure you've enabled `enableSync=true` on your tables
- Verify that you're making changes to sync-enabled tables
- Check that the sync client was created after the database schema was set up
- Ensure primary keys contain UUID data (TEXT strings or BLOB bytes)

### Sync seems slow
- Hydration is chunked internally; expect first-device restore to take longer than incremental sync
- Prefer `client.sync()` for normal interactive catch-up and `client.pushPending()` for quick upload-only flushes
- Monitor network conditions and adjust sync frequency accordingly

### "Column not found" Errors in Triggers
- This usually means you're using INTEGER primary keys instead of TEXT or BLOB
- Check that your `syncKeyColumnName` annotation matches your actual column name
- Verify that the primary key column exists and is properly defined
- For BLOB primary keys, ensure the column is defined correctly with proper type
