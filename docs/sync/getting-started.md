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

## Step 1: Enable Sync on Tables

First, decide which tables should be synchronized across devices. Add the `changeLogs=true` annotation to enable sync tracking:

```sql
-- Enable sync for this table
-- @@{ changeLogs=true }
CREATE TABLE person (
    id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);

-- @@{ changeLogs=true }
CREATE TABLE note (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT,
    person_id INTEGER REFERENCES person(id),
    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
);
```

**Important Notes:**
- Only tables with `changeLogs=true` will be synchronized
- Tables without this annotation remain local-only
- You can mix sync-enabled and local-only tables in the same database

## Step 2: Add Ktor Dependencies

Add the required Ktor dependencies to your `build.gradle.kts`:

```kotlin
commonMain.dependencies {
    // SQLiteNow library with sync support
    implementation("dev.goquick.sqlitenow:core:0.16.0")

    // Ktor for HTTP communication
    implementation("io.ktor:ktor-client-core:3.3.0")
    implementation("io.ktor:ktor-client-content-negotiation:3.3.0")
    implementation("io.ktor:ktor-serialization-kotlinx-json:3.3.0")
    implementation("io.ktor:ktor-client-auth:3.3.0")
}

androidMain.dependencies {
    implementation("io.ktor:ktor-client-okhttp:3.3.0")
}

iosMain.dependencies {
    implementation("io.ktor:ktor-client-darwin:3.3.0")
}
```

## Step 3: Configure Authentication

Create an authenticated HttpClient with JWT token management:

```kotlin
import io.ktor.client.*
import io.ktor.client.plugins.auth.*
import io.ktor.client.plugins.auth.providers.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.url
import io.ktor.serialization.kotlinx.json.*
import kotlinx.serialization.json.Json

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
                resolver = ServerWinsResolver // or ClientWinsResolver
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
            
            // Perform initial data hydration
            val hydrateResult = client.hydrate(limit = 1000, windowed = true)
            if (hydrateResult.isFailure) {
                return Result.failure(hydrateResult.exceptionOrNull() ?: Exception("Hydration failed"))
            }
            
            Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
    
    suspend fun performSync(): Result<SyncSummary> {
        val client = syncClient ?: return Result.failure(Exception("Sync not initialized"))
        
        return try {
            // Upload local changes
            val uploadResult = client.uploadOnce(limit = 1000)
            if (uploadResult.isFailure) {
                return Result.failure(uploadResult.exceptionOrNull() ?: Exception("Upload failed"))
            }
            
            // Download remote changes
            val downloadResult = client.downloadOnce(limit = 1000)
            if (downloadResult.isFailure) {
                return Result.failure(downloadResult.exceptionOrNull() ?: Exception("Download failed"))
            }
            
            Result.success(SyncSummary(
                uploaded = uploadResult.getOrNull()?.uploaded ?: 0,
                downloaded = downloadResult.getOrNull()?.applied ?: 0
            ))
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
}

data class SyncSummary(
    val uploaded: Int,
    val downloaded: Int
)
```

## Step 6: Integrate with Your App

Finally, integrate sync into your application lifecycle:

```kotlin
class App {
    private val syncManager = SyncManager(database, deviceIdManager, authManager)
    
    suspend fun onUserSignIn(userId: String) {
        // Initialize sync system
        syncManager.initializeSync().getOrThrow()
        
        // Set up sync for this user (first time or new device)
        syncManager.setupNewDevice(userId).getOrThrow()
        
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
                        val summary = result.getOrNull()!!
                        println("Sync completed: ${summary.uploaded} uploaded, ${summary.downloaded} downloaded")
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

### "No changes to upload" 
- Make sure you've enabled `changeLogs=true` on your tables
- Verify that you're making changes to sync-enabled tables
- Check that the sync client was created after the database schema was set up

### Sync seems slow
- Adjust the `limit` parameter in upload/download operations
- Consider implementing windowed hydration for large datasets
- Monitor network conditions and adjust sync frequency accordingly

For more detailed troubleshooting, see our [Troubleshooting Guide]({{ site.baseurl }}/sync/troubleshooting/).
