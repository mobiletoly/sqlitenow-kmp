---
layout: doc
title: Bootstrap & Hydration
permalink: /sync/bootstrap-hydration/
parent: Sync
---

# Bootstrap & Hydration

Bootstrap and hydration are two critical operations that prepare your app for synchronization. Understanding when and how to use them is essential for a robust sync implementation.

## Core Concepts

### User Identity
A **user** represents a person who signs into your app across multiple devices. Users are identified by a stable, unique identifier (typically from your authentication system's JWT `sub` claim).

**Key characteristics:**
- **Stable across devices**: Same user ID on phone, tablet, laptop, etc.
- **Persistent**: User ID doesn't change across app sessions
- **Unique**: Each person gets their own user ID
- **Authentication-based**: Usually derived from your auth system (Firebase, Auth0, custom JWT, etc.)

### Device Identity (Source ID)
A **device ID** (also called `sourceId`) represents a specific app installation. Each device gets its own unique identifier that persists across app launches.

**Key characteristics:**
- **Per-installation**: Each app install gets a new device ID
- **Persistent**: Survives app restarts, updates, and device reboots
- **Independent of user**: Same device can be used by different users
- **Prevents echo**: Server uses device ID to avoid sending back your own changes

## Bootstrap: Preparing the Local Database

Bootstrap initializes the sync system for a specific user on a specific device. It sets up the necessary metadata tables, triggers, and client information.

### When to Call Bootstrap
```kotlin
// Call bootstrap AFTER user signs in successfully
suspend fun onUserSignIn(userId: String) {
    val client = createSyncClient()
    client.bootstrap(userId = userId, sourceId = getDeviceId()).getOrThrow()
    
    // Now ready for sync operations
}

// Call bootstrap when switching users
suspend fun switchUser(newUserId: String) {
    // Clear old user data or use fresh database
    database.clearUserData()
    
    val client = createSyncClient()
    client.bootstrap(userId = newUserId, sourceId = getDeviceId()).getOrThrow()
}
```

### What Bootstrap Does
1. **Creates metadata tables**: `_sync_client_info`, `_sync_row_meta`, `_sync_pending`
2. **Installs triggers**: Automatically tracks changes to sync-enabled tables
3. **Records client info**: Stores user ID, device ID, and sync state
4. **Prepares for sync**: Database is now ready for upload/download operations

### Bootstrap Parameters
- **`userId`**: Stable user identifier from your auth system
- **`sourceId`**: Persistent device identifier (see Device ID Management below)

## Hydration: Initial Data Download

Hydration downloads the complete dataset for a user, typically used when setting up a new device or recovering from data loss.

### When to Use Hydration
```kotlin
// New device setup (first sign-in on this device)
suspend fun setupNewDevice(userId: String) {
    client.bootstrap(userId, deviceId).getOrThrow()
    client.hydrate(limit = 1000, windowed = true).getOrThrow()
    // Device now has complete user dataset
}

// Data recovery scenarios
suspend fun recoverFromDataLoss() {
    client.hydrate(limit = 1000, windowed = true).getOrThrow()
    // Local database restored from server
}
```

### When to Use Incremental Sync Instead
```kotlin
// Session restore (app restart with existing data)
suspend fun restoreSession(userId: String) {
    client.bootstrap(userId, deviceId).getOrThrow()
    
    // Use incremental sync to catch up on changes
    var more = true
    while (more) {
        val (applied, _) = client.downloadOnce(limit = 500).getOrThrow()
        more = applied == 500  // Continue if page was full
        if (applied == 0) break
    }
}
```

### Hydration Modes
- **`windowed = true`** (recommended): Creates consistent snapshot, safe for large datasets
- **`windowed = false`**: Simpler paging, suitable for small datasets
- **`includeSelf = false`** (default): Excludes your own changes to avoid duplicates
- **`limit`**: Page size for downloads (1000 is a good default)

## Device ID Management

Device IDs must be generated once per app installation and persisted across app restarts.

### Multiplatform Implementation
Using [Multiplatform Settings](https://github.com/russhwolf/multiplatform-settings):

```kotlin
class DeviceIdManager {
    private val settings: Settings = Settings()
    
    fun getDeviceId(): String {
        return settings.getStringOrNull("device_id") ?: generateAndSaveDeviceId()
    }

    private fun generateAndSaveDeviceId(): String {
        val deviceId = "device-${Uuid.random()}"
        settings.putString("device_id", deviceId)
        return deviceId
    }
}
```

## Application Lifecycle Integration

### App Startup Flow
```kotlin
class SyncManager {
    suspend fun initializeSync(userId: String): Result<Unit> {
        return try {
            // 1. Ensure device ID exists
            val deviceId = deviceIdManager.getDeviceId()
            
            // 2. Create sync client
            val client = database.newOversqliteClient(
                schema = "myapp",
                httpClient = authenticatedHttpClient,
                resolver = ServerWinsResolver
            )
            
            // 3. Bootstrap is always required
            client.bootstrap(userId, deviceId).getOrThrow()
            
            // 4. Determine if this is first-time setup or session restore
            val isFirstTime = !hasExistingUserData()
            
            if (isFirstTime) {
                // New device: full hydration
                client.hydrate(limit = 1000, windowed = true).getOrThrow()
            } else {
                // Existing device: incremental sync
                performIncrementalSync(client)
            }
            
            Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
}
```

### User Sign-In Flow
```kotlin
suspend fun onUserSignIn(credentials: UserCredentials) {
    try {
        // 1. Authenticate with your auth system
        val authResult = authService.signIn(credentials)
        val userId = authResult.userSub  // Stable user identifier
        
        // 2. Initialize sync for this user
        syncManager.initializeSync(userId).getOrThrow()
        
        // 3. Start periodic sync
        startPeriodicSync()
        
    } catch (e: Exception) {
        handleSignInError(e)
    }
}
```

### User Sign-Out Flow
```kotlin
suspend fun onUserSignOut() {
    // 1. Stop sync operations
    stopPeriodicSync()
    
    // 2. Clear user data (optional, depends on your app's requirements)
    database.clearUserData()
    
    // 3. Clear auth tokens
    authTokenManager.clearTokens()
    
    // Note: Device ID persists across sign-outs
}
```

## Best Practices

### Bootstrap Guidelines
- **Always call bootstrap** after user authentication
- **Call once per user session** (not on every app start)
- **Ensure business tables exist** before bootstrap
- **Handle bootstrap failures** gracefully with retry logic

### Hydration Guidelines
- **Use windowed mode** for production apps
- **Choose appropriate page size** (1000 is usually good)
- **Show progress indicators** for large datasets
- **Handle network interruptions** with resume capability

### Device ID Guidelines
- **Generate once per installation** and persist
- **Use platform-appropriate storage** (SharedPreferences, UserDefaults, etc.)
- **Include meaningful prefix** like "device-" for debugging
- **Don't reuse device IDs** across app reinstalls

---

**Next Steps**: Learn about [Sync Operations]({{ site.baseurl }}/sync/sync-operations/) to understand upload/download patterns and conflict resolution.
