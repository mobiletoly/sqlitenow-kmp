---
layout: doc
title: Sync Operations
permalink: /sync/sync-operations/
parent: Sync
---

# Sync Operations

SQLiteNow provides three core sync operations: **upload**, **download**, and **combined sync**. Understanding when and how to use each operation is crucial for building efficient sync workflows.

## Upload Operations

Upload operations send your device's local changes to the server, making them available to other devices.

### uploadOnce()

Uploads pending local changes in a single batch.

```kotlin
suspend fun uploadOnce(): Result<UploadSummary>
```

**When to use:**
- **Manual sync triggers**: User taps "sync" button
- **Background sync**: Periodic uploads of accumulated changes
- **Before critical operations**: Ensure changes are backed up before risky operations
- **App backgrounding**: Upload changes when app goes to background

**Example:**
```kotlin
val uploadResult = client.uploadOnce()
uploadResult.onSuccess { summary ->
    println("Uploaded: ${summary.applied}/${summary.total} changes")
    if (summary.conflict > 0) {
        println("Conflicts: ${summary.conflict}")
    }
}.onFailure { error ->
    println("Upload failed: ${error.message}")
}
```

### Upload Results

The `UploadSummary` provides detailed information about what happened:

```kotlin
data class UploadSummary(
    val total: Int,           // Total changes attempted
    val applied: Int,         // Successfully applied changes
    val conflict: Int,        // Changes that conflicted
    val invalid: Int,         // Invalid changes (schema errors, etc.)
    val materializeError: Int, // Server-side processing errors
    val invalidReasons: Map<String, Int>, // Breakdown of invalid reasons
    val firstErrorMessage: String?       // First error encountered
)
```

**Status meanings:**
- **Applied**: Change was successfully saved on server
- **Conflict**: Another device modified the same record (see Conflict Resolution)
- **Invalid**: Change violates server-side validation rules
- **Materialize Error**: Server couldn't process the change due to internal errors

## Download Operations

Download operations retrieve changes from other devices via the server.

### downloadOnce()

Downloads a page of changes from the server.

```kotlin
suspend fun downloadOnce(
    limit: Int = 1000,           // Page size
    includeSelf: Boolean = false, // Include your own changes
    until: Long = 0L             // Download up to this server sequence
): Result<Pair<Int, Long>>       // Returns (applied_count, next_sequence)
```

**Parameters:**
- **`limit`**: Maximum changes to download in one call (1000 is recommended)
- **`includeSelf`**: Whether to include your own changes (usually false)
- **`until`**: Stop downloading at this server sequence (0 = no limit)

**Example:**
```kotlin
val downloadResult = client.downloadOnce(limit = 500)
downloadResult.onSuccess { (applied, nextSeq) ->
    println("Downloaded and applied: $applied changes")
    println("Next server sequence: $nextSeq")
}.onFailure { error ->
    println("Download failed: ${error.message}")
}
```

### Paginated Downloads

For large datasets, download in pages until no more changes:

```kotlin
suspend fun downloadAllChanges() {
    var more = true
    var totalApplied = 0
    
    while (more) {
        val result = client.downloadOnce(limit = 1000)
        val (applied, _) = result.getOrThrow()
        
        totalApplied += applied
        more = applied == 1000  // Continue if page was full
        
        if (applied == 0) break  // No more changes
    }
    
    println("Total changes applied: $totalApplied")
}
```

## Combined Sync Operations

### syncOnce()

Convenience helper that uploads then downloads in the recommended order.

```kotlin
suspend fun OversqliteClient.syncOnce(
    limit: Int = 1000,
    includeSelf: Boolean = false
): Result<SyncRun>

data class SyncRun(
    val upload: UploadSummary,
    val downloaded: Int
)
```

**When to use:**
- **Bi-directional sync**: Multiple devices frequently editing the same data
- **Interactive sync**: User expects to see others' changes immediately
- **Simple sync workflows**: One-call solution for most sync needs

**When to avoid:**
- **Upload-heavy scenarios**: If you primarily create data, separate upload/download
- **Bandwidth constraints**: More control with separate operations
- **Complex error handling**: Need different retry logic for upload vs download

**Example:**
```kotlin
val syncResult = client.syncOnce(limit = 1000)
syncResult.onSuccess { run ->
    println("Upload: ${run.upload.applied}/${run.upload.total}")
    println("Download: ${run.downloaded} changes")
}.onFailure { error ->
    println("Sync failed: ${error.message}")
}
```

## Conflict Resolution

When multiple devices modify the same record, conflicts occur. SQLiteNow provides pluggable conflict resolution strategies.

### Resolver Interface

```kotlin
fun interface Resolver {
    fun merge(
        table: String,
        pk: String,
        serverRow: JsonElement?,    // Current server state
        localPayload: JsonElement? // Your local changes
    ): MergeResult
}

sealed class MergeResult {
    object AcceptServer : MergeResult()                    // Use server version
    data class KeepLocal(val mergedPayload: JsonElement) : MergeResult() // Use local version
}
```

### Built-in Resolvers

**ServerWinsResolver** (recommended for most apps):
```kotlin
val client = database.newOversqliteClient(
    schema = "myapp",
    httpClient = httpClient,
    resolver = ServerWinsResolver  // Server always wins conflicts
)
```

**Custom Resolver** (for advanced conflict handling):
```kotlin
object SmartResolver : Resolver {
    override fun merge(
        table: String, pk: String,
        serverRow: JsonElement?, localPayload: JsonElement?
    ): MergeResult {
        return when (table) {
            "user_preferences" -> MergeResult.KeepLocal(localPayload!!) // Client wins
            "shared_documents" -> mergeDocumentFields(serverRow, localPayload)
            else -> MergeResult.AcceptServer // Default to server wins
        }
    }
}
```

### Conflict Resolution Flow

1. **Upload detects conflict**: Server returns current server state
2. **Resolver is called**: Your resolver decides how to merge
3. **Result is applied**:
   - `AcceptServer`: Local change is discarded, server state is applied locally
   - `KeepLocal`: Merged payload is saved locally and re-uploaded

## Sync Patterns

### Periodic Background Sync

```kotlin
class SyncManager {
    private val syncScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    
    fun startPeriodicSync() {
        syncScope.launch {
            while (isActive) {
                try {
                    client.uploadOnce()
                    delay(30_000) // 30 seconds
                    
                    // Download less frequently
                    if (System.currentTimeMillis() % 120_000 < 30_000) {
                        client.downloadOnce(limit = 500)
                    }
                } catch (e: Exception) {
                    logger.e(e) { "Periodic sync failed" }
                    delay(60_000) // Back off on error
                }
            }
        }
    }
}
```

### Event-Driven Sync

```kotlin
class SyncManager {
    private val syncTrigger = MutableSharedFlow<Unit>(extraBufferCapacity = 1)
    
    init {
        // Debounced sync worker
        syncTrigger
            .debounce(2000) // Wait 2 seconds after last trigger
            .onEach { performSync() }
            .launchIn(syncScope)
    }
    
    fun triggerSync() {
        syncTrigger.tryEmit(Unit)
    }
    
    // Call this after database changes
    fun onDataChanged() {
        triggerSync()
    }
}
```

### Pause/Resume Controls

```kotlin
// Pause uploads during bulk operations
client.pauseUploads()
try {
    // Perform bulk import
    importLargeDataset()
} finally {
    client.resumeUploads()
    client.uploadOnce() // Upload all changes at once
}

// Pause downloads during critical UI flows
client.pauseDownloads()
showCriticalUserDialog()
client.resumeDownloads()
```

## Error Handling

### Upload Errors

```kotlin
val uploadResult = client.uploadOnce()
uploadResult.onFailure { error ->
    when (error) {
        is HttpException -> {
            if (error.statusCode == 401) {
                // Token expired, refresh and retry
                refreshAuthToken()
                client.uploadOnce()
            }
        }
        is NetworkException -> {
            // Network issue, retry later
            scheduleRetry()
        }
        else -> {
            // Log and report error
            logger.e(error) { "Upload failed" }
        }
    }
}
```

### Download Errors

```kotlin
val downloadResult = client.downloadOnce()
downloadResult.onFailure { error ->
    // Downloads are generally safe to retry
    // They don't modify server state
    retryDownload()
}
```

## Best Practices

### Upload Guidelines
- **Batch changes**: Don't upload after every single change
- **Handle conflicts gracefully**: Provide appropriate resolvers
- **Monitor upload results**: Check for conflicts and invalid changes
- **Retry on network errors**: Uploads are idempotent

### Download Guidelines
- **Use appropriate page sizes**: 1000 is usually optimal
- **Handle large datasets**: Use pagination for initial sync
- **Exclude self by default**: Avoid processing your own changes twice
- **Download regularly**: Keep local state fresh

### Performance Tips
- **Separate upload/download**: For upload-heavy or download-heavy scenarios
- **Use pause/resume**: Control sync during bulk operations
- **Monitor bandwidth**: Adjust sync frequency based on network conditions
- **Batch UI updates**: Update UI after sync completes, not per change

---

**Next Steps**: Learn about [Server Setup]({{ site.baseurl }}/sync/server-setup/) to deploy your sync infrastructure.
