---
layout: doc
title: Reactive Sync Updates
permalink: /sync/reactive-updates/
parent: Sync
---

# Reactive Sync Updates

SQLiteNow's reactive flows automatically update your UI when sync operations modify data. This page focuses on sync-specific patterns and behaviors.

> **📖 New to reactive flows?** Start with the [Reactive Flows recipe]({{ site.baseurl }}/recipes/reactive-flows/) to learn the basics of using `.asFlow()` with your queries.

## How Sync Triggers UI Updates

When sync operations modify your database tables, SQLiteNow's table update listener automatically:

1. **Detects table changes** during push, pull, hydrate, and recover operations
2. **Notifies reactive flows** that depend on those tables
3. **Triggers UI updates** through your existing `.asFlow()` queries
4. **Maintains consistency** between local and remote data

This happens transparently - you don't need to manually refresh your UI after sync operations.

## Sync-Specific Reactive Patterns

### Automatic UI Updates During Sync

Your existing reactive flows automatically update when sync operations modify data:

```kotlin
@Composable
fun SyncAwarePersonList() {
    var persons by remember { mutableStateOf<List<PersonEntity>>(emptyList()) }
    var isSyncing by remember { mutableStateOf(false) }

    // Standard reactive query - no sync-specific code needed
    LaunchedEffect(Unit) {
        database.person.selectAll().asFlow()
            .collect { persons = it }  // Updates during sync operations
    }

    // Sync function - UI updates happen automatically via table update listener
    suspend fun performSync() {
        isSyncing = true
        try {
            syncClient.sync()            // Pushes local changes, then pulls remote bundles
            // No manual UI refresh needed!
        } finally {
            isSyncing = false
        }
    }
}
```

### Table Update Listener Mechanism

SQLiteNow's sync system uses a table update listener that:

1. **Monitors sync operations**: Tracks which tables are modified during push, pull, hydrate, and recover
2. **Notifies flow system**: Calls `notifyTablesChanged()` with affected table names
3. **Triggers reactive queries**: Any `.asFlow()` queries watching those tables re-execute
4. **Updates UI automatically**: Compose recomposes with new data

This happens for push operations (when accepted bundles are replayed), pull operations, and
snapshot rebuild operations.

## Sync Triggers with Reactive Updates

### Event-Driven Sync

Combine manual sync triggers with automatic UI updates:

```kotlin
class SyncManager {
    private val syncTrigger = MutableSharedFlow<Unit>(extraBufferCapacity = 1)

    init {
        syncTrigger
            .debounce(2000) // Wait 2 seconds after last trigger
            .onEach { performSync() }
            .launchIn(GlobalScope)
    }

    fun triggerSync() = syncTrigger.tryEmit(Unit)

    private suspend fun performSync() {
        try {
            syncClient.sync()
            // Table update listener automatically triggers reactive flows
        } catch (e: Exception) {
            logger.e(e) { "Sync failed" }
        }
    }
}
```

### Pull-to-Refresh with Sync

```kotlin
@Composable
fun PersonListWithPullRefresh() {
    var isRefreshing by remember { mutableStateOf(false) }

    val pullRefreshState = rememberPullRefreshState(
        refreshing = isRefreshing,
        onRefresh = {
            coroutineScope.launch {
                isRefreshing = true
                try {
                    syncClient.pullToStable()
                    // UI updates automatically via table update listener
                } finally {
                    isRefreshing = false
                }
            }
        }
    )

    // Standard reactive query - no sync-specific code needed
    // Updates automatically when pull-to-refresh sync completes
}
```

## Sync-Specific Considerations

### Table Update Timing

The table update listener fires at specific points during sync:

- **Push operations**: When accepted bundles are replayed or conflicts update local rows
- **Pull operations**: When remote bundles are applied to local tables
- **Hydrate / recover**: When the staged snapshot becomes the new local state

### Conflict Resolution Impact

When conflicts occur during upload, the resolver may modify local data:

```kotlin
// If resolver chooses AcceptServer, local data changes and flows update
// If resolver chooses KeepLocal, data may stay the same or be rewritten for retry
// If resolver chooses KeepMerged(...), local data is rewritten before retry
```

Resolver policy is configured once at client construction time. `KeepLocal` and `KeepMerged(...)`
are per-conflict outcomes returned by that resolver, not separate client setup modes.

### Sync Performance

- **Batch sync operations**: Don't sync after every single change
- **Monitor sync frequency**: Balance freshness with battery/bandwidth usage

## Best Practices for Sync + Reactive Flows

### Sync Integration
- **Let table update listener handle UI updates** - don't manually refresh after sync
- **Use event-driven sync patterns** for better user experience
- **Implement pull-to-refresh** for user-initiated sync operations
- **Handle sync errors gracefully** without breaking reactive flows

### Performance
- **Debounce sync triggers** to avoid excessive sync operations
- **Use specific queries** in reactive flows to minimize update scope
- **Monitor sync impact** on UI performance during development

### Error Handling
- **Provide offline indicators** when sync operations fail
- **Implement retry logic** for failed sync operations
- **Keep reactive flows working** even when sync is unavailable

---

**Next Steps**: Learn about [Conflict Resolution]({{ site.baseurl }}/sync/conflict-resolution/) to handle data conflicts between devices.
