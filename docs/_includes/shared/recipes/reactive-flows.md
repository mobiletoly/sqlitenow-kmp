# Reactive Flows

Generated SELECT methods can observe table invalidations and re-run when related
data changes. Generated INSERT, UPDATE, and DELETE methods notify watchers
automatically.

{% if include.platform == "kmp" %}
```kotlin
LaunchedEffect(Unit) {
    database.task
        .selectAll()
        .asFlow()
        .collect { rows -> tasks = rows }
}
```

If hand-written SQL changes the same database outside generated SQLiteNow
methods, report the affected tables:

```kotlin
database.reportExternalTableChanges(setOf("task"))
```
{% elsif include.platform == "swift" %}
```swift
for try await rows in db.task.selectAll().stream() {
    tasks = rows
}
```
{% endif %}

{% if include.platform == "kmp" %}
External change reporting is rare in normal app code. It is for direct SQL,
imports, sync integrations, or other local writers that bypass generated
SQLiteNow methods.

For queries with affected tables, each `asFlow()` collection subscribes to changes
before reading its initial result. A matching invalidation during a query or slow
downstream delivery remains pending for a follow-up query. Pending invalidations
may be coalesced: the flow
observes the latest committed state rather than every intermediate database state.
A redundant query can emit an equal result when its change was already visible.

Cancel the collecting coroutine or its scope when observation is no longer needed.
Cancellation and query or collector failures release the collection's listener.
Closing the database alone does not complete an existing Kotlin query flow.
{% endif %}
