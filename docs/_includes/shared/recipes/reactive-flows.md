# Reactive Flows

Generated SELECT methods can observe table invalidations and re-run when related
data changes. Generated INSERT, UPDATE, and DELETE methods notify watchers
automatically.

{% if include.platform == "dart" %}
```dart
late final StreamSubscription<List<TaskRow>> subscription;

subscription = db.task.selectAll().watch().listen((rows) {
  setState(() => tasks = rows);
});
```

If hand-written SQL changes the same database outside generated SQLiteNow
methods, report the affected tables:

```dart
db.reportExternalTableChanges({'task'});
```
{% elsif include.platform == "kmp" %}
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
{% endif %}

External change reporting is rare in normal app code. It is for direct SQL,
imports, sync integrations, or other local writers that bypass generated
SQLiteNow methods.
