# Manage Data

INSERT, UPDATE, and DELETE statements live in `queries/` alongside SELECT files.
SQLiteNow generates typed params from named SQL parameters and notifies affected
tables after generated write methods run.

```text
queries/task/insertOne.sql
queries/task/completeById.sql
queries/task/deleteCompleted.sql
```

## INSERT

```sql
INSERT INTO task (title, completed, created_at)
VALUES (:title, 0, :createdAt);
```

{% if include.platform == "dart" %}
```dart
await db.task.insertOne(
  TaskInsertOneParams(
    title: 'Call patient',
    createdAt: DateTime.now().toUtc().toIso8601String(),
  ),
);
```
{% elsif include.platform == "kmp" %}
```kotlin
db.task.insertOne(
    TaskQuery.InsertOne.Params(
        title = "Call patient",
        createdAt = "2026-05-05T12:00:00Z",
    )
)
```
{% elsif include.platform == "swift" %}
```swift
try await db.task.insertOne(
    TaskInsertOneParams(
        title: "Call patient",
        createdAt: "2026-05-05T12:00:00Z"
    )
)
```
{% endif %}

## UPDATE

```sql
UPDATE task
SET completed = 1
WHERE id = :id;
```

{% if include.platform == "dart" %}
```dart
await db.task.completeById(TaskCompleteByIdParams(id: taskId));
```
{% elsif include.platform == "kmp" %}
```kotlin
db.task.completeById(TaskQuery.CompleteById.Params(id = taskId))
```
{% elsif include.platform == "swift" %}
```swift
try await db.task.completeById(TaskCompleteByIdParams(id: taskId))
```
{% endif %}

## DELETE

```sql
DELETE FROM task
WHERE completed = 1;
```

{% if include.platform == "dart" %}
```dart
await db.task.deleteCompleted();
```
{% elsif include.platform == "kmp" %}
```kotlin
db.task.deleteCompleted()
```
{% elsif include.platform == "swift" %}
```swift
try await db.task.deleteCompleted()
```
{% endif %}

When all writes go through generated SQLiteNow methods, watchers are notified
automatically. Use external table change reporting only for writes that bypass
SQLiteNow generated methods.
