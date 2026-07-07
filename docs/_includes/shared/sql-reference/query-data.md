# Query Data

SELECT files live under `queries/`. Subdirectories become generated namespaces
and file names become generated method names.

```text
queries/task/selectAll.sql
queries/task/selectById.sql
```

## Basic SELECT

```sql
SELECT id, title, completed, created_at
FROM task
ORDER BY created_at DESC;
```

{% if include.platform == "dart" %}
`queries/task/selectAll.sql` generates a Dart method under `db.task`:

```dart
final rows = await db.task.selectAll().asList();

for (final row in rows) {
  print(row.title);
}
```
{% elsif include.platform == "kmp" %}
`queries/task/selectAll.sql` generates a KMP method under `db.task`:

```kotlin
val rows = db.task.selectAll().asList()

rows.forEach { row ->
    println(row.title)
}
```
{% elsif include.platform == "swift" %}
`queries/task/selectAll.sql` generates a Swift method under `db.task`:

```swift
let rows = try await db.task.selectAll().list()

for row in rows {
    print(row.title)
}
```
{% endif %}

## Parameters

Named parameters become generated params objects.

```sql
SELECT id, title, completed, created_at
FROM task
WHERE id = :id;
```

{% if include.platform == "dart" %}
```dart
final task = await db.task
    .selectById(TaskSelectByIdParams(id: 42))
    .asOneOrNull();
```
{% elsif include.platform == "kmp" %}
```kotlin
val task = db.task
    .selectById(TaskQuery.SelectById.Params(id = 42))
    .asOneOrNull()
```
{% elsif include.platform == "swift" %}
```swift
let task = try await db.task
    .selectById(TaskSelectByIdParams(id: 42))
    .oneOrNull()
```
{% endif %}

## Result Cardinality

Use the runner method that matches the expected result shape:

- `asList()` for zero or more rows.
- `asOne()` when exactly one row must exist.
- `asOneOrNull()` when zero or one row is valid.

{% if include.platform == "dart" %}
Dart SELECT methods also expose `watch()` for reactive reads:

```dart
final subscription = db.task.selectAll().watch().listen((rows) {
  setState(() => tasks = rows);
});
```
{% elsif include.platform == "kmp" %}
KMP SELECT methods expose flow APIs for reactive reads:

```kotlin
database.task
    .selectAll()
    .asFlow()
    .collect { rows -> tasks = rows }
```
{% elsif include.platform == "swift" %}
Swift SELECT methods expose `AsyncThrowingStream` APIs for reactive reads:

```swift
for try await rows in db.task.selectAll().stream() {
    tasks = rows
}
```
{% endif %}
