# Complex Example

This example builds a task list with nested notes. The schema is ordinary SQL:

```sql
CREATE TABLE task (
    id     INTEGER PRIMARY KEY NOT NULL,
    title  TEXT                NOT NULL
);

CREATE TABLE task_note (
    id       INTEGER PRIMARY KEY NOT NULL,
    task_id  INTEGER             NOT NULL,
    body     TEXT                NOT NULL,
    FOREIGN KEY (task_id) REFERENCES task(id)
);
```

The query aliases joined columns so SQLiteNow can map notes into a collection:

```sql
SELECT
    t.id      AS task__id,
    t.title   AS task__title,
    n.id      AS note__id,
    n.body    AS note__body

/* @@{ dynamicField=notes,
       mappingType=collection,
       sourceTable=n,
       aliasPrefix=note__ } */

FROM task t
LEFT JOIN task_note n ON n.task_id = t.id
ORDER BY t.id, n.id;
```

{% if include.platform == "dart" %}
```dart
final tasks = await db.task.selectWithNotes().asList();

for (final task in tasks) {
  print('${task.title}: ${task.notes.length} notes');
}
```
{% elsif include.platform == "kmp" %}
```kotlin
val tasks = db.task.selectWithNotes().asList()

tasks.forEach { task ->
    println("${task.title}: ${task.notes.size} notes")
}
```
{% elsif include.platform == "swift" %}
```swift
let tasks = try await db.task.selectWithNotes().list()

for task in tasks {
    print("\(task.title): \(task.notes.count) notes")
}
```
{% endif %}

For larger schemas, keep aliases stable and move repeated join logic into views.
