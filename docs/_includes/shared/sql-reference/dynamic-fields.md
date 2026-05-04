# Dynamic Fields

Dynamic fields reshape JOIN-heavy rows into nested generated models. Add
`@@{ ... }` annotations near the SELECT or view columns that should form a
nested value.

```sql
SELECT
    t.id          AS task__id,
    t.title       AS task__title,
    n.id          AS note__id,
    n.body        AS note__body

/* @@{ dynamicField=notes,
       mappingType=collection,
       sourceTable=n,
       aliasPrefix=note__ } */

FROM task t
LEFT JOIN task_note n ON n.task_id = t.id;
```

The important pieces are:

- `dynamicField` names the generated property.
- `mappingType` chooses `perRow`, `collection`, or `entity`.
- `sourceTable` points to the table or alias that owns the nested fields.
- `aliasPrefix` tells SQLiteNow which selected columns belong to the nested
  value.

{% if include.platform == "dart" %}
Generated Dart SELECT models expose nested properties directly:

```dart
final tasks = await db.task.selectWithNotes().asList();
final firstNote = tasks.first.notes.isEmpty ? null : tasks.first.notes.first;
```
{% elsif include.platform == "kmp" %}
Generated KMP SELECT models expose nested properties directly:

```kotlin
val tasks = db.task.selectWithNotes().asList()
val firstNote = tasks.first().notes.firstOrNull()
```
{% endif %}

Use stable aliases for complex views. That keeps generated names predictable and
prevents unrelated joined columns from leaking into the top-level result.
