# Serialization

Use custom adapters when the public generated API should expose a domain type
instead of the raw SQLite storage type.

```sql
CREATE TABLE task
(
    id INTEGER PRIMARY KEY NOT NULL,

    -- @@{ field=tags, adapter=custom, propertyType=List<String> }
    tags TEXT NOT NULL
);
```

{% if include.platform == "dart" %}
```dart
final db = AppDatabase(
  path: path,
  taskAdapters: TaskAdapters(
    sqlColumnToTags: (value) => List<String>.from(jsonDecode(value)),
    tagsToSqlColumn: (tags) => jsonEncode(tags),
  ),
);
```
{% elsif include.platform == "kmp" %}
```kotlin
val db = AppDatabase(
    dbName = dbPath,
    migration = VersionBasedDatabaseMigrations(),
    taskAdapters = AppDatabase.TaskAdapters(
        sqlColumnToTags = { value -> value.jsonDecodeFromSqlite() },
        tagsToSqlColumn = { tags -> tags.jsonEncodeToSqlite() },
    ),
)
```
{% endif %}

Prefer stable serialized values for enums and timestamps. For absolute time,
store RFC3339 text with an explicit zone instead of locale-specific strings.
