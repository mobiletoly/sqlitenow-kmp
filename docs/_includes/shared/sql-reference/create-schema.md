# Create Schema

SQLiteNow schema files contain the current database shape. Put `CREATE TABLE`,
`CREATE INDEX`, and `CREATE VIEW` statements under the database `schema/`
directory.

{% if include.platform == "dart" %}
```text
lib/db/sql/AppDatabase/schema/
  task.sql
```
{% elsif include.platform == "kmp" %}
```text
src/commonMain/sql/AppDatabase/schema/
  task.sql
```
{% elsif include.platform == "swift" %}
```text
SQLiteNow/databases/AppDatabase/schema/
  task.sql
```
{% endif %}

## Table Definition

```sql
CREATE TABLE task
(
    id           INTEGER PRIMARY KEY NOT NULL,
    title        TEXT                NOT NULL,
    completed    INTEGER             NOT NULL DEFAULT 0,
    created_at   TEXT                NOT NULL
);

CREATE INDEX idx_task_completed ON task (completed);
```

SQLiteNow uses the schema to infer generated result fields, parameter types, and
adapter contracts. A table file does not generate a public model by itself; the
models come from query files that read or mutate that schema.

## Property Names

Column names are converted to idiomatic generated property names. For example,
`created_at` becomes `createdAt`.

Use field annotations when a column needs a custom generated name:

```sql
CREATE TABLE task
(
    id INTEGER PRIMARY KEY NOT NULL,

    -- @@{ field=display_title, propertyName=title }
    display_title TEXT NOT NULL
);
```

## Custom Types

Use `propertyType` and `adapter=custom` when the generated API should expose a
domain type instead of the raw SQLite storage type:

```sql
CREATE TABLE task
(
    id INTEGER PRIMARY KEY NOT NULL,

    -- @@{ field=status, adapter=custom, propertyType=TaskStatus }
    status TEXT NOT NULL
);
```

Unqualified custom types, such as `TaskStatus`, are resolved in the generated
database package. Use the fully qualified type name, for example
`propertyType=org.library.sqlite.TaskStatus`, when the type lives in another
package.

{% if include.platform == "dart" %}
Generated Dart code expects an adapter that converts between the SQLite column
value and your Dart type:

```dart
final db = AppDatabase(
  path: path,
  adapters: AppDatabaseAdapters(
    taskStatusToSql: (status) => status.name,
    sqlValueToTaskStatus: (value) => TaskStatus.parse(value as String),
  ),
);
```
{% elsif include.platform == "kmp" %}
Generated KMP code expects an adapter that converts between the SQLite column
value and your Kotlin type:

```kotlin
val db = AppDatabase(
    dbName = dbPath,
    migration = VersionBasedDatabaseMigrations(),
    taskAdapters = AppDatabase.TaskAdapters(
        sqlValueToStatus = { value -> TaskStatus.valueOf(value) },
        statusToSqlValue = { status -> status.name },
    ),
)
```
{% elsif include.platform == "swift" %}
Generated Swift code expects an adapter that converts between the SQLite value
and your Swift type:

```swift
let db = AppDatabase(
    path: databaseURL,
    adapters: AppDatabaseAdapters(
        sqlValueToStatus: { value in TaskStatus(rawValue: value) },
        statusToSqlValue: { status in status.rawValue }
    )
)
```
{% endif %}

Keep `schema/` files up to date with the latest app version. Existing database
upgrades belong in `migration/`; the schema directory describes what a fresh
install should create today.
