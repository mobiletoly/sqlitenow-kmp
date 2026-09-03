# Migrations

SQLiteNow uses SQLite's `PRAGMA user_version` to decide what migration work a
database needs when it opens.

{% if include.platform == "dart" %}
For Flutter and Dart projects, migration inputs live beside the rest of the SQL
files:

```text
lib/db/sql/AppDatabase/
  schema/
  init/
  migration/
  queries/
```
{% elsif include.platform == "kmp" %}
For Kotlin Multiplatform projects, migration inputs live beside the rest of the
shared SQL files:

```text
src/commonMain/sql/AppDatabase/
  schema/
  init/
  migration/
  queries/
```
{% elsif include.platform == "swift" %}
For Swift projects, migration inputs live beside the rest of the SQL files in
the package root:

```text
SQLiteNow/databases/AppDatabase/
  schema/
  init/
  migration/
  queries/
```
{% endif %}

`schema/` is the current schema. `migration/` is the ordered history for
databases that already exist on user devices.

## Fresh Databases Versus Existing Databases

SQLiteNow handles two different cases.

Fresh database:

- the SQLite file is empty
- there are no user tables yet
- SQLiteNow creates the current schema from `schema/`
- SQLiteNow runs `init/` SQL if present
- SQLiteNow stores the latest generated version in `PRAGMA user_version`

Existing database:

- the SQLite file already has user tables
- SQLiteNow reads `PRAGMA user_version`
- for each integer version above the current version, SQLiteNow runs that
  version's migration SQL if a matching file exists
- after that version's SQL, SQLiteNow calls `onMigrationStep` for the crossed
  boundary, whether or not a matching SQL file exists
- after all boundaries succeed, SQLiteNow writes the generated target version
  to `PRAGMA user_version` once and commits

For example, suppose an existing database has `PRAGMA user_version = 1` and
the generated target is version 2. If the project contains both `0001.sql` and
`0002.sql`, SQLiteNow:

1. skips `0001.sql` because the database is already at version 1
2. runs `0002.sql`
3. calls `onMigrationStep` with `fromVersion = 1` and `toVersion = 2`
4. writes `PRAGMA user_version = 2`
5. commits the transaction

The presence of `0002.sql` does not suppress the callback. The callback runs
after the automatic SQL migration for that boundary.

This means a new install does not replay old incremental migrations one by one.
It creates the current schema directly. Incremental migration files exist for
users upgrading from an older app version.

If the stored `PRAGMA user_version` is equal to the generated target,
SQLiteNow runs no migration SQL or callback. If the stored version is newer
than the generated target, SQLiteNow also runs no migration work and preserves
the newer value. SQLiteNow does not downgrade the database.

## Migration File Names

Migration files go under `migration/` and must start with a four-digit version:

{% if include.platform == "dart" %}
```text
lib/db/sql/AppDatabase/migration/
  0002_add_task_due_date.sql
  0003_add_task_archived.sql
```
{% elsif include.platform == "kmp" %}
```text
src/commonMain/sql/AppDatabase/migration/
  0002_add_task_due_date.sql
  0003_add_task_archived.sql
```
{% elsif include.platform == "swift" %}
```text
SQLiteNow/databases/AppDatabase/migration/
  0002_add_task_due_date.sql
  0003_add_task_archived.sql
```
{% endif %}

Accepted format:

```text
NNNN.sql
NNNN_description.sql
```

Examples:

- `0001.sql`
- `0002_add_due_date.sql`
- `0010_create_indexes.sql`

Invalid examples:

- `1.sql`
- `001.sql`
- `v001.sql`
- `add_due_date.sql`

Each version can appear once. Duplicate versions fail generation.

A migration file may contain comments and no SQL statements. Use such a file
as the target marker when the latest version only needs application code:

```sql
-- migration/0005_programmatic_only.sql
-- Version 5 is handled by onMigrationStep.
```

SQLiteNow still crosses versions with no matching file. An upgrade from version
2 to version 5 runs SQL for version 3, calls the callback for `2 -> 3`, calls it
for `3 -> 4`, runs version 5 SQL if the file contains any, and calls it for
`4 -> 5`. SQLiteNow writes `PRAGMA user_version = 5` only after every boundary
succeeds.

## Programmatic Migration Steps

Use `onMigrationStep` for row transformations that depend on application code.
Keep table, column, and index changes in numbered SQL files.

{% if include.platform == "kmp" %}
```kotlin
val migrations = VersionBasedDatabaseMigrations(
    onMigrationStep = { scope ->
        when (scope.toVersion) {
            2 -> migrateFullNames(scope)
        }
    },
)
```

The callback type is `suspend (SqliteNowMigrationScope) -> Unit`. The scoped
connection supplies `execSQL` and `usePrepared`.
{% elsif include.platform == "swift" %}
```swift
let database = AppDatabase(
    path: databaseURL,
    onMigrationStep: { scope in
        if scope.toVersion == 2 {
            try await migrateFullNames(scope)
        }
    }
)
```

The callback type is `@Sendable (SQLiteNowMigrationScope) async throws ->
Void`. The scoped connection supplies low-level `execute` and `query` methods.
{% elsif include.platform == "dart" %}
```dart
final database = AppDatabase(
  path: databasePath,
  onMigrationStep: (scope) async {
    if (scope.toVersion == 2) {
      await migrateFullNames(scope);
    }
  },
);
```

The callback returns `FutureOr<void>`. The scoped connection supplies
`execute`, `select`, and `usePrepared`.
{% endif %}

Each callback receives a scope with these values:

- `originalVersion`: the stored version when this upgrade started; it stays
  unchanged across a multi-version upgrade
- `fromVersion`: the version before the current boundary
- `toVersion`: the boundary just reached; matching SQL, if present, has run
- `targetVersion`: the newest version in the generated migration plan
- `connection`: restricted access to the same transaction-owned SQLite
  connection that ran the migration SQL

Application code uses `scope.connection`. The underlying raw connection is an
internal implementation detail. The scoped connection does not offer database
close or transaction methods. It rejects transaction-control SQL and
`PRAGMA user_version`; SQLiteNow owns both for the duration of the migration.

The callback does not run outside the migration transaction and does not start
a separate transaction. SQL from the migration file, SQL executed through
`scope.connection`, and the final `PRAGMA user_version` write all commit or
roll back together.

Await each scoped operation in the callback. When the callback returns,
SQLiteNow rejects new scoped operations and waits for operations the connection
already accepted before it advances to the next version. If database
initialization or opening is cancelled during that wait, SQLiteNow cancels
those operations, waits for their cleanup, and rolls back.

If callback SQL fails and the callback propagates the error, SQLiteNow aborts
the migration and rolls back the schema changes, callback data, and version
write. A callback may catch an operation error and perform compensating SQL;
SQLiteNow treats an error the callback catches as handled and may commit if the
rest of the migration succeeds.

For example, version 1 may store a person's name in `full_name`. Version 2 SQL
adds `first_name` and `last_name`. The `1 -> 2` callback reads `full_name`,
splits it, and writes the two new columns. Version 3 SQL can then remove
`full_name`. SQL for each version runs before its callback, so the version 2
columns exist before application code writes them.

Keep each shipped callback branch in application source while users may still
upgrade across that version. SQLiteNow calls the branch in order but does not
inspect callback code for missing cases.

## Starting Schema

For version 1, define the current first schema in `schema/task.sql`:

```sql
CREATE TABLE task (
  id INTEGER PRIMARY KEY NOT NULL,
  title TEXT NOT NULL,
  completed INTEGER NOT NULL
);
```

{% if include.platform == "dart" %}
With no `migration/` files, the generated Dart database creates this schema on a
fresh database and stores version `1`.
{% elsif include.platform == "kmp" %}
With no `migration/` files, the generated KMP database creates this schema on a
fresh database and stores version `1`.
{% elsif include.platform == "swift" %}
With no `migration/` files, the generated Swift database creates this schema on
a fresh database and stores version `1`.
{% endif %}

## Adding A Column

Suppose version 1 of the app shipped this table:

```sql
CREATE TABLE task (
  id INTEGER PRIMARY KEY NOT NULL,
  title TEXT NOT NULL,
  completed INTEGER NOT NULL
);
```

Later, version 2 of the app needs a due date. First update the current schema in
`schema/task.sql`:

```sql
CREATE TABLE task (
  id INTEGER PRIMARY KEY NOT NULL,
  title TEXT NOT NULL,
  completed INTEGER NOT NULL,
  due_at TEXT
);
```

Then add `migration/0002_add_task_due_date.sql` for existing databases:

```sql
ALTER TABLE task ADD COLUMN due_at TEXT;
```

These two files serve different users:

- A user installing the app for the first time has no SQLite database yet.
  SQLiteNow creates the database from the current `schema/` files, so it runs the
  `CREATE TABLE task (...)` statement that already includes `due_at`. It does
  not replay older migration files to build the schema from version 1.
- A user upgrading from version 1 already has a SQLite database with
  `PRAGMA user_version = 1`.
  {% if include.platform == "dart" -%}
  SQLiteNow reads that version during `open()`,
  {%- elsif include.platform == "kmp" -%}
  SQLiteNow reads that version during generated database initialization,
  {%- elsif include.platform == "swift" -%}
  SQLiteNow reads that version during `open()`,
  {%- endif %}
  does not run the main `CREATE TABLE task (...)` schema statement again, and
  instead applies migration files with a higher version. In this example it runs
  `0002_add_task_due_date.sql`, calls `onMigrationStep` for `1 -> 2` if the
  callback was supplied, then stores `PRAGMA user_version = 2` after both
  steps succeed.

If the app later adds `0003_add_task_archived.sql`, a user upgrading from
version 1 runs `0002_add_task_due_date.sql` and then
`0003_add_task_archived.sql` in order. A user already on version 2 runs only
`0003_add_task_archived.sql`. A fresh install still creates the latest schema
directly.

## Adding Data Backfills

Migration files can contain multiple statements. For example, add an `archived`
flag and backfill existing rows:

```sql
ALTER TABLE task ADD COLUMN archived INTEGER NOT NULL DEFAULT 0;

UPDATE task
SET archived = 0
WHERE archived IS NULL;

CREATE INDEX idx_task_archived ON task(archived);
```

Save that as `migration/0003_add_task_archived.sql`, and update the current
schema to include the `archived` column and index.

## Init SQL

`init/` is for fresh database seed data. It is not a replacement for migration
backfills.

Use `init/` when new installs should start with rows such as built-in lookup
values:

```sql
INSERT INTO task_label(id, name) VALUES (1, 'Inbox');
```

Use `migration/` when existing installs need their stored data transformed or
backfilled.

## Transaction And Failure Behavior

{% if include.platform == "dart" %}
SQLiteNow applies migration work during `open()` inside a transaction.
{% elsif include.platform == "kmp" %}
SQLiteNow applies migration work during generated database initialization inside
a transaction.
{% elsif include.platform == "swift" %}
SQLiteNow applies migration work during `open()` inside a transaction.
{% endif %}

If migration SQL or a callback fails or is cancelled:

- the transaction rolls back
- `PRAGMA user_version` is not advanced
{% if include.platform == "dart" %}
- the database is not marked open
- the caller receives the error from `open()`
{% elsif include.platform == "kmp" %}
- database initialization fails
- the caller receives the migration error
{% elsif include.platform == "swift" %}
- the database is not marked open
- the caller receives the error from `open()`
{% endif %}

Fix the migration SQL and reopen a new generated database instance.

The callback and its accepted scoped operations run inside the same transaction
as the migration SQL. Awaited work keeps that transaction open. SQLite can roll
back database changes, but it cannot roll back HTTP requests, filesystem writes,
or other external effects. Keep network calls and unrelated external work
outside the callback.

## Practical Workflow

When changing schema after release:

1. Update `schema/` so it represents the latest database shape.
2. Add one new `migration/NNNN_description.sql` file for existing databases.
3. Update affected queries under `queries/`.
{% if include.platform == "dart" %}
4. Regenerate Dart code:

   ```shell
   flutter pub run sqlitenow_cli generate
   ```
{% elsif include.platform == "kmp" %}
4. Regenerate KMP code:

   ```shell
   ./gradlew :composeApp:generateAppDatabase
   ```
{% elsif include.platform == "swift" %}
4. Regenerate the Swift package:

   ```shell
   swift package plugin --allow-writing-to-package-directory sqlitenow-generate
   ```
{% endif %}
5. Test both a fresh database and an upgrade from the previous version.

For the upgrade test, create a database with the old app version, close it, then
open the same database path with the new generated code and assert that data and
`PRAGMA user_version` are correct.
