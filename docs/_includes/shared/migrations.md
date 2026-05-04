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
- SQLiteNow runs migration files with a version greater than the current
  version
- after successful migration, SQLiteNow stores the latest applied version in
  `PRAGMA user_version`

This means a new install does not replay old incremental migrations one by one.
It creates the current schema directly. Incremental migration files exist for
users upgrading from an older app version.

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
  {%- endif %}
  does not run the main `CREATE TABLE task (...)` schema statement again, and
  instead applies migration files with a higher version. In this example it runs
  `0002_add_task_due_date.sql`, then stores `PRAGMA user_version = 2`.

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
{% endif %}

If a migration statement throws:

- the transaction rolls back
- `PRAGMA user_version` is not advanced
{% if include.platform == "dart" %}
- the database is not marked open
- the caller receives the error from `open()`
{% elsif include.platform == "kmp" %}
- database initialization fails
- the caller receives the migration error
{% endif %}

Fix the migration SQL and reopen a new generated database instance.

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
{% endif %}
5. Test both a fresh database and an upgrade from the previous version.

For the upgrade test, create a database with the old app version, close it, then
open the same database path with the new generated code and assert that data and
`PRAGMA user_version` are correct.
