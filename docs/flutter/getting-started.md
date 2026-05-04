---
layout: page
title: Flutter/Dart Getting Started
permalink: /flutter/getting-started/
---

# Flutter/Dart Getting Started

SQLiteNow starts from SQL files and generates Dart code around them.

You define:

- tables, indexes, and views in `schema/`
- optional seed SQL in `init/`
- optional versioned upgrades in `migration/`
- app reads and writes in `queries/`

The generator inspects those files and writes Dart source for:

- a database class with `open()`, `close()`, `transaction()`, and migrations
- typed row/result classes for SELECT statements
- typed params classes for queries with named parameters
- query namespaces based on your `queries/` folders
- adapter hooks for custom value conversion
- `asList()`, `asOne()`, `asOneOrNull()`, and `watch()` query runners
- table invalidation for generated INSERT, UPDATE, and DELETE methods

This page walks through one small Flutter database end to end. Pure Dart
packages use the same SQL and config shape, but run generation with `dart run`
instead of `flutter pub run`.

## Add SQLiteNow

Add the runtime package to dependencies and the generator package to development
dependencies:

```yaml
dependencies:
  sqlitenow_runtime: ^0.9.0

dev_dependencies:
  sqlitenow_cli: ^0.9.0
```

SQLiteNow does not require the Gradle plugin for Flutter or Dart projects.

## Create The SQL Directory

Use one directory per database. For an `AppDatabase`, a typical Flutter layout is:

```text
lib/db/sql/AppDatabase/
  schema/
  init/
  migration/
  queries/
```

Only `schema/` and `queries/` are required for the first version.
When you need to change the schema after users already have a database, add
versioned files under `migration/`; see the
[migration guide]({{ site.baseurl }}/flutter/migrations/).

## Define A Table

Create `lib/db/sql/AppDatabase/schema/task.sql`:

```sql
CREATE TABLE task (
  id INTEGER PRIMARY KEY NOT NULL,
  title TEXT NOT NULL,
  -- @@{ field=priority, adapter=custom, propertyType=String }
  priority TEXT NOT NULL,
  completed INTEGER NOT NULL
);
```

The table is ordinary SQLite. The `-- @@{ ... }` comment is a SQLiteNow
annotation. Here it says that `priority` should be passed through a custom
adapter instead of being treated as a plain generated field.

## Add Query Files

Each SQL file under `queries/` becomes a generated Dart method. The folder path
becomes the query namespace.

Create `lib/db/sql/AppDatabase/queries/task/selectAll.sql`:

```sql
SELECT id, title, priority, completed
FROM task
ORDER BY id;
```

Create `lib/db/sql/AppDatabase/queries/task/insertOne.sql`:

```sql
INSERT INTO task (id, title, priority, completed)
VALUES (:id, :title, :priority, :completed);
```

Create `lib/db/sql/AppDatabase/queries/task/completeById.sql`:

```sql
UPDATE task
SET completed = 1
WHERE id = :id;
```

Named parameters such as `:id` and `:title` become generated params fields.
The `task/` folder becomes a `task` query namespace on the generated database.

## Configure The Generator

Create `sqlitenow.yaml` at the Flutter or Dart package root:

```yaml
databases:
  AppDatabase:
    input: lib/db/sql/AppDatabase
    output: lib/db/generated
    package: app.db
    runtime: dart
    oversqlite: false
```

`input` points at your SQL directory. `output` is where generated Dart source is
written. The generated file is normal Dart source and can be imported by your
app.

## Generate Dart Code

For Flutter apps:

```shell
flutter pub run sqlitenow_cli generate
```

For pure Dart packages:

```shell
dart run sqlitenow_cli generate
```

The released `sqlitenow_cli` package includes the SQLiteNow compiler jar.
Normal Flutter and Dart users do not pass `--compiler-jar`.

After generation, `lib/db/generated/app_database.dart` contains code shaped like:

```dart
final class AppDatabase {
  AppDatabase({required String path, required AppDatabaseAdapters adapters});
  AppDatabase.inMemory({required AppDatabaseAdapters adapters});

  Future<void> open();
  Future<void> close();
  Future<T> transaction<T>(Future<T> Function() block);

  late final TaskQueries task;
}
```

Generated names come from the SQL layout:

- the `queries/task/` folder becomes the `db.task` namespace
- `insertOne.sql` becomes `insertOne(...)`
- `completeById.sql` becomes `completeById(...)`
- named parameters in an execute query become a params class named from the
  namespace and file, such as `TaskInsertOneParams`
- the SELECT result row is named from the namespace, such as `TaskRow`
- SELECT methods return `SelectRunner<RowType>`, so `selectAll.sql` returns
  `SelectRunner<TaskRow>`

For the SQL above, the generated task namespace contains methods like:

```dart
Future<void> insertOne(TaskInsertOneParams params);
Future<void> completeById(TaskCompleteByIdParams params);
SelectRunner<TaskRow> selectAll();
```

and generated models like:

```dart
final class TaskInsertOneParams {
  const TaskInsertOneParams({
    required this.id,
    required this.title,
    required this.priority,
    required this.completed,
  });

  final int id;
  final String title;
  final String priority;
  final int completed;
}

final class TaskRow {
  const TaskRow({
    required this.id,
    required this.title,
    required this.priority,
    required this.completed,
  });

  final int id;
  final String title;
  final String priority;
  final int completed;
}
```

You normally read the generated file only to understand the API shape. You edit
SQL files and regenerate instead of editing generated Dart by hand.

## Open The Database

Flutter apps usually choose a database path with app-level storage code such as
`path_provider`, then pass that path to the generated database.

```dart
import 'package:path_provider/path_provider.dart';

import 'db/generated/app_database.dart';

Future<AppDatabase> openAppDatabase() async {
  final dir = await getApplicationDocumentsDirectory();
  final db = AppDatabase(
    path: '${dir.path}/app.db',
    adapters: AppDatabaseAdapters(
      taskPriorityToSql: (value) => (value as String).toLowerCase(),
      sqlValueToTaskPriority: (value) => (value as String).toUpperCase(),
    ),
  );

  await db.open();
  return db;
}
```

`open()` creates a new SQLite database when needed, runs the generated schema
and migrations, and prepares the connection for generated queries.

## Use The Generated Queries

Generated INSERT, UPDATE, and DELETE methods take generated params objects:

```dart
await db.task.insertOne(
  const TaskInsertOneParams(
    id: 1,
    title: 'Write SQL',
    priority: 'HIGH',
    completed: 0,
  ),
);
```

SELECT methods return a `SelectRunner<T>`:

```dart
final tasks = await db.task.selectAll().asList();
final first = tasks.first;

print('${first.title}: ${first.priority}');
```

Use a transaction when several generated calls should commit or roll back
together:

```dart
await db.transaction(() async {
  await db.task.insertOne(
    const TaskInsertOneParams(
      id: 2,
      title: 'Generate Dart',
      priority: 'NORMAL',
      completed: 0,
    ),
  );

  await db.task.completeById(const TaskCompleteByIdParams(id: 1));
});
```

## Watch For Changes

When INSERT, UPDATE, and DELETE operations go through generated SQLiteNow
methods, SQLiteNow reports the affected tables automatically. Any watcher for a
query that reads those tables is notified and re-runs:

```dart
final subscription = db.task.selectAll().watch().listen((tasks) {
  // Update app state with the latest rows.
});
```

Most apps do not need to call `reportExternalTableChanges`.

Use it only when something changes the same SQLite database outside generated
SQLiteNow write methods. Common cases are:

- hand-written SQL executed directly on the underlying connection
- a migration or import routine that bypasses generated query methods
- another local database integration writing to tables SQLiteNow queries watch

After that out-of-band write, report the affected table names:

```dart
db.reportExternalTableChanges({'task'});
```

## Close The Database

Close the generated database when the owning app service or test is disposed:

```dart
await subscription.cancel();
await db.close();
```

The repository example at `dart/examples/flutter_todo` follows this same flow
with a small UI, repository class, custom adapter, transactions, and watch-based
updates.
