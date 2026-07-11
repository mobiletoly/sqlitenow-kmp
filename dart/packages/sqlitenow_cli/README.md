# sqlitenow_cli

Generate type-safe Dart database APIs from SQL with SQLiteNow.

Write ordinary SQLite schema and query files, describe the database in `sqlitenow.yaml`, and run one
command. SQLiteNow generates database lifecycle, migrations, typed query parameters, result rows,
transactions, and reactive query support for Dart and Flutter applications.

The package includes the SQLiteNow compiler. It does not require Gradle or a separate compiler
download.

## Requirements

- Dart 3.11 or newer
- Java/JDK 17 or newer available on `PATH`

## Install

Add the CLI as a development dependency and the generated-code runtime as an application
dependency. Use the same version for both packages:

```yaml
dependencies:
  sqlitenow_runtime: ^0.10.0

dev_dependencies:
  sqlitenow_cli: ^0.10.0
```

## Create your SQL files

Organize each database into schema and query directories:

```text
lib/db/sql/AppDatabase/
  schema/
    task.sql
  queries/
    task/
      insertOne.sql
      selectAll.sql
```

Define the table in `schema/task.sql`:

```sql
CREATE TABLE task (
  id INTEGER PRIMARY KEY NOT NULL,
  title TEXT NOT NULL,
  completed INTEGER NOT NULL
);
```

Define application operations as SQL. Named parameters become fields in generated Dart parameter
classes:

```sql
-- queries/task/insertOne.sql
INSERT INTO task (id, title, completed)
VALUES (:id, :title, :completed);
```

```sql
-- queries/task/selectAll.sql
SELECT id, title, completed
FROM task
ORDER BY id;
```

## Configure generation

Create `sqlitenow.yaml` at the package root:

```yaml
databases:
  AppDatabase:
    input: lib/db/sql/AppDatabase
    output: lib/db/generated
    package: app.db
    runtime: dart
    oversqlite: false
```

You can define multiple databases in the same file. Each database gets its own generated Dart API.

## Generate Dart code

For a Dart package, run:

```shell
dart run sqlitenow_cli generate
```

For a Flutter application, run through Flutter's pub wrapper:

```shell
flutter pub run sqlitenow_cli generate
```

The CLI validates the SQL model, writes Dart files to the configured `output` directory, and formats
the generated source automatically.

To use a differently named configuration file:

```shell
dart run sqlitenow_cli generate --config config/sqlitenow.yaml
```

Use `--java` when Java 17+ is installed at a non-default path:

```shell
dart run sqlitenow_cli generate --java /path/to/java
```

## Use the generated API

The SQL above generates an `AppDatabase`, a `task` query namespace, typed parameters, and typed
result rows:

```dart
import 'db/generated/app_database.dart';

Future<void> useDatabase(String databasePath) async {
  final database = AppDatabase(path: databasePath);
  await database.open();

  await database.task.insertOne(
    const TaskInsertOneParams(
      id: 1,
      title: 'Try SQLiteNow',
      completed: 0,
    ),
  );

  final tasks = await database.task.selectAll().asList();
  print('Loaded ${tasks.length} task(s)');

  await database.close();
}
```

Edit the SQL and run the generator again when the database API changes. Generated files should not
be edited by hand.

## Optional schema inspection

Set `schemaDatabaseFile` to keep the SQLite database used while inspecting your schema:

```yaml
databases:
  AppDatabase:
    input: lib/db/sql/AppDatabase
    output: lib/db/generated
    package: app.db
    runtime: dart
    oversqlite: false
    schemaDatabaseFile: build/sqlitenow/schema.db
```

The file is recreated during generation. Open it with a SQLite browser when you want to inspect the
generated tables, indexes, and views.

## Synchronization

Set `oversqlite: true` and add
[`sqlitenow_oversqlite`](https://pub.dev/packages/sqlitenow_oversqlite) when the database should
synchronize with an Oversync server. SQLiteNow then generates the sync table configuration and
client factory alongside the database API.

## Learn more

- [Flutter and Dart getting started guide](https://mobiletoly.github.io/sqlitenow-kmp/flutter/getting-started/)
- [SQLiteNow Flutter and Dart documentation](https://mobiletoly.github.io/sqlitenow-kmp/flutter/)
- [Dart console example](https://github.com/mobiletoly/sqlitenow-kmp/tree/main/dart/examples/dart_console)
- [Flutter example](https://github.com/mobiletoly/sqlitenow-kmp/tree/main/dart/examples/flutter_todo)
