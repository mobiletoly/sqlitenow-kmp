# sqlitenow_runtime

Runtime contracts for SQLiteNow Dart generated code.

This package currently targets Dart VM and Flutter native runtimes through
`package:sqlite3`. The SQLite driver is kept behind a small boundary so a web
driver can be evaluated later without changing generated query APIs.

## Install

Replace `X.Y.Z` with the latest SQLiteNow release version.

```yaml
dependencies:
  sqlitenow_runtime: ^X.Y.Z
```

Use this package with generated Dart code from `sqlitenow_cli`.

Documentation:

- Flutter/Dart guide: https://mobiletoly.github.io/sqlitenow-kmp/flutter/
- Runtime guide: https://mobiletoly.github.io/sqlitenow-kmp/flutter/runtime/

## Example

```dart
final database = SqliteNowDatabase.inMemory(
  migrations: [
    SqliteNowMigrationStep(1, (conn) {
      return conn.execute('CREATE TABLE person(id INTEGER PRIMARY KEY, name TEXT NOT NULL)');
    }),
  ],
);

await database.open();

await database.connection.execute(
  'INSERT INTO person(id, name) VALUES (?, ?)',
  parameters: [1, 'Ada'],
  affectedTables: {'person'},
);

final names = SelectRunner(
  database: database,
  affectedTables: {'person'},
  query: () {
    return database.connection.select(
      'SELECT name FROM person ORDER BY id',
      (row) => row.readString(0),
    );
  },
);

print(await names.asList());
await database.close();
```
