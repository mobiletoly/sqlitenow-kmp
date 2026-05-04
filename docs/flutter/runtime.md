---
layout: page
title: Flutter/Dart Runtime
permalink: /flutter/runtime/
---

# Flutter/Dart Runtime Reference

The getting-started guide shows the normal generated-code flow. This page is a
lower-level reference for the runtime contracts that generated Dart code uses.
Most app code should keep calling the generated database and query APIs.

## Database Lifecycle

Generated database classes wrap `SqliteNowDatabase`.

Runtime lifecycle rules:

- `open()` creates one SQLite connection.
- `open()` runs generated migrations before the generated database is ready.
- Calling `open()` on an already open database throws.
- `close()` closes the connection and invalidation stream.
- After a database has been opened and closed, it cannot be reopened. Create a
  new generated database instance instead.
- `connection` throws until `open()` succeeds and after `close()`.

Generated databases usually expose the same lifecycle surface:

```dart
Future<void> open();
Future<void> close();
bool get isOpen;
SqliteNowConnection get connection;
```

Use in-memory constructors for tests when the generated database exposes them.

## Serialized Connection Access

SQLiteNow serializes all work on a single SQLite connection. Generated SELECT
and execute methods eventually enter `SqliteNowConnection.withExclusiveAccess`.

This gives the runtime one ordered queue for:

- SELECT statements
- INSERT, UPDATE, DELETE, and raw execute statements
- prepared statements
- transactions
- migration execution
- close

Calls made from inside the current connection owner, such as generated queries
inside a transaction block, are re-entrant and run on the same connection
without deadlocking the queue.

## Migration Runtime

`open()` applies generated migration steps before the database is marked open.
The detailed file layout, versioning rules, fresh bootstrap behavior, and
upgrade examples are covered in the
[migration guide]({{ site.baseurl }}/flutter/migrations/).

## Transaction Semantics

Generated database classes delegate to `SqliteNowDatabase.transaction(...)`.

```dart
Future<T> transaction<T>(
  FutureOr<T> Function() block, {
  TransactionMode mode = TransactionMode.deferred,
});
```

Transaction modes map to SQLite begin statements:

| Mode | SQL |
| --- | --- |
| `TransactionMode.deferred` | `BEGIN` |
| `TransactionMode.immediate` | `BEGIN IMMEDIATE` |
| `TransactionMode.exclusive` | `BEGIN EXCLUSIVE` |

Nested transaction calls do not create savepoints today. A nested call
participates in the outer transaction:

- the outermost transaction issues `BEGIN`
- nested calls run inside that same transaction
- only the outermost transaction commits
- if a nested block throws, the error propagates and the outer transaction rolls
  back

Use one explicit transaction block for the unit of work you want to commit or
roll back together.

## SelectRunner Semantics

Generated SELECT methods return `SelectRunner<T>`.

| Method | Contract |
| --- | --- |
| `asList()` | Runs the query and returns all rows. |
| `asOne()` | Requires exactly one row; throws if zero or more than one row is returned. |
| `asOneOrNull()` | Returns null for zero rows; throws if more than one row is returned. |
| `watch()` | Emits the current query result on listen, then re-runs after matching table invalidations. |

`watch()` is a single-subscription stream. Query errors are delivered as stream
errors. Cancelling the subscription stops listening for invalidations.

If a generated SELECT has no affected table metadata, `watch()` still emits the
initial query result but will not receive later table invalidations.

## Invalidation Contract

Generated INSERT, UPDATE, and DELETE methods pass an `affectedTables` set to the
runtime. After the statement succeeds, the runtime reports those tables to the
database invalidation tracker.

Watch streams compare their query table set with the reported changed table set:

- table names are trimmed and lower-cased
- empty affected-table reports are ignored
- matching watchers re-run their SELECT
- non-matching watchers do nothing

Most apps do not call `reportExternalTableChanges(...)`. It is for out-of-band
writers only:

```dart
db.reportExternalTableChanges({'task'});
```

Use it after a table is changed outside generated SQLiteNow write methods, such
as direct SQL on the connection, an import routine, or another local integration
that writes into the same database file.

## Bind And Read Types

The runtime driver boundary accepts SQLite scalar values:

- `null`
- `String`
- `int`
- `double`
- `Uint8List`

Generated collection parameters for `IN` clauses are encoded as JSON arrays for
SQLite `json_each(...)` queries. Collection elements must also be scalar values.

Row readers expose strict typed accessors:

- `readString()` / `readNullableString()`
- `readInt()` / `readNullableInt()`
- `readDouble()` / `readNullableDouble()`
- `readBlob()` / `readNullableBlob()`
- `readValue()`

Non-null readers throw if the SQLite value is null. Typed readers throw if the
driver value has the wrong Dart type.

## Adapter Contract

Adapters are generated when SQL annotations request custom conversion. They run
at the edges:

- before binding a parameter into SQLite
- after reading a SQLite column into a generated row

Adapter functions should be synchronous, deterministic, and cheap. They should
convert between app-level values and SQLite scalar values supported by the
runtime driver boundary.

Generated code currently uses an adapters container specific to each database.
The getting-started guide shows the app-facing shape.

## Driver Boundary And Web Status

The public runtime currently uses `package:sqlite3` through `Sqlite3Driver`.
SQLite access is behind `SqliteNowDriver`, `SqliteNowDriverConnection`, and
`SqliteNowDriverStatement` so another driver can be added later.

Public Flutter web support is not exposed yet. Keep web assumptions outside app
code that depends on the current public runtime.
