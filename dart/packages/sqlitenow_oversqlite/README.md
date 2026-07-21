# sqlitenow_oversqlite

Add offline-first synchronization to SQLiteNow databases in Dart and Flutter applications.

`sqlitenow_oversqlite` tracks local changes, pushes them to an Oversync server, applies remote
changes to SQLite, and keeps generated reactive queries up to date. It supports explicit and
automatic downloads, conflict resolution, snapshot recovery, and multiple devices attached to the
same user account.

This package targets Dart VM and Flutter native applications. It is used with a database generated
by [`sqlitenow_cli`](https://pub.dev/packages/sqlitenow_cli) and a compatible Oversync server.

## Install

Use the same version for all SQLiteNow packages:

```yaml
dependencies:
  sqlitenow_runtime: ^0.10.0
  sqlitenow_oversqlite: ^0.10.0

dev_dependencies:
  sqlitenow_cli: ^0.10.0
```

## Generate a sync-enabled database

Enable Oversqlite for your database in `sqlitenow.yaml`:

```yaml
databases:
  AppDatabase:
    input: lib/db/sql/AppDatabase
    output: lib/db/generated
    package: app.db
    runtime: dart
    oversqlite: true
```

Mark every table that should synchronize with `enableSync=true`:

```sql
-- @@{ enableSync=true }
CREATE TABLE person (
  id TEXT PRIMARY KEY NOT NULL,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL
);
```

Generate the Dart database:

```shell
dart run sqlitenow_cli generate
```

For a Flutter application, use:

```shell
flutter pub run sqlitenow_cli generate
```

The generated database exposes `buildOversqliteConfig(...)` and
`newOversqliteClient(...)` for the synchronized tables.

Numeric wire handling is automatic. SQLite `INTEGER` and finite `REAL` values synchronize as
canonical JSON strings, exact decimals remain SQLite `TEXT` strings, and SQLite Boolean affinity
uses strict `"0"`/`"1"` ingress strings. No per-column numeric configuration is required.

## Connect and sync

Open the generated database, create an authenticated HTTP transport, and create the generated
Oversqlite client:

```dart
import 'dart:io';

import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

import 'db/generated/app_database.dart';

final database = AppDatabase(path: databasePath);
await database.open();

final httpClient = IoOversqliteHttpClient(
  baseUri: Uri.parse('https://api.example.com'),
  defaultHeaders: {
    HttpHeaders.authorizationHeader: 'Bearer $accessToken',
  },
);

final syncClient = database.newOversqliteClient(
  schema: 'app',
  httpClient: httpClient,
);

await syncClient.open();

final attachResult = await syncClient.attach(userId);
if (attachResult case AttachConnected()) {
  await syncClient.sync();
}
```

Call `open()` on each application launch. Call `attach(userId)` after authentication establishes
the current account, then use `sync()` whenever the application should exchange local and remote
changes. `AttachRetryLater` indicates that the remote attachment should be retried later.

Every remote capabilities check compares the generated local table/key contract with the server's
required `registered_table_specs`. Initial and remote attach fail before connect or snapshot work
when the contracts differ. A same-user durable attach resume remains network-free and validates on
the next operation that already contacts the server. `SyncTableContractMismatchException` exposes
sorted `serverOnlyTables`, `clientOnlyTables`, and `syncKeyMismatches`; automatic downloads treat it
as terminal. This check covers exact table and ordered sync-key compatibility, not wire-profile or
projection negotiation.

Your application owns authentication. `IoOversqliteHttpClient` sends the headers you provide along
with the Oversqlite source identity managed by the local database.

## Sync operations

- `sync()` pushes pending local changes and pulls remote changes until stable.
- `pushPending()` uploads pending local changes without running a pull.
- `pullToStable()` downloads and applies remote changes without running a push.
- `syncStatus()` reports pending local and remote work.
- `rebuild()` restores the local synchronized state from a server snapshot.
- `syncThenDetach()` synchronizes and disconnects the current account.

Normal `sync()` and `pullToStable()` automatically recover checkpoints when server history no
longer matches local state. See the [sync documentation](https://mobiletoly.github.io/sqlitenow-kmp/sync/core-concepts/)
for recovery behavior and pending-work handling.

Generated SQLiteNow writes are captured automatically. Remote changes applied by Oversqlite also
invalidate generated reactive queries, so active query streams receive the updated rows.

## Automatic downloads

Automatic downloads are optional and disabled by default:

```dart
final downloads = syncClient.startAutomaticDownloads();

// Stop the worker when its owning application scope shuts down.
await downloads.stop();
```

Configure polling and bundle-change watching through the generated `newOversqliteClient(...)`
arguments.

## Learn more

- [Flutter and Dart sync guide](https://mobiletoly.github.io/sqlitenow-kmp/flutter/sync/)
- [SQLiteNow Flutter and Dart documentation](https://mobiletoly.github.io/sqlitenow-kmp/flutter/)
- [Oversync server setup](https://mobiletoly.github.io/sqlitenow-kmp/sync/server-setup/)
- [Flutter example](https://github.com/mobiletoly/sqlitenow-kmp/tree/main/dart/examples/flutter_todo)
