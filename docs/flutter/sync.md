---
layout: page
title: Flutter/Dart Sync
permalink: /flutter/sync/
---

# Flutter/Dart Sync

Flutter and Dart projects use the generated Dart database plus
`package:sqlitenow_oversqlite` for oversqlite sync.

## Add Packages

Replace `X.Y.Z` with the latest SQLiteNow release version:

```yaml
dependencies:
  sqlitenow_runtime: ^X.Y.Z
  sqlitenow_oversqlite: ^X.Y.Z

dev_dependencies:
  sqlitenow_cli: ^X.Y.Z
```

## Enable Sync Generation

Set `oversqlite: true` for the database in `sqlitenow.yaml`:

```yaml
databases:
  AppDatabase:
    input: lib/db/sql/AppDatabase
    output: lib/db/generated
    package: app.db
    runtime: dart
    oversqlite: true
```

Use `enableSync=true` on every table that should participate in sync:

```sql
-- @@{ enableSync=true }
CREATE TABLE person (
  id TEXT PRIMARY KEY NOT NULL,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL
);
```

Generated sync databases expose `buildOversqliteConfig(...)` and
`newOversqliteClient(...)`.

## Create The Client

Your app owns authentication. Pass an HTTP client that sends the credentials
expected by your server:

```dart
import 'dart:io';

import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

final httpClient = IoOversqliteHttpClient(
  baseUri: Uri.parse('https://api.myapp.com'),
  defaultHeaders: {
    HttpHeaders.authorizationHeader: 'Bearer $token',
  },
);

final syncClient = db.newOversqliteClient(
  schema: 'myapp',
  httpClient: httpClient,
);
```

## Open, Attach, And Sync

Call `open()` on every app launch, then call `attach(userId)` whenever an
authenticated session exists:

```dart
await syncClient.open();
await syncClient.attach('user123');
await syncClient.sync();
```

`attach(...)` may return `AttachConnected` or `AttachRetryLater`. Treat
`AttachRetryLater` as a lifecycle response and retry later rather than as a
local database failure.

Initial and remote attachment validates the server's canonical
`registered_table_specs` before connect or snapshot work. A same-user durable
resume remains network-free, and the next operation that contacts the server
validates it. If tables or ordered sync keys differ,
`SyncTableContractMismatchException` reports sorted `serverOnlyTables`,
`clientOnlyTables`, and `syncKeyMismatches` before any sync mutation. Automatic
downloads terminate on this incompatibility instead of retrying forever.

This is exact table/key compatibility checking, not wire-profile or projection
negotiation.

## Automatic Downloads

Automatic downloads are optional and default-off. They download authoritative
remote data through the same pull path as explicit sync operations.

```dart
final automaticDownloads = syncClient.startAutomaticDownloads();

// Later, when the owning app scope shuts down:
await automaticDownloads.stop();
```

## Shared Concepts And Server Setup

- [Core Concepts]({{ site.baseurl }}/sync/core-concepts/)
- [Server Setup]({{ site.baseurl }}/sync/server-setup/)
