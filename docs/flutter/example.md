---
layout: page
title: Flutter Example
permalink: /flutter/example/
---

# Flutter Example

The repository includes a Flutter todo example at
`dart/examples/flutter_todo`.

It demonstrates:

- SQL schema and query files under `lib/src/db/sql/TodoDatabase`.
- `sqlitenow.yaml` pointing at SQL input and generated Dart output.
- Generated database code under `lib/src/db/generated`.
- Custom adapters.
- Database open and close.
- Inserts, updates, transactions, and SELECT runners.
- `watch()` streams and invalidation.
- Widget, repository, and integration tests.

## Generate

From `dart/examples/flutter_todo`:

```shell
flutter pub get
flutter pub run sqlitenow_cli generate
```

The normal release path uses the compiler jar embedded in
`sqlitenow_cli`. Do not pass `--compiler-jar` unless you are working
inside this repository against a custom local compiler artifact.

## Run Tests

```shell
flutter test
```

To run the integration test, start an Android emulator or iOS simulator and run:

```shell
flutter test integration_test -d <device-id>
```

## App Storage

The example uses `path_provider` to choose an app documents directory for the
SQLite database file. SQLiteNow does not require a separate Flutter plugin for
that; the generated Dart database accepts the path selected by your app.
