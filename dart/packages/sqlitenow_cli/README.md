# sqlitenow_cli

Dart-facing generator command for SQLiteNow.

Add it as a dev dependency:

```yaml
dev_dependencies:
  sqlitenow_cli: ^0.10.0
```

For pure Dart packages:

```shell
dart run sqlitenow_cli generate
```

For Flutter applications, run the command through Flutter's pub wrapper so the
Flutter SDK dependencies in the app can be resolved:

```shell
flutter pub run sqlitenow_cli generate
```

The command reads `sqlitenow.yaml`, invokes the packaged Gradle-free SQLiteNow
compiler artifact with the Dart backend, and writes generated Dart files to the
configured output directory.

To persist the inspected schema database during generation, add
`schemaDatabaseFile` to a database entry:

```yaml
databases:
  AppDatabase:
    input: lib/db/sql/AppDatabase
    output: lib/db/generated
    package: app.db
    runtime: dart
    schemaDatabaseFile: build/sqlitenow/schema.db
```

The file is recreated on each generation run and can be opened with a SQLite
client to inspect tables, indexes, views, and inferred schema state.

Release packages embed `lib/src/compiler/sqlitenow-compiler.jar`. Flutter and
Dart users do not pass `--compiler-jar` for normal consumption.

During local repository development, `--compiler-jar` remains available as an
explicit override:

```shell
dart run sqlitenow_cli generate --compiler-jar ../../sqlitenow-compiler/build/libs/sqlitenow-compiler-0.10.0-compiler.jar
```

Documentation: https://mobiletoly.github.io/sqlitenow-kmp/flutter/
