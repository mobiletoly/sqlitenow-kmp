# Inspection

Inspection is about making generated schema and SQL behavior visible while you
iterate.

{% if include.platform == "dart" %}
For Flutter and Dart, keep generated output in a stable folder such as
`lib/db/generated` so your IDE can inspect generated params, result rows, and
adapters as you edit SQL:

```shell
flutter pub run sqlitenow_cli generate
dart analyze
flutter test
```

When you need to inspect the schema state that the compiler inferred, persist
the inspected schema database from `sqlitenow.yaml`:

```yaml
databases:
  AppDatabase:
    input: lib/db/sql/AppDatabase
    output: lib/db/generated
    package: app.db
    runtime: dart
    schemaDatabaseFile: build/sqlitenow/schema.db
```

The file is recreated when generation runs, and you can open it with a SQLite
client to inspect tables, indexes, views, and inferred schema state.
{% elsif include.platform == "kmp" %}
For KMP, persist the inspected schema database from the Gradle plugin:

```kotlin
sqliteNow {
    databases {
        create("AppDatabase") {
            packageName.set("com.example.app.db")
            schemaDatabaseFile.set(layout.projectDirectory.file("tmp/schema.db"))
        }
    }
}
```

The file is recreated when generation runs, and you can open it with a SQLite
client to inspect tables, indexes, views, and inferred schema state.
{% endif %}
