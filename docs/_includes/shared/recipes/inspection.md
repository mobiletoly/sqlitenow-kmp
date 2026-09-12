# Inspection

Inspection is about making generated schema and SQL behavior visible while you
iterate.

{% if include.platform == "kmp" %}
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
{% elsif include.platform == "swift" %}
For Swift, enable debug generation in `SQLiteNow.json` when you need to inspect
the compiler's file-backed schema database:

```json
{
  "schemaVersion": 1,
  "databases": [
    {
      "databaseName": "AppDatabase",
      "swiftPackageName": "AppDatabaseSQLiteNow",
      "swiftTargetName": "AppDatabaseSQLiteNow",
      "runtime": "core",
      "debug": true
    }
  ]
}
```

The plugin writes the inspected schema database under `.build/sqlitenow/schema/`.
Open that file with a SQLite client to inspect tables, indexes, views, and
inferred schema state.
{% endif %}
