---
layout: doc
title: Inspection
permalink: /recipes/inspection/
parent: Recipes
---

# Inspection

Persist and inspect generated schemas to speed up migrations and tooling workflows.

## Create a schema database file

SQLiteNow builds your schema by executing all SQL assets against an in-memory database. You can
persist that schema to disk to inspect tables, run ad-hoc queries, or feed external tooling.

Enable the `schemaDatabaseFile` option in the Gradle plugin configuration:

```kotlin
sqliteNow {
    databases {
        create("NowSampleDatabase") {
            packageName.set("dev.goquick.sqlitenow.samplekmp.db")
            schemaDatabaseFile.set(layout.projectDirectory.file("tmp/schema.db"))
        }
    }
}
```

The specified database file is recreated each time you run the `generate<Database>Database` task.
Open it with your favorite SQLite client or IDE tooling to verify schema changes before shipping.
