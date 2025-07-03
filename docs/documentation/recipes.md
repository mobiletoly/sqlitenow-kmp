---
layout: doc
title: Recipes
permalink: /documentation/recipes/
parent: Documentation
---

# Recipes

Useful recipes and patterns for using SQLiteNow in your projects.


---
## Serialize/deserialize list

It is up to you what strategy to pick to serialize/deserialize list of items to SQLite.
One of the popular options is to use JSON and we already added helpers for that in the library.

Here is an example of annotation:

```sql
CREATE TABLE Comment
(
    id INTEGER PRIMARY KEY NOT NULL,

    -- @@field=tags @@adapter
    -- @@propertyType=List<String>
    tags TEXT
)
```

And then in your adapters you can use `jsonDecodeFromSqlite` and `jsonEncodeToSqlite` helpers:

```kotlin
val db = SampleDatabase(
    dbName = resolveDatabasePath("sample.db"),
    migration = VersionBasedDatabaseMigrations(),
    commentAdapters = SampleDatabase.CommentAdapters(
        sqlColumnToTags = {
            it?.jsonDecodeFromSqlite() ?: emptyList()
        },
        tagsToSqlColumn = {
            it?.jsonEncodeToSqlite()
        }
    )
)
```

---
## Serialize/deserialize timestamps

SQLiteNow provides helpers for converting between `LocalDateTime` and SQLite timestamp string.
You can use them in your adapters as shown in the example below:

```sql
CREATE TABLE Comment
(
    id INTEGER PRIMARY KEY NOT NULL,

    -- @@field=created_at @@adapter
    -- @@propertyType=kotlinx.datetime.LocalDateTime
    created_at TEXT NOT NULL DEFAULT current_timestamp
)
```

And then in your adapters you can use `toSqliteTimestamp` and `fromSqliteTimestamp` helpers:

```kotlin
val db = SampleDatabase(
    dbName = resolveDatabasePath("sample.db"),
    migration = VersionBasedDatabaseMigrations(),
    commentAdapters = SampleDatabase.CommentAdapters(
        sqlColumnToCreatedAt = {
            LocalDateTime.fromSqliteTimestamp(it)
        },
        createdAtToSqlColumn = {
            it.toSqliteTimestamp()
        },
    )
)
```

---
## Create schema file

In order to generate SQLiteNow schemas - we use in-memory created SQLite database.
This database is created from all SQL files in `schema/` directory.
If you want to inspect the generated schema - you can create a file-based database from the same SQL files.

For this you can use `schemaDatabaseFile` property in sqliteNow plugin:

```kotlin
sqliteNow {
    databases {
        create("NowSampleDatabase") {
            packageName.set("dev.goquick.sqlitenow.samplekmp.db")
            schemaDatabaseFile.set(layout.projectDirectory.file("tmp/schema.sqlite"))
        }
    }
}
```

This will result in `schema.sqlite` file being created in `tmp/` directory every time you run
**generateNowSampleDatabase** task. This can be convenient if you want to inspect the generated schema
or if you want to associate your .sql files with database schema in some external tool (or in IntelliJ
SQL plugin etc).
