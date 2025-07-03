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
