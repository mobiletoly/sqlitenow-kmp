---
layout: doc
title: Recipes
permalink: /documentation/recipes/
parent: Documentation
---

# Recipes

Useful recipes and patterns for using SQLiteNow in your projects.

<br>

-----

## Serialize/deserialize list

It is up to you what strategy to pick to serialize/deserialize list of items to SQLite.
One of the popular options is to use JSON and we already added helpers for that in the library.

Here is an example of annotation:

```sql
CREATE TABLE Comment
(
    id   INTEGER PRIMARY KEY NOT NULL,

    -- @@{ field=tags, adapter=custom, propertyType=List<String> }
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

<br>

---

## Serialize/deserialize timestamps

SQLiteNow provides helpers for converting between `LocalDateTime` and SQLite timestamp string.
You can use them in your adapters as shown in the example below:

```sql
CREATE TABLE Comment
(
    id         INTEGER PRIMARY KEY NOT NULL,

    -- @@{ field=created_at, adapter=custom, propertyType=kotlinx.datetime.LocalDateTime }
    created_at TEXT                NOT NULL DEFAULT current_timestamp
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

<br>

---

## Serialize/deserialize enums

There are different strategies to serialize/deserialize enums to database. It could be a
serialization by enum constant name, by unique integer number assigned to each enum constant,
or by some other unique value, such as string name. We suggest not to use enum ordinal or enum
name, as it is not guaranteed to be stable across different versions of your application,
so our suggestion is to assign a stable unique text value to each enum constant. For this we
have created a helper class `EnumByValueLookup` that you can use to create a lookup between
enum constant and its unique value. Here is an example:

```sql
CREATE TABLE PersonAddress
(
    id           INTEGER PRIMARY KEY NOT NULL,

    -- @@{ field=address_type, adapter=custom, propertyType=AddressType }
    address_type TEXT                NOT NULL
)
```

```kotlin
enum class AddressType(val value: String) {
    HOME("home"),
    WORK("work");

    // This construction introduces .from() extension function to lookup enum constant by its value,
    // such as "home" or "work"
    companion object : EnumByValueLookup<String, AddressType>(entries.associateBy { it.value })
}
```

```kotlin
val db = SampleDatabase(
    dbName = resolveDatabasePath("sample.db"),
    migration = VersionBasedDatabaseMigrations(),
    personAddressAdapters = SampleDatabase.PersonAddressAdapters(
        addressTypeToSqlColumn = { it.value },
        sqlColumnToAddressType = { AddressType.from(it) },
    )
)
```

<br>

---

## Create database schema file

In order to generate SQLiteNow schemas - we use in-memory created SQLite database.
This database is created from all SQL files in `schema/` directory.
If you want to inspect the generated schema - you can create a file-based database from the same SQL
files.

For this you can use `schemaDatabaseFile` property in sqliteNow plugin:

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

This will result in `schema.db` file being created in `tmp/` directory every time you run
**generateNowSampleDatabase** task. This can be convenient if you want to inspect the generated
schema or if you want to associate your .sql files with database schema in some external
tool (or in IntelliJ SQL plugin etc).
