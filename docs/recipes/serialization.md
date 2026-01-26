---
layout: doc
title: Serialization
permalink: /recipes/serialization/
parent: Recipes
---

# Serialization

Strategies and helpers for converting between Kotlin types and the SQLite storage format.

## Serialize collections with JSON

You are free to choose your own strategy for storing collections. A common pattern is to serialize
the data as JSON. SQLiteNow ships utility helpers so you can keep the SQL clean and reuse adapters.

```sql
CREATE TABLE comment
(
    id   INTEGER PRIMARY KEY NOT NULL,

    -- @@{ field=tags, adapter=custom, propertyType=List<String> }
    tags TEXT
)
```

Then, configure your adapters with `jsonDecodeFromSqlite` and `jsonEncodeToSqlite`:

```kotlin
val db = SampleDatabase(
    dbName = resolveDatabasePath(dbName = "sample.db", appName = "SampleApp"),
    migration = VersionBasedDatabaseMigrations(),
    commentAdapters = SampleDatabase.CommentAdapters(
        sqlColumnToTags = { value -> value?.jsonDecodeFromSqlite() ?: emptyList() },
        tagsToSqlColumn = { tags -> tags?.jsonEncodeToSqlite() }
    )
)
```

## Serialize timestamps

SQLiteNow provides helpers for converting `LocalDateTime` values to and from the timestamp strings
stored in SQLite.

```sql
CREATE TABLE comment
(
    id         INTEGER PRIMARY KEY NOT NULL,

    -- @@{ field=created_at, adapter=custom, propertyType=kotlinx.datetime.LocalDateTime }
    created_at TEXT    NOT NULL DEFAULT current_timestamp
)
```

```kotlin
val db = SampleDatabase(
    dbName = resolveDatabasePath(dbName = "sample.db", appName = "SampleApp"),
    migration = VersionBasedDatabaseMigrations(),
    commentAdapters = SampleDatabase.CommentAdapters(
        sqlColumnToCreatedAt = { value -> LocalDateTime.fromSqliteTimestamp(value) },
        createdAtToSqlColumn = { instant -> instant.toSqliteTimestamp() },
    )
)
```

## Serialize enums with stable values

Prefer serializing enums using stable values instead of names or ordinals. The `EnumByValueLookup`
helper simplifies mapping between enum constants and their persisted representation.

```sql
CREATE TABLE person_address
(
    id           INTEGER PRIMARY KEY NOT NULL,

    -- @@{ field=address_type, adapter=custom, propertyType=AddressType }
    address_type TEXT    NOT NULL
)
```

```kotlin
enum class AddressType(val value: String) {
    HOME("home"),
    WORK("work");

    companion object : EnumByValueLookup<String, AddressType>(entries.associateBy { it.value })
}
```

```kotlin
val db = SampleDatabase(
    dbName = resolveDatabasePath(dbName = "sample.db", appName = "SampleApp"),
    migration = VersionBasedDatabaseMigrations(),
    personAddressAdapters = SampleDatabase.PersonAddressAdapters(
        addressTypeToSqlColumn = { it.value },
        sqlColumnToAddressType = { value -> AddressType.from(value) },
    )
)
```
