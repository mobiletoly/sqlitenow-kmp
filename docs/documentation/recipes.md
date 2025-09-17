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
CREATE TABLE comment
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
CREATE TABLE comment
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
CREATE TABLE person_address
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

## Reactive UI Updates with Flows

SQLiteNow automatically tracks when your database tables change and can notify your UI through reactive flows. This keeps your interface up-to-date without manual refresh calls.

### Basic Reactive Query

Use the generated `.asFlow()` method to create queries that automatically update when data changes:

```kotlin
@Composable
fun PersonList() {
    var persons by remember { mutableStateOf<List<PersonEntity>>(emptyList()) }
    var isLoading by remember { mutableStateOf(true) }

    LaunchedEffect(Unit) {
        database.person
            .selectAll(PersonQuery.SelectAll.Params(limit = -1, offset = 0))
            .asFlow()                    // Reactive flow
            .flowOn(Dispatchers.IO)      // DB operations on IO thread
            .collect { personList ->
                persons = personList      // UI updates automatically
                isLoading = false
            }
    }

    // UI automatically updates when data changes
    LazyColumn {
        items(persons) { person ->
            PersonCard(person = person)
        }
    }
}
```

### With Data Transformation

Transform data on the IO thread before updating UI:

```kotlin
LaunchedEffect(Unit) {
    database.person
        .selectAll(PersonQuery.SelectAll.Params(limit = -1, offset = 0))
        .asFlow()
        .map { personList ->
            // Transform data on IO thread
            personList.map { person ->
                person.copy(
                    displayName = "${person.firstName} ${person.lastName}".trim(),
                    isRecent = person.createdAt > recentThreshold
                )
            }
        }
        .flowOn(Dispatchers.IO)
        .collect { transformedPersons ->
            persons = transformedPersons
        }
}
```

### Related Data Updates

Reactive flows work across related tables:

```kotlin
@Composable
fun PersonWithComments(personId: ByteArray) {
    var person by remember { mutableStateOf<PersonEntity?>(null) }
    var comments by remember { mutableStateOf<List<CommentEntity>>(emptyList()) }

    LaunchedEffect(personId.toList()) {
        // Person data flow
        launch {
            database.person
                .selectById(PersonQuery.SelectById.Params(id = personId))
                .asFlow()
                .flowOn(Dispatchers.IO)
                .collect { personResult ->
                    person = personResult.firstOrNull()
                }
        }

        // Comments data flow
        launch {
            database.comment
                .selectByPersonId(CommentQuery.SelectByPersonId.Params(personId = personId))
                .asFlow()
                .flowOn(Dispatchers.IO)
                .collect { commentList ->
                    comments = commentList
                }
        }
    }
}
```

### Performance Tips

- **Use LaunchedEffect**: Automatically cancels when composable leaves composition
- **Apply flowOn(Dispatchers.IO)**: Keep database operations off the main thread
- **Debounce rapid changes**: Use `.debounce(100)` for high-frequency updates
- **Limit query scope**: Use specific queries instead of broad `SELECT *` statements

**Note**: When using SQLiteNow's sync features, reactive flows automatically update during sync operations. See [Reactive Sync Updates]({{ site.baseurl }}/sync/reactive-updates/) for sync-specific patterns.

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
