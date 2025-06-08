# SQLiteNow

A Kotlin Multiplatform library for type-safe SQLite database access, inspired by [SQLDelight](https://github.com/cashapp/sqldelight).

Full documentation is available in the [`/docs`](./docs) directory.

## Overview

SQLiteNow generates Kotlin code from your SQL files, giving you full control over your queries while maintaining type safety. Unlike SQLDelight, which supports multiple database engines, SQLiteNow is focused exclusively on SQLite, allowing for deeper integration and SQLite-specific optimizations.

## Key Features

- **Pure SQL Control** - Write your queries in SQL files, get type-safe Kotlin code
- **Comment-based Annotations** - Control code generation using simple `-- @@annotation` comments in your SQL
- **No IDE Plugin Required** - Works with any editor, uses Gradle plugin for code generation
- **Kotlin Multiplatform** - Supports all KMP targets using [androidx.sqlite](https://developer.android.com/kotlin/multiplatform/sqlite) driver
- **SQLite Focused** - Optimized specifically for SQLite features and capabilities

## Why SQLiteNow exists if SQLDelight is really awesome

First of all, I wanted to target specifically SQLite in Kotlin Multiplatform environment, it is my platform
of choice as of now for mobile development. SQLiteNow built on top of using multiplatform SQLite driver from AndroidX.

Second, since we use comment-based annotations (and I'm planning to add user-defined annotations in the future),
no plugin is required (like in case of SQLDelight), so you can easily use just a regular SQL files
that will be validated by your IDE or other external tools for correctness.

Third, I wanted to have a more flexible and extensible code generation system that can be easily extended
and customized. For example, you can declare this SQL statement in your file:  

```sql
-- @@sharedResult=PersonEntity
-- @@implements=com.example.app.PersonEssentialFields
-- @@excludeOverrideFields=[phone, birthDate]
SELECT * FROM Person
LIMIT :limit OFFSET :offset
```

This will generate shared data class `PersonEntity` that implements my custom `PersonEssentialFields` interface
but because this interface does not include phone and birthDate fields - they will be excluded.

## How it works

1. Write your SQL queries in `.sql` files
2. Add annotations using SQL comments to control code generation
3. Run the Gradle plugin to generate type-safe Kotlin code
4. Use the generated database classes in your application

Full example is available in the [`/sample-kmp`](./sample-kmp) directory.

## Getting Started

Add the following to your `build.gradle.kts` file:

```kotlin
plugins {
    id("dev.goquick.sqlitenow") version "0.0.1"
}

// ...

sqliteNow {
    databases {
        // TODO change database name if needed
        create("NowSampleDatabase") {
            // TODO change package name
            packageName.set("com.example.app.db")
        }
    }
}
```

In you common source directory (**commonMain** by default) create `sql/NowSampleDatabase` directory
for your SQL files and create 4 empty subdirectories (even if you don't use some of them): 
- **/schema** - this is where you put your CREATE TABLE, CREATE INDEX and other schema related statements
- **/queries** - this is where you put your queries (such as SELECT, DELETE, etc.)
- **/init** - this is where you **optionally** put your INSERT INTO and other statements that should be executed
  after schema is created to provision tables with some default data (if needed)
- **/migration** - this is where you **optionally** put your migration scripts

Create `schema/Person.sql` file with the following content:

```sql
CREATE TABLE Person
(
    id         INTEGER PRIMARY KEY NOT NULL,
    first_name TEXT                NOT NULL,
    last_name  TEXT                NOT NULL,
    email      TEXT                NOT NULL UNIQUE,
  
    -- @@field=phone @@propertyName=userPhone
    phone      TEXT,

    -- @@field=birth_date @@propertyType=kotlinx.datetime.LocalDate @@adapter
    birth_date TEXT,

    -- @@field=created_at @@propertyType=kotlinx.datetime.LocalDateTime @@adapter
    created_at TEXT                NOT NULL DEFAULT current_timestamp
);

CREATE INDEX idx_person_email ON Person (email);
```

Just to quickly explain what is going on here:
- **-- @@field=phone @@propertyName=userPhone** - this will generate `userPhone` property in `Person` data class for `phone` column
- **-- @@field=birth_date @@propertyType=kotlinx.datetime.LocalDate @@adapter** - this will generate `birthDate` property
  for `birth_date` column and expects adapter to be provided for `String` <=> `LocalDate` conversion.
  **@@adapter** annotation requests plugin to generate column adapter that developer must supply.
- **-- @@field=created_at @@propertyType=kotlinx.datetime.LocalDateTime @@adapter** - this will generate `createdAt` property
  for `created_at` column and will use adapter to convert between `LocalDateTime` and `String` (since SQLite does not
  have `DATETIME` type). This field has a default value of `current_timestamp` so it will be automatically set to current
  timestamp when the row is inserted.

Now when we defined the schema - we can create our first query. Queries should reside
in `sql/NowSampleDatabase/queries/[namespace]` directory (normally you can use your table name as a namespace,
but is really up to you, you can create as many namespaces as you want, for example to deal with complex
queries that span multiple tables). In our case create `queries/person/selectAll.sql` file with the following content:

```sql
SELECT * FROM Person
LIMIT :limit OFFSET :offset;
```

This will generate Person's `selectAll` query that will return `List<Person>` and will have `limit` and `offset` parameters.

You can add more queries to the same namespace, for example `queries/person/add.sql`:

```sql
INSERT INTO Person (email, first_name, last_name, phone, birth_date, created_at)
VALUES (:email, :firstName, :lastName, :phone, :birthDate, :createdAt);
```

This will generate `add` query that you can use to add new Person entity to the table.

Here is how you are going to use it in your code:

```kotlin
// Call this somewhere in your app initialization code
val db = NowSampleDatabase(
    resolveDatabasePath("sample.db"),
    // Adapters to resolve type conversion for Person table,
    // usually generated for columns marked with @@adapter annotation
    personAdapters = NowSampleDatabase.PersonAdapters(
        birthDateToSqlColumn = { 
            it?.toSqliteDate()
        },
        createdAtToSqlColumn = { 
            it.toSqliteTimestamp() 
        },
        sqlColumnToBirthDate = {
            it?.let { LocalDate.fromSqliteDate(it) }
        },
        sqlColumnToCreatedAt = {
            LocalDateTime.fromSqliteTimestamp(it)
        },
    )
)

// ...

// Call this somewhere in your app code, e.g. in LaunchedEffect
val personList: List<Person> = db.person
    .selectAll(
      Person.SelectAll.Params(
          limit = -1,     // no limits
          offset = 0      // start from the first record
      )
    )
    .executeAsList()
```
