---
layout: page
title: Getting Started
permalink: /getting-started/
---

This guide will help you set up SQLiteNow in your Kotlin Multiplatform project and create your first type-safe database queries.

## Prerequisites

- Kotlin Multiplatform project
- Gradle 8.0 or higher
- Kotlin 2.x

## Installation

First add SQLiteNow version to your `libs.version.toml` file

```toml
[version]
sqlite = "2.5.1"
sqlitenow = "0.1.10"
kotlinx-datetime = "0.6.2"

[libraries]
sqlitenow-kmp = { module = "dev.goquick.sqlitenow:core", version.ref = "sqlitenow" }
sqlite-bundled = { module = "androidx.sqlite:sqlite-bundled", version.ref = "sqlite" }
kotlinx-datetime = { module = "org.jetbrains.kotlinx:kotlinx-datetime", version.ref = "kotlinx-datetime" }

[plugins]
sqlitenow = { id = "dev.goquick.sqlitenow", version.ref = "sqlitenow" }
```

Add the SQLiteNow Gradle plugin and runtime dependency to your `composeApp/build.gradle.kts` file:

```kotlin
plugins {
    // ...
    alias(libs.plugins.sqlitenow)
}

kotlin {
    sourceSets {
        commonMain.dependencies {
            // ...
            implementation(libs.sqlitenow.kmp)
            implementation(libs.sqlite.bundled)
            implementation(libs.kotlinx.datetime)
        }
    }
}
```

Most likely you would need to update your project's root-level `build.gradle.kts` file as well:

```kotlin
plugins {
    // ...
    alias(libs.plugins.sqlitenow) apply false
}
```

Configure the plugin:

```kotlin
sqliteNow {
    databases {
        create("SampleDatabase") {
            packageName.set("com.example.app.db")
        }
    }
}
```

This will create **generateSampleDatabase** task that you can use to generate your database code.
Generated code will be added to `build/generated/sqlitenow/code` directory (not to your source directory)
and will be under `com.example.app.db`, and will be available to your `commonMain` source set.

_Note:_ If you don't see **generateSampleDatabase** task in your Gradle tasks list - try
running `./gradlew build` first.

## Project Structure

Create the following directory structure in your `commonMain` source set:

```
src/commonMain/sql/SampleDatabase/
├── schema/          # CREATE TABLE, CREATE INDEX statements (mandatory)
├── queries/         # SELECT, INSERT, UPDATE, DELETE queries (mandatory)
├── init/            # Initial data (optional)
└── migration/       # Migration scripts (optional)
```

## Create Your First Schema

Create `schema/Person.sql` with your table definition:

```sql
CREATE TABLE Person
(
    id         INTEGER PRIMARY KEY NOT NULL,
    first_name TEXT                NOT NULL,
    last_name  TEXT                NOT NULL,
    email      TEXT                NOT NULL UNIQUE,

    -- this annotation will generate `userPhone` property for `phone` column, instead of `phone` name,
    -- in this case it's not very useful, but shows how to assign custom name to property
    --
    -- @@field=phone @@propertyName=userPhone
    phone      TEXT,

    -- this annotation will generate `birthDate` property for `birth_date` column
    -- and expects adapter to be provided for String <=> LocalDate conversion
    --
    -- @@field=birth_date @@propertyType=kotlinx.datetime.LocalDate @@adapter
    birth_date TEXT,

    -- this annotation will generate `createdAt` property for `created_at` column
    -- and will use adapter to convert between LocalDateTime and String
    --
    -- @@field=created_at @@propertyType=kotlinx.datetime.LocalDateTime @@adapter
    created_at TEXT                NOT NULL DEFAULT current_timestamp
);

CREATE INDEX idx_person_email ON Person (email);
```

When you run **generateSampleDatabase** task - it will generate `Person` data class
in `com.example.app.db` package. This class will be your main entry point for all queries
related to `Person` table. You are not ready to run **generateSampleDatabase** task yet,
because we don't have any queries yet.

## Create Your First Query

Each query should be in its own file and generated code will use file name as a query name (you 
can override the name with `-- @@name=YourName` annotation).
The file path will be used to determine the query namespace. For example,
`queries/person/selectAll.sql` will generate `selectAll` query in `person` namespace.
You can create namespace based on your needs, for example you can use namespace per table,
or if you have complex queries that span multiple tables you can group them in a separate namespace.

For our first query create `queries/person/selectAll.sql`:

```sql
SELECT * FROM Person
LIMIT :limit OFFSET :offset;
```

This generates a `selectAll` query that returns `List<Person>` with `limit` and `offset` parameters.

Create `queries/person/add.sql`:

```sql
INSERT INTO Person (email, first_name, last_name, phone, birth_date)
VALUES (:email, :firstName, :lastName, :phone, :birthDate);
```

This generates an `add` query for inserting new Person records.

## Generate Code

Run the Gradle plugin to generate your database code:

```bash
./gradlew :composeApp:generateSampleDatabase
```

(or `./gradlew build` if you are OK with building your entire codebase)

Generated code will be added to `build/generated/sqlitenow/code` directory (not to your source directory)
and will be available to your `commonMain` source set. 

## Use the Generated Code

### Initial setup for Android (optional step)

This step is needed only if you are using Android platform and if you want
to use `resolveDatabasePath` helper function (that resolves to platform-specific
database documents directory). Not needed for non-Android platforms and not
needed if you already have your own way to resolve database path.

```kotlin
class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        
        // Add this call to provide android context to SQLiteNow framework
        setupAndroidAppContext(this.applicationContext)

        // Rest of your initialization code...
    }
}
```

### Initialize the Database

```kotlin
// Initialize your database
// Normally you want to find a better place for this code, but for the sake of example it's here
val db = SampleDatabase(
    resolveDatabasePath("sample.db"),
    personAdapters = SampleDatabase.PersonAdapters(
        // serialize LocalDate to SQLite date string for `birth_date` column
        birthDateToSqlColumn = {
            it?.toSqliteDate()
        },
        // deserialize SQLite date string to LocalDate for `birth_date` column
        sqlColumnToBirthDate = {
            it?.let { LocalDate.fromSqliteDate(it) }
        },
        // deserialize SQLite timestamp string to LocalDateTime for `created_at` column
        sqlColumnToCreatedAt = {
            LocalDateTime.fromSqliteTimestamp(it)
        },
    ),
    migration = VersionBasedDatabaseMigrations()
)
```

### Query Data

```kotlin
LaunchedEffect(Unit) {
    // Open the database (you need to find a better place for this code, but for the sake of example it's here)
    db.open()
    
    // Query all persons
    val personList: List<Person.SelectAll.Result> = db.person
        .selectAll(
            Person.SelectAll.Params(
                limit = -1,
                offset = 0
            )
        )
        .asList()
}
```

### Insert Data

```kotlin
// Add a new person
db.person
    .add(
        Person.Add.Params(
            firstName = "John",
            lastName = "Doe",
            email = "john.doe@example.com",
            phone = "123-456-7890",
            birthDate = LocalDate(year = 1990, monthNumber = 1, dayOfMonth = 1),
        )
    )
    .execute()
```

### Reactive Queries

SQLiteNow supports reactive queries with Flow:

```kotlin
// Listen for changes to the Person table
db.person
    .selectAll(Person.SelectAll.Params(limit = -1, offset = 0))
    .asFlow()
    .collect { personList ->
        println("Person list: $personList")
    }
```

This will automatically re-execute the query whenever the `Person` table changes and emit the new result.

## Next Step

Check out our [Full Documentation]({{ site.baseurl }}/documentation/) guide to set up SQLiteNow in your project.
