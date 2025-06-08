---
layout: doc
title: Create Schema
permalink: /documentation/create-schema/
parent: Documentation
---

# Create Schema

Learn how to define your database schema using SQL files with SQLiteNow annotations.

## Directory Structure

SQLiteNow expects your SQL files to be organized in a specific directory structure within your `commonMain` source set:

```
src/commonMain/sql/SampleDatabase/
├── schema/          # CREATE TABLE, CREATE INDEX statements
├── queries/         # SELECT, INSERT, UPDATE, DELETE queries
├── init/           # Initial data (optional)
└── migration/      # Migration scripts (optional)
```

## Basic Table Definition

Create your table definitions in the `schema/` directory. By default, SQLiteNow converts column names to camelCase properties. You can customize property names using annotations:

**File: `schema/Person.sql`**

```sql
CREATE TABLE Person (
    id INTEGER PRIMARY KEY NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,

    -- @@field=phone @@propertyName=userPhone
    phone TEXT,

    created_at TEXT NOT NULL DEFAULT current_timestamp
);

CREATE INDEX idx_person_email ON Person (email);
```

This generates a `Person` data class with:
- `id: Long`
- `firstName: String`
- `lastName: String`
- `email: String`
- `userPhone: String?` (custom name via annotation)
- `createdAt: String`

## Custom Types with Adapters

For complex types that don't map directly to SQLite types, use the `@@adapter` annotation:

```sql
CREATE TABLE Person (
    id INTEGER PRIMARY KEY NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,

    -- @@field=birth_date @@adapter
    -- @@propertyType=kotlinx.datetime.LocalDate
    birth_date TEXT,

    -- @@field=created_at @@adapter
    -- @@propertyType=kotlinx.datetime.LocalDateTime
    created_at TEXT NOT NULL DEFAULT current_timestamp,
);
```

This generates a `Person` data class with custom types:
- `birthDate: kotlinx.datetime.LocalDate?`
- `createdAt: kotlinx.datetime.LocalDateTime`

## Nullability Control

```sql
CREATE TABLE Person (
    id INTEGER PRIMARY KEY NOT NULL,

    -- @@field=phone @@nullable
    phone TEXT NOT NULL,  -- Column is NOT NULL but property will be nullable

    -- @@field=bio @@nonNull
    bio TEXT              -- Column allows NULL but property will be non-null
);
```

## Available Annotations

| Annotation | Purpose | Example |
|------------|---------|---------|
| `@@field=column_name` | Target a specific column | `@@field=user_name` |
| `@@propertyName=name` | Custom property name | `@@propertyName=userName` |
| `@@propertyType=Type` | Custom property type | `@@propertyType=kotlinx.datetime.LocalDate` |
| `@@adapter` | Request type adapter generation | `@@adapter` |
| `@@nullable` | Make property nullable | `@@nullable` |
| `@@nonNull` | Make property non-null | `@@nonNull` |

## Complete Example

Here's a comprehensive example from the sample project:

```sql
CREATE TABLE Person (
    id INTEGER PRIMARY KEY NOT NULL,

    -- @@field=first_name @@propertyName=myFirstName
    first_name VARCHAR(255) NOT NULL,

    -- @@field=last_name @@propertyName=myLastName
    last_name TEXT NOT NULL,

    email TEXT NOT NULL UNIQUE,
    phone TEXT,

    -- @@field=birth_date @@adapter
    -- @@propertyType=kotlinx.datetime.LocalDate
    birth_date TEXT,

    -- @@field=created_at @@adapter
    -- @@propertyType=kotlinx.datetime.LocalDateTime
    created_at TEXT NOT NULL DEFAULT current_timestamp,
);

-- Indexes
CREATE INDEX idx_person_name ON Person (last_name, first_name);
CREATE INDEX idx_person_email ON Person (email);
```

This generates a `Person` data class with:
- `id: Long`
- `myFirstName: String` (custom property name)
- `myLastName: String` (custom property name)
- `email: String`
- `phone: String?`
- `birthDate: kotlinx.datetime.LocalDate?` (custom type with adapter)
- `createdAt: kotlinx.datetime.LocalDateTime` (custom type with adapter)

## Type Adapters

When you use `@@adapter`, SQLiteNow generates adapter data classes that you must provide when creating the database. Based on the example above, this generates:

```kotlin
public class SampleDatabase(
  dbName: String,
  migration: DatabaseMigrations,
  private val personAdapters: PersonAdapters,
  // ... other adapters
) : SqliteNowDatabase(dbName = dbName, migration = migration) {

  public data class PersonAdapters(
    public val sqlColumnToBirthDate: (String?) -> LocalDate?,
    public val birthDateToSqlColumn: (LocalDate?) -> String?,
    public val sqlColumnToCreatedAt: (String) -> LocalDateTime,
  )
}
```

You provide the adapter implementations when creating the database:

```kotlin
val db = SampleDatabase(
    dbName = "sample.db",
    migration = VersionBasedDatabaseMigrations(),
    personAdapters = SampleDatabase.PersonAdapters(
        sqlColumnToBirthDate = { it?.let { LocalDate.parse(it) } },
        birthDateToSqlColumn = { it?.toString() },
        sqlColumnToCreatedAt = { LocalDateTime.parse(it) }
    )
)
```

## Next Steps

Once you've defined your schema, learn how to [Query Data]({{ site.baseurl }}/documentation/query-data/) to interact with your tables.

## Related Topics

- [Getting Started]({{ site.baseurl }}/getting-started/) - Basic setup guide
- [How it Works]({{ site.baseurl }}/how-it-works/) - Understanding the code generation process
