---
layout: doc
title: Create Schema
permalink: /documentation/create-schema/
parent: Documentation
---

# Create Schema

Learn how to define your database schema using SQL files with SQLiteNow annotations.

## Directory Structure

SQLiteNow expects your SQL files to be organized in a specific directory structure within
your `commonMain` source set:

```
src/commonMain/sql/SampleDatabase/
├── schema/          # CREATE TABLE, CREATE INDEX statements
├── queries/         # SELECT, INSERT, UPDATE, DELETE queries
├── init/            # Initial data (optional)
└── migration/       # Migration scripts (optional)
```

## Basic Table Definition

Create your table definitions in the `schema/` directory. By default, SQLiteNow converts column
names to camelCase properties. You can customize property names using annotations:

**File: `schema/Person.sql`**

```sql
CREATE TABLE person
(
    id         INTEGER PRIMARY KEY NOT NULL,
    first_name TEXT                NOT NULL,
    last_name  TEXT                NOT NULL,
    email      TEXT                NOT NULL UNIQUE,

    -- @@{ field=phone, propertyName=userPhone }
    phone      TEXT,

    created_at TEXT                NOT NULL DEFAULT current_timestamp
);

CREATE INDEX idx_person_email ON Person (email);
```

While this construction will not generate any Kotlin data structures, it is still required to be
present, because it is used to determine the schema of the database. Data classes will be
generated later,  based on the SELECT, INSERT, UPDATE, DELETE queries.

As for now it is important to understand that listed columns are associated with
the following properties:

- `id: Long`
- `firstName: String`
- `lastName: String`
- `email: String`
- `userPhone: String?` (custom name via annotation)
- `createdAt: String`

**Note: In current version when working with field-level annotations (such as `@@{ field=..., propertyName=... }` etc),
you must always use `field` annotation to target specific column. It is a good idea to keep field-level annotations
as close to the column definition as possible, but it is not required to do so.**

## Custom Types with Adapters

For complex types that don't map directly to SQLite types, use the combination of `propertyType`
and `adapter` annotations:

```sql
CREATE TABLE person
(
    id         INTEGER PRIMARY KEY NOT NULL,
    first_name TEXT                NOT NULL,
    last_name  TEXT                NOT NULL,
    email      TEXT                NOT NULL UNIQUE,

    -- @@{ field=birth_date, adapter=custom, propertyType=kotlinx.datetime.LocalDate }
    birth_date TEXT,

    -- @@{ field=created_at, adapter=custom, propertyType=kotlinx.datetime.LocalDateTime }
    created_at TEXT                NOT NULL DEFAULT current_timestamp,
    
    -- @@{ field=is_active, adapter=default, propertyType=Boolean }
    is_active INTEGER NOT NULL DEFAULT 1
);
```

This will result in the following properties:

- `birthDate: kotlinx.datetime.LocalDate?`
- `createdAt: kotlinx.datetime.LocalDateTime`

There are two values can be assigned to `adapter` annotation: `default` and `custom`:
- `default` value means that developer will be required to provide conversion adapter if `propertyType` is
  custom type and will not be required for built-in types (such as `String`, `Int`, `Long`, etc.), because
  SQLiteNow can handle some of the built-in types out of the box.
- `custom` value means that adapter will be required to provide regardless of `propertyType` value.

**Note: If you specify propertyType, in most cases you don't need to specify `adapter` annotation,
because SQLiteNow will try to automatically determine if adapter is required or not, based on the
type of `propertyType`.**

## Forced Nullability Control

With database schema, you can define columns as `NOT NULL` or `NULL`. However, in some cases you
may want to override this behavior for specific properties. You can use `notNull` annotation to
do so:

```sql
CREATE TABLE person
(
    id    INTEGER PRIMARY KEY NOT NULL,

    -- Column is NOT NULL but generated property will be nullable
    -- @@{ field=phone, notNull=false }
    phone TEXT                NOT NULL,

    -- Column allows NULL but property will be non-null
    -- @@{ field=bio, notNull=true }
    bio   TEXT
);
```

It can be useful when you want to make property nullable despite of `NOT NULL` constraint in database,
or vice versa. Also it can be useful if SQLiteNow is not able to guess the nullability of the property
correctly, for example in SELECT statement in the code such as:

```sql
SELECT 
    COUNT(*) AS total_person_count,
    *
FROM Person;
```

`total_person_count` column is not defined in the table and by default SQLiteNow will
treat it as nullable. In this case you can use `notNull` annotation to override this behavior:

```sql
SELECT 
    -- @@{ field=total_person_count, notNull=true }
    COUNT(*) AS total_person_count,
    *
FROM Person;
```

It will result in `totalPersonCount: Long` property that is guaranteed to be non-null.

## Statement-level Annotations

Statement-level annotations can be used to customize the generated code for specific statements.
For example, you can use `name` annotation to customize the generated class name or change
`propertyNameGenerator` to change the generated property name format.


| Annotation                        | Purpose                                   | Example                                        |
|-----------------------------------|-------------------------------------------|------------------------------------------------|
| `name=Name`                       | Custom class name                         | `name=PersonWithAddressEntity`                 |
| `propertyNameGenerator=generator` | Change property name format               | `propertyNameGenerator=lowerCamelCase`         |
| `sharedResult=Name`               | Class name reused across multiple queires | `sharedResult=PersonEntity`                    |
| `implements=Interface`            | Implement interface                       | `implements=PersonEssentialFields`             |
| `excludeOverrideFields=fields`    | Exclude fields from override              | `excludeOverrideFields=['phone', 'birthDate']` |

## Field-level Annotations

| Annotation                    | Purpose                           | Example                                              |
|-------------------------------|-----------------------------------|------------------------------------------------------|
| `name=Name`                   | Custom class name                 | `name=PersonEntity`                                  |
| `field=sql_column_name`       | Target a specific column          | `field=user_name`                                    |
| `propertyName=name`           | Custom generated property name    | `field=user_name, propertyName=myUserName`           |
| `propertyType=type`           | Custom property type              | `field=birth_date, propertyType=LocalDate`           |
| `adapter={custom or default}` | Request type adapter generation   | `field=birth_date, adapter=custom`                   |
| `notNull={true or false}`     | Control property nullability      | `field=phone, notNull=false`                         |
| `dynamicField=name`           | Generate non-table dynamic field  | `dynamicField=addresses, propertyType=List<String>`  |
| `defaultValue=value`          | Default value for dynamic field   | `dynamicField=addresses, defaultValue=listOf()`   |

## Example

Here's a more complete example:

```sql
CREATE TABLE person
(
    id         INTEGER PRIMARY KEY NOT NULL,

    /* block-level comments are supported:
       @@{ field=first_name,
           propertyName=myFirstName } */
    first_name TEXT                NOT NULL,

    -- @@{ field=last_name, propertyName=myLastName }
    last_name  TEXT                NOT NULL,

    email      TEXT                NOT NULL UNIQUE,
  
    -- @@{ field=phone, adapter=custom, notNull=true }
    phone      TEXT,

    -- Multi-line comments are supported:
    -- @@{ field=birth_date,
    --     propertyType=kotlinx.datetime.LocalDate }
    birth_date TEXT,

    -- @@{ field=created_at, propertyType=kotlinx.datetime.LocalDateTime }
    created_at TEXT                NOT NULL DEFAULT current_timestamp
);

-- Indexes
CREATE INDEX idx_person_name ON Person (last_name, first_name);
CREATE INDEX idx_person_email ON Person (email);
```

This will result in the following properties:

- `id: Long`
- `myFirstName: String` (custom property name)
- `myLastName: String` (custom property name)
- `email: String`
- `phone: String?` (custom adapter will be requested from a developer, can be useful
  to convert phone number format, validate phone number and throw exception if invalid,
  etc.)
- `birthDate: kotlinx.datetime.LocalDate?` (custom type with adapter)
- `createdAt: kotlinx.datetime.LocalDateTime` (custom type with adapter)

## Type Adapters

When you use `adapter=custom` annotation (or when you use custom type without specifying adapter),
SQLiteNow generates adapter data classes that you must provide when creating the database.
Based on the example above, this generates:

```kotlin
class SampleDatabase(
    dbName: String,
    migration: DatabaseMigrations,
    private val personAdapters: PersonAdapters,
    // ... other adapters if needed
) : SqliteNowDatabase(dbName = dbName, migration = migration) {

    data class PersonAdapters(
        val sqlColumnToBirthDate: (String?) -> LocalDate?,
        val birthDateToSqlColumn: (LocalDate?) -> String?,
        val sqlColumnToCreatedAt: (String) -> LocalDateTime,
        val phoneToSqlColumn: (String) -> String
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
        sqlColumnToCreatedAt = { LocalDateTime.parse(it) },
        phoneToSqlColumn = { it }
    )
)
```

Code generator scans through all CREATE TABLE/VIEW definitions and all SELECT queries within
a specific namespace (e.g. Person) and generate adapter parameters that developer must to provide
for all columns that have `adapter=custom` annotation. Some adapter parameters may be merged if they have
the identical function signature (to make adapters list less verbose).

## Next Steps

[Query Data]({{ site.baseurl }}/documentation/query-data/) - Once you've defined your schema, learn how
to interact with your tables.
