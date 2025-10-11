---
layout: doc
title: Query Data
permalink: /documentation/query-data/
parent: Documentation
---

# Query Data

Learn how to write SELECT queries to retrieve data from your database as lists, single items,
or reactive flows.

## Query Organization

SELECT queries are organized in the `queries/` directory using namespaces (subdirectories).
Typically, you'll create one namespace per table, but you can organize them however makes
sense for your application.

```
src/commonMain/sql/SampleDatabase/queries/
├── person/
│   ├── selectAll.sql
│   ├── selectById.sql
```

## Basic SELECT Queries

### Simple SELECT ALL

**File: `queries/person/selectAll.sql`**

```sql
SELECT * FROM Person
LIMIT :limit OFFSET :offset;
```

This generates a complete query object with query input parameters and output result:

```kotlin
object PersonQuery {
    object SelectAll {
        data class Params(
            val limit: Long,
            val offset: Long
        )

        data class Result(
            val id: Long,
            val firstName: String,
            val lastName: String,
            val email: String,
            val userPhone: String?,
            val birthDate: kotlinx.datetime.LocalDate?,
            val createdAt: kotlinx.datetime.LocalDateTime,
        )
        
        // ...
    }
}
```

As you can see both Params and Result classes are generated automatically based on the SQL query
and reside under `PersonQuery/SelectAll` object (object name is generated from file name).

### SELECT by ID

**File: `queries/person/selectById.sql`**

```sql
SELECT * FROM Person
WHERE id = :id;
```

This generates:

```kotlin
object PersonQuery {
    object SelectById {
        data class Params(
            val id: Long
        )

        data class Result(
            val id: Long,
            val firstName: String,
            val lastName: String,
            val email: String,
            val userPhone: String?,
            val birthDate: kotlinx.datetime.LocalDate?,
            val createdAt: kotlinx.datetime.LocalDateTime,
        )
        
        // ...
    }
}
```

As you can see both Params and Result classes are generated automatically based on the SQL query
and reside under `PersonQuery/SelectAll` object.

## Execution Methods

SQLiteNow generates four different execution methods for SELECT queries:

### asList()
Returns all matching rows as a List. Best for queries that return multiple rows:

```kotlin
val persons: List<PersonQuery.SelectAll.Result> = db.person
    .selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
    .asList()
```

### asOne()
Returns exactly one row. Throws an exception if zero or multiple rows are found:

```kotlin
val person: PersonQuery.SelectById.Result = db.person
    .selectById(PersonQuery.SelectById.Params(id = 1))
    .asOne()
```

### asOneOrNull()
Returns one row or null if no rows are found. Throws an exception if multiple rows are found:

```kotlin
val person: PersonQuery.SelectById.Result? = db.person
    .selectById(PersonQuery.SelectById.Params(id = 1))
    .asOneOrNull()
```

### asFlow()
Returns a reactive Flow that re-executes the query when relevant tables change:

```kotlin
db.person
    .selectAll(PersonQuery.SelectAll.Params(limit = -1, offset = 0))
    .asFlow()
    .collect { persons ->
        println("Persons updated: ${persons.size}")
        updateUI(persons)
    }
```

The Flow automatically re-executes the query when any table used in the query is
modified by INSERT, UPDATE, or DELETE operations.


## Query Annotations

You can use annotations in SELECT queries to customize the generated code and override
schema-level annotations:

### Custom Class Names

Assuming that you have file `queries/person/selectSummary.sql`:

```sql
-- @@{ class=PersonSummary }
SELECT id, first_name, last_name FROM Person;
```

This generates `PersonQuery/PersonSummary` object instead of `PersonQuery/SelectSummary`.

### Overriding Schema Annotations

Query annotations can override schema-level annotations for specific queries:

```sql
SELECT 
    id, 
    first_name,
    last_name,
    -- @@{ field=phone, propertyName=contactPhone }
    phone
FROM Person;
```

This generates `contactPhone` property instead of `userPhone` defined via annotation in the schema.
You can override any schema-level annotation in a query (including property name, property type,
nullability etc). It means that **SELECT** annotations have higher priorities than annotations
in **CREATE TABLE**.


### Reusing Result Classes

You can point multiple queries at the same generated data class by giving them the same
`queryResult` name. The generator now creates that data class once at the namespace level and
reuses it everywhere, rather than nesting it under a `SharedResult` container like earlier
iterations of the plugin.

For example, consider two queries:

**File: `queries/person/selectActive.sql`**

```sql
-- @@{ queryResult=Row }
SELECT * FROM Person WHERE active = 1;
```

**File: `queries/person/selectNew.sql`**

```sql
-- @@{ queryResult=Row }
SELECT * FROM Person WHERE created_at > :since;
```

Both files generate a single Kotlin data class named `PersonRow` (emitted to
`dev.goquick.sqlitenow.librarytest.db.PersonRow`). Each query extension function then returns or
maps from that shared class. If you also supply `mapTo=…`, the generated reader accepts a mapper
lambda so both queries can project the shared `PersonRow` into whatever domain type you need.

## Collection Parameters

SqliteNow supports `IN` clauses with Collection parameters.

```sql
SELECT * FROM Person 
WHERE id IN :ids;
```

It generates:
```kotlin
data class Params(
    val ids: Collection<Long>
)
```

And can be used as:

```kotlin
val personList: List<PersonQuery.SelectAll.Result> = db.person
    .selectAll(PersonQuery.SelectAll.Params(ids = listOf(1L, 2L, 3L)))
    .asList()
```


### Helping the Generator with `sqlTypeHint`

SQLite exposes reliable column metadata only for fields that map directly to table columns. When
a SELECT result comes from an expression—such as `CAST(...)`, a `GROUP_CONCAT`, or a nested view
column—the JDBC driver often reports the type as `NUMERIC` and marks it nullable even if the SQL
clearly returns text or a non-null value. The generator uses that metadata to choose statement
getters (`getLong`, `getText`, …) and to decide whether adapters receive nullable values. To keep
the emitted code aligned with your SQL, you can override the inferred type with `sqlTypeHint` and,
optionally, `notNull`:

```sql
SELECT
    -- @@{ field=address_summary, sqlTypeHint=TEXT, notNull=true,
    --     propertyType=kotlin.String }
    (
        SELECT GROUP_CONCAT(pa.street || ' ' || pa.city, ', ')
        FROM person_address pa
        WHERE pa.person_id = p.id
    ) AS address_summary
FROM Person p;
```

This forces the generator to treat `address_summary` as a non-null TEXT column, producing
`statement.getText(...)` and a `(String) -> String` adapter instead of the default
`statement.getLong(...)`/`String?`. Use `sqlTypeHint` whenever computed columns (or view fields
through `SELECT *`) show up with the wrong getter or nullability in the generated sources.


## Next Steps

[Manage Data]({{ site.baseurl }}/documentation/manage-data/) - Learn about INSERT, UPDATE, DELETE operations
