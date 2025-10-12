---
layout: doc
title: Manage Data
permalink: /documentation/manage-data/
parent: Documentation
---

# Manage Data

Learn how to INSERT, UPDATE, and DELETE data in your database using SQLiteNow's generated code.

## Query Organization

Data modification queries are organized in the `queries/` directory alongside SELECT queries:

```
src/commonMain/sql/SampleDatabase/queries/
├── person/
│   ├── selectAll.sql
│   ├── add.sql              # INSERT
│   ├── updateEmail.sql      # UPDATE
│   └── deleteByIds.sql      # DELETE
```

## INSERT Queries

### Basic INSERT

**File: `queries/person/add.sql`**

```sql
INSERT INTO Person (first_name, last_name, email, phone, birth_date)
VALUES (:firstName, :lastName, :email, :phone, :birthDate);
```

This generates a query object with input parameters:

```kotlin
object PersonQuery {
    object Add {
        data class Params(
            val firstName: String,
            val lastName: String,
            val email: String,
            val phone: String?,
            val birthDate: kotlinx.datetime.LocalDate?
        )

        // ...
    }
}
```

As you can see Params class is generated automatically based on the SQL query parameters
and reside under `PersonQuery/Add` object (object name is generated from file name).


## UPDATE Queries

**File: `queries/person/updateEmail.sql`**

```sql
UPDATE Person
SET email = :email
WHERE id = :id;
```

This generates:

```kotlin
object PersonQuery {
    object UpdateEmail {
        data class Params(
            val email: String,
            val id: Long
        )

        // ...
    }
}
```

As you can see Params class is generated automatically based on the SQL query parameters
and reside under `PersonQuery/UpdateEmail` object.

## DELETE Queries

**File: `queries/person/deleteByIds.sql`**

```sql
DELETE FROM Person
WHERE id IN :ids;
```

This generates:

```kotlin
object PersonQuery {
    object DeleteByIds {
        data class Params(
            val ids: Collection<Long>
        )

        // ...
    }
}
```

As you can see Params class is generated automatically based on the SQL query parameters.
SQLiteNow automatically handles Collection parameters for IN clauses.

## Executing Modifications

All generated execute statements run immediately when you call them — no extra `execute()` step required.

```kotlin
// Insert a new person
db.person.add(
    PersonQuery.Add.Params(
        firstName = "John",
        lastName = "Doe",
        email = "john@example.com",
        phone = "+1234567890",
        birthDate = LocalDate(1990, 1, 1),
    )
)
```

```kotlin
// Update an email address
db.person.updateEmail(
    PersonQuery.UpdateEmail.Params(
        id = 1,
        email = "newemail@example.com",
    )
)
```

```kotlin
// Delete multiple people by ID
db.person.deleteByIds(
    PersonQuery.DeleteByIds.Params(
        ids = listOf(1L, 2L, 3L),
    )
)
```

Returning statements expose `list`, `one`, and `oneOrNull` helpers:

```kotlin
val insertParams = PersonQuery.AddReturning.Params(
    firstName = "Alice",
    lastName = "Smith",
    email = "alice@example.com",
    phone = null,
    birthDate = LocalDate(1992, 5, 15),
)

val inserted = db.person.add.one(insertParams)
val allRows = db.person.add(insertParams)       // operator fun invoke -> List
val maybeRow = db.person.add.oneOrNull(insertParams)
```

## Next Steps

[Initialize Data]({{ site.baseurl }}/documentation/initialize-data/) - Learn about provisioning initial data
