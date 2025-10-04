---
layout: doc
title: Dynamic Fields
permalink: /documentation/dynamic-fields/
parent: Documentation
---

# Dynamic Fields

Dynamic fields let you reshape JOIN-heavy result sets into richer Kotlin models. By adding inline
`@@{ ... }` annotations you can embed nested objects, return grouped collections, or reuse existing
result classes without writing the plumbing by hand.

Every dynamic field annotation begins with `dynamicField=…` and specifies a `mappingType`:

- **`perRow`** – attach a nested object to each row while keeping the overall result flat.
- **`collection`** – group child rows into a collection that hangs off the parent entity.
- **`entity`** – embed another generated result class (often the "main" entity from a view) as a
  nested property.

You can combine multiple dynamic fields inside one query; the generator takes care of constructor
calls, grouping, and default handling.

<br/>

-----
## Mapping Type: `perRow`

`perRow` is a good fit for optional 1:1 joins. The generated result still returns one row per JOIN
match, but each row carries an embedded object.

Example taken from `LibraryTestDatabase/queries/person/selectWithPerRowMapping.sql`:

```sql
SELECT
    p.id,
    p.first_name,
    p.last_name,
    p.email,
    p.phone,
    p.birth_date,
    p.created_at,

    a.id          AS address__id,
    a.person_id   AS address__person_id,
    a.address_type AS address__address_type,
    a.street      AS address__street,
    a.city        AS address__city,
    a.state       AS address__state,
    a.postal_code AS address__postal_code,
    a.country     AS address__country,
    a.is_primary  AS address__is_primary,
    a.created_at  AS address__created_at

/* @@{ dynamicField=address,
       mappingType=perRow,
       propertyType=PersonAddressRow,
       sourceTable=a,
       aliasPrefix=address__ } */

FROM person p
LEFT JOIN person_address a ON p.id = a.person_id
WHERE p.id = :personId;
```

The generated Kotlin data class is:

```kotlin
public data class PersonSelectWithPerRowMappingResult(
    val id: Long,
    val myFirstName: String,
    val myLastName: String,
    val email: String,
    val phone: String?,
    val birthDate: LocalDate?,
    val createdAt: LocalDateTime,
    val address: PersonAddressRow?,
)
```

The `aliasPrefix` strips `address__` before mapping the columns to `PersonAddressRow`. Because the
join is optional, the generator keeps the nested object nullable.

<br/>

-----
## Mapping Type: `collection`

Use `collection` when you want a single row per parent entity with a grouped collection of child
records. Both statement-level and field-level `collectionKey` values are required so the generator
knows how to bucket the rows.

Example from `LibraryTestDatabase/queries/person/selectAllWithAddresses.sql`:

```sql
/* @@{ queryResult=PersonWithAddressRow, collectionKey=person_id } */
SELECT
    p.id         AS person_id,
    p.first_name,
    p.last_name,
    p.email,
    p.phone,
    p.birth_date,
    p.created_at,

    a.id         AS address__id,
    a.person_id  AS address__person_id,
    a.address_type AS address__address_type,
    a.street     AS address__street,
    a.city       AS address__city,
    a.state      AS address__state,
    a.postal_code AS address__postal_code,
    a.country    AS address__country,
    a.is_primary AS address__is_primary,
    a.created_at AS address__created_at

/* @@{ dynamicField=addresses,
       mappingType=collection,
       propertyType=List<PersonAddressRow>,
       sourceTable=a,
       collectionKey=address__id,
       aliasPrefix=address__,
       notNull=true,
       defaultValue=listOf() } */

FROM person p
LEFT JOIN person_address a ON p.id = a.person_id
ORDER BY p.id
LIMIT :limit OFFSET :offset;
```

The flattened JOIN rows are grouped by `collectionKey=person_id`, then mapped to
`PersonWithAddressRow`, whose `addresses` property is a `List<PersonAddressRow>`. Because we supplied
`defaultValue=listOf()` and `notNull=true`, the generated mapper emits an empty list when no matching
addresses exist.

You can add additional dynamic fields alongside the collection (for example, the real query also adds
`comments` as another collection).

<br/>

-----
## Mapping Type: `entity`

`entity` reuses an existing result data class and embeds it as a nested property. This is handy when
another query or view already defines the "main" entity shape and you want to compose a richer
projection around it.

Example from `LibraryTestDatabase/queries/person/selectWithEntityMapping.sql`:

```sql
SELECT
    p.id,
    p.first_name,
    p.last_name,
    p.email,
    p.phone,
    p.birth_date,
    p.created_at,

    p.id         AS person_details__id,
    p.first_name AS person_details__first_name,
    p.last_name  AS person_details__last_name,
    p.email      AS person_details__email,
    p.phone      AS person_details__phone,
    p.birth_date AS person_details__birth_date,
    p.created_at AS person_details__created_at

/* @@{ dynamicField=personDetails,
       mappingType=entity,
       propertyType=PersonRow,
       sourceTable=p,
       aliasPrefix=person_details__,
       notNull=true } */

FROM person p
WHERE p.id = :personId;
```

The generated Kotlin type is simply:

```kotlin
public data class PersonSelectWithEntityMappingResult(
    val personDetails: PersonRow,
)
```

Because `PersonRow` is already generated for other queries, the entity mapping just passes the aliased
columns through to that constructor.

<br/>

-----
## Additional Options

Dynamic fields share a few helper properties regardless of the mapping type:

- **`notNull=true`** — declare that the nested value is required. Only use this when the SQL guarantees
  the join will produce a row.
- **`defaultValue=`** — supply a Kotlin expression used when no rows are found (commonly `listOf()` for
  collections).
- **`sourceTable=`** — the table alias contributing columns to the dynamic field.
- **`aliasPrefix=`** — optional prefix stripped from aliased column names before matching them to
  properties.

Mixing dynamic fields is fully supported: you can embed an `entity` for the main object, attach a few
`perRow` dependents, and add `collection` statements for repeated children. SQLiteNow stitches the
pieces together so your Kotlin models stay expressive without manual mapping code.
