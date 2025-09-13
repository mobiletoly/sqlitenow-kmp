---
layout: doc
title: Collection Mapping
permalink: /documentation/collection/
parent: Documentation
---

# Collection Mapping

Learn how to map JOIN query results to nested data structures using `mappingType=perRow` and
`mappingType=collection` annotations.

## Overview

SQLiteNow supports advanced mapping of JOIN query results into nested data structures. Instead of
flat result objects, you can create rich domain models with embedded objects and collections.

Two mapping types are available:

- **`perRow`**: Maps joined data to nested objects, returning one row per JOIN result (flat
  structure). This is not really a collection mapping, but it is useful when you want to map
  joined data to nested objects.
- **`collection`**: Groups joined data into collections, returning one row per main entity with
  nested collections

<br>

-----
## Flat Structure Mapping with 'perRow'

Use `mappingType=perRow` to map joined table data into nested objects while maintaining a flat row
structure. Each JOIN result becomes a separate row in your result set.

### Example: Person with Addresses (Flat Structure)

**File: `queries/person/selectAllWithAddresses.sql`**

```sql
SELECT p.id,
       p.first_name,
       p.last_name,

       a.id AS address_id,
       a.street,
       a.city

/* @@{ dynamicField=address,
       mappingType=perRow,
       propertyType=my.app.Address,
       sourceTable=a
       removeAliasPrefix=address_
   } */

FROM Person p
         LEFT JOIN person_address a ON p.id = a.person_id
```

This generates:

```kotlin
object PersonQuery {
    object SelectAllWithAddresses {
        data class Result(
            val id: Long,
            val firstName: String,
            val lastName: String,

            // This is result of using annotations: dynamicField=address, sourceTable=a
            // All columns with alias 'a.*' are mapped to this object
            // This field can be null if there is no matching address
            val address: my.app.Address?
        )
    }
}
```

**Result structure**: If a table has 1 person with 2 addresses, and 1 person with no addresses,
you get 3 rows:

```
Row 1: person_1 | address = {... address_1}
Row 2: person_1 | address = {... address_2}
Row 3: person_2 | address = null
```

Each row contains the complete person data plus one address nested object. 
Multiple `perRow` mappings are possible in a single query.

**Note** that `mappingType=perRow` (or `collection`) does not create data structure for
`my.app.Address`, this data structure must already exist in your codebase and have all required
properties. If you don't want to create this data structure manually, you can always
create SQL SELECT query that selects all required fields for `Address` and use `sharedResult`
annotation to generate it automatically. 

Check [Collection Recipes]({{ site.baseurl }}/documentation/collection-recipes/) for more details.

### Key Annotations for `perRow`

- **`dynamicField`**: Name of the property in the result class


- **`mappingType=perRow`**: Indicates flat row structure with nested objects


- **`propertyType`**: Type of the nested object (`my.app.Address`)


- **`sourceTable`**: Table alias for the joined table (`a` for `person_address` table)


- **`removeAliasPrefix`**: Prefix to remove from column names (optional). In the example above,
  `removeAliasPrefix=address_` will remove `address_` prefix from all column name aliases, so
  `address_id` will be mapped to `id` property of `Address` object.


- **`notNull=true`**: Control nullability of the nested object (optional). With this you can make
  the nested object non-nullable, but use it only if you are sure that the joined table will
  always have a value for the selected rows.

<br>

------
## Grouped Collection Mapping with 'collection'

Use `mappingType=collection` to group multiple joined records into collections within your result.
This returns one row per main entity with all related data grouped into collections.

### Example: Person with Grouped Addresses

**File: `queries/person/selectWithGroupedAddresses.sql`**

```sql
-- @@{ collectionKey=person_id }
SELECT p.id AS person_id,
       p.first_name,
       p.last_name,

       a.id AS address_id,
       a.street,
       a.city

/* @@{ dynamicField=addresses,
       mappingType=collection,
       propertyType=List<my.app.Address>,
       sourceTable=a,
       collectionKey=address_id
       removeAliasPrefix=address_ 
       notNull=true } */

FROM Person p
         LEFT JOIN person_address a ON p.id = a.person_id
```

This generates:

```kotlin
object PersonQuery {
    object SelectWithGroupedAddresses {
        data class Params(
            val personId: Long
        )

        data class Result(
            val personId: Long,
            val firstName: String,
            val lastName: String,
            val addresses: List<Address>  // Collection from JOIN
        )
    }
}
```

**Result structure**: If a person has 2 addresses, you get 1 result:

```
Row 1: person_1 | addresses = [aaddress_1, address_2]
```

The person data appears once with all addresses grouped into a collection.

### Key Annotations for `collection`

- **Statement-level `collectionKey`**: Unique identifier for the main table (required for grouping)
- **Field-level `collectionKey`**: Unique identifier for the collection items
- **`mappingType=collection`**: Indicates grouped collection structure
- **`propertyType`**: Collection type (e.g., `List<Address>`)

<br>

-----
## Key Differences: `perRow` vs `collection`

While in many cases `perRow` is not as useful as `collection` mapping, it can be useful in some
cases, for example when you deal with 1:1 relationships. Or when `collection` mapping is not
flexible enough for what you need, and you want to perform more complex grouping, filtering
and other transformations.

Understanding when to use each mapping type:

### `perRow` - Flat Structure

- **Use case**: When you want each JOIN result as a separate row
- **Result**: Multiple rows, one per JOIN match
- **Example**: Person with 3 addresses = 3 result rows
- **Good for**: Pagination, filtering individual relationships, simple processing

### `collection` - Grouped Structure

- **Use case**: When you want related data grouped together
- **Result**: One row per main entity with nested collections
- **Example**: Person with 3 addresses = 1 result row with List<Address>
- **Good for**: Complete entity loading, reducing data transfer, complex domain models

<br>

-----
## Collection Key Formats

The `collectionKey` annotation supports two formats:

### 1. Alias.Column Format

You can specify the collection key using the table alias and column name, separated by a dot.

```sql
/* @@{ collectionKey=id } */
SELECT p.id,
       p.name,
       a.id AS address_id
FROM Person p
         LEFT JOIN Address a ON p.id = a.person_id
```

The `id` referred to `p.id` because collectionKey will be searching for `id` in `p` table.

### 2. Aliased Column Format

```sql
/* @@{ collectionKey=person_id } */
SELECT 
    p.id AS person_id,
    p.name,
    a.id AS address_id
FROM Person p
         LEFT JOIN Address a ON p.id = a.person_id
```

<br>

-----
## Best Practices

1. **Use table aliases**: Always use table aliases in SELECT statements for clarity
2. **Consistent naming**: Use consistent naming patterns for collection keys
3. **Limit collections**: Be mindful of performance with large collections
4. **NULL handling**: Consider LEFT JOIN nullability in your domain logic
5. **Index optimization**: Ensure proper indexes on JOIN columns for performance
