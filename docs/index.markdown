---
layout: home
title: Home
---

A Kotlin Multiplatform library for type-safe, SQLite database access, inspired by SQLDelight.
Unlike other popular frameworks (such as Room) - it is full SQL-first framework. Write your
queries in SQL files, get type-safe Kotlin code generated automatically. And no runtime
annotations overhead, everything is generated at compile time.

## Overview

SQLiteNow generates Kotlin code from your SQL files, giving you full control over your queries while
maintaining type safety. Unlike SQLDelight, which supports multiple database engines, SQLiteNow is
focused exclusively on SQLite, allowing for deeper integration and SQLite-specific optimizations.

**Built for Multi-Device Apps**: SQLiteNow includes an optional synchronization system that handles
data sync between multiple devices, with automatic conflict resolution, change tracking, and
offline-first architecture. Perfect for mobile apps that need to work seamlessly across phones,
tablets, and other devices.

## Key Features

### Type-Safe SQL Generation
- **Pure SQL Control** - Write your queries in SQL files, get type-safe Kotlin code
- **Comment-based Annotations** - Control code generation using simple `-- @@{ annotations }`
  comments in your SQL.
- **No IDE Plugin Required** - Works with any editor, uses Gradle plugin for code generation
- **Kotlin Multiplatform** - Supports all KMP targets
  using [androidx.sqlite](https://developer.android.com/kotlin/multiplatform/sqlite) driver
- **SQLite Focused** - Optimized specifically for SQLite features and capabilities
- **Migration support** - Migration scripts are supported to manage database schema changes

### Multi-Device Synchronization
- **Built-in Sync System** - Complete synchronization solution for multi-device applications
- **Conflict Resolution** - Automatic conflict resolution with pluggable strategies (Server Wins, Client Wins, etc.)
- **Change Tracking** - Automatic tracking of INSERT, UPDATE, DELETE operations
- **Offline-First** - Works seamlessly offline, syncs when connection is available
- **JWT Authentication** - Secure sync with customizable authentication via HttpClient
- **Incremental Sync** - Efficient sync with pagination and change-based updates

(does not require to use Type-Safe SQL Generation, can be used with other SQLite libraries as well)

## Why SQLiteNow exists if SQLDelight is really awesome

Even if you don't care about multi-device synchronization, SQLiteNow has few key differences
from SQLDelight:

First of all, I wanted to target specifically SQLite in Kotlin Multiplatform environment, it is my
platform of choice as of now for mobile development. SQLiteNow built on top of using multiplatform
SQLite driver from AndroidX.

Second, since we use comment-based annotations, no plugin is required (like in case of SQLDelight),
so you can easily use just a regular SQL files that will be validated by your IDE or other external
tools for correctness.

Third, I wanted to have a more flexible and extensible code generation system that can be easily
extended and customized. I use hexagonal architecture in my code, but sometimes converting between
multiple layers is tiresome, so I wanted to have a way to generate code that will be very close
to my domain layer, without sacrificing the ability to write pure SQL queries.

Here is the brief example:

```sqlite
/* @@{ queryResult=Person,
       implements=com.example.app.PersonEssentialFields,
       excludeOverrideFields=[phone, birthDate] } */
SELECT id, first_name, last_name, email, phone, birth_date, created_at
FROM Person
```

This will generate shared data class **Person** that implements my custom
**PersonEssentialFields** interface but because this interface does not include `phone` and
`birthDate` fields - they will be excluded.

Note: `excludeOverrideFields` supports simple wildcard patterns. You can use globs like
`schedule__*` to exclude many fields at once. Patterns are matched against:
- the generated property name (e.g., `joinedScheduleActivityId`),
- the SQL column label/alias (e.g., `schedule__activity_id`), and
- the original column name.

Example:

```sqlite
-- @@{ queryResult=Row, implements=MyInterface,
--      excludeOverrideFields=[id, packageDocs, schedule__*] }
SELECT
  sch.activity_id AS schedule__activity_id,
  sch.mandatory_to_setup AS schedule__mandatory_to_setup,
  sch.repeat AS schedule__repeat
FROM schedule sch
```
All `schedule__*` columns (and their generated property names) are excluded from overrides.

Here is another one that you place in your **queries/person/selectAllWithAddresses.sql** file:

```sqlite
-- @@{ queryResult=PersonWithAddresses }
SELECT p.id,
       p.first_name,
       p.last_name,
       p.email,
       p.created_at,

       a.address_type,
       a.postal_code,
       a.country,
       a.street,
       a.city,
       a.state

/* @@{ dynamicField=addresses,
       mappingType=collection,
       propertyType=List<Address>,
       sourceTable=a,
       collectionKey=address_id } */

FROM Person p
         LEFT JOIN PersonAddress a ON p.id = a.person_id
ORDER BY p.id, a.address_type
LIMIT :limit OFFSET :offset
```

This will generate **PersonQuery.SelectAllWithAddresses.Result** data class (since you
have not specified `queryResult` annotation - SQLiteNow will automatically pick name
for you). This class has `addresses: List<Address>` property that contains all home
addresses for the person. Another class **PersonQuery.SelectAllWithAddresses.Params** will be
generated as well with `limit` and `offset` parameters to pass parameters to the query.
And yes, we support passing lists as parameters for `IN` clauses.

Ah, and you can define your own adapters to convert between SQLite and your domain types
and register them for seamless integration. We provide few built-in adapters as well,
such as converting from TEXT to Kotlin's date/time etc.

## How it works

1. Write your SQL queries in `.sql` files
2. Add annotations using SQL comments to control code generation
3. Run the Gradle plugin to generate type-safe Kotlin code
4. Use the generated database classes in your application

Full example is available in the [`/sample-kmp`](https://github.com/mobiletoly/sqlitenow-kmp/tree/main/sample-kmp) directory.

## Multi-Device Synchronization (optional)

SQLiteNow includes a complete synchronization system for building multi-device applications.
Simply annotate your tables with `enableSync=true` and the sync system handles the rest.

**Key Requirements:**
- **Primary keys must be TEXT type containing UUIDs** (not INTEGER)
- **Use `syncKeyColumnName` annotation** for custom primary key column names
- **JWT authentication** via customizable HttpClient

**The sync system automatically handles:**
- **Change tracking** for all sync-enabled tables
- **Conflict resolution** when the same record is modified on multiple devices
- **Incremental sync** to minimize bandwidth usage
- **Error handling** and retry logic
- **Authentication** via customizable HttpClient

Full sync example is available in the [`/samplesync-kmp`](https://github.com/mobiletoly/sqlitenow-kmp/tree/main/samplesync-kmp) directory.

## Next Steps

Ready to get started? Check out our guides:

- **[Getting Started]({{ site.baseurl }}/getting-started/)** - Set up SQLiteNow in your project
- **[Sync Getting Started]({{ site.baseurl }}/sync/getting-started/)** - Enable multi-device synchronization
- **[Full Documentation]({{ site.baseurl }}/documentation/)** - Complete API reference and advanced features
