# SQLiteNow

[![Kotlin Multiplatform](https://img.shields.io/badge/Kotlin-Multiplatform-blue?logo=kotlin)](https://kotlinlang.org/docs/multiplatform.html)
[![Maven Central](https://img.shields.io/maven-central/v/dev.goquick.sqlitenow/dev.goquick.sqlitenow.gradle.plugin?logo=apache-maven&label=Maven%20Central)](https://central.sonatype.com/artifact/dev.goquick.sqlitenow/dev.goquick.sqlitenow.gradle.plugin)
[![CI](https://img.shields.io/github/actions/workflow/status/mobiletoly/sqlitenow-kmp/gradle.yml?branch=main&logo=github&label=CI)](https://github.com/mobiletoly/sqlitenow-kmp/actions/workflows/gradle.yml)
[![License](https://img.shields.io/github/license/mobiletoly/sqlitenow-kmp?logo=apache&label=License)](LICENSE)

A Kotlin Multiplatform library for type-safe, SQLite database access, inspired by SQLDelight.
Unlike other popular frameworks (such as Room) - it is full SQL-first framework. Write your
queries in SQL files, get type-safe Kotlin code generated automatically. And no runtime annotations
overhead, everything is generated at compile time.

**Sync-Ready**: SQLiteNow includes a complete synchronization system for multi-device applications,
allowing seamless data sync across devices with conflict resolution and offline-first capabilities.

## Overview

SQLiteNow generates Kotlin code from your SQL files, giving you full control over your queries
while maintaining type safety. Unlike SQLDelight, which supports multiple database engines,
SQLiteNow is focused exclusively on SQLite, allowing for deeper integration and SQLite-specific
optimizations.

Full documentation is available in the https://mobiletoly.github.io/sqlitenow-kmp/ pages.

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

### Optional Multi-Device Synchronization

- **Built-in Sync System** - Complete synchronization solution for multi-device applications
- **Conflict Resolution** - Automatic conflict resolution with pluggable strategies (Server Wins,
  Client Wins, etc.)
- **Change Tracking** - Automatic tracking of INSERT, UPDATE, DELETE operations
- **Offline-First** - Works seamlessly offline, syncs when connection is available
- **JWT Authentication** - Secure sync with customizable authentication via HttpClient
- **Incremental Sync** - Efficient sync with pagination and change-based updates


## Components

Client-side framework components:
- **SQLiteNow Generator** - The code generation component of the SQLiteNow framework that
  generates type-safe Kotlin code from SQL files.
- **SQLiteNow Library** - The core library that provides convenient APIs for database access.
- **OverSqlite** - The sync component of the SQLiteNow framework that enables seamless data
  sharing across multiple devices with automatic conflict resolution, change tracking, and
  offline-first capabilities.

Server-side components:
- **OverSync** - Sync server that provides an adapter library for data synchronization.
  Currently we have **go-oversync** implementation in Go with PostgreSQL as data store.
  Visit this link for more information: https://github.com/mobiletoly/go-oversync

**It is important to mention** that you can use SQLiteNow Generator and SQLiteNow Library without
using OverSqlite for synchronization. And vice versa - you can use OverSqlite for synchronization
of SQLite database with PostgreSQL without using SQLiteNow Generator and SQLiteNow Library.


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
have not specified `sharedResult` annotation - SQLiteNow will automatically pick name
for you). This class has `addresses: List<Address>` property that contains all home
addresses for the person. Another class **PersonQuery.SelectAllWithAddresses.Params** will be
generated as well with `limit` and `offset` parameters to pass parameters to the query.
And yes, we support passing lists as parameters for `IN` clauses.

Ah, and you can define your own adapters to convert between SQLite and your domain types
and register them for seamless integration. We provide few built-in adapters as well,
such as converting from TEXT to Kotlin's date/time etc.

## How it works

### Code Generation
1. Write your SQL queries in `.sql` files
2. Add annotations using SQL comments to control code generation
3. Run the Gradle plugin to generate type-safe Kotlin code
4. Use the generated database classes in your application

Full examples is available in the [`/sample-kmp`](./sample-kmp) directory.

## Multi-Device Synchronization (optional)

SQLiteNow includes a complete synchronization system for building multi-device applications.
Simply annotate your tables with `enableSync=true` and the sync system handles the rest:

```sql
-- Enable sync for this table
-- @@{ enableSync=true }
CREATE TABLE person (
    id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);
```

Then use the generated sync client in your application:

```kotlin
// Create authenticated HTTP client with JWT token refresh and base URL
val httpClient = HttpClient {
    install(Auth) {
        bearer {
            loadTokens { /* load saved token */ }
            refreshTokens { /* refresh when expired */ }
        }
    }
    defaultRequest {
        url("https://api.myapp.com")
    }
}

// Create sync client
val syncClient = db.newOversqliteClient(
    schema = "myapp",
    httpClient = httpClient,
    resolver = ServerWinsResolver // or ClientWinsResolver
)

// Bootstrap new device
syncClient.bootstrap(userId = "user123", sourceId = "device456")

// Perform full sync (upload local changes, download remote changes)
val uploadResult = syncClient.uploadOnce()
val downloadResult = syncClient.downloadOnce(limit = 500)
```

The sync system automatically handles:
- **Change tracking** for all sync-enabled tables
- **Conflict resolution** when the same record is modified on multiple devices
- **Incremental sync** to minimize bandwidth usage
- **Error handling** and retry logic
- **Authentication** via customizable HttpClient

Full example is available in the [`/samplesync-kmp`](./samplesync-kmp) directory.

## Documentation

Full documentation is available in the https://mobiletoly.github.io/sqlitenow-kmp/
