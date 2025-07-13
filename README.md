# SQLiteNow

[![Kotlin Multiplatform](https://img.shields.io/badge/Kotlin-Multiplatform-blue?logo=kotlin)](https://kotlinlang.org/docs/multiplatform.html)
[![Maven Central](https://img.shields.io/maven-central/v/dev.goquick.sqlitenow/dev.goquick.sqlitenow.gradle.plugin?logo=apache-maven&label=Maven%20Central)](https://central.sonatype.com/artifact/dev.goquick.sqlitenow/dev.goquick.sqlitenow.gradle.plugin)
[![CI](https://img.shields.io/github/actions/workflow/status/mobiletoly/sqlitenow-kmp/gradle.yml?branch=main&logo=github&label=CI)](https://github.com/mobiletoly/sqlitenow-kmp/actions/workflows/gradle.yml)
[![License](https://img.shields.io/github/license/mobiletoly/sqlitenow-kmp?logo=apache&label=License)](LICENSE)

A Kotlin Multiplatform library for type-safe, SQLite database access,
inspired by SQLDelight. Unlike other popular frameworks (such as Room) - it is full SQL-first
framework. Write your queries in SQL files, get type-safe Kotlin code generated automatically.
And no runtime annotations overhead, everything is generated at compile time.

Full documentation is available in the https://mobiletoly.github.io/sqlitenow-kmp/ pages.

## Overview

SQLiteNow generates Kotlin code from your SQL files, giving you full control over your queries while
maintaining type safety. Unlike SQLDelight, which supports multiple database engines, SQLiteNow is
focused exclusively on SQLite, allowing for deeper integration and SQLite-specific optimizations.

## Key Features

- **Pure SQL Control** - Write your queries in SQL files, get type-safe Kotlin code
- **Comment-based Annotations** - Control code generation using simple `-- @@{ annotations }`
  comments in your SQL.
- **No IDE Plugin Required** - Works with any editor, uses Gradle plugin for code generation
- **Kotlin Multiplatform** - Supports all KMP targets
  using [androidx.sqlite](https://developer.android.com/kotlin/multiplatform/sqlite) driver
- **SQLite Focused** - Optimized specifically for SQLite features and capabilities
- **Migration support** - Migration scripts are supported to manage database schema changes

## Why SQLiteNow exists if SQLDelight is really awesome

First of all, I wanted to target specifically SQLite in Kotlin Multiplatform environment, it is my
platform of choice as of now for mobile development. SQLiteNow built on top of using multiplatform
SQLite driver from AndroidX.

Second, since we use comment-based annotations (and I'm planning to add user-defined annotations in
the future), no plugin is required (like in case of SQLDelight), so you can easily use just a
regular SQL files that will be validated by your IDE or other external tools for correctness.

Third, I wanted to have a more flexible and extensible code generation system that can be easily
extended and customized. I use hexagonal architecture in my code, but sometimes converting between
multiple layers is tiresome, so I wanted to have a way to generate code that will be very close
to my domain layer, without sacrificing the ability to write pure SQL queries.

Here is the brief example:

```sqlite
/* @@{ sharedResult=Person,
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
-- @@{ sharedResult=PersonWithAddresses }
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

1. Write your SQL queries in `.sql` files
2. Add annotations using SQL comments to control code generation
3. Run the Gradle plugin to generate type-safe Kotlin code
4. Use the generated database classes in your application

Full example is available in the [`/sample-kmp`](./sample-kmp) directory.

## Documentation

Full documentation is available in the https://mobiletoly.github.io/sqlitenow-kmp/

