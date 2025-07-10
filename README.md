# SQLiteNow

[![Kotlin Multiplatform](https://img.shields.io/badge/Kotlin-Multiplatform-blue?logo=kotlin)](https://kotlinlang.org/docs/multiplatform.html)
[![Maven Central](https://img.shields.io/maven-central/v/dev.goquick.sqlitenow/dev.goquick.sqlitenow.gradle.plugin?logo=apache-maven&label=Maven%20Central)](https://central.sonatype.com/artifact/dev.goquick.sqlitenow/dev.goquick.sqlitenow.gradle.plugin)
[![CI](https://img.shields.io/github/actions/workflow/status/mobiletoly/sqlitenow-kmp/gradle.yml?branch=main&logo=github&label=CI)](https://github.com/mobiletoly/sqlitenow-kmp/actions/workflows/gradle.yml)
[![License](https://img.shields.io/github/license/mobiletoly/sqlitenow-kmp?logo=apache&label=License)](LICENSE)

A Kotlin Multiplatform library for type-safe SQLite database access, inspired by SQLDelight.

Full documentation is available in the https://mobiletoly.github.io/sqlitenow-kmp/ pages.

## Overview

SQLiteNow generates Kotlin code from your SQL files, giving you full control over your queries while maintaining type safety.
Unlike SQLDelight, which supports multiple database engines, SQLiteNow is focused exclusively on SQLite, 
allowing for deeper integration and SQLite-specific optimizations.

## Key Features

- **Pure SQL Control** - Write your queries in SQL files, get type-safe Kotlin code
- **Comment-based Annotations** - Control code generation using simple `-- @@{ annotations }` comments in your SQL
- **No IDE Plugin Required** - Works with any editor, uses Gradle plugin for code generation
- **Kotlin Multiplatform** - Supports all KMP targets using [androidx.sqlite](https://developer.android.com/kotlin/multiplatform/sqlite) driver
- **SQLite Focused** - Optimized specifically for SQLite features and capabilities
- **Migration support** - Migration scripts are supported to manage database schema changes

## Why SQLiteNow exists if SQLDelight is really awesome

First of all, I wanted to target specifically SQLite in Kotlin Multiplatform environment, it is my platform
of choice as of now for mobile development. SQLiteNow built on top of using multiplatform SQLite driver from AndroidX.

Second, since we use comment-based annotations (and I'm planning to add user-defined annotations in the future),
no plugin is required (like in case of SQLDelight), so you can easily use just a regular SQL files
that will be validated by your IDE or other external tools for correctness.

Third, I wanted to have a more flexible and extensible code generation system that can be easily extended
and customized. For example, you can declare this SQL statement in your file:  

```sqlite
-- @@{  sharedResult=PersonEntity,
--      implements=com.example.app.PersonEssentialFields,
--      excludeOverrideFields=[phone, birthDate]  }
SELECT * FROM Person
LIMIT :limit OFFSET :offset
```

This will generate shared data class `PersonEntity` that implements my custom `PersonEssentialFields` interface
but because this interface does not include phone and birthDate fields - they will be excluded.

## How it works

1. Write your SQL queries in `.sql` files
2. Add annotations using SQL comments to control code generation
3. Run the Gradle plugin to generate type-safe Kotlin code
4. Use the generated database classes in your application

Full example is available in the [`/sample-kmp`](./sample-kmp) directory.

## Documentation

Full documentation is available in the https://mobiletoly.github.io/sqlitenow-kmp/

