---
layout: page
title: Overview
permalink: /overview/
---

SQLiteNow generates Kotlin code from your SQL files, giving you full control over your queries while maintaining type safety. Unlike SQLDelight, which supports multiple database engines, SQLiteNow is focused exclusively on SQLite, allowing for deeper integration and SQLite-specific optimizations.

## Why SQLiteNow?

### SQLite-First Approach

SQLiteNow is built specifically for SQLite in Kotlin Multiplatform environments. It leverages the multiplatform SQLite driver from AndroidX, providing optimal performance and compatibility across all KMP targets.

### Comment-Based Annotations

Since we use comment-based annotations (with plans for user-defined annotations), no IDE plugin is required. You can use regular SQL files that will be validated by your IDE or other external tools for correctness.

### Flexible Code Generation

SQLiteNow provides a flexible and extensible code generation system that can be easily extended and customized. For example:

```sql
-- @@{ sharedResult=PersonEntity
--     implements=com.example.app.PersonEssentialFields}
--     excludeOverrideFields=[phone, birthDate] }
SELECT * FROM Person
LIMIT :limit OFFSET :offset
```

This generates a shared data class `PersonEntity` that implements your custom `PersonEssentialFields` interface, with specific fields excluded from override modifiers.

## Key Features

### Pure SQL Control
Write your queries in SQL files and get type-safe Kotlin code generated automatically. No need to learn a custom DSL or query language.

### Comment-Based Annotations
Control code generation using simple `-- @@{ annotation annotation ... }` comments directly in your SQL files:

- `field` - Customize property names and types
- `adapter` - Add custom type converters
- `sharedResult` - Share data classes across queries
- `implements` - Make generated classes implement interfaces
- `notNull` - Control nullability
- And many more...

### No IDE Plugin Required
Works with any text editor or IDE. Uses a Gradle plugin for code generation, so you can integrate it into any build system.

### Kotlin Multiplatform Support
Supports all KMP targets including:
- Android
- iOS (arm64, x64, simulator arm64)
- JVM
- And more as they become available

### SQLite-Specific Optimizations
Since we focus exclusively on SQLite, we can provide:
- Better introspection of SQLite-specific features
- Optimized query execution
- SQLite-specific type mappings
- Support for SQLite extensions and pragmas

## Architecture

SQLiteNow follows a simple architecture:

1. **SQL Files** - You write standard SQL in `.sql` files
2. **Annotations** - Add generation hints using SQL comments
3. **Gradle Plugin** - Processes SQL files and generates Kotlin code
4. **Generated Code** - Type-safe database classes and query methods
5. **Runtime** - Uses androidx.sqlite for cross-platform database access

## Next Step

Ready to get started? Check out our [Getting Started]({{ site.baseurl }}/getting-started/) guide to
set up SQLiteNow in your project.
