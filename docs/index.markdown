---
layout: home
---

A Kotlin Multiplatform library for type-safe SQLite database access, inspired by SQLDelight.

## Key Features

- **Pure SQL Control** - Write your queries in SQL files, get type-safe Kotlin code
- **Comment-based Annotations** - Control code generation using simple `-- @@annotation` comments in your SQL
- **No IDE Plugin Required** - Works with any editor, uses Gradle plugin for code generation
- **Kotlin Multiplatform** - Supports KMP targets using [androidx.sqlite](https://developer.android.com/kotlin/multiplatform/sqlite) driver
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
-- @@sharedResult=PersonEntity
-- @@implements=com.example.app.PersonEssentialFields
-- @@excludeOverrideFields=[phone, birthDate]
SELECT * FROM Person
LIMIT :limit OFFSET :offset
```

This will generate shared data class `PersonEntity` that implements my custom `PersonEssentialFields` interface
but because this interface does not include phone and birthDate fields - they will be excluded.

[Get Started →]({{ site.baseurl }}/getting-started/){: .btn .btn-primary}
