---
layout: doc
title: Migration
permalink: /documentation/migration/
parent: Documentation
---

# Migration

Learn how to manage database schema changes using SQLiteNow's migration system.

## Directory Structure

Migration files are organized alongside your schema and queries:

```
src/commonMain/sql/SampleDatabase/
├── schema/          # Latest schema (always current)
├── queries/         # SELECT, INSERT, UPDATE, DELETE queries
├── init/            # Initial data
└── migration/       # Versioned migration files
    ├── 0001.sql
    ├── 0002.sql
    ├── 0003.sql
    └── .....sql
```

## How Migration Works

### Schema Directory

The `schema/` directory must always contain the **most recent** schema definitions, even when you
alter table in migration files, you must always reflect the final state in the schema files.
These files are only executed when the database is created for the first time, and it is important
to note that migration files are not executed when the database is created for the first time,
this is why you must always keep the schema files up to date. Also, annotations in schema files
are used to generate adapters and other code, annotations in migration files are ignored.

### Migration Directory

The `migration/` directory contains versioned migration files that update existing databases from
one version to another.

## Migration Files

Migration files are numbered sequentially starting from `0001.sql`:

**File: `migration/0001.sql`**

```sql
-- Migration from version 0 (initial version) to version 1
ALTER TABLE Person
    ADD COLUMN middle_name TEXT;
CREATE INDEX idx_person_middle_name ON Person (middle_name);
```

**File: `migration/0002.sql`**

```sql
-- Migration from version 1 to version 2
CREATE TABLE user_preferences
(
    id      INTEGER PRIMARY KEY NOT NULL,
    user_id INTEGER             NOT NULL,
    theme   TEXT                NOT NULL DEFAULT 'light',
    FOREIGN KEY (user_id)       REFERENCES person (id)
);
```

Each migration file represents a single version increment and contains the SQL statements needed to upgrade from the
previous version.

## Generated Migration Class

SQLiteNow automatically generates a `VersionBasedDatabaseMigrations` class that contains all your migration files
and knows how to apply them.

## Using Migrations

### Database Initialization

Pass the generated migration class to your database constructor:

```kotlin
val db = SampleDatabase(
    dbName = "sample.db",
    migration = VersionBasedDatabaseMigrations(),
    // ... other parameters
)
```

### Version Tracking

SQLiteNow automatically tracks the database version by storing it in the database file's metadata.
When your app starts:

1. **New Database**: Creates schema from `schema/` directory, sets version to latest
2. **Existing Database**: Checks current version, applies necessary migrations sequentially

## Migration Process

### Adding a New Migration

1. **Update Schema Files**: Modify files in `schema/` to reflect the latest structure
2. **Create Migration File**: Add new numbered migration file (e.g., `0004.sql`)
3. **Build Project** (or run gradle generate task): SQLiteNow regenerates the migration class

## Best Practices

### Sequential Numbering

Files must be numbered sequentially starting from `0001.sql`.

- ✅ `0001.sql`, `0002.sql`, `0003.sql`
- ❌ `1.sql`, `2.sql`, `migration1.sql`

For SqliteNow it does not really matter if you have `0001.sql`, `0002.sql`, `0015.sql` or `1.sql`, `2.sql`, `15.sql`
but it is recommended to keep it sequential for better readability and easier maintenance.

### One-Way Migrations

Migrations should only move forward. Never modify existing migration files after they've been released.

### Schema Consistency

Always ensure your `schema/` files match the result of applying all migrations to an empty database.

## Example Workflow

1. **Initial Release**: Database created from `schema/` files (version 0 → latest)
2. **Update 1**: Add `migration/0001.sql`, update `schema/` files
3. **Update 2**: Add `migration/0002.sql`, update `schema/` files
4. **Update 3**: Add `migration/0003.sql`, update `schema/` files

Users upgrading from any previous version will have all necessary migrations applied automatically.

## Next Steps

[Recipes]({{ site.baseurl }}/recipes/) - Learn about common patterns and best practices
