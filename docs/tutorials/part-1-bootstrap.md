---
layout: doc
title: Part 1 – Bootstrapping SQLiteNow in MoodTracker
permalink: /tutorials/part-1-bootstrap/
parent: Tutorials
nav_order: 1
---

# Part 1 – Bootstrapping SQLiteNow in MoodTracker

Welcome aboard! This first article shows how SQLiteNow fits into a fresh Kotlin
Multiplatform "MoodTracker" project. We will:

- hook in the published Gradle plugin,
- add the runtime libraries the generated code relies on,
- describe our schema and queries in plain SQL files,
- run the generator and inspect the produced Kotlin, and
- call the generated routers from shared code without hand-written DTOs.

## Step 1 – Enable the SQLiteNow Gradle Plugin

Start with a fresh Kotlin Multiplatform project using the "Shared Compose UI" template in Android
Studio, selecting Android, iOS, and Desktop targets. SQLiteNow also supports JVM server, JS, and
Wasm/browser builds (and can coexist with those later), but we'll keep the tutorial focused on the
mobile/desktop trio to stay approachable.

SQLiteNow operates through a Gradle plugin that scans SQL assets and emits Kotlin under
`build/generated/sqlitenow/…`. Add the plugin ID to `composeApp/build.gradle.kts` next to your
existing Compose/KMP plugins (to keep article brief, we are not going to use toml version catalog):

```kotlin
plugins {
    // … existing plugin declarations …
    id("dev.goquick.sqlitenow") version "<latest-version>"
}
```
While you are configuring the module, enable the opt-in flags we will rely on once richer types
arrive in Part 2 (there is no harm in turning them on now):
```kotlin
kotlin {
    compilerOptions {
        freeCompilerArgs.add("-opt-in=kotlin.time.ExperimentalTime")
        freeCompilerArgs.add("-opt-in=kotlin.uuid.ExperimentalUuidApi")
    }
    // …
}
```
These keep the Kotlin compiler happy when we start working with `kotlin.uuid.Uuid` and friends.

Gradle will expose a `generateMoodTrackerDatabase` task once this line is in place.


## Step 2 – Add Runtime Dependencies

The generated code needs the SQLiteNow runtime (for `SqliteNowDatabase`, migrations,
reactive helpers) plus the bundled SQLite driver used on supported native/JVM targets.
Keep the runtime in `commonMain`, and add `sqlite-bundled` only in platform source sets
that publish it (for example `androidMain`; do not put it in `commonMain` when using `wasmJs`).

Feel free to replace version with more recent one.

```kotlin
kotlin {
    // …
    sourceSets {
        commonMain.dependencies {
            implementation("dev.goquick.sqlitenow:core:<latest-version>")
            implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.6.1")
        }
        androidMain.dependencies {
            implementation("androidx.sqlite:sqlite-bundled:2.6.1")
        }
    }
}
```

This keeps the shared source set pure Kotlin while letting Android reuse the bundled driver. The
date-time library will come into play in the next article; adding it now avoids another round of
Gradle edits later.
Finish the build script changes by pointing SQLiteNow at your upcoming SQL folder. Each
`create("…")` block defines a database and the package the generator will use for its
Kotlin output:

```kotlin
sqliteNow {
    databases {
        create("MoodTrackerDatabase") {
            packageName.set("dev.goquick.sample.moodtracker.db")
            debug = false // set true when you need verbose logging while debugging SQL
        }
    }
}
```

This configuration is what links `sql/MoodTrackerDatabase/` (see below) to the
generated Kotlin under `build/generated/sqlitenow/...`. The argument to `create(...)` establishes
both the name of the Gradle task (`generateMoodTrackerDatabase`), the folder the plugin scans,
and the name of the generated entry point class (`MoodTrackerDatabase`). Keep the string in sync
with the SQL directory so everything lines up.

## Step 3 – Describe the Database in SQL

SQLiteNow keeps SQL front and centre. Create the folder scaffold under
`composeApp/src/commonMain/sql/MoodTrackerDatabase/`:

```
schema/mood_entry.sql
queries/mood_entry/add.sql
queries/mood_entry/selectRecent.sql
```

SQLiteNow expects this layout for every database under `src/commonMain/sql/<DatabaseName>/`
(see the [Create Schema]({{ site.baseurl }}/documentation/create-schema/) guide for the full breakdown):

- **schema** – Here goes the SQL schema: `CREATE TABLE`, `CREATE VIEW`, `CREATE INDEX`,
  and similar statements. These files teach SQLiteNow what columns exist and which
  constraints apply. Annotations in SQL comments teach SQLiteNow how to map columns to
  Kotlin types.
- **queries** – Executable statements: every `SELECT`, `INSERT`, `UPDATE`, or `DELETE` lives
  here. These statements are what actually trigger Kotlin generation (routers, data
  classes, parameter types). The directory structure under `queries/` becomes the
  namespace, so `queries/mood_entry/add.sql` will produce `MoodEntryQuery.Add`.
- **init** *(optional)* – Seed data executed once when the database is created.
- **migration** *(optional)* – Versioned scripts (`0001_update_name_column.sql`, etc.) that 
  run during upgrades.

For this first pass we only need `schema/` and `queries/`; the other folders become useful
once we start shipping migrations or default content. Schema files can bundle multiple
statements (for example the table definition plus an index), while each file under
`queries/` should hold exactly one statement so the generated API stays predictable.

Good naming conventions keep intent obvious:

- `add.sql` – basic insert; if you only have one insert it can also handle a RETURNING clause.
- `addReturningId.sql` – insert with a `RETURNING id` clause.
- `addReturningRow.sql` – insert that returns the full row (like our example).
- `selectOrderedByName.sql` – select with ordering or filters.

Short, descriptive file names make it easy to understand generated API names later on.
SQLiteNow derives Kotlin identifiers from the path: the folder under `queries/` becomes the
namespace (`mood_entry` → `MoodEntryQuery`) and the file name is converted to PascalCase
(`addReturningRow.sql` → `AddReturningRow`, so the full API is `MoodEntryQuery.AddReturningRow`).
Keeping names explicit avoids surprises in the generated source.

Schema definition:

```sql
CREATE TABLE mood_entry (
    id TEXT PRIMARY KEY NOT NULL,
    entry_time TEXT NOT NULL,
    mood_score INTEGER NOT NULL,
    note TEXT
);

CREATE INDEX idx_mood_entry_entry_time ON mood_entry(entry_time);
```

Save the snippet above as `schema/mood_entry.sql`. Now add the two query files referenced in the
directory listing so the generator has statements to process.

Insertion query with `RETURNING` (place this in `queries/mood_entry/add.sql`; the annotation tells
SQLiteNow which row class to create):

```sql
-- @@{ queryResult=MoodEntryRow }
INSERT INTO mood_entry (id, entry_time, mood_score, note)
VALUES (:id, :entryTime, :moodScore, :note)
RETURNING id, entry_time, mood_score, note;
```

Recent entries query (save to `queries/mood_entry/selectRecent.sql`; note how we reuse the same
`queryResult` name). In Part 2 we will replace this query with a dynamic-field version that also
returns tags, so treat this as a stepping stone:

```sql
-- @@{ queryResult=MoodEntryRow }
SELECT id, entry_time, mood_score, note
FROM mood_entry
ORDER BY entry_time DESC
LIMIT :limit;
```

Because annotations live in SQL comments, any editor or linter you already use will continue
to understand these files, and the queries you place in this directory—like the SELECT
above—are what actually trigger Kotlin generation (`MoodEntryRow`, router methods, parameter
classes, and so on).

What happens if you drop the `queryResult` annotation? SQLiteNow will still generate a result class,
but it will default to a unique name per statement (for example `MoodEntryAddResult`). By naming it
explicitly you can:

- share the same Kotlin data class across multiple statements (INSERT, SELECT, UPDATE,
  DELETE) that expose the same columns,
- optionally map different statements into existing domain models with `mapTo=…`, and
- keep callers stable even if you rename SQL files.

If you choose not to set `queryResult`, SQLiteNow falls back to an auto-generated name scoped
to the statement, and other queries won't automatically reuse it.
When multiple SELECT or INSERT statements in different files point at the same
columns and share a `queryResult` name, SQLiteNow emits that data class only once
and every router reuses it. For example, both `moodEntry/addReturningRow.sql` and
`moodEntry/selectRecent.sql` return `MoodEntryRow`, keeping callers consistent even as
queries grow.

### Comment-style annotations 101

SQLiteNow's superpower is that you steer code generation with lightweight annotations written
directly inside SQL comments. They follow a `-- @@{ key=value, … }` syntax (HOCON style) so
the SQL remains valid for every tool. A few essentials:

- **Statement-level annotations** sit above a statement. In the example above we used
  `-- @@{ queryResult=MoodEntryRow }` to ask for the generated data class to be called
  `MoodEntryRow`. You can also rename queries (`name=…`), tweak property naming
  (`propertyNameGenerator=camelCase`), or map results into existing entities
  (`mapTo=dev.goquick.SomeType`).
- **Column-level annotations** appear next to a column definition or select item:
  `-- @@{ field=entry_time, propertyName=loggedAt }` would rename the generated property,
  while `adapter=custom` or `propertyType=kotlinx.datetime.LocalDateTime` instruct SQLiteNow to
  use adapters for complex types.
- **Reusable defaults** live entirely in SQL comments, so they work with any editor, diff
  tool, or migration review process—you don't need IDE plugins.

The [Create Schema]({{ site.baseurl }}/documentation/create-schema/) 
and [Query Data]({{ site.baseurl }}/documentation/query-data/) guides include full reference tables,
but the key idea is that every Kotlin artefact is controlled from the SQL file you already
own. Having comment-style annotations part of the SQL keeps the entire workflow self-contained and
editor-agnostic.

## Step 4 – Generate the Kotlin Sources

With the SQL files in place when you run the generator:

```bash
./gradlew :composeApp:generateMoodTrackerDatabase
```

Gradle spins up an embedded SQLite engine, validates every statement, and writes Kotlin under:

```
composeApp/build/generated/sqlitenow/code/dev/goquick/sample/moodtracker/db/
```

And you will be able to see:

- `MoodTrackerDatabase.kt` – the class you will instantiate from shared code,
- `MoodEntryQuery.kt` – constants, affected table sets, and parameter types,
- `MoodEntryRow.kt` – the generated data class shared by multiple queries,
- `VersionBasedDatabaseMigrations.kt` – migration plumbing aligned with SQLite's
  `user_version`.

Inside your `sqliteNow` block keep `debug = false` for production builds; flip it to `true`
whenever you want verbose logging and query tracing, it will print every SQL statement
(with bound values) as it runs.

Managed routers and helper functions follow predictable naming: the folder under
`queries/` creates a router property on `MoodTrackerDatabase` (e.g.
`queries/mood_entry/...` → `database.moodEntry`), each SQL file becomes a Kotlin
member (`add.sql` → `MoodEntryQuery.Add`), and helper methods such as `one`,
`oneOrNull`, or `asList` come from SQLiteNow so you can choose how many rows to expect.

## Step 5 – Use the Generated API

Drop a slim factory and repository into
`composeApp/src/commonMain/kotlin/dev/goquick/sample/moodtracker/data/`.

Factory (opens the database once and exposes the generated migrations):

```kotlin
class MoodDatabaseFactory(
    private val dbName: String = "mood_tracker.db",
    private val debug: Boolean = false,
) {
    suspend fun create(): MoodTrackerDatabase {
        val appName = "MoodTracker"
        val resolvedName = resolveDatabasePath(dbName = dbName, appName = appName)
        val database = MoodTrackerDatabase(
            dbName = resolvedName,
            migration = VersionBasedDatabaseMigrations(),
            debug = debug,
        )
        database.open()
        return database
    }
}
```

`resolveDatabasePath` comes from the SQLiteNow runtime (`dev.goquick.sqlitenow.common.resolveDatabasePath`)
and maps friendly filenames to the correct location on each target. On JVM/desktop you must supply
an app-specific name (used to pick the OS app-data directory); other platforms ignore the value.
Because the JVM implementation uses `appName` as a directory segment, path-unsafe characters will
throw an exception.

Repository (notice we return `MoodEntryRow` straight from SQLiteNow—no extra DTO layer):

```kotlin
class MoodEntryRepository(private val database: MoodTrackerDatabase) {

    data class NewMoodEntry(
        val id: String,
        val entryTime: String,
        val moodScore: Long,
        val note: String?,
    )

    suspend fun add(entry: NewMoodEntry): MoodEntryRow {
        return database.moodEntry.add.one(
            MoodEntryQuery.Add.Params(
                id = entry.id,
                entryTime = entry.entryTime,
                moodScore = entry.moodScore,
                note = entry.note,
            )
        )
    }

    suspend fun recent(limit: Int): List<MoodEntryRow> {
        return database.moodEntry.selectRecent(
            MoodEntryQuery.SelectRecent.Params(limit = limit.toLong())
        ).asList()
    }
}
```

At this stage you can open the database from any KMP target, insert a row, and fetch it back
with type-safe Kotlin. In [Part 2]({{ site.baseurl }}/tutorials/part-2-tags-and-filters/) we will layer annotations and adapters on top so these APIs can
work with `LocalDateTime`, UUID wrappers, and richer domain models without changing the SQL files.

## Where We Stand

You now have a working SQLiteNow pipeline:

- SQL assets live in the repo and double as schema documentation.
- `generateMoodTrackerDatabase` validates them and emits Kotlin on every build.
- Shared code opens `MoodTrackerDatabase` and uses generated routers directly.
- There is zero runtime reflection and no manual DTO mapping—the generated `MoodEntryRow`
  is perfectly serviceable as your domain entity.

## Next Time

In [Part 2]({{ site.baseurl }}/tutorials/part-2-tags-and-filters/) we will extend the schema with mood tags, wire adapters for richer types, and start
streaming results with SQLiteNow's reactive helpers. See you there!
