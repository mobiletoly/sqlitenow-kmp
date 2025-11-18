---
layout: doc
title: Part 2 – Tags, Filters, and Richer Types
permalink: /tutorials/part-2-tags-and-filters/
parent: Tutorials
nav_order: 2
---

# Part 2 – Tags, Filters, and Richer Types

In [Part 1]({{ site.baseurl }}/tutorials/part-1-bootstrap/) we bootstrapped a simple database and proved we could insert and fetch mood entries.
Now we extend the schema with tags, show how SQLiteNow handles relationships, and lean on
column-level annotations to surface Kotlin types such as `Uuid` and `Int` without manual casts.

## Recap: What We Have
- `mood_entry` table with a couple of queries (`add`, `selectRecent`).
- Kotlin syntheses wired through `MoodEntryRepository`.

## Step 1 – Extend the Schema for Tags
Create two new schema files and update the existing one under
`composeApp/src/commonMain/sql/MoodTrackerDatabase/schema/`:

```sql
-- mood_entry.sql
CREATE TABLE mood_entry (
    -- @@{ field=id, propertyType=kotlin.uuid.Uuid }
    id TEXT PRIMARY KEY NOT NULL,
    
    -- @@{ field=entry_time, propertyType=kotlinx.datetime.LocalDateTime }
    entry_time TEXT NOT NULL,
    
    -- @@{ field=mood_score, propertyType=kotlin.Int }
    mood_score INTEGER NOT NULL,

    note TEXT
);

CREATE INDEX idx_mood_entry_entry_time ON mood_entry(entry_time);
```

```sql
-- mood_tag.sql
CREATE TABLE mood_tag (
    -- @@{ field=id, propertyType=kotlin.uuid.Uuid }
    id TEXT PRIMARY KEY NOT NULL,
    
    name TEXT NOT NULL
);

CREATE UNIQUE INDEX idx_mood_tag_name ON mood_tag(name);
```

```sql
-- mood_entry_tag.sql
CREATE TABLE mood_entry_tag (
    -- @@{ field=entry_id, propertyType=kotlin.uuid.Uuid }
    entry_id TEXT NOT NULL,
    
    -- @@{ field=tag_id, propertyType=kotlin.uuid.Uuid }
    tag_id TEXT NOT NULL,
    
    PRIMARY KEY (entry_id, tag_id)
);

CREATE INDEX idx_mood_entry_tag_entry_id ON mood_entry_tag(entry_id);
CREATE INDEX idx_mood_entry_tag_tag_id ON mood_entry_tag(tag_id);
```

A few important notes:
- We split schema files by table name. Keeping each table in its own file makes migrations
  easier to reason about.
- Schema files can contain multiple statements. We use that to declare the table followed by
  the relevant indexes.
- Capturing full timestamps (`entry_time`) keeps our eventual UI simple: the shared code can call
  `Clock.System.now()` and persist both the date and time without extra columns.
- The field-level annotations (`propertyType=…`) instruct SQLiteNow to surface strongly typed
  Kotlin values. Once you declare `entry_time` as
  `kotlinx.datetime.LocalDateTime` in the schema, the generator applies that type to every query
  touching the column—no extra annotations or `adapter=default` flags required.


## Step 2 – Insert and Query Tags
Create tag and join queries under `queries/mood_tag/` and `queries/mood_entry_tag/`.

```sql
-- queries/mood_tag/add.sql
-- @@{ queryResult=MoodTagRow }
INSERT INTO mood_tag (id, name)
VALUES (:id, :name)
RETURNING id, name;
```

```sql
-- queries/mood_tag/selectAll.sql
-- @@{ queryResult=MoodTagRow }
SELECT id, name
FROM mood_tag
ORDER BY name;
```

```sql
-- queries/mood_entry_tag/add.sql
INSERT INTO mood_entry_tag (entry_id, tag_id)
VALUES (:entryId, :tagId);
```

## Step 3 – Reuse Mood Entries for Tag Filtering
Add two more queries under `queries/mood_entry/`:

```sql
-- add.sql (updated)
-- @@{ queryResult=MoodEntryRow }
INSERT INTO mood_entry (id, entry_time, mood_score, note)
VALUES (:id, :entryTime, :moodScore, :note)
RETURNING id, entry_time, mood_score, note;
```

```sql
-- selectByTag.sql
-- @@{ queryResult=MoodEntryRow }
SELECT DISTINCT
    mood_entry.id,
    mood_entry.entry_time,
    mood_entry.mood_score,
    mood_entry.note
FROM mood_entry
JOIN mood_entry_tag met ON met.entry_id = mood_entry.id
WHERE met.tag_id = :tagId
ORDER BY mood_entry.entry_time DESC;
```

```sql
-- selectRecentWithTags.sql
/* @@{ queryResult=MoodEntryWithTags, collectionKey=entry_id } */
SELECT
    -- @@{ field=entry_id, propertyName=id }
    me.id AS entry_id,

    me.entry_time,
    me.mood_score,
    me.note,
    mt.id   AS tag__id,
    mt.name AS tag__name

/* @@{ dynamicField=tags,
       mappingType=collection,
       propertyType=List<MoodTagRow>,
       sourceTable=mt,
       aliasPrefix=tag__,
       collectionKey=tag__id,
       notNull=true,
       defaultValue=listOf() } */

FROM mood_entry AS me
LEFT JOIN mood_entry_tag AS met ON met.entry_id = me.id
LEFT JOIN mood_tag AS mt ON mt.id = met.tag_id
ORDER BY me.entry_time DESC
LIMIT :limit;
```

The first two statements keep using `MoodEntryRow`, so the generator emits that class once and every
query reuses it. You can now delete the earlier `selectRecent.sql` created in Part 1—the new
`selectRecentWithTags` statement demonstrates a dynamic-field collection:
SQLiteNow groups rows by the parent `entry_id`, maps the aliased `tag__…` columns into
`MoodTagRow`, and emits a `MoodEntryWithTags` result whose `tags` property defaults to `listOf()`
when no matches exist. No manual grouping or de-duplication code is required.

Two highlights to notice:

- `-- @@{ field=entry_id, propertyName=id }` tells SQLiteNow to expose the column as `id` in the
  generated data class even though the SQL must alias it to `entry_id` for grouping. Without the
  override the constructor would expect `entryId`, which would give us a duplicate field next to the
  dynamic collection.
- Each tag column uses a `tag__` prefix because the dynamic-field annotation specifies
  `aliasPrefix=tag__`. The generator strips that prefix when building each `MoodTagRow`, so the SQL
  is free to project joined columns without colliding with the parent entry fields.


## Step 4 – Wire Kotlin Repositories
Our Part 1 setup already added `kotlinx-datetime`, so `LocalDateTime` is available in shared code.
The schema annotations we added earlier change the generated Kotlin signatures in two
important ways:
- `MoodTrackerDatabase` now exposes adapter groups such as `MoodEntryAdapters` and
  `MoodEntryTagAdapters`. Each property inside those groups corresponds to a column-level
  annotation that asked for a custom type, so we must provide lambdas that convert between `Uuid`
  or `LocalDateTime` and the underlying `TEXT` representation.
- Parameter classes like `MoodEntryQuery.Add.Params` now expect `LocalDateTime`
  (alongside `Uuid` and `Int`), which means our repositories can drop string parsing or
  integer-widening helpers.

Update the factory so it wires those adapters once when constructing the database. Notice how the
field names turn into adapter property names (`entryTimeToSqlValue`, `sqlValueToEntryTime`, etc.):

```kotlin
// MoodDatabaseFactory.kt (excerpt)
suspend fun create(): MoodTrackerDatabase {
    val uuidToSql: (Uuid) -> String = { it.toString() }
    val sqlToUuid: (String) -> Uuid = { Uuid.parse(it) }
    val localDateTimeToSql: (LocalDateTime) -> String = { it.toSqliteTimestamp() }
    val sqlToLocalDateTime: (String) -> LocalDateTime = { LocalDateTime.fromSqliteTimestamp(it) }

    val resolvedName = if (dbName.startsWith(":")) dbName else resolveDatabasePath(dbName)
    val database = MoodTrackerDatabase(
        dbName = resolvedName,
        migration = VersionBasedDatabaseMigrations(),
        debug = debug,
        moodEntryAdapters = MoodTrackerDatabase.MoodEntryAdapters(
            idToSqlValue = uuidToSql,
            entryTimeToSqlValue = localDateTimeToSql,
            sqlValueToId = sqlToUuid,
            sqlValueToEntryTime = sqlToLocalDateTime,
        ),
        moodEntryTagAdapters = MoodTrackerDatabase.MoodEntryTagAdapters(
            entryIdToSqlValue = uuidToSql,
            tagIdToSqlValue = uuidToSql,
        ),
    )
    database.open()
    return database
}
```

Each adapter property name mirrors the column it was declared on. Supplying the lambdas here keeps
the generated code platform-agnostic—every target receives the same conversion logic.
`resolveDatabasePath` comes from the SQLiteNow runtime (`dev.goquick.sqlitenow.common.resolveDatabasePath`)
and maps friendly filenames to platform-specific storage locations.

Now add the repositories. Thanks to the annotations, every parameter class uses `Uuid` and
`LocalDateTime` directly, and the select-by-tag helper only needs a single `toString()`
because we did not add a parameter annotation for `:tagId` yet. We also surface the new
collection-aware query so callers can fetch entries with their tags in one shot—or subscribe to the
reactive flow exposed by the generated router.

```kotlin
// MoodEntryRepository.kt
class MoodEntryRepository(private val database: MoodTrackerDatabase) {

    data class NewMoodEntry(
        val entryTime: LocalDateTime,
        val moodScore: Int,
        val note: String?,
        val id: Uuid = Uuid.random(),
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

    suspend fun recentWithTags(limit: Int): List<MoodEntryWithTags> {
        return database.moodEntry.selectRecentWithTags(
            MoodEntryQuery.SelectRecentWithTags.Params(limit = limit.toLong())
        ).asList()
    }

    fun recentWithTagsFlow(limit: Int): Flow<List<MoodEntryWithTags>> {
        return database.moodEntry.selectRecentWithTags(
            MoodEntryQuery.SelectRecentWithTags.Params(limit = limit.toLong())
        ).asFlow()
    }
}
```

```kotlin
// MoodTagRepository.kt
class MoodTagRepository(private val database: MoodTrackerDatabase) {

    suspend fun addTag(name: String): MoodTagRow {
        val params = MoodTagQuery.Add.Params(
            id = Uuid.random(),
            name = name,
        )
        return database.moodTag.add.one(params)
    }

    suspend fun allTags(): List<MoodTagRow> = database.moodTag.selectAll.asList()

    suspend fun assignTag(entryId: Uuid, tagId: Uuid) {
        database.moodEntryTag.add(
            MoodEntryTagQuery.Add.Params(entryId = entryId, tagId = tagId)
        )
    }

    suspend fun entriesForTag(tagId: Uuid): List<MoodEntryRow> {
        return database.moodEntry.selectByTag(
            MoodEntryQuery.SelectByTag.Params(tagId = tagId.toString())
        ).asList()
    }
}
```

Make sure to import `dev.goquick.sample.moodtracker.db.MoodEntryWithTags` and
`kotlinx.coroutines.flow.Flow` at the top of the file.

Also remove the old `recent` helper (and the `selectRecent.sql` file) introduced in Part 1. From this
point on the app relies entirely on `MoodEntryWithTags`, even when no tags are associated with an
entry—the generated mapper simply returns an empty list.

The compiler opt-ins we added back in Part 1 let us call `Uuid.random()` without sprinkling extra
`@OptIn` annotations through the code. Because we already pulled in `kotlinx-datetime`,
`LocalDateTime` is available everywhere without extra setup.

Notice how the generated API mirrors our file system:
- `queries/mood_tag/add.sql` becomes `database.moodTag.add`.
- `queries/mood_entry_tag/add.sql` becomes `database.moodEntryTag.add(params)`.
- `queries/mood_entry/selectByTag.sql` exposes `database.moodEntry.selectByTag(…)`.

## Step 5 – Regenerate and Compile
Run the usual command after updating SQL and Kotlin files:

```bash
./gradlew :composeApp:generateMoodTrackerDatabase
```

We also added instrumentation checks in the android instrumentation source set
(`MoodEntryRepositoryTest.kt` and `MoodTagRepositoryTest.kt`) to cover entry creation, tag
assignment, lookups, and `LocalDateTime` round-tripping end-to-end.

Everything should compile without additional casts. UUIDs, `LocalDateTime`, and `Int` properties now
map natively while `notNull=true` protects you from nullable surprises.

## What We Learned
- Schema files can host multiple DDL statements; query files stay one statement per file.
- `propertyType` and `notNull` annotations give you fine-grained control over how SQLiteNow
  surfaces Kotlin types (with adapters inferred automatically for common targets such as `Uuid`
  and `LocalDateTime`).
- Shared `queryResult` names keep generated models consistent across multiple queries.
- Adapters are injected through the generated database constructor so you can define the actual
  conversion logic in one place.

Next up we can explore reactive flows or more advanced result mappings, but at this point you
already have a strongly typed tagging system running across Android, iOS, and Desktop. When you're
ready to wire everything into a Compose UI, continue with
[Part 3 – Reactive Mood Dashboard]({{ site.baseurl }}/tutorials/part-3-reactive-ui/).
