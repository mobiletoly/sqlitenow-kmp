---
layout: doc
title: Synchronized Schema Evolution
permalink: /sync/schema-evolution/
parent: Sync
parent_url: /sync/
---

# Synchronized schema evolution

SQLiteNow supports a deliberately bounded first-release profile for changing a synchronized schema. A successful numbered SQLite migration or callback is not, by itself, proof that a sync-generation transition is supported. The opening must use an explicit directed `OversqliteUpgradePlan`, discover the exact requested target generation, and become ready only after the existing U0-U8 opening completes.

Unsupported descriptor differences are rejected before U0. The opening reports `upgrade_plan_mismatch` with the `repair_configuration` action; no new opening journal, freeze, source replacement, snapshot state, or target publication is created. Close that opening before continuing to use the admitted old contract or before retrying with a corrected plan.

## M01-M30 compatibility matrix

| ID | First-release disposition |
| --- | --- |
| M01 | Every old synchronized table and canonical table identity must remain in the target. Declaration ordering is not identity. |
| M02 | Adding a synchronized table is supported after SQLiteNow proves that no table, view, index, or trigger already uses its name. The application migration must create the exact target table during U4. |
| M03 | Omitting or retiring a synchronized table is deferred and rejected before U0. |
| M04 | Dropping a synchronized table or removing its active registration is rejected; a target omission never authorizes local data discard. |
| M05 | Adding a nullable non-key column with a current wire codec is supported when application migration creates the target schema and target snapshot rows provide values. |
| M06 | Adding a non-null non-key column with a current wire codec is supported when the ready target has completed application-owned transition writes/backfill and every target snapshot value is valid. |
| M07 | Omitting a non-key, non-FK column is supported. The target snapshot reconstructs synchronized rows; the server retains physical data needed by supported old generations. |
| M08 | A rename is an old-column omission plus a distinct new-column addition under M05 or M06. No alias, field mapping, or history reinterpretation exists. |
| M09 | Changing a shared descriptor wire type is rejected before U0; use M08. Generated/hidden columns and incompatible SQLite affinity fail target validation at U4. Exact declared representation, collation, and semantic domain are application-owned and require migration tests. |
| M10 | Changing nullability on the same shared column is rejected while a supported generation exposes it. Use a replacement column. |
| M11 | A SQL default is not a sync compatibility mechanism. It may be ordinary target DDL but cannot synthesize history or incoming values. |
| M12 | Backfill is server/application authority. Callback-written synchronized values have no preservation promise because U5-U8 reconstruct them. |
| M13 | Primary/sync key columns, order, codecs, nullability, table identity, and scope semantics must remain exact. |
| M14 | Adding, removing, renaming, reordering, or changing a primary/sync key is rejected before U0. |
| M15 | `WITHOUT ROWID` is not generation-descriptor identity. SQLiteNow preserves declared local-only objects but does not automatically prove or reject a synchronized-table rowid-representation change; applications must avoid that evolution or validate it explicitly. |
| M16 | Every old synchronized FK must remain exact, including endpoints, columns, actions, deferrability, and ordering. |
| M17 | A target may add a single-column FK from a new nullable non-key child column. Both endpoints must use the same wire type, and the parent may be an existing or newly added synchronized table. Other FK changes are rejected before U0. |
| M18 | A local-only FK that can affect synchronized or preserved rows belongs in a separate same-generation local-only migration. |
| M19 | Application indexes/views may be created by the target migration when they do not alter sync identity. Generation discovery does not describe or prove them; the owning application test must execute and assert them after ready. |
| M20 | Application triggers on synchronized tables are rejected unless already part of the accepted generated/runtime contract. Incompatible triggers fail pre-U0; migration-created incompatibilities roll back. |
| M21 | Runtime-owned sync indexes, triggers, and control objects remain coordinator-owned. |
| M22 | Declared local-only tables, rows, indexes, triggers, and application ledger remain unchanged during a generation edge. |
| M23 | Intentional local-only transformation is supported only as a separate same-generation local-only edge. |
| M24 | Local-only columns embedded in synchronized rows are not a preservation strategy; use a declared disjoint local-only table. |
| M25 | The migration callback may run target DDL, validation, and temporary application logic, but cannot preserve synchronized values or transform local-only state in the same generation edge. |
| M26 | Multiple local-version steps inside one directed generation edge are supported. |
| M27 | A sparse direct edge, such as generation 1 to 3, is supported only with one explicit directed plan; no intermediate generation is inferred. |
| M28 | Missing direct edge, downgrade, changed plan identity, or a target outside this profile is rejected before new mutation. |
| M29 | A supported old writer remains writable while target rollout is paused; target unavailability never retires, freezes, or relabels it. |
| M30 | An exact retired generation proof receives the existing non-retryable `retired_schema_generation`/`upgrade_application` result and preserves local intent. |

## Replacement-column pattern

For a representation or nullability change, keep the old synchronized column only for generations that still expose it and add a distinct target column. The server/application transition writes and backfill own conversion. The target database migration must create the exact target shape; SQLiteNow does not add aliases, defaults, or client-side conversion to make old data fit the new column.

```sql
-- Old generation: title TEXT NOT NULL
-- Target generation: title TEXT NOT NULL, title_json JSON NULL
-- The application/server backfill owns title -> title_json conversion.
```

Do not use a default value to repair missing target snapshot values. Existing schema/protocol validation remains the failure path.

## Added-table pattern

Add the table to the target generation descriptor and create the same shape in the target local migration. SQLiteNow checks the database namespace before U0 and repeats the check inside the writer transaction. An existing table, view, index, or trigger with the same case-insensitive name returns `upgrade_plan_mismatch` with `repair_configuration`. SQLiteNow does not rename, adopt, or drop the conflicting object.

```sql
CREATE TABLE tag (
    id TEXT PRIMARY KEY NOT NULL,
    label TEXT NOT NULL
);
```

Fresh target installs must create `tag` from the current schema. Existing installs must create it in the numbered migration that the directed upgrade plan selects. Target snapshots provide its synchronized rows during U5-U8.

## Added nullable-FK pattern

Add the child column and its FK in the target descriptor and target local migration. The new child column must be nullable, non-key, and absent from the old descriptor. The parent can belong to an old table or a table added by the same target generation.

```sql
CREATE TABLE category (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL
);

ALTER TABLE note ADD COLUMN category_id TEXT NULL
    REFERENCES category(id) ON DELETE SET NULL;
```

SQLiteNow rejects an FK that removes or changes an old relationship, uses a non-null child, targets incompatible wire types, or attaches to an old-visible child column. The migration must create the exact target constraint. Target snapshot rows supply `NULL` and non-`NULL` values; SQLiteNow runs ordinary schema, row, and FK validation before publication.

## Enforcement timing and application work

SQLiteNow compares the installed and target descriptors before U0. It also checks added-table namespace availability at that point and again inside the U0 writer transaction. A pre-U0 mismatch leaves the opening journal, managed rows, source identity, and `PRAGMA user_version` unchanged.

During U4, the application-owned numbered migration and `onMigrationStep` callback must create the exact target schema. SQLiteNow then validates the migrated schema. A DDL, callback, schema, or FK failure rolls back U4 and leaves a resumable opening journal. Fix the application migration and reopen with the same plan identity. U5-U8 apply the authoritative target snapshot and publish the database/client pair only after target schema and rows pass validation.

Application database reads and writes remain unavailable until the opening reaches ready. Keep any last-known view in an application-owned cache outside the coordinated-opening lifecycle.

KMP passes the directed plan to the generated database opening:

```kotlin
val plan = OversqliteUpgradePlan(
    migrationId = "notes-g1-g2",
    fromGenerationId = 1,
    toGenerationId = 2,
    fromLocalVersion = 2,
    toLocalVersion = 3,
    preservedLocalTables = setOf("drafts"),
)
val opening = db.openForSync(config, httpClient, upgradePlan = plan)
val status = opening.advance()
check(status.outcome == OversqliteOpeningOutcome.READY) {
    "Schema upgrade requires ${status.nextAction}: ${status.diagnosticCode}"
}
val ready = checkNotNull(opening.readyOrNull())
```

Swift uses the generated plan and opening types:

```swift
let plan = SQLiteNowSyncUpgradePlan(
    migrationId: "notes-g1-g2",
    fromGenerationId: 1,
    toGenerationId: 2,
    fromLocalVersion: 2,
    toLocalVersion: 3,
    preservedLocalTables: ["drafts"]
)
let opening = try await db.openForSync(
    baseURL: serviceURL,
    auth: auth,
    config: targetConfig,
    upgradePlan: plan
)
let status = try await opening.advance()
guard status.outcome == .ready, opening.readyOrNil() != nil else {
    throw SQLiteNowError.sync(
        message: "Schema upgrade requires \(status.nextAction.rawValue): \(status.diagnosticCode)"
    )
}
```

## Local-only transformations

Do not transform declared local-only state inside a generation-changing edge. Use two durable openings:

1. Keep the installed sync generation and open a same-generation local-only edge with a higher local version. Declare every local-only table and complete the local migration.
2. Confirm that opening is ready, then start a separate explicit installed-to-target generation edge. Declare the transformed local-only tables as the preservation baseline.

The process may stop between those ready boundaries. On reopen, the completed local-only state remains the baseline; SQLiteNow does not combine callbacks, infer the second edge, or create a hidden copy-out store.

KMP keeps the two edges explicit:

```kotlin
val localPlan = OversqliteUpgradePlan(
    migrationId = "drafts-v2-v3",
    fromGenerationId = 1,
    toGenerationId = 1,
    fromLocalVersion = 2,
    toLocalVersion = 3,
    preservedLocalTables = setOf("drafts"),
)
val localOpening = dbV3.openForSync(oldConfig, httpClient, upgradePlan = localPlan)
val localStatus = localOpening.advance()
check(localStatus.outcome == OversqliteOpeningOutcome.READY) {
    "Local migration requires ${localStatus.nextAction}"
}
checkNotNull(localOpening.readyOrNull())
localOpening.close()

val generationPlan = OversqliteUpgradePlan(
    migrationId = "notes-g1-g2",
    fromGenerationId = 1,
    toGenerationId = 2,
    fromLocalVersion = 3,
    toLocalVersion = 4,
    preservedLocalTables = setOf("drafts"),
)
val generationOpening = dbV4.openForSync(targetConfig, httpClient, upgradePlan = generationPlan)
val generationStatus = generationOpening.advance()
check(generationStatus.outcome == OversqliteOpeningOutcome.READY) {
    "Generation migration requires ${generationStatus.nextAction}"
}
val ready = checkNotNull(generationOpening.readyOrNull())
```

Swift uses the same two KMP-owned edges:

```swift
let localPlan = SQLiteNowSyncUpgradePlan(
    migrationId: "drafts-v2-v3",
    fromGenerationId: 1, toGenerationId: 1,
    fromLocalVersion: 2, toLocalVersion: 3,
    preservedLocalTables: ["drafts"]
)
let localOpening = try await dbV3.openForSync(
    baseURL: serviceURL, auth: auth, config: oldConfig, upgradePlan: localPlan
)
let localStatus = try await localOpening.advance()
guard localStatus.outcome == .ready, localOpening.readyOrNil() != nil else {
    throw SQLiteNowError.sync(message: "Local migration requires \(localStatus.nextAction.rawValue).")
}
try await localOpening.close()

let generationPlan = SQLiteNowSyncUpgradePlan(
    migrationId: "notes-g1-g2",
    fromGenerationId: 1, toGenerationId: 2,
    fromLocalVersion: 3, toLocalVersion: 4,
    preservedLocalTables: ["drafts"]
)
let generationOpening = try await dbV4.openForSync(
    baseURL: serviceURL, auth: auth, config: targetConfig, upgradePlan: generationPlan
)
let generationStatus = try await generationOpening.advance()
guard generationStatus.outcome == .ready, generationOpening.readyOrNil() != nil else {
    throw SQLiteNowError.sync(message: "Generation migration requires \(generationStatus.nextAction.rawValue).")
}
```

## Application-owned validation

The server operator must finish transition writes and backfill before advertising the target generation as ready. Old writers omit target-only fields; transition logic must derive or preserve the target representation for those writes. Record readiness against the exact generation fingerprint and backfill revision.

The application migration test must also assert physical and business facts that discovery does not encode. Check required indexes and views in `sqlite_schema`, exercise application triggers, verify chosen TEXT/BLOB representations, inspect collation and deferrability where the application depends on them, and run `PRAGMA foreign_key_check`. Keep these assertions in the application migration suite. SQLiteNow validates the descriptor-visible shape and integrity boundaries, but it cannot infer application meaning from arbitrary SQL.

For example, an application may own a target-only lookup index and reporting view, plus the local-only trigger from the first ready boundary:

```sql
CREATE INDEX projects_title_lookup ON projects(title);
CREATE VIEW active_project_people AS
SELECT people.id, people.name, projects.title
FROM people JOIN projects ON projects.id = people.project_id;

-- Local-only object, created by the separate same-generation edge.
CREATE TRIGGER drafts_touch AFTER UPDATE OF label ON drafts
BEGIN
    UPDATE drafts SET normalized = upper(new.label) WHERE id = new.id;
END;
```

Assert those application-owned objects and SQLite integrity explicitly after the target opening reaches ready:

```kotlin
val connection = ready.database.connection()
connection.prepare("SELECT count(*) FROM pragma_foreign_key_check").use { statement ->
    check(statement.step())
    check(statement.getLong(0) == 0L)
}
connection.prepare(
    "SELECT count(*) FROM sqlite_schema " +
        "WHERE name IN ('projects_title_lookup','active_project_people','drafts_touch')",
).use { statement ->
    check(statement.step())
    check(statement.getLong(0) == 3L)
}
```

Backfill, indexes, views, triggers, representations, and business semantics remain application-owned. Passing discovery or reaching ready does not turn those responsibilities into SQLiteNow guarantees.

## Returning devices

| Situation | Required result |
| --- | --- |
| One or multiple missed generations, old remains supported | Supply one direct plan from the installed generation to the supported target. Numeric adjacency is irrelevant. |
| Target rollout paused | Target opening remains retryably unavailable; an independent old writer retains its existing authority. |
| Exact retired generation with settled intent | Preserve local-only data, validate the direct edge, and advance with target replacement. The old proof is never used for upload. |
| Exact retired generation with dirty, frozen, conflicted, rejected, or ambiguous intent | Persist `OLD_GENERATION_UNAVAILABLE / RESTORE_SUPPORT` and do not create U0, migrate, reset, rotate a source, or retry unchanged-generation transport. Export, support-assisted recovery, or explicitly audited abandonment is an application/operations decision. |
| No explicit direct edge, or a plan targeting the wrong generation | Fail before U0 with unchanged local bytes/schema/source/journal census. Repair the application configuration and supply the exact installed-to-target edge. |
| Installed identity exists but its retained old descriptor is missing or corrupt | Fail before U0 with unchanged local bytes/schema/source/journal census. Restore the compatible retained support file/build or use a support-owned recovery path. |

The application owner determines release channels, notice/support windows, and the user-facing recovery policy before production retirement. SQLiteNow does not promise automatic upload of unresolved intent after retirement.
