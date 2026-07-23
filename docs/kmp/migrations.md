---
layout: page
title: KMP Migrations
permalink: /kmp/migrations/
---

## One-time web import

The default JS and Wasm browser runtime persists directly to deterministic
OPFS. Before opening a new target, it can discover retained database bytes from
the released `SqliteNow/<dbName>.sqlite3` OPFS location or the
`SqliteNow` / `sqlite-databases` / `<dbName>` IndexedDB entry without creating
either legacy store. An explicitly supplied `SqlitePersistence` is also a
custom import source; the worker requests `load(dbName)` only when a source is
needed.

Migration imports the complete SQLite bytes through official `OpfsDb.importDb`.
It does not execute the old runtime, convert rows, translate schema, rewrite
sync payloads, or delete the source. Strict migration-intent and health markers
make retry fail closed: healthy targets remain authoritative, and recovery may
replace only an intent-owned partial target. The runtime validates the SQLite
header and page boundaries, integrity, foreign keys, schema, `user_version`,
retained source hash, and reopen before committing health. Generated schema
migrations run afterward if `user_version` is behind.

The import temporarily owns the complete source byte array. The Phase 7
reference migrated an authentic `67,182,592`-byte released-format database with
peak explicitly owned bytes equal to the source size and a structural bound
below `2 * sourceBytes + 1 MiB`; browser heap telemetry was unavailable.
Ordinary direct writes export no whole-database snapshots.

`SqliteConnectionConfig`, `SqlitePersistence`,
`IndexedDbSqlitePersistence`, and `persistSnapshotNow()` remain
source-compatible. On the direct worker, custom persistence is import-only:
`persist()` and `clear()` are never called, `autoFlushPersistence` has no
snapshot-export role, and `persistSnapshotNow()` is a no-op. These compatibility
surfaces do not select IndexedDB or snapshot storage as the default backend.

{% include shared/migrations.md platform="kmp" %}
