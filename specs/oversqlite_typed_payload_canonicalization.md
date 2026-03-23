# Oversqlite Typed Payload Canonicalization

Status: Phases 1-4 complete

Implementation readiness note:

- this spec is intentionally narrow and implementation-driving
- it is motivated by a real replay bug in KMP Oversqlite where `REAL` values could round-trip through
  Kotlin `Double` and leave phantom dirty rows after a successful push
- this spec covers only client-side payload typing, canonicalization, replay equality, and the test
  matrix needed to keep those rules stable
- the `nethttp_server` fixture change in this spec does not require a migration path; the example
  database may be dropped and recreated during implementation
- handoff state:
  - Phase 1: KMP core canonicalization is complete
  - Phase 2: shared `nethttp_server` fixture and KMP real-server verification is complete
  - Phase 3: Go test-boundary cleanup is complete
  - Phase 4: Go typed parity coverage is complete
  - verification note: this follow-up reran the Go-side shared-fixture verification and relied on
    the already-landed Phase 2 KMP shared-fixture coverage without changing KMP runtime code

## Summary

Oversqlite currently treats too much row state as generic JSON text.

That is acceptable for transport, but it is not acceptable as the internal truth for replay,
authoritative apply, or local dirty-state comparison. The result is type-fragile behavior:

- `REAL` and `DOUBLE` can drift textually when re-rendered through host-language floating-point values
- BLOB and UUID values can have multiple valid encodings
- boolean and integer-backed semantic types can compare equal semantically but not textually

This spec introduces a typed payload contract for Oversqlite:

1. classify each synced column into a stable `ColumnKind`
2. centralize local-state and wire encoding/decoding behind one typed codec boundary
3. replace replay's JSON-text equality with column-wise type-aware equality
4. add explicit post-push replay invariants
5. require coverage at unit, fake-server, and `nethttp_server` real-server layers

This spec does not change the sync protocol or app-facing API.

## Problem

The current implementation still distributes payload logic across several places:

- schema inspection in `TableInfo.kt`
- local row serialization and payload binding in `OversqliteLocalStore.kt`
- tolerant local/wire blob and UUID parsing in `OversqlitePayloadCodec.kt`
- replay comparison in `OversqlitePushWorkflow.kt`

That has two maintainability problems:

- column typing is inferred ad hoc via repeated `declaredType.contains(...)` checks
- replay correctness depends on whichever code path happened to serialize the row last

The recent `REAL` bug is the clearest example:

- dirty payload captured one decimal text
- replay re-read the live row using `getDouble()`
- re-rendered JSON text differed slightly
- replay concluded the row was still dirty
- `pullToStable()` then correctly rejected incremental pull because local dirty state remained

This is not a transport bug. It is a client-side canonicalization bug caused by weak typing rules.

## Goals

- make payload handling type-aware instead of string-shape-aware
- make replay comparison semantically stable for all supported column kinds
- keep local-state encoding and wire encoding explicit and separate
- make type rules live in one place instead of being scattered through the runtime
- preserve current protocol shape and public API
- make `nethttp_server` cover the same risky type paths as unit and fake-server tests

## Non-goals

- redesign the sync wire format
- add per-column adapter hooks in Oversqlite itself
- infer application-domain types beyond what Oversqlite can prove from schema shape
- add lossy normalization such as rounding `REAL` values
- add sample-app-specific servers or sample-app-specific library tests

## Design Principles

### 1. Transport JSON is not the internal source of truth

JSON is only the transport shape. Internally, replay and apply logic must reason in terms of typed
column values.

### 2. Local-state encoding and wire encoding are different concerns

Oversqlite already distinguishes:

- local/internal representations used in `_sync_dirty_rows`
- authoritative wire representations accepted from remote payloads

That distinction is correct and must remain explicit.

### 3. Equality must follow column semantics, not incidental text

If two values are the same according to the column kind, replay must treat them as equal even if
their textual JSON rendering differs.

### 4. Type classification must be centralized

The runtime should not re-decide "is this blob-ish" or "is this numeric-ish" at every call site.

## Proposed Design

## 1. Add `ColumnKind` to schema metadata

Extend `ColumnInfo` in `TableInfo.kt` with a derived kind:

```kotlin
enum class ColumnKind {
    TEXT,
    INTEGER,
    REAL,
    BLOB,
    UUID_BLOB,
}
```

`ColumnKind` is runtime-facing and intentionally coarse. It describes how Oversqlite must encode,
decode, bind, and compare values.

### 1.1 Classification rules

Classification is based on declared type plus sync metadata already known to Oversqlite.

Rules:

- `UUID_BLOB`
  - primary-key BLOB sync keys
  - BLOB foreign-key references that Oversqlite already treats as UUID-valued references
- `BLOB`
  - other declared BLOB columns
- `REAL`
  - declared `REAL`, `FLOAT`, or `DOUBLE` columns
- `INTEGER`
  - declared integer-like columns not classified above
- `TEXT`
  - everything else

Important:

- this spec does not require introducing app-specific semantic kinds such as date/time or enum
- those may continue to be plain `TEXT` from Oversqlite's point of view

## 2. Centralize value handling behind one typed codec boundary

Create one internal codec surface that owns:

- reading typed values from SQLite rows
- encoding local dirty payload values
- decoding local dirty payload values
- decoding authoritative wire payload values
- binding typed values back into SQLite statements
- comparing local/live and pushed values for replay

An acceptable shape is:

```kotlin
internal object OversqliteValueCodec {
    fun readLocalPayloadValue(statement: SqliteStatement, index: Int, column: ColumnInfo): JsonElement
    fun decodeLocalPayloadValue(column: ColumnInfo, value: JsonElement): TypedValue
    fun decodeWirePayloadValue(column: ColumnInfo, value: JsonElement): TypedValue
    fun bindTypedValue(statement: SqliteStatement, index: Int, column: ColumnInfo, value: TypedValue)
    fun equivalent(column: ColumnInfo, left: JsonElement, right: JsonElement): Boolean
}
```

`TypedValue` does not need to be public and does not need to mirror Kotlin domain types.

An acceptable internal representation is:

```kotlin
sealed interface TypedValue {
    data object Null : TypedValue
    data class Text(val value: String) : TypedValue
    data class Integer(val value: Long) : TypedValue
    data class Real(val canonicalText: String) : TypedValue
    data class Blob(val bytes: ByteArray) : TypedValue
}
```

The important part is not the exact type names. The important part is that equality and binding go
through one typed path.

## 3. Canonical representations by kind

### 3.1 `TEXT`

Local payload:

- JSON string carrying the exact SQLite text value

Wire payload:

- JSON string carrying the exact wire text value

Equality:

- exact string equality

### 3.2 `INTEGER`

Local payload:

- JSON number when the value is a valid integer

Wire payload:

- JSON number when the value is a valid integer

Equality:

- integer numeric equality

Important:

- Oversqlite must not compare integer columns via string rendering

### 3.3 `REAL`

Local payload:

- preserve SQLite's own numeric text as produced by SQLite, not a host-language re-rendering
- reading a local row for replay must use a SQLite-owned serialization path such as SQL-side
  `json_object(...)` generation or an equivalent SQLite text path
- driver-level `getDouble()` or driver-rendered numeric text is not acceptable for replay

Wire payload:

- accept canonical JSON numeric values from the server

Equality:

- compare by canonical numeric text produced by a dedicated real-number normalizer
- do not compare via raw source text
- do not compare via host-language `Double.toString()`

The real-number normalizer for replay must:

- accept JSON numeric text only
- reject NaN and infinities
- produce one deterministic canonical string for semantically equal finite numeric values

For this phase, one acceptable implementation is:

- parse the JSON numeric text into a decimal-normalizing form
- emit canonical plain decimal or scientific notation consistently

Another acceptable implementation is:

- store and compare the exact SQLite-produced numeric text for local reads and the exact wire numeric
  text after one shared canonicalization function

What is not acceptable:

- re-reading SQLite `REAL` values through `getDouble()` and comparing the resulting JSON text
- relying on driver-specific textual rendering of floating-point values when SQLite can provide the
  canonical JSON value directly

### 3.4 `BLOB`

Local payload:

- canonical lowercase hex string

Wire payload:

- canonical base64 string

Equality:

- byte equality

Important:

- replay and apply must never compare BLOB payloads by string encoding alone

### 3.5 `UUID_BLOB`

Local payload:

- canonical lowercase 32-hex or canonical UUID text is acceptable as input
- one canonical local representation must be chosen for dirty-row storage

Wire payload:

- canonical UUID string for UUID-valued sync keys and UUID-valued blob references

Equality:

- 16-byte equality

## 4. Replay equality must be column-wise and type-aware

Current replay logic in `OversqlitePushWorkflow.kt` uses canonicalized JSON text for whole payload
comparison. That is too weak for typed correctness.

New rule:

- replay compares row payloads column-by-column
- each column comparison uses the typed codec and the column's `ColumnKind`
- object key order and incidental numeric rendering must not affect the result

Whole-row equality is therefore:

1. both payloads must contain the same managed columns
2. for each managed column:
   - decode according to the column kind
   - compare according to that kind's equality rule

If a managed column is missing, replay must fail loudly rather than silently guess.

## 5. Binding must consume typed values, not JSON heuristics

`bindPayloadValue(...)` in `OversqliteLocalStore.kt` should stop deciding behavior from:

- `primitive.isString`
- `toLongOrNull()`
- `toDoubleOrNull()`
- repeated `declaredType.contains(...)`

Instead:

- decode once through the typed codec
- bind according to the resulting `TypedValue`

This removes a class of bugs where a string happens to look numeric or where the same lexical form
can be interpreted differently across local and wire payloads.

## 6. Post-push replay invariants

After a successful committed-bundle replay for a push initiated by this client:

- `_sync_push_outbound` must be empty for the committed bundle
- the corresponding dirty rows must either be cleared or intentionally requeued by explicit replay
  logic

Add an invariant check after successful replay:

- if committed push replay finishes with unexpected residual `_sync_dirty_rows`, fail the operation
  with a diagnostic error

The diagnostic must include enough information to identify:

- table
- key
- uploaded payload
- live payload
- per-column diff if available

This is intentionally fail-fast. Silent residual dirty state is what made the original bug confusing.

## Test Plan

## 1. Unit tests

Required in `:library:jvmTest`:

- `REAL` local-read replay regression
  - inserted SQLite `REAL`
  - push snapshot payload
  - live row re-read
  - equality holds without second sync
- `REAL` canonicalization cases
  - same numeric value with different but equivalent JSON renderings
  - reject NaN and infinities
- BLOB local-vs-wire encoding
  - local hex
  - wire base64
  - equality by bytes
- UUID blob/reference cases
  - canonical UUID wire text
  - local hex input
  - byte equality
- integer and nullable column round-trips

## 2. Fake-server contract tests

Required in the fake chunked-server suite:

- stale remote bundle + local own push + `REAL` column
- `pushPending()` leaves no phantom dirty rows
- immediate `pullToStable()` succeeds in the same sync sequence

This is the layer that should keep reproducing the exact bug we just fixed.

## 3. `nethttp_server` real-server e2e

The generic library real-server fixture must be extended to cover risky typed columns.

Required server-fixture change:

- add one dedicated synced fixture table in `nethttp_server` for typed-value coverage rather than
  expanding unrelated topology tables such as `users` or `posts`
- the preferred shape is one table, for example `typed_rows`, with columns equivalent to:
  - `id UUID PRIMARY KEY`
  - `name TEXT NOT NULL`
  - `note TEXT NULL`
  - `count BIGINT NULL`
  - `enabled BOOLEAN NOT NULL`
  - `rating DOUBLE PRECISION NULL`
  - `data BYTEA NULL`
  - `created_at TIMESTAMPTZ NULL`
- that table must be registered for sync in the `nethttp_server` fixture
- no migration support is required for this fixture change
- implementation may assume the local example database will be removed and recreated before the
  updated server is restarted

Required e2e cases:

- client A push with typed row in the dedicated typed fixture table
- client B push own typed row in the same table after stale view
- client B pull in the same sync sequence
- no second sync required
- exact value assertions for:
  - `REAL`
  - BLOB length and bytes
  - nullable columns
  - integer/boolean-like fields
  - timestamp preservation by instant semantics where offset formatting may be normalized

Important:

- do not use `samplesync-server` as a library test dependency
- the generic library real-server suite must remain based on `nethttp_server`

## Cross-client parity follow-up

The work above is implemented for the KMP Oversqlite client and verified against the shared
`nethttp_server` typed fixture. That is necessary, but not sufficient.

The Go Oversqlite client uses the same protocol and now shares the same fixture, so it must be kept
honest against the same risky type paths.

This follow-up is explicitly about parity, not about re-architecting the Go client to look like the
KMP client.

Goals:

- prove that Go Oversqlite preserves the same typed-value behavior as KMP on the shared
  `typed_rows` fixture
- catch regressions where one client treats `REAL`, nullable `BYTEA`, or timestamp-like text
  differently from the other
- keep the parity work tied to the same `nethttp_server` contract rather than creating a second
  fixture server
- keep `go test ./oversqlite` self-contained instead of making ordinary package tests depend on a
  heavyweight shared-server harness

Required scope:

- Go unit coverage for the risky typed cases that can be isolated locally
- Go fake-server or transport-level regressions where the bug class is specific to push/pull/replay
- a separate Go e2e package that owns full client/server parity against the shared
  `nethttp_server` implementation and the shared `typed_rows` fixture
  - preferred package location: a dedicated package outside `go-oversync/oversqlite`, for example
    `go-oversync/oversqlite_e2e`

Test-boundary rule for Go:

- tests that validate the `oversqlite` package in isolation stay under `go-oversync/oversqlite`
- tests that start a server, provision PostgreSQL schemas, or validate full client/server behavior
  belong in a separate Go e2e package
- existing true e2e tests that currently live under `go-oversync/oversqlite` should be moved to
  that dedicated Go e2e package as part of the parity follow-up
- `go test ./oversqlite` must remain green without booting the shared `nethttp_server` implementation
  or depending on a manually started external process

Required parity assertions:

- `REAL` values do not require a second sync after push replay
- nullable `BYTEA` remains `NULL`, not empty binary data
- non-null `BYTEA` preserves exact length and bytes
- nullable and non-null scalar fields preserve their expected values
- timestamp-like values are asserted by instant semantics where the server or driver may normalize
  offset formatting

Non-goals for the follow-up:

- forcing identical internal implementation structure across KMP and Go
- extending the typed fixture again unless a newly discovered bug class genuinely needs it
- making default `go test ./oversqlite` depend on a manually started external server

## Phase Flow

Execute the remaining work in order.

- Phase 1 establishes the KMP runtime rules
- Phase 2 proves those rules against the shared external fixture
- Phase 3 fixes the Go test boundary so parity work has the right home
- Phase 4 adds Go typed parity coverage and closes the cross-client gap

Flow constraints:

- do not reopen KMP runtime design work unless a newly discovered parity bug proves the current spec is insufficient
- do not start Go typed e2e parity inside `go-oversync/oversqlite`; Phase 3 must establish the separate Go e2e package first
- do not mark cross-client parity complete until both clients prove the same `typed_rows` contract

## Phase 1. KMP Core Canonicalization

Goal:

- make KMP Oversqlite type-aware and replay-stable without changing the wire protocol

Exit condition:

- local read, decode, bind, replay, and fail-fast invariants are implemented and covered by unit and fake-server tests

Checklist:

- [x] Step 1: add `ColumnKind` to `ColumnInfo` and centralize classification in `TableInfo.kt`
- [x] Step 2: introduce one typed codec boundary for read, local decode, wire decode, bind, and equality
- [x] Step 3: refactor local row serialization in `OversqliteLocalStore.kt` to use the typed codec
- [x] Step 4: refactor payload binding and authoritative apply paths to use the typed codec instead of ad hoc JSON heuristics
- [x] Step 5: replace replay whole-payload JSON-text equality with column-wise typed equality
- [x] Step 6: add a fail-fast residual-dirty invariant after committed push replay, with useful diagnostics
- [x] Step 7: add or expand unit tests for:
  - `REAL` replay stability
  - `REAL` canonicalization edge cases
  - BLOB local-vs-wire encoding
  - UUID BLOB/reference handling
  - integer and nullable round-trips
- [x] Step 8: add or expand fake-server contract tests for stale-remote plus own-push plus immediate-pull on typed rows

## Phase 2. Shared Fixture And KMP Real-Server Verification

Goal:

- prove the KMP typed rules against the shared `nethttp_server` fixture rather than only against fake transport

Exit condition:

- the shared typed fixture exists, KMP real-server e2e is green, and the broader verification matrix is green

Checklist:

- [x] Step 9: extend `nethttp_server` with one dedicated typed fixture table and register it for sync
- [x] Step 10: remove the old local example database for `nethttp_server` and restart the example server against the recreated schema
- [x] Step 11: add `nethttp_server` real-server e2e covering:
  - client A push typed row
  - client B own push from stale view
  - client B immediate pull without second sync
  - exact assertions for `REAL`, BLOB, nullable, integer/boolean-like, and timestamp fields
- [x] Step 12: run the full verification set for this work and keep the real-server typed-value tests green

## Phase 3. Go Test-Boundary Cleanup

Goal:

- give Go typed parity work the correct home without making `go test ./oversqlite` depend on heavyweight server setup

Exit condition:

- the Go e2e package exists, existing true e2e tests are moved, and `go test ./oversqlite` is again limited to self-contained package tests

Checklist:

- [x] Step G1: inventory current Go Oversqlite typed coverage and map it to the shared `typed_rows`
  contract
  - inspect existing Go tests under `go-oversync/oversqlite`
  - identify which `typed_rows` fields are already covered and which are missing
  - identify which existing Go tests are true e2e and therefore should move out of the
    `oversqlite` package
  - record concrete missing cases for `REAL`, nullable `BYTEA`, non-null `BYTEA`, nullable scalar
    fields, and timestamp semantics before changing tests
- [x] Step G2: create the separate Go e2e package and move existing true e2e tests into it
  - introduce a dedicated Go e2e package for shared-server client/server tests
  - preferred location: `go-oversync/oversqlite_e2e` unless a better existing integration-test
    package boundary already exists
  - move existing tests such as the current Go oversqlite end-to-end and conflict end-to-end flows
  out of `go-oversync/oversqlite` into that package
  - keep `go test ./oversqlite` focused on self-contained package tests after the move

## Phase 4. Go Typed Parity Coverage

Goal:

- prove Go Oversqlite behaves like KMP on the shared typed fixture and close the remaining cross-client gap

Exit condition:

- Go unit, regression, and e2e typed coverage is green and the shared-contract parity gate passes for both clients

Checklist:

- [x] Step G3: add the smallest missing Go unit regressions for value-shape handling
  - add unit-level regressions where no server round-trip is needed
  - cover exact risky shapes, especially:
    - equivalent `REAL` values that must not force a second sync
    - nullable `BYTEA` staying `NULL` instead of empty bytes
    - non-null `BYTEA` exact byte preservation
    - timestamp/text assertions using instant semantics where offset normalization is possible
  - keep these tests close to the current Go codec/replay helpers rather than hiding them in large
    end-to-end cases
- [x] Step G4: add Go push/pull/replay regression tests on the shared typed fixture
  - extend Go Oversqlite tests to create and sync `typed_rows`
  - add the exact stale-remote plus own-push plus immediate-pull regression for a `REAL` row
  - add a nullable `BYTEA` regression on the same fixture
  - assert exact row state after replay instead of broad convergence-only checks
- [x] Step G5: add Go e2e parity tests against the shared `nethttp_server` implementation and the
  shared `typed_rows` table
  - use the `nethttp_server` server package from the new Go e2e package
  - do not require a manually started external server for default Go e2e runs
  - create at least one typed-row scenario that exercises:
    - peer row already on server
    - local typed row push from stale view
    - immediate pull without second sync
    - exact assertions for `REAL`, nullable `BYTEA`, non-null `BYTEA`, nullable scalar fields, and
      timestamp semantics
- [x] Step G6: run Go-side verification for the parity follow-up
  - run `go test ./oversqlite` and keep it free of moved e2e/server-boot responsibilities
  - run focused Go Oversqlite unit/regression tests first
  - run the new separate Go e2e package
  - run `go test ./examples/nethttp_server/...`
- [x] Step G7: run cross-client verification on the shared fixture
  - rerun the KMP typed real-server test and the Go typed real-server test against the same
    `typed_rows` contract
  - the clients do not need to hit the same manually running process, but they must prove against
    the same `nethttp_server` implementation and fixture semantics
  - only mark parity complete once both clients pass on the same contract

## Acceptance Criteria

- no replay path depends on `Double.toString()` or equivalent host-language float rendering
- no BLOB/UUID equality depends on chosen string encoding
- type classification is defined in one place and reused by read, decode, bind, and compare paths
- the `REAL` stale-remote replay bug is covered by unit and fake-server tests
- `nethttp_server` has at least one real-server test that exercises typed-value replay, including a
  `REAL` column
- library tests do not depend on `samplesync-server`
- the spec remains explicit about implementation state: KMP work complete, Go parity follow-up
  pending
- Go parity follow-up is only complete once Phases 3-4 are complete, Steps G1-G7 are checked off,
  and the moved Go e2e tests no longer live under `go-oversync/oversqlite`
