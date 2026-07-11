# Oversqlite Shared Contracts

This directory contains language-neutral Oversqlite fixtures shared by the KMP
reference implementation and the Dart port.

Contract fixtures are split into wire fixtures, behavior fixtures, and runtime
SQL-state fixtures. Wire fixtures pin protocol and JSON shapes. Behavior
fixtures execute runtime workflows and pin user-visible outcomes plus selected
durable state. Runtime-state fixtures pin the local `_sync_*` SQL catalog and
durable SQL rows directly. Any KMP or Dart change that intentionally changes
local sync schema, trigger behavior, dirty-row capture, wire protocol behavior,
or final sync state must update or add the relevant fixture in the same change.

Pull/snapshot fixtures lock retained-history checkpoint recovery. `history_pruned` is reserved for
unavailable past history, while `checkpoint_ahead` rejects a checkpoint beyond current server
history. KMP and Dart must persist the rebuild gate before snapshot work, automatically resume
checkpoint recovery from normal sync/pull, preserve unresolved offline work in the durable outbox,
and change the checkpoint only in the final authoritative snapshot transaction.

## Canonical JSON and numeric contract

`canonical-json/jcs-typed-numerics.json` is authoritative for KMP and Dart and is mirrored by
equivalent Go vectors with a checksum drift guard. Canonical bytes use RFC 8785 JCS; this is not an
arbitrary-precision extension to JCS. Schema-typed exact int64 and decimal values are JSON strings,
while approximate values are finite binary64 JSON numbers. Hash-only row ordinals and row/base
versions are decimal strings in the logical hash input.

The fixture also defines ordinary SQLite `INTEGER`/`TEXT`/`REAL` binding and rejection behavior,
request and committed hashes, and the fresh-database compatibility disposition. No custom SQLite
declared type, canonicalization fallback, compatibility alias, or mixed-version fixture is
permitted.

Cross-implementation realserver conformance is part of the same drift-prevention
workflow. Behavior changes that affect live Oversync server semantics must keep
the shared fixtures current and must run the relevant KMP realserver wrapper
plus the Dart opt-in realserver test before the contract change is accepted.
Each required lane starts against a newly recreated PostgreSQL database and fresh local client
databases. Client tests do not reset a running server through HTTP.

Local schema fixtures live under `local-schema/<fixture-name>/`:

- `schema.sql` creates the application schema.
- `config.json` declares the managed sync tables.
- `write-transitions.json` declares local write-transition SQL cases.
- `schema.expected.json` is the canonical SQLite catalog after KMP local
  runtime initialization.
- `write-transitions.expected.json` is the expected dirty-row and application
  row-count output after each declared transition.
- `business-rich-v0` is the canonical live-server application schema contract
  used by KMP, Dart, `examples/nethttp_server`, and `examples/mobile_flow`.
  Keep the protocol version (`protocol_version: "v1"`) and numeric
  `schema_version` values separate from this fixture name. The fixture name
  describes application schema shape, not the wire protocol generation.

Rich schema fixtures live under `rich-schema/`:

- `business-rich-v0.json` is the logical manifest for the live-server rich
  schema. It pins table names, generated sync keys, columns, primary keys,
  foreign keys, nullability, and logical SQLite types.
- KMP runtime and generated-database tests plus Dart generated-database tests
  must consume this manifest. Do not update Dart `RichRealServerDb`, KMP
  generated realserver fixtures, or manual KMP rich realserver schemas without
  updating this manifest in the same change.
- The manifest describes the application schema contract only. It does not
  introduce runtime metadata versioning and must stay separate from Oversqlite
  protocol versions and local `_sync_*` schema versions.

Local schema/config validation fixtures live in
`local-schema/config-validation.json`:

- Cases declare `schemaSql`, an Oversqlite config shape, optional
  post-initialization SQL, expected query results, and explicit expected error
  message fragments.
- The fixture covers composite primary keys, composite foreign keys,
  unsupported integer-like key types, accepted BLOB primary keys, and rejection
  of nullable `TEXT`/`BLOB` visible keys before any `_sync_*` runtime mutation,
  application-owned `_sync_*` tables, anonymous apply-mode suppression, and
  invalid table registrations.
- Sync-managed local visible keys must declare `NOT NULL` explicitly even when
  SQLite reports the column as the primary key. Existing NULL rows and schema
  repair/recreation remain application-owned; rejection preserves prior local
  rows, checkpoints, sources, outbox state, and triggers.
- Runners must fail on declared message fragments or exception categories, not
  incidental stack traces. Dart does not add Dart-only runtime metadata or
  compatibility migrations; local development databases should be recreated
  when shared `_sync_*` contracts change.

Protocol HTTP request fixtures live under `protocol-http/`:

- `requests.json` records request method, base-path-preserving path, query
  parameters, `Oversync-Source-ID` header, and request body for every public
  Oversqlite HTTP operation.
- Source identity is a transport header contract. Request bodies for connect,
  push session create/chunk/commit, snapshot session, source replacement, and
  watch must not contain top-level `source_id`.
- KMP and Dart runners must exercise their real request-building code. Dart
  uses `IoOversqliteHttpClient` against a local server so base URI resolution is
  covered by the fixture.

Protocol handshake fixtures live under `protocol-handshake/`:

- `connect.json` records shared capabilities and connect response shapes.
- Each case includes the capabilities response, optional connect response,
  whether local pending rows should be advertised, and the expected lifecycle
  interpretation.
- Runners must verify `GET sync/capabilities`, `POST sync/connect`, the
  `Oversync-Source-ID` request header, connect request JSON, unsupported
  capability behavior, retry-later behavior, started-empty attach, local-seed
  attach, and remote-authoritative fail-closed behavior for phases that have
  not yet implemented snapshot hydrate.

Push fixtures live under `push/`:

- `basic-insert.json` records the push-session create, chunk, commit,
  committed-replay, conflict, and expected local state shapes for the first
  outbound wire slice.
- `behavior/recovery.json` executes the shared push recovery behavior contract.
  Strict `already_committed` mismatches must remain source-sequence errors with
  the prepared outbox intact. Replayed `committed_remote` mismatches and pruned
  committed replay must mark `rebuild_required` and preserve the committed
  outbox for snapshot rebuild. Bad committed replay `bundle_hash` responses
  must fail as protocol errors without clearing the committed outbox.
  Transient `committed_bundle_not_found` responses must be retried before the
  committed outbox is classified as failed. Pre-commit retry must keep the
  prepared outbox replayable.
- `behavior/apply.json` executes committed-replay/apply-mode behavior that is
  easy for runtimes to drift on. Committed replay must run with deferred foreign
  keys, and committed push must not skip `last_bundle_seq_seen` when the server
  commits a non-contiguous bundle sequence. Strict committed replay compares
  payloads with table-column semantics, including RFC3339-equivalent timestamp
  instants, numeric representation equivalence, and NULL BLOB fields, while
  still rejecting typed values that are not semantically equivalent.
- Wire runners decode the same request and response bodies in KMP and Dart.
  Behavior runners execute the same fixture cases in KMP and Dart and assert
  final durable `_sync_*` state.

Pull and snapshot fixtures live under `pull-snapshot/`:

- `basic.json` records populated-table adoption baseline history, incremental
  pull responses, history-pruned fallback,
  snapshot session/chunk responses, source replacement requests, source
  replacement errors, expected post-apply state summaries, and validation-only
  wire cases. Validation-only cases declare `expectedPullErrorContains`,
  `expectedSnapshotSessionErrorContains`, or
  `expectedSnapshotChunkErrorContains`; chunk validation may also declare
  `snapshotChunkAfterRowOrdinal`.
- `behavior/apply.json` executes incremental-pull and snapshot-rebuild
  behavior. Pull application and final snapshot application must run with
  deferred foreign keys so authoritative rows can contain cyclic app-table rows.
  Pull scripts may use `pull_incremental_bundles` or `pull_sequence`; snapshot
  scripts use `snapshot_sequence` with ordered sessions and per-snapshot chunk
  maps. Steps may use `setupSql` to seed fixture-local durable `_sync_*` state
  before invoking public client actions.
- Pull/snapshot behavior fixtures may assert durable state such as
  `lastBundleSeqSeen`, `rebuildRequired`, snapshot-stage row counts, outbox row
  counts, row-state counts, and source-rotation replacement rows when app-table
  state alone is not enough to prove parity.
- Runners should decode and validate the same pull, snapshot, and recovery
  bodies in KMP and Dart, with executable workflow tests proving the final
  local `_sync_*` state.

Watch fixtures live under `watch/`:

- `basic.json` records enabled, disabled, and omitted `bundle_change_watch`
  capability responses, canonical `event: bundle` SSE lines, heartbeat
  comments, unknown events, malformed bundle events, and representative watch
  setup error bodies.
- `behavior/automatic-downloads.json` executes the shared automatic-download
  behavior contract for non-OK watch setup fallback, malformed stream fallback,
  EOF reconnect, heartbeat-only streams, held-open healthy streams, explicit
  stop/cancel of an active stream, durable reconnect catch-up from
  `last_bundle_seq_seen`, unsupported-server polling fallback, and
  server-originated write wakeups.
- A healthy open `/sync/watch` stream is a wake-up channel and suppresses timer
  fallback pulls while it remains active. `/sync/pull` remains the authoritative
  data path after watch events, EOF, malformed streams, setup failures, and
  explicit stop/cancel.
- KMP and Dart runners must consume this same fixture file. Watch fixtures are
  read-only verification fixtures; there is no regeneration command because
  the upstream `/sync/watch` wire shape is owned by
  `/Users/pochkin/Projects/my/go-oversync/swagger/two_way_sync.yaml`.

Invalidation behavior fixtures live under `invalidation/behavior/`:

- `basic.json` executes the shared affected-table invalidation contract for
  incremental pull, snapshot hydrate, committed replay, conflict rewrite, sync
  union, successful detach, blocked detach, and no-op detach.
- Cases declare the watched app tables and whether an emission is expected.
  Runtime metadata-only changes must not emit app-table invalidation.
- KMP runners assert `SqliteNowDatabase` reactive flows. Dart runners assert
  `SqliteNowDatabase.invalidationTracker.watchTables(...)`.

Lifecycle behavior fixtures live under `lifecycle/behavior/`:

- `basic.json` executes attach, `syncThenDetach`, explicit rebuild, source-info,
  detach, restart, remote-authoritative replace, and cleanup-rollback flows that
  should not drift between runtimes.
- `source-recovery-replacement.json` executes persisted source-recovery
  replacement flows. It pins reuse of an existing replacement source, fail-closed
  divergent remote replacements, stale or non-fresh replacement source rows,
  invalid replacement snapshot responses, and unknown persisted recovery reasons.
- Scripted snapshot-session errors may include `sourceId` and
  `replacedBySourceId`; runners emit them as the wire fields `source_id` and
  `replaced_by_source_id`.
- Lifecycle fixtures are behavior fixtures with stronger local-state checks:
  they assert public operation results plus the normalized durable `_sync_*`
  rows after each fixture step.
- Add lifecycle cases when attach/detach/restart/recovery flow logic changes.
  Add or update runtime-state fixtures when the SQL catalog or durable internal
  transition itself changes independently of public lifecycle flow.

Runtime SQL-state fixtures live under `runtime-state/`:

- `schema/v0.json` records the normalized `_sync_*` table catalog after opening
  a fresh users database: table names, columns, indexes, dirty/guard triggers,
  singleton/default rows, managed table rows, source rows, and runtime-owned
  PRAGMA expectations.
- `transitions/basic.json` executes durable mutation scenarios that should not
  drift between runtimes: bootstrap state, prepared outbox freeze,
  committed-remote replay, local-intent rebase, pull checkpoint advance, and
  source-recovery gates.
- Runtime-state fixtures assert internal SQL state directly. Behavior fixtures
  still own user-visible workflow outcomes. Live server semantic changes still
  require realserver coverage in addition to fixture updates.
- Dart does not support in-place migration between incompatible local metadata
  layouts. Delete and recreate Dart development databases when `_sync_*`
  contracts change. Any metadata versioning must be shared across KMP and Dart
  rather than Dart-only.

Protocol/JSON field changes require wire fixture updates. Retry, rebuild,
conflict, source recovery, attach/detach lifecycle, outbox, dirty-row, or
automatic-download behavior changes require behavior fixture updates. `_sync_*`
SQL schema, trigger, index, checkpoint, outbox, dirty-row replay, source
recovery, or rebuild gate changes require runtime-state fixture updates.
Server-semantic changes require realserver coverage in addition to fixture
tests. A behavior, lifecycle, or runtime-state change is not complete until both
KMP and Dart shared fixture runners pass.

Run the KMP shared local contract runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedLocalContractFixtureTest
```

To intentionally refresh expected local contract outputs after a reviewed
behavior change:

```shell
OVERSQLITE_CONTRACTS_UPDATE=true ./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedLocalContractFixtureTest
```

Run the KMP shared local config validation runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedLocalConfigFixtureTest
```

Run the KMP shared protocol handshake runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedProtocolHandshakeFixtureTest
```

Run the KMP shared protocol HTTP request runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedProtocolHttpRequestFixtureTest
```

Run the KMP shared push fixture runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedPushFixtureTest
```

Run the KMP shared push behavior fixture runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedPushBehaviorFixtureTest
```

Run the KMP shared pull/snapshot fixture runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedPullSnapshotFixtureTest
```

Run the KMP shared pull/snapshot behavior fixture runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedPullSnapshotBehaviorFixtureTest
```

Run the KMP shared watch fixture runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedBundleChangeWatchFixtureTest
```

Run the KMP shared watch behavior fixture runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedWatchBehaviorFixtureTest
```

Run the KMP shared rich-schema manifest runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedRichSchemaManifestFixtureTest
```

Run the KMP generated rich-schema manifest runner with:

```shell
./gradlew :platform-oversqlite-test:composeApp:jvmTest --tests dev.goquick.sqlitenow.oversqlite.platform.GeneratedRichSchemaManifestJvmTest
```

Run the KMP shared invalidation behavior fixture runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedInvalidationBehaviorFixtureTest
```

Run the KMP shared lifecycle behavior fixture runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedLifecycleBehaviorFixtureTest
```

Run the KMP shared runtime-state fixture runners with:

```shell
./gradlew :library-oversqlite:jvmTest \
  --tests dev.goquick.sqlitenow.oversqlite.SharedRuntimeStateSchemaFixtureTest \
  --tests dev.goquick.sqlitenow.oversqlite.SharedRuntimeStateTransitionFixtureTest
```

To intentionally refresh KMP-owned runtime-state expected outputs after a
reviewed `_sync_*` or lifecycle-flow state change:

```shell
OVERSQLITE_RUNTIME_STATE_CONTRACTS_UPDATE=true ./gradlew :library-oversqlite:jvmTest \
  --tests dev.goquick.sqlitenow.oversqlite.SharedRuntimeStateSchemaFixtureTest \
  --tests dev.goquick.sqlitenow.oversqlite.SharedRuntimeStateTransitionFixtureTest \
  --tests dev.goquick.sqlitenow.oversqlite.SharedLifecycleBehaviorFixtureTest
```

Run the Dart shared watch fixture runner with:

```shell
cd dart && flutter test packages/sqlitenow_oversqlite/test/bundle_change_watch_test.dart
```

Run the Dart shared behavior fixture runners with:

```shell
cd dart && flutter test \
  packages/sqlitenow_oversqlite/test/lifecycle_behavior_fixture_test.dart \
  packages/sqlitenow_oversqlite/test/runtime_state_schema_fixture_test.dart \
  packages/sqlitenow_oversqlite/test/runtime_state_transition_fixture_test.dart \
  packages/sqlitenow_oversqlite/test/local_config_fixture_test.dart \
  packages/sqlitenow_oversqlite/test/push_behavior_fixture_test.dart \
  packages/sqlitenow_oversqlite/test/shared_pull_snapshot_fixture_test.dart \
  packages/sqlitenow_oversqlite/test/pull_snapshot_behavior_fixture_test.dart \
  packages/sqlitenow_oversqlite/test/watch_behavior_fixture_test.dart \
  packages/sqlitenow_oversqlite/test/invalidation_behavior_fixture_test.dart \
  packages/sqlitenow_oversqlite/test/protocol_http_request_fixture_test.dart
```

After starting `go-oversync/examples/nethttp_server`, run Dart live server
conformance with:

```shell
cd dart && OVERSQLITE_REALSERVER_TESTS=true flutter test packages/sqlitenow_oversqlite/test/realserver_conformance_test.dart
```

Run Dart generated rich-schema live server conformance with:

```shell
cd dart && OVERSQLITE_REALSERVER_TESTS=true flutter test packages/sqlitenow_oversqlite/test/realserver_rich_schema_test.dart
```

Run Dart generated metadata and rich-schema manifest coverage with:

```shell
cd dart && flutter test packages/sqlitenow_oversqlite/test/generated_metadata_test.dart
```

Run Dart live-server conformance, watch, and rich-schema wrappers with:

```shell
cd dart && scripts/oversqlite_realserver_all.sh
```

Run Dart heavy live-server stress coverage with:

```shell
cd dart && OVERSQLITE_REALSERVER_TESTS=true OVERSQLITE_REALSERVER_HEAVY=true flutter test packages/sqlitenow_oversqlite/test/realserver_heavy_test.dart
```

Run the Dart heavy live-server wrapper with:

```shell
cd dart && scripts/oversqlite_realserver_all_heavy.sh
```

Dart wrapper defaults match the KMP host and Android conventions:
`OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL` defaults to `http://localhost:8080`
and `OVERSQLITE_ANDROID_REAL_SERVER_SMOKE_BASE_URL` defaults to
`http://10.0.2.2:8080`. Set `OVERSQLITE_RUN_FLUTTER_ANDROID_SMOKE=true` to
include the Flutter Android smoke test in the Dart wrapper.
