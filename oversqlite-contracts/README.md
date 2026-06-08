# Oversqlite Shared Contracts

This directory contains language-neutral Oversqlite fixtures shared by the KMP
reference implementation and the Dart port.

Contract fixtures are split into wire fixtures and behavior fixtures. Wire
fixtures pin protocol and JSON shapes. Behavior fixtures execute runtime
workflows and pin durable local `_sync_*` and application-table outcomes. Any
KMP or Dart change that intentionally changes local sync schema, trigger
behavior, dirty-row capture, wire protocol behavior, or final sync state must
update or add the relevant fixture in the same change.

Cross-implementation realserver conformance is part of the same drift-prevention
workflow. Behavior changes that affect live Oversync server semantics must keep
the shared fixtures current and must run the relevant KMP realserver wrapper
plus the Dart opt-in realserver test before the phase is considered complete.

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
  outbox for snapshot rebuild. Pre-commit retry must keep the prepared outbox
  replayable.
- Wire runners decode the same request and response bodies in KMP and Dart.
  Behavior runners execute the same fixture cases in KMP and Dart and assert
  final durable `_sync_*` state.

Pull and snapshot fixtures live under `pull-snapshot/`:

- `basic.json` records incremental pull responses, history-pruned fallback,
  snapshot session/chunk responses, source replacement requests, source
  replacement errors, and expected post-apply state summaries.
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
  EOF reconnect, and heartbeat-only streams.
- KMP and Dart runners must consume this same fixture file. Watch fixtures are
  read-only verification fixtures; there is no regeneration command because
  the upstream `/sync/watch` wire shape is owned by
  `/Users/pochkin/Projects/my/go-oversync/swagger/two_way_sync.yaml`.

Protocol/JSON field changes require wire fixture updates. Retry, rebuild,
conflict, source recovery, outbox, dirty-row, or automatic-download behavior
changes require behavior fixture updates. Server-semantic changes require
realserver coverage in addition to fixture tests. A behavior change is not
complete until both KMP and Dart behavior fixture runners pass.

Run the KMP shared local contract runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedLocalContractFixtureTest
```

To intentionally refresh expected local contract outputs after a reviewed
behavior change:

```shell
OVERSQLITE_CONTRACTS_UPDATE=true ./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedLocalContractFixtureTest
```

Run the KMP shared protocol handshake runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedProtocolHandshakeFixtureTest
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

Run the KMP shared watch fixture runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedBundleChangeWatchFixtureTest
```

Run the KMP shared watch behavior fixture runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedWatchBehaviorFixtureTest
```

Run the Dart shared watch fixture runner with:

```shell
cd dart && flutter test packages/sqlitenow_oversqlite/test/bundle_change_watch_test.dart
```

Run the Dart shared behavior fixture runners with:

```shell
cd dart && flutter test \
  packages/sqlitenow_oversqlite/test/push_behavior_fixture_test.dart \
  packages/sqlitenow_oversqlite/test/watch_behavior_fixture_test.dart
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

Run Dart heavy live-server stress coverage with:

```shell
cd dart && OVERSQLITE_REALSERVER_TESTS=true OVERSQLITE_REALSERVER_HEAVY=true flutter test packages/sqlitenow_oversqlite/test/realserver_heavy_test.dart
```
