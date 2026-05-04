# Oversqlite Shared Contracts

This directory contains language-neutral Oversqlite fixtures shared by the KMP
reference implementation and the Dart port.

Contract fixtures are part of the sync behavior contract. Any KMP or Dart
change that intentionally changes local sync schema, trigger behavior,
dirty-row capture, wire protocol behavior, or final sync state must update or
add the relevant fixture in the same change.

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
  outbound workflow slice.
- Runners should decode the same request and response bodies in KMP and Dart
  and keep final dirty/outbox/row-state expectations aligned with executable
  workflow tests.

Pull and snapshot fixtures live under `pull-snapshot/`:

- `basic.json` records incremental pull responses, history-pruned fallback,
  snapshot session/chunk responses, source replacement requests, source
  replacement errors, and expected post-apply state summaries.
- Runners should decode and validate the same pull, snapshot, and recovery
  bodies in KMP and Dart, with executable workflow tests proving the final
  local `_sync_*` state.

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

Run the KMP shared pull/snapshot fixture runner with:

```shell
./gradlew :library-oversqlite:jvmTest --tests dev.goquick.sqlitenow.oversqlite.SharedPullSnapshotFixtureTest
```

After starting `go-oversync/examples/nethttp_server`, run Dart live server
conformance with:

```shell
cd dart && OVERSQLITE_REALSERVER_TESTS=true flutter test packages/sqlitenow_oversqlite/test/realserver_conformance_test.dart
```

Run Dart heavy live-server stress coverage with:

```shell
cd dart && OVERSQLITE_REALSERVER_TESTS=true OVERSQLITE_REALSERVER_HEAVY=true flutter test packages/sqlitenow_oversqlite/test/realserver_heavy_test.dart
```
