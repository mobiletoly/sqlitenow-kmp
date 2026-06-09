# sqlitenow_oversqlite

Dart Oversqlite runtime support for SQLiteNow generated sync metadata.

The package includes local sync metadata, lifecycle client state, HTTP protocol
handshake, push, pull, snapshot rebuild, conflict resolution, and realserver
conformance coverage for the Oversqlite protocol.

Use this package together with generated Dart code from `sqlitenow_cli`
when the database config sets `oversqlite: true`.

Run normal package coverage with:

```shell
flutter test packages/sqlitenow_oversqlite
```

Run live realserver conformance after starting
`go-oversync/examples/nethttp_server`:

```shell
scripts/oversqlite_realserver_all.sh
```

Run the opt-in heavy realserver stress suite with:

```shell
scripts/oversqlite_realserver_all_heavy.sh
```

Set `OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL` to override the default
`http://localhost:8080`. The optional Flutter Android live-server suite uses
`OVERSQLITE_ANDROID_REAL_SERVER_SMOKE_BASE_URL`, defaulting to
`http://10.0.2.2:8080`, and runs when
`OVERSQLITE_RUN_FLUTTER_ANDROID_SMOKE=true`.

## Heavy Realserver Parity

The Dart heavy suite targets behavior parity with the KMP realserver `*Heavy`
wrappers, not a one-to-one copy of every KMP platform wrapper.

| KMP heavy behavior group | Dart heavy coverage |
| --- | --- |
| Multi-chunk push, pull, committed replay, and snapshot retrieval | Covered by `small chunks and interleaved same-user writers converge` and `stale follower pruned under load rebuilds from snapshot` with intentionally small upload and download limits. Dart uses `downloadLimit` for snapshot chunk fetch size. |
| Long-horizon writer/follower convergence | Covered by repeated writer/follower rounds in the interleaved writer and stale follower tests. |
| Long-horizon divergent writer convergence | Covered by repeated client-wins conflict rounds after prior bundles. |
| Stale follower after history pruning | Covered through the retained-floor test endpoint and snapshot rebuild convergence. |
| Repeated conflict convergence | Covered by repeated `ClientWinsResolver` conflicts after prior committed bundles. |
| Source recovery or source retirement | Covered after several committed bundles, including old-source rejection and follow-up sync through the replacement source. |
| Same-user multi-device convergence | Covered by two Dart writers plus an observer converging through the same user scope. |
| Shared connection or concurrent local usage stress | Covered by concurrent reads while a shared Dart database catches up through live pulls. Dart does not expose the KMP alias-star generated query surface, so that exact generated-query shape is not applicable. |
| Rich schema, typed rows, BLOB, cascade, and FK topology stress | Covered by `realserver_rich_schema_test.dart` using generated Dart oversqlite metadata. It exercises FK topology, typed nullable rows, BLOB payloads, BLOB primary-key sync tables, and cascade behavior. |

Flutter Android runtime coverage lives in the `flutter_todo` example as an
opt-in integration suite. After starting the real server and an Android
emulator, run from the Dart workspace:

```shell
OVERSQLITE_RUN_FLUTTER_ANDROID_SMOKE=true scripts/oversqlite_realserver_all.sh
```

The flag name is retained for compatibility, but it now runs the expanded
Flutter Android live-server suite: smoke, lifecycle, conflicts, rich generated
schema, and bundle-change watch. To include scaled Android stress coverage for
chunked push, stale follower snapshot rebuild, and concurrent reads while
syncing, run the heavy wrapper with both flags:

```shell
OVERSQLITE_RUN_FLUTTER_ANDROID_SMOKE=true scripts/oversqlite_realserver_all_heavy.sh
```

For a single Flutter Android test file, pass the same Dart defines directly:

```shell
cd examples/flutter_todo
flutter test integration_test/realserver_smoke_test.dart -d emulator-5554 \
  --dart-define=OVERSQLITE_REALSERVER_TESTS=true \
  --dart-define=OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL=http://10.0.2.2:8080
```

Flutter Android intentionally reuses host Dart realserver coverage for broad
protocol matrix confidence. The Android suite repeats the contract families
that exercise device runtime behavior: file-backed SQLite, generated metadata,
IO HTTP/SSE transport, source lifecycle, conflict recovery, chunking, snapshot
rebuild, typed/BLOB rows, cascades, and concurrent reads while syncing.
