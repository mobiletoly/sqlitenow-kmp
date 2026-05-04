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
OVERSQLITE_REALSERVER_TESTS=true flutter test packages/sqlitenow_oversqlite/test/realserver_conformance_test.dart
```

Run the opt-in heavy realserver stress suite with:

```shell
OVERSQLITE_REALSERVER_TESTS=true OVERSQLITE_REALSERVER_HEAVY=true flutter test packages/sqlitenow_oversqlite/test/realserver_heavy_test.dart
```

Set `OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL` to override the default
`http://localhost:8080`.

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
| Rich schema, typed rows, BLOB, cascade, and FK topology stress | Follow-up gap for Dart generated rich-schema realserver fixtures. The current Dart heavy suite uses the manual business subset schema already used by normal Dart realserver conformance. |
