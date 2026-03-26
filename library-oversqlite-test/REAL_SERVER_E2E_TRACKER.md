# Real-Server E2E Tracker

This tracker covers the manual opt-in KMP `oversqlite` tests that run against a manually started
`go-oversync` `nethttp_server`.

Scope:

- Android emulator/device tests only
- real HTTP against `nethttp_server`
- not part of regular automated test runs

## How To Run

Start `nethttp_server` from `go-oversync` first, then run:

```bash
./gradlew :library-oversqlite-test:composeApp:connectedAndroidDeviceTest \
  -Pandroid.testInstrumentationRunnerArguments.class=dev.goquick.sqlitenow.oversqlite.e2e.RealServerBasicContractTest \
  -Pandroid.testInstrumentationRunnerArguments.oversqliteRealServer=true \
  -Pandroid.testInstrumentationRunnerArguments.oversqliteE2EBaseUrl=http://10.0.2.2:8080 \
  --no-daemon
```

The tests reset the local example server state through `POST /test/reset` before each scenario.
The tracked phase-8 key-shape contract for this lane is:

- local sync keys may be `TEXT PRIMARY KEY` or `BLOB PRIMARY KEY`
- generated annotation-driven sync config and manual `SyncTable(...)` config should both work
- unsupported integer-like sync keys should fail before any sync mutation is attempted

## Current Coverage

### RealServerBasicContractTest

File:
- `library-oversqlite-test/composeApp/src/androidDeviceTest/kotlin/dev/goquick/sqlitenow/oversqlite/e2e/RealServerBasicContractTest.kt`

Checklist:
- [x] Bootstrap two devices for one user against the real server
- [x] Push from device A and pull on device B using `users` and `posts`
- [x] Verify both devices converge on the committed server state
- [x] Hydrate a fresh device from real-server snapshot state
- [x] Reset example server state before each scenario
- [x] Manual `SyncTable(...)` config remains covered for `TEXT PRIMARY KEY` local sync tables

## Planned Next Slices

### RealServerConflictTest

Goal:
- same-user two-device conflicting edits against real `nethttp_server`

File:
- `library-oversqlite-test/composeApp/src/androidDeviceTest/kotlin/dev/goquick/sqlitenow/oversqlite/e2e/RealServerConflictTest.kt`

Checklist:
- [x] Device A and device B modify the same row independently
- [x] Push in conflicting order and verify a clean observer still converges to server-authoritative state
- [x] Assert the conflicting device keeps local dirty rows and remains pull-blocked until the caller resolves it

### RealServerFkTopologyTest

Goal:
- exercise the richer server schema beyond `users` and `posts`

File:
- `library-oversqlite-test/composeApp/src/androidDeviceTest/kotlin/dev/goquick/sqlitenow/oversqlite/e2e/RealServerFkTopologyTest.kt`

Tables:
- `categories`
- `teams`
- `team_members`

Checklist:
- [x] Self-referential rows survive hydrate and pull apply
- [x] FK cycle rows survive hydrate and pull apply
- [x] Multi-device convergence holds for supported FK topology

### RealServerBlobAndCascadeTest

Goal:
- verify BLOB payloads and dependent-row behavior against the real server

File:
- `library-oversqlite-test/composeApp/src/androidDeviceTest/kotlin/dev/goquick/sqlitenow/oversqlite/e2e/RealServerBlobAndCascadeTest.kt`

Tables:
- `files`
- `file_reviews`

Checklist:
- [x] Push and pull `BLOB PRIMARY KEY` local sync rows through real HTTP
- [x] Hydrate BLOB-backed rows from snapshot state
- [x] Dependent-row behavior converges across devices

### RealServerGeneratedConfigTest

Goal:
- verify annotation-generated sync table metadata works end-to-end against the real server and
  keep an explicit rejection lane for unsupported local key types

File:
- `library-oversqlite-test/composeApp/src/androidDeviceTest/kotlin/dev/goquick/sqlitenow/oversqlite/e2e/RealServerGeneratedConfigTest.kt`

Checklist:
- [x] Generated annotation-driven sync config converges against real `nethttp_server`
- [x] Generated sync metadata remains explicit in client setup, not implicit runtime defaults
- [x] Unsupported integer local sync keys fail bootstrap before sync mutation is attempted

### RealServerRecoveryTest

Goal:
- cover destructive rebuild and source rotation semantics against the real server

File:
- `library-oversqlite-test/composeApp/src/androidDeviceTest/kotlin/dev/goquick/sqlitenow/oversqlite/e2e/RealServerRecoveryTest.kt`

Checklist:
- [x] Recover rebuilds local state from snapshot
- [x] Recover rotates local source identity as expected
- [x] Post-recover sync continues successfully

### RealServerStressTest

Goal:
- repeated multi-round push/pull churn against the real server with enough rows to force
  multiple pull roundtrips and multiple `pushPending()` rounds per device on the baseline
  `users` and `posts` tables

File:
- `library-oversqlite-test/composeApp/src/androidDeviceTest/kotlin/dev/goquick/sqlitenow/oversqlite/e2e/RealServerStressTest.kt`

Checklist:
- [x] Device A seeds a large enough dataset through multiple `pushPending()` rounds
- [x] Device B pulls the dataset through multiple pull roundtrips and converges fully
- [x] Device B adds another large enough change set through multiple `pushPending()` rounds
- [x] Device A pulls the return change set through multiple pull roundtrips and converges fully
- [x] Multiple rounds of edits across two devices
- [x] Final convergence after repeated push/pull cycles
- [x] No local dirty rows remain after the run

Suggested shape:
- configure low `downloadLimit` in the test client to force repeated pull roundtrips
- use multiple `pushPending()` rounds per device; the dedicated chunked-push lane covers
  one-round dirty sets that exceed a single upload chunk
- use one shared `userId` with two different `sourceId`s
- assert final row counts and representative row contents on both devices

### RealServerChunkedPushTransportTest

Goal:
- prove the Android real-server lane succeeds when one local dirty set exceeds a single upload
  chunk

File:
- `library-oversqlite-test/composeApp/src/androidDeviceTest/kotlin/dev/goquick/sqlitenow/oversqlite/e2e/RealServerChunkedPushTransportTest.kt`

Checklist:
- [x] One device accumulates an offline dirty set larger than `uploadLimit` before calling `pushPending()`
- [x] The push succeeds through the real server in one logical bundle
- [x] A second device pulls the committed rows and converges fully
- [x] No local dirty rows remain after the run

### RealServerRichSchemaStressTest

Goal:
- stress the richer `nethttp_server` schema under multi-roundtrip sync, not just the baseline
  `users` and `posts` pair

Tables:
- `categories`
- `teams`
- `team_members`
- `files`
- `file_reviews`

Checklist:
- [x] Large-volume self-referential `categories` data converges across two devices
- [x] Large-volume cyclic `teams` and `team_members` data converges across two devices
- [x] Large-volume `files` and `file_reviews` data converges across two devices
- [x] Rich-schema stress still requires multiple pull roundtrips and multiple `pushPending()` rounds
- [x] Final convergence holds across all participating tables
- [x] No local dirty rows remain after the run

### RealServerLongHorizonStaleFollowerStressTest

Goal:
- mimic one actively used device and one rarely synced device over a long period, with the stale
  follower staying clean most of the time and catching up only occasionally

File:
- `library-oversqlite-test/composeApp/src/androidDeviceTest/kotlin/dev/goquick/sqlitenow/oversqlite/e2e/RealServerLongHorizonStaleFollowerStressTest.kt`

Schema scope:
- `users`
- `posts`
- `categories`
- `teams`
- `team_members`
- `files`
- `file_reviews`

Execution sketch:
- bootstrap both devices
- both hydrate once so they start from the same baseline
- device A performs many deterministic rounds of local work and pushes after each round
- device B stays offline / unsynced for most rounds
- device B syncs only at a few widely spaced checkpoints
- final phase optionally forces `history_pruned` so the stale follower rebuilds through hydrate

Deterministic workload shape:
- use a fixed seeded schedule, not randomness
- each round should mix:
  - updates to existing rows
  - inserts of new rows
  - deletes of selected rows
  - new dependent-row creation
  - BLOB-bearing row changes
- keep some “hot” rows that are edited repeatedly across many rounds

Checklist:
- [x] Device A runs many rich-schema rounds while device B remains stale
- [x] Device B later catches up through multiple pull roundtrips
- [x] The workload includes repeated edits to hot rows, not only append-only inserts
- [x] The workload includes BLOB-bearing rows as part of the normal long-horizon mix
- [x] Each leader round exceeds one upload chunk by using intentionally low `uploadLimit`
- [x] Base stale-follower lane covers the incremental catch-up path without prune/rebuild
- [x] Final convergence holds across all participating tables
- [x] FK integrity holds across all participating tables
- [x] No local dirty rows remain after clean convergence

### RealServerStaleFollowerPruneRecoveryStressTest

Goal:
- force a stale follower past the retained bundle floor and prove `pullToStable()` rebuilds through
  snapshot hydration while preserving the current source identity

File:
- `library-oversqlite-test/composeApp/src/androidDeviceTest/kotlin/dev/goquick/sqlitenow/oversqlite/e2e/RealServerStaleFollowerPruneRecoveryStressTest.kt`

Checklist:
- [x] A follower syncs once, then stays stale while the leader advances rich-schema history
- [x] The example server test helper raises `retained_bundle_floor` above the follower's last seen bundle
- [x] Follower `pullToStable()` rebuilds through the `history_pruned` path instead of failing dirty or diverging
- [x] Source identity stays stable because the rebuild path is snapshot hydration, not destructive recovery
- [x] Final convergence still holds across hot rows, rich-schema batch rows, and FK integrity checks

### RealServerLongHorizonDivergentWriterStressTest

Goal:
- mimic two real devices used over a long period where both accumulate local work, but one device
  syncs much less often

File:
- `library-oversqlite-test/composeApp/src/androidDeviceTest/kotlin/dev/goquick/sqlitenow/oversqlite/e2e/RealServerLongHorizonDivergentWriterStressTest.kt`

Schema scope:
- `users`
- `posts`
- `categories`
- `teams`
- `team_members`
- `files`
- `file_reviews`

Execution sketch:
- bootstrap both devices
- device A syncs frequently
- device B stays offline longer and performs local writes between rare sync points
- some rounds intentionally target overlapping rows so conflicts can happen
- clean observer semantics or post-resolution continuation should still be proven explicitly

Deterministic workload shape:
- fixed round schedule and row naming
- weighted operation mix per round:
  - many updates
  - some inserts
  - some deletes
  - some dependent-row creation
  - some BLOB-bearing row changes
- include hot rows edited repeatedly by both devices

Checklist:
- [x] Both devices perform rich-schema work over many rounds
- [x] Device B syncs much less often than device A
- [x] At least one overlapping-write phase creates real conflict pressure
- [x] Conflicting device behavior matches the fail-closed contract
- [x] Clean observer or post-resolution path still converges to server-authoritative state
- [x] Rich-schema and BLOB-bearing tables remain covered during divergent writing
- [x] Final state assertions cover counts, representative row contents, and FK integrity
- [x] Post-conflict continuation or recovery path is proven, not assumed

### RealServerThreeDeviceConvergenceTest

Goal:
- prove three active devices can take turns authoring committed rich-schema state and still
  converge cleanly through the real server

File:
- `library-oversqlite-test/composeApp/src/androidDeviceTest/kotlin/dev/goquick/sqlitenow/oversqlite/e2e/RealServerThreeDeviceConvergenceTest.kt`

Execution sketch:
- bootstrap and hydrate three devices for one user
- device A seeds the hot graph plus one rich-schema batch
- device B pulls, authors the next committed round, and pushes
- device C pulls, authors the next committed round, and pushes
- device A catches up, authors the final committed round, and pushes
- all three devices pull to stable and converge

Checklist:
- [x] Three different source identities participate in one shared user history
- [x] All three devices author at least one committed bundle in the scenario
- [x] Rich-schema, FK-heavy, and BLOB-bearing tables remain covered
- [x] Low `uploadLimit` and `downloadLimit` keep chunking and multi-roundtrip pulls exercised
- [x] Final state assertions cover bundle sequence, hot-row state, representative batch presence, and FK integrity
- [x] No local dirty rows remain after clean convergence

## Notes

- Keep the basic contract file small and stable.
- Add new functionality in separate files under the `e2e` package.
- Prefer deterministic assertions over large opaque soak runs first.
- Prefer the stale-follower long-horizon lane before the divergent-writer lane.
- Stress should come from long horizon, rich schema, and repeated round structure, not from
  nondeterministic randomness.
