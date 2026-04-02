# Oversqlite Test Suite Reorganization

## Status

Draft.

Related specs:
[`oversqlite-v1-pruning-and-source-recovery.md`](./oversqlite-v1-pruning-and-source-recovery.md)
[`oversqlite-destructive-detach-source-rotation.md`](./oversqlite-destructive-detach-source-rotation.md)

Relevant current modules:
[`../library/build.gradle.kts`](../library/build.gradle.kts)
Target platform harness:
[`../platform-oversqlite-test/composeApp/build.gradle.kts`](../platform-oversqlite-test/composeApp/build.gradle.kts)

## Summary

This spec is oversqlite-specific only. It does not define a repository-wide testing policy for
unrelated modules.

The goal is to reorganize oversqlite tests into three clear suites:

- `comprehensive`
- `platform`
- `realserver`

Their responsibilities are:

- `comprehensive` is the default local confidence suite
- `platform` re-runs comprehensive behavior families on concrete runtime surfaces, still without the
  real server
- `realserver` re-runs comprehensive behavior families against the real server and also contains
  stress and real-server-only scenarios

This spec assumes oversqlite remains pre-release. Breaking changes to test names, package names,
Gradle entry points, and suite layout are acceptable if the resulting structure is clearer. No
compatibility-preserving migration path is required for the current draft naming.

## Problem

The current oversqlite test layout mixes several responsibilities together:

- local deterministic correctness
- real-server correctness
- heavy/stress scenarios
- concrete runtime execution surfaces such as Android instrumentation

That creates avoidable confusion:

- `:library` contains local contract tests, mirrored realserver tests, and a targeted real-server
  shared-connection stress entry point
- `platform-oversqlite-test:composeApp` contains runtime-harness tests split between `platform`
  and `realserver`, while the historical Android layout used a vague `e2e` package,
  but many of those tests are not truly "end-to-end" in a distinct architectural sense; they are
  ordinary lifecycle/contract scenarios or heavier real-server scenarios
- the repository does not clearly distinguish "default local confidence" from "runtime-surface
  confidence" from "final real-server confidence"

As a result:

- it is harder to decide what to run during ordinary development
- it is harder to decide where a new oversqlite test belongs
- it is harder to know whether a failing test points to local behavior, runtime-surface behavior,
  or real-server integration

## Audited Current Layout

### `:library`

Primary current oversqlite test surfaces include:

- local/common coverage in
  `library/src/commonTest/kotlin/dev/goquick/sqlitenow/oversqlite/`
- JVM coverage in
  `library/src/jvmTest/kotlin/dev/goquick/sqlitenow/oversqlite/`
- Android device-test environment helpers in
  `library/src/androidDeviceTest/kotlin/dev/goquick/sqlitenow/oversqlite/`
- cross-target real-server coverage in
  `library/src/commonTest/kotlin/dev/goquick/sqlitenow/oversqlite/RealServerComprehensiveTest.kt`
- targeted real-server shared-connection coverage in
  `library/src/commonTest/kotlin/dev/goquick/sqlitenow/oversqlite/RealServerSharedConnectionStressTest.kt`
- a custom JVM real-server task in
  `library/build.gradle.kts`

### Current Android harness

The current Android oversqlite harness includes:

- Android device tests under
  `platform-oversqlite-test/composeApp/src/androidDeviceTest/kotlin/dev/goquick/sqlitenow/oversqlite/`
- ordinary lifecycle/contract scenarios such as `RealServerBasicContractTest`,
  `RealServerLifecycleContractTest`, and `RealServerSyncThenDetachContractTest`
- heavier scenarios such as `RealServerStressTest`,
  `RealServerLongHorizonDivergentWriterStressTest`,
  `RealServerLongHorizonStaleFollowerStressTest`,
  `RealServerSharedConnectionStressTest`, and
  `RealServerStaleFollowerPruneRecoveryStressTest`

The current `e2e` label is therefore too broad to act as a useful organizing principle.

## Phase 1 Decisions

### Canonical Comprehensive Behavior Catalog

All three suites mirror this catalog:

1. open/bootstrap/attach lifecycle
2. push/pull/connect lifecycle and stable hydration
3. detach and sync-then-detach
4. conflict and convergence behavior
5. recovery, rebuild, and source rotation
6. blob/file/cascade/fk topology behavior
7. typed-row and generated-config behavior
8. transport chunking and replay/catch-up behavior

### Final Package And Naming Scheme

The final naming scheme is:

- `dev.goquick.sqlitenow.oversqlite`
  for `:library` comprehensive tests and shared host-side helpers
- `dev.goquick.sqlitenow.oversqlite.platform`
  for platform-harness tests and helpers that mirror comprehensive behavior without the live server
- `dev.goquick.sqlitenow.oversqlite.realserver`
  for live-server tests and helpers in both `:library` and `platform-oversqlite-test`

The final class naming direction is:

- `*Comprehensive*` for host-side canonical `:library` coverage where a suite-specific rename adds
  clarity
- `Platform*` for runtime-harness mirroring tests
- `RealServer*` for live-server coverage
- descriptive suffixes such as `Lifecycle`, `Conflict`, `SharedConnectionStress`,
  `PruneRecoveryStress`, and `GeneratedConfig`

The `e2e` label is retired as a default bucket. Android runtime-harness tests move under
`platform` or `realserver` naming instead.

### Platform Harness Matrix

| Platform | Harness module/path | `platform` entry point | `realserver` entry point | Status |
| --- | --- | --- | --- | --- |
| Android | `platform-oversqlite-test/composeApp` `androidDeviceTest` | `:platform-oversqlite-test:composeApp:connectedAndroidDeviceTest` with `OVERSQLITE_PLATFORM_TESTS=true` | `:platform-oversqlite-test:composeApp:connectedAndroidDeviceTest` with `OVERSQLITE_REALSERVER_TESTS=true` | exists |
| iOS | `platform-oversqlite-test/composeApp` `iosSimulatorArm64Test` | `:platform-oversqlite-test:composeApp:iosSimulatorArm64Test` with `OVERSQLITE_PLATFORM_TESTS=true` | `:platform-oversqlite-test:composeApp:iosSimulatorArm64Test` with `OVERSQLITE_REALSERVER_TESTS=true` | added as target/harness in this reorg |
| macOS | `platform-oversqlite-test/composeApp` `macosArm64Test` | `:platform-oversqlite-test:composeApp:macosArm64Test` with `OVERSQLITE_PLATFORM_TESTS=true` | `:platform-oversqlite-test:composeApp:macosArm64Test` with `OVERSQLITE_REALSERVER_TESTS=true` | added as target/harness in this reorg |
| JVM | `platform-oversqlite-test/composeApp` `jvmTest` | `:platform-oversqlite-test:composeApp:jvmTest` with `OVERSQLITE_PLATFORM_TESTS=true` | `:platform-oversqlite-test:composeApp:jvmTest` with `OVERSQLITE_REALSERVER_TESTS=true` | added as target/harness in this reorg |
| JS | `platform-oversqlite-test/composeApp` `jsNodeTest` | `:platform-oversqlite-test:composeApp:jsNodeTest` with `OVERSQLITE_PLATFORM_TESTS=true` | `:platform-oversqlite-test:composeApp:jsNodeTest` with `OVERSQLITE_REALSERVER_TESTS=true` | added as target/harness in this reorg |
| Wasm | `platform-oversqlite-test/composeApp` `wasmJsBrowserTest` | `:platform-oversqlite-test:composeApp:wasmJsBrowserTest` with `OVERSQLITE_PLATFORM_TESTS=true` | `:platform-oversqlite-test:composeApp:wasmJsBrowserTest` with `OVERSQLITE_REALSERVER_TESTS=true` | added as target/harness in this reorg |

### Target Harness Structure

The agreed structure under `platform-oversqlite-test` is:

- `src/commonTest/.../platform/`
  for cross-platform platform-suite mirrors
- `src/commonTest/.../realserver/`
  for cross-platform runtime-harness real-server mirrors
- `src/androidDeviceTest/.../platform/`
  for Android-only platform tests and helpers
- `src/androidDeviceTest/.../realserver/`
  for Android-only live-server tests and helpers
- platform-specific env/path actuals live beside their relevant target source sets

This keeps shared suite intent in `:library` while giving all concrete runtime surfaces one
umbrella harness home.

### Android Harness Split

Android runtime-harness tests are classified as follows:

- `platform`
  - `AndroidLifecycleSmokeTest`
  - `AndroidMockWebServerParityTest`
  - `TableInfoProviderTest`
- `realserver`
  - `RealServerBasicContractTest`
  - `RealServerBlobAndCascadeTest`
  - `RealServerChunkedPushTransportTest`
  - `RealServerConflictTest`
  - `RealServerFkTopologyTest`
  - `RealServerGeneratedConfigTest`
  - `RealServerLifecycleContractTest`
  - `RealServerLongHorizonDivergentWriterStressTest`
  - `RealServerLongHorizonStaleFollowerStressTest`
  - `RealServerRecoveryTest`
  - `RealServerRichSchemaStressTest`
  - `RealServerSharedConnectionStressTest`
  - `RealServerStaleFollowerPruneRecoveryStressTest`
  - `RealServerStressTest`
  - `RealServerSyncThenDetachContractTest`
  - `RealServerThreeDeviceConvergenceTest`
  - `RealServerTypedRowsTest`

### Inventory

#### `:library`

| Current test class | Current path | Behavior family | Target suite |
| --- | --- | --- | --- |
| `BundleHashTest` | `library/src/commonTest/.../BundleHashTest.kt` | bundle hashing and transport state | `comprehensive` |
| `CrossTargetSyncIntegrationTest` | `library/src/commonTest/.../CrossTargetSyncIntegrationTest.kt` | lifecycle, convergence, recovery, transport chunking | `comprehensive` |
| `OversqliteRemoteApiPathResolutionTest` | `library/src/commonTest/.../OversqliteRemoteApiPathResolutionTest.kt` | remote API path resolution | `comprehensive` |
| `RealServerComprehensiveTest` | `library/src/commonTest/.../RealServerComprehensiveTest.kt` | real-server mirrored catalog | `realserver` |
| `RealServerSharedConnectionStressTest` | `library/src/commonTest/.../RealServerSharedConnectionStressTest.kt` | real-server shared-connection stress | `realserver` |
| `BundleMultiDeviceContractTest` | `library/src/jvmTest/.../BundleMultiDeviceContractTest.kt` | multi-device bundle semantics | `comprehensive` |
| `BundlePullContractTest` | `library/src/jvmTest/.../BundlePullContractTest.kt` | pull semantics | `comprehensive` |
| `BundlePushContractTest` | `library/src/jvmTest/.../BundlePushContractTest.kt` | push semantics | `comprehensive` |
| `BundleSnapshotContractTest` | `library/src/jvmTest/.../BundleSnapshotContractTest.kt` | snapshot semantics | `comprehensive` |
| `LifecycleContractTest` | `library/src/jvmTest/.../LifecycleContractTest.kt` | open/attach/detach lifecycle | `comprehensive` |
| `OversqlitePayloadCodecTest` | `library/src/jvmTest/.../OversqlitePayloadCodecTest.kt` | payload codec | `comprehensive` |
| `OversqliteReplayPlannerTest` | `library/src/jvmTest/.../OversqliteReplayPlannerTest.kt` | replay planning | `comprehensive` |
| `OversqliteSqliteNowInvalidationTest` | `library/src/jvmTest/.../OversqliteSqliteNowInvalidationTest.kt` | invalidation/reactivity | `comprehensive` |
| `StatementCacheTest` | `library/src/jvmTest/.../StatementCacheTest.kt` | statement caching | `comprehensive` |

#### `platform-oversqlite-test`

| Current test class | Current path | Behavior family | Target suite |
| --- | --- | --- | --- |
| `AndroidLifecycleSmokeTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../AndroidLifecycleSmokeTest.kt` | Android lifecycle/runtime parity | `platform` |
| `AndroidMockWebServerParityTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../AndroidMockWebServerParityTest.kt` | Android runtime parity with local/mock server | `platform` |
| `TableInfoProviderTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../TableInfoProviderTest.kt` | Android SQLite metadata/runtime behavior | `platform` |
| `PlatformParityTest` | `platform-oversqlite-test/composeApp/src/commonTest/.../platform/PlatformParityTest.kt` | cross-platform runtime mirroring of lifecycle/convergence/recovery | `platform` |
| `RealServerHarnessTest` | `platform-oversqlite-test/composeApp/src/commonTest/.../realserver/RealServerHarnessTest.kt` | cross-platform runtime real-server mirrored catalog | `realserver` |
| `RealServerHarnessSharedConnectionStressTest` | `platform-oversqlite-test/composeApp/src/commonTest/.../realserver/RealServerHarnessSharedConnectionStressTest.kt` | cross-platform runtime shared-connection stress | `realserver` |
| `RealServerBasicContractTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerBasicContractTest.kt` | open/attach/push/pull | `realserver` |
| `RealServerBlobAndCascadeTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerBlobAndCascadeTest.kt` | blob and cascade behavior | `realserver` |
| `RealServerChunkedPushTransportTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerChunkedPushTransportTest.kt` | transport chunking | `realserver` |
| `RealServerConflictTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerConflictTest.kt` | conflict resolution | `realserver` |
| `RealServerFkTopologyTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerFkTopologyTest.kt` | fk topology | `realserver` |
| `RealServerGeneratedConfigTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerGeneratedConfigTest.kt` | generated config / typed schema | `realserver` |
| `RealServerLifecycleContractTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerLifecycleContractTest.kt` | lifecycle / detach / reattach | `realserver` |
| `RealServerLongHorizonDivergentWriterStressTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerLongHorizonDivergentWriterStressTest.kt` | long-horizon stress | `realserver` |
| `RealServerLongHorizonStaleFollowerStressTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerLongHorizonStaleFollowerStressTest.kt` | long-horizon stale follower stress | `realserver` |
| `RealServerRecoveryTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerRecoveryTest.kt` | recovery and rebuild | `realserver` |
| `RealServerRichSchemaStressTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerRichSchemaStressTest.kt` | rich schema stress | `realserver` |
| `RealServerSharedConnectionStressTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerSharedConnectionStressTest.kt` | shared connection stress | `realserver` |
| `RealServerStaleFollowerPruneRecoveryStressTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerStaleFollowerPruneRecoveryStressTest.kt` | prune recovery stress | `realserver` |
| `RealServerStressTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerStressTest.kt` | mixed stress | `realserver` |
| `RealServerSyncThenDetachContractTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerSyncThenDetachContractTest.kt` | sync-then-detach | `realserver` |
| `RealServerThreeDeviceConvergenceTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerThreeDeviceConvergenceTest.kt` | three-device convergence | `realserver` |
| `RealServerTypedRowsTest` | `platform-oversqlite-test/composeApp/src/androidDeviceTest/.../realserver/RealServerTypedRowsTest.kt` | typed rows | `realserver` |

### Rollout Order

The agreed rollout order is:

1. rename and rewire `platform-oversqlite-test` as the umbrella harness area
2. establish explicit suite gating and entry points
3. reorganize `:library` comprehensive and shared `realserver` naming
4. split Android harness tests into `platform` and `realserver`
5. wire shared cross-platform `platform` and `realserver` runs for JVM, iOS, macOS, JS, and Wasm
6. update docs, AGENTS, and runbooks

## Goals

- make it obvious what to run during normal local development
- make it obvious what to run when validating runtime surfaces without a live server
- make it obvious what to run for final real-server validation
- keep a single clear home for each test responsibility
- ensure regular development does not depend on a real server
- keep CI/CD limited to oversqlite `comprehensive` coverage for now
- ensure real-server validation mirrors comprehensive behavior families instead of becoming a
  separate ad-hoc test world

## Non-Goals

- reducing oversqlite coverage
- changing oversqlite runtime behavior as part of this spec
- requiring every current test to keep its current package name, class name, or task name
- creating a separate top-level stress suite
- defining a repository-wide taxonomy for non-oversqlite modules

## Decision

### 1. Oversqlite Uses Three Top-Level Suites

The oversqlite test structure must be organized around three suites:

- `comprehensive`
- `platform`
- `realserver`

These suites are based on test responsibility, not on the historical source-set or module where the
tests currently happen to live.

### 2. `comprehensive` Is The Default Local Confidence Suite

`comprehensive` must be the suite a developer can run locally, regularly, and with high confidence
without needing a real server or emulator/simulator/browser harness.

`comprehensive`:

- does not use the real server
- does not require Android emulators, iOS simulators, browser harnesses, or other special runtime
  surfaces
- may use mock/fake/local-controlled server behavior where server-like flows must be exercised
- may still use real SQLite, real oversqlite lifecycle code, real serialization, and non-trivial
  local integration behavior inside one process

`comprehensive` is the default development safety net.

### 3. `platform` Re-Runs Comprehensive Behavior Families On Concrete Runtime Surfaces

`platform` exists to validate that comprehensive behavior still holds when exercised through
concrete runtime surfaces, but still without relying on the real server.

`platform`:

- mirrors the major behavior families covered by `comprehensive`
- runs on concrete runtime surfaces such as Android emulator/device, iOS simulator, browser/Wasm,
  and any other platform harness that materially changes runtime behavior
- still does not use the real server
- may use local/mock/fake server behavior where transport-style flows must be exercised

`platform` is not a separate behavioral catalog. It is runtime-surface validation of the same core
oversqlite behavior families.

### 4. `realserver` Mirrors Comprehensive Coverage Against The Live Server

`realserver` exists to validate real integration with the live server.

`realserver`:

- re-runs the same major behavior families covered by `comprehensive`
- runs on concrete runtime surfaces where real integration is meaningful
- uses the actual server and actual transport
- may also include stress scenarios and additional real-server-only checks

There is no separate top-level `stress` suite. Stress and heavier scenarios are part of
`realserver`.

### 5. One Canonical Behavior Catalog Must Drive All Three Suites

The repository should define one canonical set of major oversqlite behavior families, for example:

- open/bootstrap/attach
- pull/push/connect lifecycle
- detach and sync-then-detach
- conflict/convergence behavior
- recovery/rebuild/source rotation
- file/blob/cascade/topology behavior
- typed row / generated config behavior

`comprehensive` must cover those families locally.

`platform` should mirror those families where runtime-surface validation adds value.

`realserver` should mirror those families against the live server and may add:

- stress
- long-horizon scenarios
- shared-connection scenarios
- stale-follower/pruning pressure
- other server-specific validation cases

### 6. `:library` Owns The Primary Comprehensive And Shared Real-Server Definitions

The primary home for oversqlite test intent should be `:library`.

In particular:

- `:library` should own the primary `comprehensive` suite
- `:library` should own the shared scenario definitions and helpers that the other suites mirror
- `:library` should own the shared `realserver` scenario catalog and helpers
- `:library` may host host-side `realserver` runs where no special runtime harness is required

This means the current realserver-related code under
`library/src/commonTest/kotlin/dev/goquick/sqlitenow/oversqlite/` is not expected to move
wholesale into `platform-oversqlite-test`.

The intended split is:

- shared oversqlite scenario definitions and shared realserver helpers remain in `:library`
- concrete runtime-harness executions of `platform` and `realserver` live in
  `platform-oversqlite-test` and its platform-specific harness structure

### 7. `platform-oversqlite-test` Becomes The Platform-Harness Home

Concrete runtime harnesses should exist to run `platform` and `realserver` suites on specific
surfaces.

The current `library-oversqlite-test` module should be renamed to `platform-oversqlite-test`.

In the intended end state, `platform-oversqlite-test` is the umbrella home for platform harnesses,
including:

- Android
- iOS
- macOS
- JVM
- JS/Wasm

These harnesses do not define new suite types. They are execution surfaces for `platform` and
`realserver`, organized under the `platform-oversqlite-test` area rather than treated as unrelated
test modules.

### 8. The `e2e` Label Should Be Removed As The Default Organizing Term

The current `e2e` package naming is not precise enough.

The preferred direction is:

- use `comprehensive`, `platform`, and `realserver` naming at the suite/runbook level
- use more specific intent in package/class naming such as `Lifecycle`, `Conflict`,
  `PruneRecovery`, `SharedConnection`, `LongHorizon`, and similar behavior labels

`e2e` may remain only if it is narrowed to a very specific meaning, but it should not remain the
default bucket for unrelated oversqlite scenarios.

### 9. Suite Gating Must Be Explicit

Suite selection should follow this model:

- `comprehensive` runs by default and does not require an enable switch
- `platform` is opt-in
- `realserver` is opt-in

The intended suite-level gates are:

- `OVERSQLITE_PLATFORM_TESTS=true`
- `OVERSQLITE_REALSERVER_TESTS=true`

`realserver` heavy-mode behavior is controlled separately:

- if `OVERSQLITE_REALSERVER_TESTS=true`, `realserver` tests run
- if `OVERSQLITE_REALSERVER_TESTS` is absent or `false`, `realserver` tests do not run at all,
  including stress/heavier scenarios
- if `OVERSQLITE_REALSERVER_TESTS=true` and `OVERSQLITE_REALSERVER_HEAVY=true`, heavy real-server
  scenarios run in full heavy mode
- if `OVERSQLITE_REALSERVER_TESTS=true` and `OVERSQLITE_REALSERVER_HEAVY` is absent or `false`,
  heavy real-server scenarios still run, but in their lighter/default mode where applicable

The older `OVERSQLITE_REAL_SERVER_SHARED_STRESS` style of gating should be retired in favor of the
suite-level `realserver` gate plus the separate heavy-mode switch.

## Target Matrix

| Suite | Real server | Emulator / simulator / browser harness required | Purpose |
| --- | --- | --- | --- |
| `comprehensive` | No | No | default local confidence |
| `platform` | No | Yes | comprehensive behavior on concrete runtime surfaces |
| `realserver` | Yes | Yes, where real integration depends on the concrete surface | final real integration validation plus stress/heavier scenarios |

## Target Structure

### `:library`

- owns `comprehensive`
- owns the shared behavior catalog and helpers that `platform` and `realserver` mirror
- owns shared/host-side `realserver` coverage where a special runtime harness is not needed

### Runtime harness modules

Current:

- `:platform-oversqlite-test:composeApp` as the current runtime-harness module

Target structure:

- rename `library-oversqlite-test` to `platform-oversqlite-test`
- place Android, iOS, macOS, JVM, and JS/Wasm platform harnesses under that area
- define the exact module/path structure for each harness as part of the implementation plan

These harnesses run:

- `platform`
- `realserver`

They should not become a second independent definition of oversqlite behavior.

They are execution surfaces, not the canonical home of shared oversqlite realserver logic.

## Migration Rules

### Naming

- avoid `Smoke` for suites that actually represent canonical real-server coverage
- avoid `e2e` as the generic bucket for oversqlite runtime harness tests
- do not create a separate top-level `stress` suite
- allow specific test names such as `SharedConnectionStressTest` or `LongHorizonStressTest` inside
  `realserver` where that name is actually descriptive

### Ownership

- every major oversqlite behavior family must have a home in `comprehensive`
- `platform` should mirror comprehensive behavior families rather than invent its own test catalog
- `realserver` should mirror comprehensive behavior families and may add extra real-server-only
  cases
- shared realserver scenario definitions and helpers should remain in `:library`
- runtime harnesses should execute `platform` and `realserver`; they should not define a fourth
  suite type

### Verification Policy

- day-to-day development should rely primarily on `comprehensive`
- runtime-surface confidence without server dependency should come from `platform`
- final live integration confidence should come from `realserver`
- `platform` and `realserver` should be explicit opt-in lanes

### CI/CD Policy

For now, only `comprehensive` is part of the GitHub Actions / CI/CD requirement.

- `comprehensive` should be the required CI/CD suite
- `platform` should be run locally when runtime-surface validation is needed
- `realserver` should be run locally when final live integration validation is needed
- `realserver` heavy mode should remain local-only and explicitly enabled

This is an execution-policy decision, not a statement that `platform` or `realserver` are
unimportant. They remain part of the intended oversqlite validation model, but they are not
required CI/CD lanes at this stage.

This policy is oversqlite-specific. It must not break existing non-oversqlite library/core test
coverage that currently runs through broad Gradle target tasks such as `:library:jvmTest` or
`:library:iosSimulatorArm64Test`.

The intended rule is:

- broad target tasks may continue to run in CI/CD
- non-oversqlite tests inside those target tasks should continue to run as they do today
- oversqlite `platform` and `realserver` tests inside those target tasks must be opt-in gated and
  therefore skipped by default unless explicitly enabled

This means the spec does not require immediate removal of current simulator/browser-capable target
tasks from CI/CD. Instead, the transition requirement is that oversqlite suite gating must ensure
those broad tasks continue to run existing non-oversqlite coverage while oversqlite `platform` and
`realserver` coverage remains disabled by default.

## Acceptance Criteria

- the oversqlite test plan is clearly described in terms of `comprehensive`, `platform`, and
  `realserver`
- `comprehensive` can be run locally without a real server and without emulator/simulator/browser
  harnesses
- only `comprehensive` is required in CI/CD for now
- `platform` exists to mirror comprehensive behavior families on concrete runtime surfaces without
  the real server
- `realserver` mirrors comprehensive behavior families against the live server
- `realserver` also contains stress/heavier/server-only scenarios without introducing a separate
  top-level stress suite
- `realserver` stress/heavier scenarios do not run at all unless `OVERSQLITE_REALSERVER_TESTS=true`
- `OVERSQLITE_REALSERVER_HEAVY=true` increases heavy-scenario intensity rather than acting as a
  separate suite gate
- `OVERSQLITE_REAL_SERVER_SHARED_STRESS` is no longer the authoritative gate; it must be removed or
  retained only as a temporary documented compatibility alias to `OVERSQLITE_REALSERVER_HEAVY`
- existing non-oversqlite library/core tests continue to run under current broad target tasks
  without being suppressed by oversqlite suite gating
- the current Android harness no longer acts as a vague `e2e` bucket
- docs and runbooks describe what each suite is for and when to run it

## Work Checklist

Implementation should proceed in phases. Earlier phases define structure and naming; later phases
move code, add harnesses, and update runbooks.

### Phase 1: Catalog And Naming Decisions

- [x] Produce an inventory table that maps every current oversqlite test class to:
      current module/path, major behavior family, and target suite (`comprehensive`, `platform`, or
      `realserver`).
- [x] Produce a platform harness matrix with one row for Android, iOS, macOS, JVM, and JS/Wasm,
      recording for each platform:
      harness module/path, `platform` entry point, `realserver` entry point, and whether that
      harness already exists or must be added.
- [x] Decide the exact target module/path structure under `platform-oversqlite-test` for Android,
      iOS, macOS, JVM, and JS/Wasm harnesses.
- [x] Write down the canonical comprehensive behavior catalog that all suites should mirror.
- [x] Decide the final package naming scheme for `comprehensive`, `platform`, and `realserver`.
- [x] Decide the final class naming scheme for suite-level tests and helpers.
- [x] Decide the final fate of the current `e2e` label and record the replacement path/package
      names.
- [x] Decide which current Android harness tests move to `platform` and which move to `realserver`.
- [x] Decide the concrete harness rollout order for Android, iOS, macOS, JVM, and JS/Wasm.

### Phase 2: Establish `comprehensive` In `:library`

- [x] Reorganize `:library` tests so the `comprehensive` suite is clearly identifiable by package,
      class naming, or Gradle entry point.
- [x] Move or rename shared helpers so `comprehensive` uses intent-based names rather than legacy
      smoke/e2e wording.
- [x] Ensure every major behavior family in the canonical catalog has at least one
      `comprehensive` home in `:library`.
- [x] Ensure `comprehensive` tests do not require a real server.
- [x] Ensure `comprehensive` tests do not require emulator/simulator/browser harnesses.
- [x] Add or rename a documented Gradle entry point for the `comprehensive` lane.
- [x] Ensure oversqlite suite gating does not suppress unrelated non-oversqlite tests that still
      run inside the same broad target tasks.

### Phase 3: Establish `platform` In Runtime Harnesses

- [x] Rename `library-oversqlite-test` to `platform-oversqlite-test`.
- [x] Update module wiring for that rename, including Gradle/settings paths, module includes, and
      runbook references.
- [x] Reorganize the current Android harness under
      `platform-oversqlite-test/...`
      so `platform` tests are clearly separated from `realserver` tests.
- [x] Remove `e2e` as the default Android package bucket if it no longer has a precise meaning.
- [x] Ensure Android `platform` tests mirror the canonical comprehensive behavior families without a
      live server dependency.
- [x] Add or rename documented Gradle entry points for the Android `platform` lane.
- [x] Create the agreed iOS harness/module for `platform` and wire its agreed entry point.
- [x] Create the agreed macOS harness/module for `platform` and wire its agreed entry point.
- [x] Create the agreed JVM harness/module for `platform` and wire its agreed entry point.
- [x] Create the agreed JS/Wasm harness/module for `platform` and wire its agreed entry point.
- [x] For each new harness, document which comprehensive behavior families it mirrors.

### Phase 4: Establish `realserver`

- [x] Rename or relocate `RealServerSmokeTest` in `:library` to explicit `realserver` naming.
- [x] Move or rename shared real-server helpers in `:library` so they match `realserver` naming.
- [x] Ensure the shared `realserver` catalog mirrors the canonical comprehensive behavior families.
- [x] Keep shared realserver scenario definitions/helpers in `:library` rather than moving them
      wholesale into runtime-harness modules.
- [x] Reorganize heavier real-server scenarios such as shared-connection, long-horizon, and
      prune-recovery cases under the `realserver` suite rather than a separate top-level stress
      bucket.
- [x] Reorganize Android runtime-harness real-server tests into explicit `realserver` structure.
- [x] Ensure Android, iOS, macOS, JVM, and JS/Wasm `realserver` harnesses are organized under the
      `platform-oversqlite-test` area according to the agreed target module/path structure.
- [x] Create the agreed iOS harness/module for `realserver` and wire its agreed entry point.
- [x] Create the agreed macOS harness/module for `realserver` and wire its agreed entry point.
- [x] Create the agreed JVM harness/module for `realserver` and wire its agreed entry point.
- [x] Create the agreed JS/Wasm harness/module for `realserver` and wire its agreed entry point.
- [x] Ensure targeted heavier real-server runs remain possible without defining a separate top-level
      suite.
- [x] Ensure the current custom task `jvmRealServerSharedStress` fits the final `realserver`
      naming and runbook model.
- [x] Replace `OVERSQLITE_REAL_SERVER_SHARED_STRESS` style gating with the new
      `OVERSQLITE_REALSERVER_TESTS` plus `OVERSQLITE_REALSERVER_HEAVY` model.
- [x] Update all known task wiring and docs that currently reference
      `OVERSQLITE_REAL_SERVER_SHARED_STRESS`, including `library/build.gradle.kts` and `AGENTS.md`.
- [x] Ensure heavy real-server scenarios do not run at all unless `OVERSQLITE_REALSERVER_TESTS=true`.
- [x] Ensure heavy real-server scenarios still run in lighter/default mode when
      `OVERSQLITE_REALSERVER_TESTS=true` and `OVERSQLITE_REALSERVER_HEAVY` is absent or `false`.
- [x] Ensure heavy real-server scenarios run in full heavy mode when both
      `OVERSQLITE_REALSERVER_TESTS=true` and `OVERSQLITE_REALSERVER_HEAVY=true`.

### Phase 5: CI/CD And Runbook Wiring

- [x] Add or rename Gradle entry points so `comprehensive`, `platform`, and `realserver` are clear
      in the runbook.
- [x] Keep GitHub Actions / CI/CD wired only to `comprehensive` for now.
- [x] Audit current broad CI/CD target tasks and confirm that any simulator/browser-capable lanes
      continue to run only non-oversqlite or `comprehensive`-eligible oversqlite coverage by
      default.
- [x] Document `platform` and `realserver` explicitly as local-only lanes for now.
- [x] Add or document platform-harness entry points for Android, iOS, macOS, JVM, and JS/Wasm as
      they are introduced.
- [x] Add or document suite-level enable switches for `platform` and `realserver`.
- [x] Add or document the separate `OVERSQLITE_REALSERVER_HEAVY` switch for heavy real-server mode.
- [x] Ensure current CI/CD broad target tasks continue to run existing non-oversqlite library/core
      coverage while oversqlite `platform` and `realserver` remain skipped by default.

### Phase 6: Documentation

- [x] Update `AGENTS.md` so oversqlite verification instructions are organized around
      `comprehensive`, `platform`, and `realserver`.
- [x] In `AGENTS.md`, document both how to run each oversqlite suite and when each suite should be
      used during normal development, runtime-surface validation, and final real-server validation.
- [x] Update references to `library-oversqlite-test` so they reflect the new
      `platform-oversqlite-test` naming and structure.
- [x] Update any oversqlite docs/runbooks that still use smoke/e2e/stress terminology as primary
      suite names.
- [x] Add a short explanation of the three-suite model so future tests do not drift back into
      ambiguous buckets.
- [x] Document explicitly that CI/CD currently runs only `comprehensive`, while `platform` and
      `realserver` are local-only.
- [x] Document the suite enable switches and explain the difference between ordinary `realserver`
      runs and `realserver` heavy mode.
- [x] Document that oversqlite suite gating is scoped to oversqlite tests and does not disable
      existing non-oversqlite coverage inside broad target tasks.

### Phase 7: Verification

- [x] Run the `comprehensive` lane(s) after reorganization.
- [x] Run the Android `platform` lane after reorganization.
- [x] Run the Android `realserver` lane after reorganization.
- [x] Run the iOS `platform` and `realserver` lanes after they are added.
- [x] Run the macOS `platform` and `realserver` lanes after they are added.
- [x] Run the JVM `platform` and `realserver` lanes after they are added.
- [x] Run the JS/Wasm `platform` and `realserver` lanes after they are added.
- [x] Run at least one heavier real-server scenario after reorganization.

### Phase 8: Suite Entry-Point Cleanup

- Broad target execution surfaces that remain generic target tasks include:
  - `:library:jvmTest`
  - `:library:iosSimulatorArm64Test`
  - `:library:macosArm64Test`
  - `:library:jsTest`
  - `:library:wasmJsBrowserTest`
  - `:platform-oversqlite-test:composeApp:jvmTest`
  - `:platform-oversqlite-test:composeApp:connectedAndroidDeviceTest`
  - `:platform-oversqlite-test:composeApp:iosSimulatorArm64Test`
  - `:platform-oversqlite-test:composeApp:macosArm64Test`
  - `:platform-oversqlite-test:composeApp:jsNodeTest`
  - `:platform-oversqlite-test:composeApp:wasmJsBrowserTest`

The explicit oversqlite suite entry points are:

- `oversqliteComprehensive`
- `oversqlitePlatformAll`
- `oversqlitePlatformAndroid`
- `oversqlitePlatformJvm`
- `oversqlitePlatformIosSimulatorArm64`
- `oversqlitePlatformMacosArm64`
- `oversqlitePlatformJsNode`
- `oversqlitePlatformWasmBrowser`
- `oversqliteRealserverAll`
- `oversqliteRealserverAllHeavy`
- `oversqliteRealserverJvm`
- `oversqliteRealserverJvmHeavy`
- `oversqliteRealserverAndroid`
- `oversqliteRealserverAndroidHeavy`
- `oversqliteRealserverJvmHarness`
- `oversqliteRealserverJvmHarnessHeavy`
- `oversqliteRealserverIosSimulatorArm64`
- `oversqliteRealserverIosSimulatorArm64Heavy`
- `oversqliteRealserverMacosArm64`
- `oversqliteRealserverMacosArm64Heavy`
- `oversqliteRealserverJsNode`
- `oversqliteRealserverJsNodeHeavy`
- `oversqliteRealserverWasmBrowser`
- `oversqliteRealserverWasmBrowserHeavy`

Supporting module-local suite tasks remain in `:library` for the host-side catalog:

- `:library:oversqliteComprehensiveJvm`
- `:library:oversqliteRealserverJvm`
- `:library:jvmRealServerSharedConnectionStress`

- [x] Inventory the remaining broad target test tasks in `:library` and
      `:platform-oversqlite-test` and record which of them are generic target tasks versus
      intended oversqlite suite entry points.
- [x] Write down the mismatch explicitly for each remaining broad target task that can be mistaken
      for a suite lane, including tasks in both `:library` and `:platform-oversqlite-test`,
      especially simulator/emulator/browser-backed tasks such as
      `:library:iosSimulatorArm64Test`,
      `:library:wasmJsBrowserTest`, and
      `:platform-oversqlite-test:composeApp:iosSimulatorArm64Test`.
- [x] Decide the final oversqlite-specific Gradle entry points for `comprehensive`,
      `platform`, and `realserver` across both `:library` and `:platform-oversqlite-test`
      so suite selection no longer depends on broad target task names.
- [x] Decide the final shape of those suite entry points, for example:
      aggregate root tasks, per-platform suite tasks, or another explicit structure, and document
      that decision before wiring implementation tasks.
- [x] Add explicit host-side oversqlite `comprehensive` entry point(s) that do not require
      emulator, simulator, or browser runtime infrastructure.
- [x] Ensure simulator/emulator/browser-backed oversqlite execution is described and invoked as
      `platform` or `realserver`, not as `comprehensive`, even when it still runs through broad
      target tasks internally.
- [x] Keep existing broad target tasks in `:library` and `:platform-oversqlite-test` available for
      generic target verification rather than deleting or repurposing them as oversqlite suite
      names.
- [x] Verify that the new suite entry points do not reduce oversqlite behavior coverage compared to
      the pre-cleanup target-task surface.
- [x] Verify that the new suite entry points do not suppress or regress non-oversqlite coverage in
      broad `:library` target tasks and do not accidentally change the generic target-task behavior
      of `:platform-oversqlite-test`.
- [x] Update runbooks and `AGENTS.md` so they distinguish clearly between:
      broad target verification tasks and oversqlite suite-specific entry points across both
      `:library` and `:platform-oversqlite-test`.
- [x] Add a short note to the spec/runbook explaining that broad target tasks are execution
      surfaces, while `comprehensive`, `platform`, and `realserver` are suite semantics.

## Design Notes

### Why Three Suites?

Because they map directly to how oversqlite should be developed and validated:

- `comprehensive` for ordinary daily development confidence
- `platform` for runtime-surface confidence without infrastructure dependency
- `realserver` for final live integration confidence

This is a clearer progression than the current mix of contract/smoke/e2e/stress labels.

### Why Keep `comprehensive` Separate From `platform`?

Because the user requirement is explicit: regular development should not depend on emulator,
simulator, browser, or real-server infrastructure. `comprehensive` preserves that property.

### Why Keep Stress Inside `realserver`?

Because heavy scenarios are still real-server validation. They should not become a separate
top-level suite that competes with the mirrored comprehensive catalog.
