# Oversqlite Test Guide

This document is the maintainer-facing source of truth for how oversqlite tests are organized,
what each suite is responsible for, and which Gradle commands should be used.

## Goals

The oversqlite test layout is organized around three suite semantics:

- `comprehensive`
- `platform`
- `realserver`

These are suite concepts, not target-task names.

Broad Gradle target tasks such as `:library:jvmTest`,
`:library:iosSimulatorArm64Test`, or
`:platform-oversqlite-test:composeApp:iosSimulatorArm64Test`
remain generic execution surfaces. They are still useful for broad library verification, but they
are not the preferred maintainer entry points for oversqlite suites.

## Suite Model

### `comprehensive`

`comprehensive` is the default oversqlite confidence suite.

It should:

- be runnable without the live server
- avoid emulator/simulator/browser runtime requirements
- cover the canonical local oversqlite behavior catalog
- remain the only oversqlite suite required in CI/CD for now

The canonical home of `comprehensive` is `:library`.

### `platform`

`platform` re-runs comprehensive behavior families on concrete runtime surfaces without the live
server.

It should:

- mirror the same behavior families that `comprehensive` covers
- run on Android emulator/device, iOS simulator, JVM runtime harness, macOS, JS Node, and Wasm
  browser surfaces
- stay local-only for now

The canonical runtime-harness home of `platform` is `:platform-oversqlite-test:composeApp`.

### `realserver`

`realserver` re-runs comprehensive behavior families against the actual server and also owns
server-specific and heavier scenarios.

It should:

- mirror the same major behavior families as `comprehensive`
- use the real server and real transport
- include heavier scenarios such as shared-connection, long-horizon, and prune-recovery cases
- stay local-only for now

`realserver` has two homes:

- shared JVM-side catalog and helpers in `:library`
- concrete runtime-harness execution in `:platform-oversqlite-test:composeApp`

## Ownership

### `:library`

`:library` owns:

- the primary `comprehensive` catalog
- shared oversqlite behavior definitions and helpers
- shared JVM-side `realserver` coverage

Relevant suite tasks:

- `:library:oversqliteComprehensiveJvm`
- `:library:oversqliteRealserverJvm`
- `:library:jvmRealServerSharedConnectionStress`

### `:platform-oversqlite-test:composeApp`

`platform-oversqlite-test` is the runtime-harness home for:

- `platform`
- `realserver`

It provides Android, JVM, iOS, macOS, JS Node, and Wasm browser runtime surfaces.

## Behavior Responsibility

`comprehensive` is the source of truth for the behavior catalog.

`platform` and `realserver` are expected to mirror that behavior catalog at the behavior-family
level. They should not invent a separate primary catalog.

Major behavior families currently include:

- open/bootstrap/attach
- pull/push/connect lifecycle
- detach and sync-then-detach
- conflict and convergence behavior
- recovery/rebuild/source rotation
- file/blob/cascade/topology behavior
- typed row and generated-config behavior
- local replay/payload/planning/invalidation internals

## Entry Points

Prefer these suite-level tasks over broad target tasks.

### Comprehensive

| Purpose                            | Command                             |
|------------------------------------|-------------------------------------|
| Full host-side comprehensive suite | `./gradlew oversqliteComprehensive` |

### Platform

| Purpose             | Command                                         |
|---------------------|-------------------------------------------------|
| Full platform sweep | `./gradlew oversqlitePlatformAll`               |
| JVM                 | `./gradlew oversqlitePlatformJvm`               |
| Android emulator    | `./gradlew oversqlitePlatformAndroid`           |
| iOS simulator       | `./gradlew oversqlitePlatformIosSimulatorArm64` |
| macOS               | `./gradlew oversqlitePlatformMacosArm64`        |
| JS Node             | `./gradlew oversqlitePlatformJsNode`            |
| Wasm browser        | `./gradlew oversqlitePlatformWasmBrowser`       |

### Realserver

Before running `realserver`, start a compatible `go-oversync/examples/nethttp_server`.

Use `http://10.0.2.2:8080` from Android emulator/device tasks and `http://localhost:8080` from
JVM, macOS, JS Node, Wasm browser, and iOS simulator tasks.

The preferred wrapper tasks already default to those base URLs:

- host wrappers default to `http://localhost:8080`
- Android wrappers default to `http://10.0.2.2:8080`

Optional overrides:

- host wrappers: `OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL=...`
- Android wrappers: `OVERSQLITE_ANDROID_REAL_SERVER_SMOKE_BASE_URL=...`

| Purpose                             | Command                                                                                                            |
|-------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| Full realserver sweep               | `./gradlew oversqliteRealserverAll`                                                                                |
| Full realserver sweep in heavy mode | `./gradlew oversqliteRealserverAllHeavy`                                                                           |
| Shared JVM realserver catalog       | `./gradlew oversqliteRealserverJvm`                                                                                |
| Shared JVM heavy scenario           | `./gradlew oversqliteRealserverJvmHeavy`                                                                           |
| Android emulator                    | `./gradlew oversqliteRealserverAndroid`                                                                            |
| Android emulator heavy mode         | `./gradlew oversqliteRealserverAndroidHeavy`                                                                       |
| JVM runtime harness                 | `./gradlew oversqliteRealserverJvmHarness`                                                                         |
| JVM runtime harness heavy mode      | `./gradlew oversqliteRealserverJvmHarnessHeavy`                                                                    |
| iOS simulator                       | `./gradlew oversqliteRealserverIosSimulatorArm64`                                                                  |
| iOS simulator heavy mode            | `./gradlew oversqliteRealserverIosSimulatorArm64Heavy`                                                             |
| macOS                               | `./gradlew oversqliteRealserverMacosArm64`                                                                         |
| macOS heavy mode                    | `./gradlew oversqliteRealserverMacosArm64Heavy`                                                                    |
| JS Node                             | `./gradlew oversqliteRealserverJsNode`                                                                             |
| JS Node heavy mode                  | `./gradlew oversqliteRealserverJsNodeHeavy`                                                                        |
| Wasm browser                        | `./gradlew oversqliteRealserverWasmBrowser`                                                                        |
| Wasm browser heavy mode             | `./gradlew oversqliteRealserverWasmBrowserHeavy`                                                                   |

## Suite Gates

Suite gating is explicit:

- `OVERSQLITE_PLATFORM_TESTS=true`
- `OVERSQLITE_REALSERVER_TESTS=true`
- `OVERSQLITE_REALSERVER_HEAVY=true`

Maintainers normally should not need to pass these manually when using the suite entry points above.
The wrapper tasks set the appropriate environment or instrumentation arguments.

The wrapper semantics are:

- `oversqlitePlatform*` enables `OVERSQLITE_PLATFORM_TESTS`
- `oversqliteRealserver*` enables `OVERSQLITE_REALSERVER_TESTS`
- `*Heavy` additionally enables `OVERSQLITE_REALSERVER_HEAVY`

If broad target tasks are used directly, these switches still matter.

## Broad Target Tasks Vs Suite Tasks

This distinction is important.

Broad target tasks:

- are generic target verification surfaces
- may run non-oversqlite coverage
- may compile or execute more than one conceptual suite
- should remain available for broad library verification

Suite tasks:

- express maintainer intent directly
- are the preferred oversqlite commands
- hide the per-platform env/runner wiring
- make it clear whether the requested lane is `comprehensive`, `platform`, or `realserver`

Examples of broad target tasks that are not suite names:

- `:library:jvmTest`
- `:library:iosSimulatorArm64Test`
- `:library:wasmJsBrowserTest`
- `:platform-oversqlite-test:composeApp:jvmTest`
- `:platform-oversqlite-test:composeApp:connectedAndroidDeviceTest`
- `:platform-oversqlite-test:composeApp:iosSimulatorArm64Test`

## Current Matrix

| Suite           | Module / Area                                         | Platforms                                                                         |
|-----------------|-------------------------------------------------------|-----------------------------------------------------------------------------------|
| `comprehensive` | `:library`                                            | host-side JVM entry point today                                                   |
| `platform`      | `:platform-oversqlite-test:composeApp`                | Android, JVM, iOS, macOS, JS Node, Wasm browser                                   |
| `realserver`    | `:library` and `:platform-oversqlite-test:composeApp` | shared JVM plus Android, JVM, iOS, macOS, JS Node, Wasm browser runtime harnesses |

## CI/CD Policy

For now:

- oversqlite `comprehensive` is the only required CI/CD suite
- oversqlite `platform` is local-only
- oversqlite `realserver` is local-only

This policy is oversqlite-specific. It must not be confused with broader repository test coverage
that still runs through generic target tasks.

## Maintainer Guidance

Use these defaults:

- normal oversqlite work: `./gradlew oversqliteComprehensive`
- runtime-surface validation: one of the `oversqlitePlatform*` tasks, or `oversqlitePlatformAll`
- final live integration validation: one of the `oversqliteRealserver*` tasks, or
  `oversqliteRealserverAll`
- heavier live integration validation: `oversqliteRealserverAllHeavy` or the relevant `*Heavy`
  surface task

When updating oversqlite test structure, keep this file aligned with:

- root wrapper tasks in `build.gradle.kts`
- suite-local tasks in `library/build.gradle.kts`
- runtime-harness notes in `platform-oversqlite-test/README.md`
