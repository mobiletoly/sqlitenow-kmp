# SQLiteNow Core Test Guide

This document is the maintainer-facing source of truth for how core SQLiteNow tests are organized,
what each surface is responsible for, and which Gradle commands should be used.

## Goals

Core SQLiteNow testing is organized around two responsibilities:

- `direct`
- `platform`

These are responsibility names, not Gradle target-task names.

Broad Gradle target tasks such as `:library-core:jvmTest` or `:platform-core-test:harness:jvmTest`
remain generic execution surfaces. They are still useful, but the preferred maintainer entry points
for generated-code runtime validation are the `corePlatform*` wrapper tasks.

## Model

### `direct`

`direct` is the low-level runtime surface.

It should:

- validate runtime primitives and target-specific behavior directly in `:library-core`
- remain the primary CI/CD surface for core SQLiteNow
- continue to cover shared host, JS, Wasm, Apple, and Linux runtime behavior

The canonical home of `direct` is `:library-core`.

### `platform`

`platform` is the generated-code runtime harness surface.

It should:

- validate generated queries, mappings, invalidation, adapters, and higher-level database behavior
- run those checks on concrete runtime surfaces instead of only Android instrumentation
- stay local-only for now

The canonical home of `platform` is `:platform-core-test:harness`.

## Ownership

### `:library-core`

`:library-core` owns the `direct` surface.

Representative entry points:

- `:library-core:jvmTest`
- `:library-core:jsTest`
- `:library-core:wasmJsBrowserTest`
- `:library-core:iosSimulatorArm64Test`
- `:library-core:macosArm64Test`
- Linux direct-runtime verification commands

### `:platform-core-test:harness`

`:platform-core-test:harness` owns the `platform` surface.

It provides Android, JVM, iOS simulator, macOS, JS Node, and Wasm browser runtime surfaces for
generated-code validation.

## Entry Points

Prefer these commands during normal maintenance.

### Direct

| Purpose | Command |
| --- | --- |
| JVM direct runtime | `./gradlew :library-core:jvmTest` |
| JS direct runtime | `./gradlew :library-core:jsTest` |
| Wasm direct runtime | `./gradlew :library-core:wasmJsBrowserTest` |
| Apple direct runtime | `./gradlew :library-core:iosSimulatorArm64Test :library-core:macosArm64Test` |
| Linux direct runtime | `./gradlew :library-core:linuxX64Test :library-core:compileTestKotlinLinuxArm64` |

### Platform

Generate the shared test database first when you want a direct harness entry point:

- `./gradlew :platform-core-test:harness:generateLibraryTestDatabase`

Preferred wrapper tasks:

| Purpose | Command |
| --- | --- |
| Full local platform sweep | `./gradlew corePlatformAll` |
| Android emulator/device | `./gradlew corePlatformAndroid` |
| JVM | `./gradlew corePlatformJvm` |
| iOS simulator | `./gradlew corePlatformIosSimulatorArm64` |
| macOS | `./gradlew corePlatformMacosArm64` |
| JS Node | `./gradlew corePlatformJsNode` |
| Wasm browser | `./gradlew corePlatformWasmBrowser` |

`corePlatformAll` is a best-effort convenience for a fully provisioned local machine. It is not a
host-agnostic guarantee.

## Host Prerequisites

- `corePlatformAndroid` requires an Android emulator or connected device.
- `corePlatformIosSimulatorArm64` and `corePlatformMacosArm64` require macOS.
- `corePlatformJsNode` requires the normal Kotlin/JS Node toolchain.
- `corePlatformWasmBrowser` requires a working local browser test environment.
- `corePlatformAll` assumes all of the above are available on the current machine.

## Broad Target Tasks Vs Wrapper Tasks

This distinction matters.

Broad target tasks:

- are generic target verification surfaces
- are useful for direct-runtime work in `:library-core`
- may run more than the specific maintainer intent you care about

Wrapper tasks:

- express intent for the generated-code runtime harness
- keep core platform validation local-only
- avoid turning `:platform-core-test:harness` into a default CI/CD surface

Examples of broad target tasks that are not the preferred `platform` entry points:

- `:platform-core-test:harness:jvmTest`
- `:platform-core-test:harness:connectedAndroidDeviceTest`
- `:platform-core-test:harness:iosSimulatorArm64Test`
- `:platform-core-test:harness:macosArm64Test`
- `:platform-core-test:harness:jsNodeTest`
- `:platform-core-test:harness:wasmJsBrowserTest`

## CI/CD Policy

For now:

- `direct` in `:library-core` remains the primary CI/CD surface
- `platform` in `:platform-core-test:harness` is local-only
- GitHub Actions must use qualified `:library-core:*` task paths for shared target names
- GitHub Actions must not run `corePlatform*`
- GitHub Actions must not run `:platform-core-test:harness:build`, `check`, or similar broad
  aggregate harness tasks

## Maintainer Guidance

Use these defaults:

- low-level runtime work: the relevant `:library-core:*` direct task
- generated-code/runtime parity work: the relevant `corePlatform*` wrapper
- full generated-code local sweep on a provisioned machine: `./gradlew corePlatformAll`

Keep this document aligned with:

- root wrapper tasks in `build.gradle.kts`
- harness wiring in `platform-core-test/harness/build.gradle.kts`
- command guidance in `AGENTS.md`
