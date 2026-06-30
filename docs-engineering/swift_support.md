# SQLiteNow Swift Support Engineering Notes

This file is the durable engineering summary for the Swift branch specs. The
ignored files under `specs/` are execution checklists; keep durable decisions
here when deleting completed spec files.

## Current Product Shape

The Swift lane is generated Swift source over reusable KMP Apple runtime
binaries.

- Core generated packages import `SQLiteNowCoreRuntime` internally.
- Sync generated packages import the combined `SQLiteNowSyncRuntime` internally.
- App source imports only the generated database package, for example
  `AppDatabaseSQLiteNow`.
- The target architecture is not an app-specific Kotlin/Native framework per
  database.
- A public `SWIFT` compiler backend and Swift-native core/oversqlite runtime
  remain deferred.

Generated packages contain Swift source, runtime binary target references, and
`.sqlitenow/package-manifest.json`. Generated output is owned by SQLiteNow and
must not be hand-edited.

## Local Workflows

There are two supported local Swift workflows.

The Gradle-owned workflow is for repository and KMP builds. KMP modules enable
`swiftPackage {}` in the SQLiteNow Gradle DSL and run generated package tasks
such as `packageDebugSwiftPackage`.

The Gradle-free SwiftPM/Xcode workflow is for Swift app repositories. It uses:

- `swift/plugin/sqlitenow-plugin`
- package-root `SQLiteNow.json`
- SQL under `SQLiteNow/databases/<DatabaseName>` by default
- generated packages under `SQLiteNowGenerated/<swiftPackageName>` by default
- `swift package plugin --allow-writing-to-package-directory sqlitenow-generate`

The command plugin may be launched from Xcode through the package plugin UI.
When launched from a nested `.xcodeproj`, the plugin-provided default package
root is the project directory. Consumers must pass
`--package-root <Swift package root>` explicitly when `SQLiteNow.json`, default
SQL input, or default generated output live above the `.xcodeproj`; `--config`
alone changes only the config file path.
Swift app repositories still need Java 17 or newer because the current generator
tool is Java-backed.

## Configuration Contract

`SQLiteNow.json` uses `schemaVersion: 1` and a top-level non-empty `databases`
array. Each database entry requires:

- `databaseName`
- `swiftPackageName`
- `swiftTargetName`
- `runtime`, either `core` or `sync`
- exactly one runtime artifact source: `runtimeXcframeworkDirectory` or
  `runtimeArtifact`

Defaults:

- `sqlDirectory`: `SQLiteNow/databases/<databaseName>` from the package root
- `outputDirectory`: `SQLiteNowGenerated/<swiftPackageName>` from the package
  root
- `metadataPackageName`: derived from `databaseName`
- runtime module: `SQLiteNowCoreRuntime` for core or `SQLiteNowSyncRuntime` for
  sync
- minimum platforms: iOS 15 and macOS 14
- `requestedAppleTargets`: optional, but limited to `macosArm64`, `iosArm64`,
  and `iosSimulatorArm64`

The SwiftPM plugin rejects unknown fields, blank required names, duplicate
database names, duplicate Swift package names, unsupported runtime values,
unsupported Apple targets, output directories outside the package root, and
runtime artifact shape errors.

Native Swift runtime artifacts intentionally do not include `macosX64`,
`iosX64`, or other `x86_64` Apple slices. The public KMP target matrix also
omits Apple x64 targets; Apple support is arm64-only.

## Release Distribution Model

The development checkout does not keep a root SwiftPM `Package.swift`.

Release automation stages a text-only SwiftPM distribution tree under
`build/swiftpm-plugin-distribution/sqlitenow-kmp` and publishes that tree as a
bare SemVer SwiftPM tag such as `0.11.0`. The ordinary repository release tag
continues to use the `vX.Y.Z` form.

Release assets live on the matching GitHub Release and are not committed to the
SwiftPM tag:

- `SQLiteNowCompiler-<version>.artifactbundle.zip`
- `SQLiteNowCoreRuntime-<version>.xcframework.zip`
- `SQLiteNowSyncRuntime-<version>.xcframework.zip`

The compiler artifact bundle contains the Java-backed compiler and still
requires Java 17 on the consumer machine. Do not put jars, zips, artifact
bundles, or XCFrameworks in Git as normal source.

Release order:

1. Build Swift runtime release artifacts.
2. Build the SwiftPM compiler artifact bundle.
3. Stage and inspect the text-only SwiftPM plugin distribution.
4. Validate a clean SwiftPM consumer with local release artifacts.
5. Upload runtime and compiler release assets.
6. Validate a local distribution Git tag against uploaded HTTPS assets.
7. Publish the SwiftPM plugin distribution tag.
8. Validate a clean SwiftPM consumer from the public GitHub tag.
9. Validate a clean Xcode consumer from the public GitHub tag.
10. Publish Maven Central artifacts.

Public docs must not claim GitHub `.package(url:from:)` support until the HTTPS
asset path and public SwiftPM tag have passed clean-consumer validation with
`SQLITENOW_COMPILER_JAR` unset.

## Current Release Gate

Phase 13D remains blocked until a real release provides:

- a `vX.Y.Z` GitHub Release with compiler, core runtime, and sync runtime assets
- a bare `X.Y.Z` SwiftPM distribution tag
- valid HTTPS asset URLs and checksums
- passing `swiftPmPluginPublicTagCleanConsumerProof` and
  `swiftPmPluginPublicTagXcodeConsumerProof`

Keep local checkout/path-based instructions available for repository
development. Keep Java-free generator distribution, build-tool-plugin
automation, and live Swift realserver support claims deferred until separately
implemented and validated.

## Validation Entry Points

Useful Swift gates:

- `./gradlew swiftHybridSupportGate`
- `./gradlew swiftHybridSupportRealserverGate`
- `./gradlew swiftXcodeOnlyCoreSupportGate`
- `./gradlew swiftXcodeOnlySyncSupportGate`
- `./gradlew swiftPmPluginCleanConsumerProof`
- `./gradlew swiftPmPluginLocalTagCleanConsumerProof`
- `./gradlew swiftPmPluginPublicTagCleanConsumerProof`
- `./gradlew swiftPmPluginPublicTagXcodeConsumerProof`
- `./gradlew swiftExternalXcodeConsumerProof`
- `swift test --package-path swift/plugin/sqlitenow-plugin`

The public-tag proof requires real HTTPS release assets and the published
SwiftPM distribution tag. Local artifact proofs are useful before publishing,
but they are not evidence for public GitHub package support.
