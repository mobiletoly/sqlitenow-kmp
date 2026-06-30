# SwiftPM plugin release automation

This directory contains support files for the SwiftPM command-plugin release
lane owned by `gradle/swiftpm-plugin-release.gradle.kts`.

## Release shape

- The development checkout does not keep a root `Package.swift`.
- `stageSwiftPmPluginDistribution` creates a text-only SwiftPM package tree at
  `build/swiftpm-plugin-distribution/sqlitenow-kmp`.
- The staged package root contains a generated `Package.swift` rendered from
  `Package.swift.template`.
- The released SwiftPM tag contains source and metadata only. It must not
  contain jars, zips, artifact bundles, XCFrameworks, or other binary payloads.
- The `SQLiteNowCompiler` binary target points at the GitHub Release asset
  `SQLiteNowCompiler-<version>.artifactbundle.zip`.
- Generated Swift runtime packages point at the GitHub Release XCFramework zip
  assets for core and sync runtimes.
- The compiler remains Java-backed and requires Java 17 on the machine running
  the SwiftPM plugin.
- Runtime and compiler release zips are created with reproducible archive
  settings so rerunning the same inputs should keep SwiftPM checksums stable.

## Proof modes

- `swiftPmPluginCleanConsumerProof` owns the local-artifact proof mode and
  validates a local file-backed SwiftPM tag using local compiler/runtime
  artifacts. This is the cheapest local proof.
- `swiftPmPluginLocalTagCleanConsumerProof` owns the remote-artifact/local-tag
  proof mode and validates a local file-backed SwiftPM tag whose manifest still
  points at remote HTTPS release assets.
- `swiftPmPluginPublicTagCleanConsumerProof` owns the public-tag proof mode and
  validates a clean consumer against the published HTTPS Git tag.
- `swiftPmPluginPublicTagXcodeConsumerProof` owns the public-tag Xcode proof
  mode and additionally builds the Xcode consumer template outside the
  repository.
- `swiftExternalXcodeConsumerProof` owns the local-artifact Xcode proof mode and
  additionally builds the Xcode consumer template outside the repository.

## Release order

1. Build Swift runtime release artifacts.
2. Build the SwiftPM compiler artifact bundle.
3. Stage and inspect the text-only SwiftPM plugin distribution.
4. Validate a clean SwiftPM consumer with local release artifacts.
5. Upload runtime and compiler release assets.
6. Validate the local distribution Git tag against the uploaded assets.
7. Publish the SwiftPM plugin distribution tag.
8. Validate a clean SwiftPM consumer from the public GitHub tag.
9. Validate a clean Xcode consumer from the public GitHub tag.
10. Publish Maven Central artifacts.

The asset upload must happen before remote-artifact clean-consumer proofs
because SwiftPM binary targets require resolvable HTTPS artifacts.
