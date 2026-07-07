// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "SQLiteNow",
    platforms: [
        .iOS(.v15),
        .macOS(.v14),
    ],
    products: [
        .plugin(
            name: "SQLiteNowGeneratePlugin",
            targets: ["SQLiteNowGeneratePlugin"]
        ),
        .executable(
            name: "sqlite-now-generate",
            targets: ["SQLiteNowGenerateTool"]
        ),
        .library(
            name: "SQLiteNowCoreRuntime",
            targets: ["SQLiteNowCoreRuntime"]
        ),
        .library(
            name: "SQLiteNowSyncRuntime",
            targets: ["SQLiteNowSyncRuntime"]
        ),
        .library(
            name: "SQLiteNowCoreSupport",
            targets: ["SQLiteNowCoreSupport"]
        ),
        .library(
            name: "SQLiteNowSyncSupport",
            targets: ["SQLiteNowSyncSupport"]
        ),
    ],
    targets: [
        .binaryTarget(
            name: "SQLiteNowCompiler",
            url: "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v0.11.0/SQLiteNowCompiler-0.11.0.artifactbundle.zip",
            checksum: "dd45c9cda45985ca5a7d9b8a298527512bb36d0997cd1d41aa828f2ac51bbc94"
        ),
        .binaryTarget(
            name: "SQLiteNowCoreRuntime",
            url: "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v0.11.0/SQLiteNowCoreRuntime-0.11.0.xcframework.zip",
            checksum: "410e0cce06a4bc467954002ad91231649633b2b0f1d2326da81d1ae3dfdccd47"
        ),
        .binaryTarget(
            name: "SQLiteNowSyncRuntime",
            url: "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v0.11.0/SQLiteNowSyncRuntime-0.11.0.xcframework.zip",
            checksum: "bdd55418a5eda6255e633e0f1adaa388b9df511fca9e8842baf31945c1617a56"
        ),
        .target(
            name: "SQLiteNowCoreSupport",
            dependencies: ["SQLiteNowCoreRuntime"],
            path: "swift/support/Sources/SQLiteNowCoreSupport"
        ),
        .target(
            name: "SQLiteNowSyncSupport",
            dependencies: ["SQLiteNowSyncRuntime"],
            path: "swift/support/Sources/SQLiteNowSyncSupport"
        ),
        .target(
            name: "SQLiteNowGenerateCore",
            path: "swift/plugin/sqlitenow-plugin/Sources/SQLiteNowGenerateCore",
            resources: [
                .copy("Resources"),
            ]
        ),
        .executableTarget(
            name: "SQLiteNowGenerateTool",
            dependencies: ["SQLiteNowGenerateCore"],
            path: "swift/plugin/sqlitenow-plugin/Sources/SQLiteNowGenerateTool"
        ),
        .plugin(
            name: "SQLiteNowGeneratePlugin",
            capability: .command(
                intent: .custom(
                    verb: "sqlitenow-generate",
                    description: "Generate SQLiteNow Swift package sources"
                ),
                permissions: [
                    .writeToPackageDirectory(reason: "Generate SQLiteNow package output under SQLiteNowGenerated"),
                ]
            ),
            dependencies: ["SQLiteNowGenerateTool", "SQLiteNowCompiler"],
            path: "swift/plugin/sqlitenow-plugin/Plugins/SQLiteNowGeneratePlugin"
        ),
        .testTarget(
            name: "SQLiteNowGenerateCoreTests",
            dependencies: ["SQLiteNowGenerateCore"],
            path: "swift/plugin/sqlitenow-plugin/Tests/SQLiteNowGenerateCoreTests"
        ),
    ]
)
