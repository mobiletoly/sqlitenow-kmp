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
            url: "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v0.15.0/SQLiteNowCompiler-0.15.0.artifactbundle.zip",
            checksum: "ac96972661d9b80580a96576a3c955047690d129308aaf25c75c27582743912f"
        ),
        .binaryTarget(
            name: "SQLiteNowCoreRuntime",
            url: "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v0.15.0/SQLiteNowCoreRuntime-0.15.0.xcframework.zip",
            checksum: "0ecdfd7b92f78f9899c37f27fedd568ce4eb6811d40dad6a1a09152813e51e43"
        ),
        .binaryTarget(
            name: "SQLiteNowSyncRuntime",
            url: "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v0.15.0/SQLiteNowSyncRuntime-0.15.0.xcframework.zip",
            checksum: "b02dedcbb1bf5589c16ef0a04389260f2963906feceab4d08e0be4f329a9fb87"
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
