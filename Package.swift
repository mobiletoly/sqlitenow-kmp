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
            url: "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v0.17.0/SQLiteNowCompiler-0.17.0.artifactbundle.zip",
            checksum: "f74604b49a5274e801903148f169e6fa4127e90b7de76b3eeb2d677db4c15152"
        ),
        .binaryTarget(
            name: "SQLiteNowCoreRuntime",
            url: "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v0.17.0/SQLiteNowCoreRuntime-0.17.0.xcframework.zip",
            checksum: "af7b5d2b3e5eecace1be3d99cca1524f31bc7a217e6a372d84bc7b2d271528ba"
        ),
        .binaryTarget(
            name: "SQLiteNowSyncRuntime",
            url: "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v0.17.0/SQLiteNowSyncRuntime-0.17.0.xcframework.zip",
            checksum: "cc6ac5ff450691240e9a240b9af5ec44cc41f89482c0c630521cf6a9d3ae7793"
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
