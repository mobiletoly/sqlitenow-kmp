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
            url: "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v0.16.0/SQLiteNowCompiler-0.16.0.artifactbundle.zip",
            checksum: "dbe9b2188c2d254fbedef580f0a06b2c66a20105199de71127e6a54c46f303b9"
        ),
        .binaryTarget(
            name: "SQLiteNowCoreRuntime",
            url: "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v0.16.0/SQLiteNowCoreRuntime-0.16.0.xcframework.zip",
            checksum: "619a3c642b47c2b15c3836d22f45c1024f60b32e72f588405247f9aa38bb70b2"
        ),
        .binaryTarget(
            name: "SQLiteNowSyncRuntime",
            url: "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v0.16.0/SQLiteNowSyncRuntime-0.16.0.xcframework.zip",
            checksum: "9cf5f34a5f65c65d9d3781f6d80afc9171c9a482aec5aa093451458386b1b734"
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
