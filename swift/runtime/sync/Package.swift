// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "SQLiteNowSyncRuntimeSmoke",
    platforms: [
        .iOS(.v15),
        .macOS(.v14),
    ],
    products: [
        .library(
            name: "SQLiteNowSyncRuntimeSmoke",
            targets: ["SQLiteNowSyncRuntime"]
        ),
    ],
    targets: [
        .binaryTarget(
            name: "SQLiteNowSyncRuntime",
            path: "build/runtime/SQLiteNowSyncRuntime.xcframework"
        ),
        .testTarget(
            name: "SQLiteNowSyncRuntimeTests",
            dependencies: ["SQLiteNowSyncRuntime"]
        ),
    ]
)
