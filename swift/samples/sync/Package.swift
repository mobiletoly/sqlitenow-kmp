// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "SQLiteNowSampleSyncSwift",
    platforms: [
        .iOS(.v15),
        .macOS(.v14),
    ],
    dependencies: [
        .package(path: "../../plugin/sqlitenow-plugin"),
    ],
    targets: [
        .target(
            name: "SQLiteNowSampleSyncSwift"
        ),
    ]
)
