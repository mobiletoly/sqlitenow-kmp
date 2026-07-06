// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "SQLiteNowSampleSwift",
    platforms: [
        .iOS(.v15),
        .macOS(.v14),
    ],
    dependencies: [
        .package(path: "../../plugin/sqlitenow-plugin"),
    ],
    targets: [
        .target(
            name: "SQLiteNowSampleSwift"
        ),
    ]
)
