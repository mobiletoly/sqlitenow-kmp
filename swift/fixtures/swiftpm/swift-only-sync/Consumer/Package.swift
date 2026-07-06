// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "SQLiteNowSwiftOnlySyncConsumer",
    platforms: [
        .macOS(.v14),
    ],
    dependencies: [
        .package(path: "../SQLiteNowGenerated/SwiftOnlySyncDatabaseSQLiteNow"),
    ],
    targets: [
        .testTarget(
            name: "SQLiteNowSwiftOnlySyncConsumerTests",
            dependencies: [
                .product(
                    name: "SwiftOnlySyncDatabaseSQLiteNow",
                    package: "SwiftOnlySyncDatabaseSQLiteNow"
                ),
            ]
        ),
    ]
)
