// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "SQLiteNowSwiftOnlyCoreConsumer",
    platforms: [
        .macOS(.v14),
    ],
    dependencies: [
        .package(path: "../SQLiteNowGenerated/SwiftOnlyCoreDatabaseSQLiteNow"),
    ],
    targets: [
        .testTarget(
            name: "SQLiteNowSwiftOnlyCoreConsumerTests",
            dependencies: [
                .product(
                    name: "SwiftOnlyCoreDatabaseSQLiteNow",
                    package: "SwiftOnlyCoreDatabaseSQLiteNow"
                ),
            ]
        ),
    ]
)
