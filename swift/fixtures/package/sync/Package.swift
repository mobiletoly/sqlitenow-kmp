// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "SyncFixtureTests",
    platforms: [
        .macOS(.v14),
    ],
    dependencies: [
        .package(path: "../build/swift-package/SyncFixtureDatabaseSQLiteNow"),
    ],
    targets: [
        .testTarget(
            name: "SyncFixtureTests",
            dependencies: [
                .product(name: "SyncFixtureDatabaseSQLiteNow", package: "SyncFixtureDatabaseSQLiteNow"),
            ]
        ),
    ]
)
