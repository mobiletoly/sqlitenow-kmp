// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "CoreFixtureTests",
    platforms: [
        .macOS(.v14),
    ],
    dependencies: [
        .package(path: "../build/swift-package/CoreFixtureDatabaseSQLiteNow"),
    ],
    targets: [
        .testTarget(
            name: "CoreFixtureTests",
            dependencies: [
                .product(name: "CoreFixtureDatabaseSQLiteNow", package: "CoreFixtureDatabaseSQLiteNow"),
            ]
        ),
    ]
)
