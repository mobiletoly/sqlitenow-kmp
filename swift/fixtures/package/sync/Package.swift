// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "SyncFixtureTests",
    platforms: [
        .iOS(.v15),
        .macOS(.v14),
    ],
    dependencies: [
        .package(path: "../build/swift-package/CoordinatedOpeningGenerationTwoDatabaseSQLiteNow"),
    ],
    targets: [
        .target(
            name: "SyncFixtureDatabaseSQLiteNow",
            dependencies: [
                .product(name: "CoordinatedOpeningGenerationTwoDatabaseSQLiteNow", package: "CoordinatedOpeningGenerationTwoDatabaseSQLiteNow"),
            ],
            path: "build/generated/sqlitenow/swift-product/SyncFixtureDatabaseSQLiteNow"
        ),
        .target(
            name: "CoordinatedOpeningGenerationOneDatabaseSQLiteNow",
            dependencies: [
                .product(name: "CoordinatedOpeningGenerationTwoDatabaseSQLiteNow", package: "CoordinatedOpeningGenerationTwoDatabaseSQLiteNow"),
            ],
            path: "build/generated/sqlitenow/swift-product/CoordinatedOpeningGenerationOneDatabaseSQLiteNow"
        ),
        .target(
            name: "SchemaEvolutionGenerationOneDatabaseSQLiteNow",
            dependencies: [
                .product(name: "CoordinatedOpeningGenerationTwoDatabaseSQLiteNow", package: "CoordinatedOpeningGenerationTwoDatabaseSQLiteNow"),
            ],
            path: "build/generated/sqlitenow/swift-product/SchemaEvolutionGenerationOneDatabaseSQLiteNow"
        ),
        .target(
            name: "SchemaEvolutionLocalTwoDatabaseSQLiteNow",
            dependencies: [
                .product(name: "CoordinatedOpeningGenerationTwoDatabaseSQLiteNow", package: "CoordinatedOpeningGenerationTwoDatabaseSQLiteNow"),
            ],
            path: "build/generated/sqlitenow/swift-product/SchemaEvolutionLocalTwoDatabaseSQLiteNow"
        ),
        .target(
            name: "SchemaEvolutionGenerationTargetDatabaseSQLiteNow",
            dependencies: [
                .product(name: "CoordinatedOpeningGenerationTwoDatabaseSQLiteNow", package: "CoordinatedOpeningGenerationTwoDatabaseSQLiteNow"),
            ],
            path: "build/generated/sqlitenow/swift-product/SchemaEvolutionGenerationTargetDatabaseSQLiteNow"
        ),
        .target(
            name: "SchemaEvolutionInvalidTargetDatabaseSQLiteNow",
            dependencies: [
                .product(name: "CoordinatedOpeningGenerationTwoDatabaseSQLiteNow", package: "CoordinatedOpeningGenerationTwoDatabaseSQLiteNow"),
            ],
            path: "build/generated/sqlitenow/swift-product/SchemaEvolutionInvalidTargetDatabaseSQLiteNow"
        ),
        .testTarget(
            name: "SyncFixtureTests",
            dependencies: [
                "SyncFixtureDatabaseSQLiteNow",
                "CoordinatedOpeningGenerationOneDatabaseSQLiteNow",
                "SchemaEvolutionGenerationOneDatabaseSQLiteNow",
                "SchemaEvolutionLocalTwoDatabaseSQLiteNow",
                "SchemaEvolutionGenerationTargetDatabaseSQLiteNow",
                "SchemaEvolutionInvalidTargetDatabaseSQLiteNow",
                .product(name: "CoordinatedOpeningGenerationTwoDatabaseSQLiteNow", package: "CoordinatedOpeningGenerationTwoDatabaseSQLiteNow"),
            ]
        ),
    ]
)
