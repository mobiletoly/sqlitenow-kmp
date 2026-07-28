// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "SQLiteNowPlugin",
    platforms: [
        .macOS(.v14),
    ],
    products: [
        .plugin(
            name: "SQLiteNowGeneratePlugin",
            targets: ["SQLiteNowGeneratePlugin"]
        ),
    ],
    targets: [
        .target(
            name: "SQLiteNowGenerateCore",
            resources: [
                .copy("Resources"),
            ]
        ),
        .executableTarget(
            name: "SQLiteNowGenerateTool",
            dependencies: ["SQLiteNowGenerateCore"]
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
            dependencies: ["SQLiteNowGenerateTool"]
        ),
        .testTarget(
            name: "SQLiteNowGenerateCoreTests",
            dependencies: ["SQLiteNowGenerateCore"]
        ),
    ]
)
