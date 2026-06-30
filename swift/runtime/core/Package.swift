// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "SQLiteNowCoreRuntimeSmoke",
    platforms: [
        .iOS(.v15),
        .macOS(.v14),
    ],
    products: [
        .library(
            name: "SQLiteNowCoreRuntimeSmoke",
            targets: ["SQLiteNowCoreRuntime"]
        ),
    ],
    targets: [
        .binaryTarget(
            name: "SQLiteNowCoreRuntime",
            path: "build/runtime/SQLiteNowCoreRuntime.xcframework"
        ),
        .testTarget(
            name: "SQLiteNowCoreRuntimeTests",
            dependencies: ["SQLiteNowCoreRuntime"]
        ),
    ]
)
