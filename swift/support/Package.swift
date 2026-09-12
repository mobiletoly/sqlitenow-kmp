// swift-tools-version: 6.0
import PackageDescription
import Foundation
let a3TestBridge = ProcessInfo.processInfo.environment["OVERSQLITE_A3_SWIFT_TEST_BRIDGE"] == "1"
let runtimePath = a3TestBridge
    ? "../runtime/sync/build/a3-test/runtime/SQLiteNowSyncRuntime.xcframework"
    : "../runtime/sync/build/runtime/SQLiteNowSyncRuntime.xcframework"
let package = Package(
    name: "SQLiteNowStaticSupportOwnership",
    platforms: [.iOS(.v15), .macOS(.v14)],
    targets: [
        .binaryTarget(name: "SQLiteNowSyncRuntime", path: runtimePath),
        .target(name: "SQLiteNowSyncSupport", dependencies: ["SQLiteNowSyncRuntime"]),
        .testTarget(name: "SQLiteNowSyncSupportTests", dependencies: ["SQLiteNowSyncSupport"],
                    swiftSettings: a3TestBridge ? [.define("A3_TEST_BRIDGE")] : [])
    ]
)
