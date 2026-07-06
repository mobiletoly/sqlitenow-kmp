import Foundation
@testable import SQLiteNowGenerateCore
import XCTest

final class SQLiteNowGenerateCoreTests: XCTestCase {
    private var tempRoot: URL!

    override func setUpWithError() throws {
        tempRoot = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("sqlitenow-generate-core-tests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempRoot, withIntermediateDirectories: true)
    }

    override func tearDownWithError() throws {
        try? FileManager.default.removeItem(at: tempRoot)
    }

    func testCommandLineConfigPathResolutionUsesFinalPackageRootRegardlessOfArgumentOrder() throws {
        let packageRoot = tempRoot.appendingPathComponent("App")
        let expectedConfigPath = packageRoot
            .appendingPathComponent("Config/SQLiteNow.json")
            .standardizedFileURL

        let configFirst = try CommandLineOptions.parse(
            arguments: [
                "--config", "Config/SQLiteNow.json",
                "--package-root", packageRoot.path,
            ],
            currentDirectory: tempRoot
        )
        let packageRootFirst = try CommandLineOptions.parse(
            arguments: [
                "--package-root", packageRoot.path,
                "--config", "Config/SQLiteNow.json",
            ],
            currentDirectory: tempRoot
        )

        XCTAssertEqual(try XCTUnwrap(configFirst.configPath), expectedConfigPath)
        XCTAssertEqual(try XCTUnwrap(packageRootFirst.configPath), expectedConfigPath)
    }

    func testCommandLineUserPackageRootOverridesPluginPrependedRoot() throws {
        let xcodeProjectRoot = tempRoot.appendingPathComponent("App/iosApp/iosApp.xcodeproj")
        let packageRoot = tempRoot.appendingPathComponent("App")

        let options = try CommandLineOptions.parse(
            arguments: [
                "--package-root", xcodeProjectRoot.path,
                "--config", "Config/SQLiteNow.json",
                "--package-root", packageRoot.path,
            ],
            currentDirectory: tempRoot
        )

        XCTAssertEqual(options.packageRoot, packageRoot.standardizedFileURL)
        XCTAssertEqual(
            try XCTUnwrap(options.configPath),
            packageRoot.appendingPathComponent("Config/SQLiteNow.json").standardizedFileURL
        )
    }

    func testCommandLineParsesCompilerExecutable() throws {
        let options = try CommandLineOptions.parse(
            arguments: [
                "--package-root", tempRoot.path,
                "--compiler-executable", "Tools/sqlitenow-compiler",
                "--compiler-jar", "Compiler/sqlitenow-compiler.jar",
            ],
            currentDirectory: tempRoot
        )

        XCTAssertEqual(options.compilerExecutablePath, "Tools/sqlitenow-compiler")
        XCTAssertEqual(options.compilerJarPath, "Compiler/sqlitenow-compiler.jar")
    }

    func testConfigDefaultsMapToCompilerPackageRequest() throws {
        _ = try createSQLDirectory(databaseName: "AppDatabase")
        let runtimeDir = try createRuntimeDirectory()
        let config = """
        {
          "schemaVersion": 1,
          "databases": [
            {
              "databaseName": "AppDatabase",
              "swiftPackageName": "AppDatabaseSQLiteNow",
              "swiftTargetName": "AppDatabaseSQLiteNow",
              "runtime": "core",
              "runtimeXcframeworkDirectory": "\(runtimeDir)"
            }
          ]
        }
        """
        let configPath = try writeConfig(config)

        let loaded = try SQLiteNowConfigLoader.load(configPath: configPath)
        let invocations = try SQLiteNowCompilerRequestBuilder.buildInvocations(
            config: loaded,
            configPath: configPath,
            packageRoot: tempRoot,
            compilerVersion: "1.2.3"
        )

        XCTAssertEqual(invocations.count, 1)
        let request = try jsonObject(invocations.single().requestJSON)
        XCTAssertEqual(request["contractVersion"] as? Int, 2)
        XCTAssertEqual(request["generationMode"] as? String, "swiftProductPackage")
        XCTAssertEqual(request["databaseName"] as? String, "AppDatabase")
        XCTAssertEqual(request["sqlDirectory"] as? String, "SQLiteNow/databases/AppDatabase")
        XCTAssertEqual(request["metadataPackageName"] as? String, "sqlitenow.generated.appdatabase.metadata")
        XCTAssertEqual(request["compilerOutputDirectory"] as? String, ".build/sqlitenow/compiler/AppDatabase")
        XCTAssertEqual(request["swiftPackageOutputDirectory"] as? String, "SQLiteNowGenerated/AppDatabaseSQLiteNow")
        XCTAssertEqual(request["swiftPackageName"] as? String, "AppDatabaseSQLiteNow")
        XCTAssertEqual(request["swiftTargetName"] as? String, "AppDatabaseSQLiteNow")
        XCTAssertEqual(request["runtime"] as? String, "core")
        XCTAssertEqual(request["runtimeModuleName"] as? String, "SQLiteNowCoreRuntime")
        XCTAssertEqual(request["runtimeXcframeworkDirectory"] as? String, runtimeDir)
        XCTAssertEqual(request["sqliteNowVersion"] as? String, "1.2.3")
        XCTAssertNil(request["schemaDatabaseFile"])
        XCTAssertEqual(
            try invocations.single().directoriesToCreate.map(\.path),
            [
                tempRoot
                    .appendingPathComponent(".build/sqlitenow/requests")
                    .standardizedFileURL
                    .path
            ]
        )
    }

    func testDebugRequestsUseToolOwnedSchemaDatabaseFile() throws {
        _ = try createSQLDirectory(databaseName: "DebugDatabase")
        let runtimeDir = try createRuntimeDirectory()
        let config = """
        {
          "schemaVersion": 1,
          "databases": [
            {
              "databaseName": "DebugDatabase",
              "swiftPackageName": "DebugDatabaseSQLiteNow",
              "swiftTargetName": "DebugDatabaseSQLiteNow",
              "runtime": "core",
              "runtimeXcframeworkDirectory": "\(runtimeDir)",
              "debug": true
            }
          ]
        }
        """
        let configPath = try writeConfig(config)

        let loaded = try SQLiteNowConfigLoader.load(configPath: configPath)
        let invocation = try SQLiteNowCompilerRequestBuilder.buildInvocations(
            config: loaded,
            configPath: configPath,
            packageRoot: tempRoot,
            compilerVersion: "1.2.3"
        ).single()

        let request = try jsonObject(invocation.requestJSON)
        XCTAssertEqual(request["debug"] as? Bool, true)
        XCTAssertEqual(request["schemaDatabaseFile"] as? String, ".build/sqlitenow/schema/DebugDatabase.db")
        XCTAssertEqual(
            invocation.directoriesToCreate.map(\.path),
            [
                tempRoot
                    .appendingPathComponent(".build/sqlitenow/requests")
                    .standardizedFileURL
                    .path,
                tempRoot
                    .appendingPathComponent(".build/sqlitenow/schema")
                    .standardizedFileURL
                    .path
            ]
        )
    }

    func testLocalRuntimeZipMapsToRuntimeArtifactRequest() throws {
        let sqlDir = try createSQLDirectory(databaseName: "ZipDatabase")
        let runtimeZip = try createRuntimeZip()
        let config = """
        {
          "schemaVersion": 1,
          "databases": [
            {
              "databaseName": "ZipDatabase",
              "sqlDirectory": "\(sqlDir)",
              "swiftPackageName": "ZipDatabaseSQLiteNow",
              "swiftTargetName": "ZipDatabaseSQLiteNow",
              "runtime": "core",
              "runtimeArtifact": {
                "kind": "localZip",
                "path": "\(runtimeZip)",
                "checksum": "abc123",
                "sqliteNowVersion": "1.2.3"
              }
            }
          ]
        }
        """
        let configPath = try writeConfig(config)

        let loaded = try SQLiteNowConfigLoader.load(configPath: configPath)
        let invocation = try SQLiteNowCompilerRequestBuilder.buildInvocations(
            config: loaded,
            configPath: configPath,
            packageRoot: tempRoot,
            compilerVersion: "9.9.9"
        ).single()

        let request = try jsonObject(invocation.requestJSON)
        let artifact = try XCTUnwrap(request["runtimeArtifact"] as? [String: Any])
        XCTAssertNil(request["runtimeXcframeworkDirectory"])
        XCTAssertEqual(artifact["kind"] as? String, "localZip")
        XCTAssertEqual(artifact["path"] as? String, runtimeZip)
        XCTAssertEqual(artifact["checksum"] as? String, "abc123")
        XCTAssertEqual(artifact["sqliteNowVersion"] as? String, "1.2.3")
        XCTAssertEqual(request["sqliteNowVersion"] as? String, "1.2.3")
    }

    func testRemoteRuntimeZipMapsToRuntimeArtifactRequest() throws {
        let sqlDir = try createSQLDirectory(databaseName: "RemoteZipDatabase")
        let runtimeURL = "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v1.2.3/SQLiteNowCoreRuntime-1.2.3.xcframework.zip"
        let config = """
        {
          "schemaVersion": 1,
          "databases": [
            {
              "databaseName": "RemoteZipDatabase",
              "sqlDirectory": "\(sqlDir)",
              "swiftPackageName": "RemoteZipDatabaseSQLiteNow",
              "swiftTargetName": "RemoteZipDatabaseSQLiteNow",
              "runtime": "core",
              "runtimeArtifact": {
                "kind": "remoteZip",
                "url": "\(runtimeURL)",
                "checksum": "abc123",
                "sqliteNowVersion": "1.2.3"
              }
            }
          ]
        }
        """
        let configPath = try writeConfig(config)

        let loaded = try SQLiteNowConfigLoader.load(configPath: configPath)
        let invocation = try SQLiteNowCompilerRequestBuilder.buildInvocations(
            config: loaded,
            configPath: configPath,
            packageRoot: tempRoot,
            compilerVersion: "9.9.9"
        ).single()

        let request = try jsonObject(invocation.requestJSON)
        let artifact = try XCTUnwrap(request["runtimeArtifact"] as? [String: Any])
        XCTAssertNil(request["runtimeXcframeworkDirectory"])
        XCTAssertEqual(artifact["kind"] as? String, "remoteZip")
        XCTAssertNil(artifact["path"])
        XCTAssertEqual(artifact["url"] as? String, runtimeURL)
        XCTAssertEqual(artifact["checksum"] as? String, "abc123")
        XCTAssertEqual(artifact["sqliteNowVersion"] as? String, "1.2.3")
        XCTAssertEqual(request["sqliteNowVersion"] as? String, "1.2.3")
    }

    func testReleaseDistributionRuntimeArtifactsMapToCompilerPackageRequests() throws {
        _ = try createSQLDirectory(databaseName: "ReleaseCoreDatabase")
        _ = try createSQLDirectory(databaseName: "ReleaseSyncDatabase")
        let releaseDistribution = releaseDistribution()
        let config = """
        {
          "schemaVersion": 1,
          "databases": [
            {
              "databaseName": "ReleaseCoreDatabase",
              "swiftPackageName": "ReleaseCoreDatabaseSQLiteNow",
              "swiftTargetName": "ReleaseCoreDatabaseSQLiteNow",
              "runtime": "core"
            },
            {
              "databaseName": "ReleaseSyncDatabase",
              "swiftPackageName": "ReleaseSyncDatabaseSQLiteNow",
              "swiftTargetName": "ReleaseSyncDatabaseSQLiteNow",
              "runtime": "sync"
            }
          ]
        }
        """
        let configPath = try writeConfig(config)

        let loaded = try SQLiteNowConfigLoader.load(configPath: configPath)
        let invocations = try SQLiteNowCompilerRequestBuilder.buildInvocations(
            config: loaded,
            configPath: configPath,
            packageRoot: tempRoot,
            compilerVersion: "1.2.3",
            releaseDistribution: releaseDistribution
        )

        let coreRequest = try jsonObject(try invocations.single { $0.databaseName == "ReleaseCoreDatabase" }.requestJSON)
        let coreArtifact = try XCTUnwrap(coreRequest["runtimeArtifact"] as? [String: Any])
        XCTAssertNil(coreRequest["runtimeXcframeworkDirectory"])
        XCTAssertEqual(coreRequest["runtimeModuleName"] as? String, "SQLiteNowCoreRuntime")
        XCTAssertEqual(coreArtifact["kind"] as? String, "remoteZip")
        XCTAssertEqual(coreArtifact["url"] as? String, "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v1.2.3/SQLiteNowCoreRuntime-1.2.3.xcframework.zip")
        XCTAssertEqual(coreArtifact["checksum"] as? String, "corechecksum")
        XCTAssertEqual(coreArtifact["sqliteNowVersion"] as? String, "1.2.3")
        XCTAssertEqual(coreRequest["sqliteNowVersion"] as? String, "1.2.3")

        let syncRequest = try jsonObject(try invocations.single { $0.databaseName == "ReleaseSyncDatabase" }.requestJSON)
        let syncArtifact = try XCTUnwrap(syncRequest["runtimeArtifact"] as? [String: Any])
        XCTAssertNil(syncRequest["runtimeXcframeworkDirectory"])
        XCTAssertEqual(syncRequest["runtimeModuleName"] as? String, "SQLiteNowSyncRuntime")
        XCTAssertEqual(syncArtifact["kind"] as? String, "remoteZip")
        XCTAssertEqual(syncArtifact["url"] as? String, "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v1.2.3/SQLiteNowSyncRuntime-1.2.3.xcframework.zip")
        XCTAssertEqual(syncArtifact["checksum"] as? String, "syncchecksum")
        XCTAssertEqual(syncArtifact["sqliteNowVersion"] as? String, "1.2.3")
        XCTAssertEqual(syncRequest["sqliteNowVersion"] as? String, "1.2.3")
    }

    func testExplicitRuntimeArtifactOverridesReleaseDistributionRuntimeArtifact() throws {
        let sqlDir = try createSQLDirectory(databaseName: "ExplicitRemoteDatabase")
        let explicitURL = "https://example.com/SQLiteNowCoreRuntime-1.2.3.xcframework.zip"
        let config = """
        {
          "schemaVersion": 1,
          "databases": [
            {
              "databaseName": "ExplicitRemoteDatabase",
              "sqlDirectory": "\(sqlDir)",
              "swiftPackageName": "ExplicitRemoteDatabaseSQLiteNow",
              "swiftTargetName": "ExplicitRemoteDatabaseSQLiteNow",
              "runtime": "core",
              "runtimeArtifact": {
                "kind": "remoteZip",
                "url": "\(explicitURL)",
                "checksum": "explicitchecksum",
                "sqliteNowVersion": "1.2.3"
              }
            }
          ]
        }
        """
        let configPath = try writeConfig(config)

        let loaded = try SQLiteNowConfigLoader.load(configPath: configPath)
        let invocation = try SQLiteNowCompilerRequestBuilder.buildInvocations(
            config: loaded,
            configPath: configPath,
            packageRoot: tempRoot,
            compilerVersion: "1.2.3",
            releaseDistribution: releaseDistribution()
        ).single()

        let request = try jsonObject(invocation.requestJSON)
        let artifact = try XCTUnwrap(request["runtimeArtifact"] as? [String: Any])
        XCTAssertEqual(artifact["url"] as? String, explicitURL)
        XCTAssertEqual(artifact["checksum"] as? String, "explicitchecksum")
    }

    func testRejectsMissingRuntimeArtifactWhenNoReleaseDistributionIsAvailable() throws {
        _ = try createSQLDirectory(databaseName: "NoReleaseMetadataDatabase")
        let config = """
        {
          "schemaVersion": 1,
          "databases": [
            {
              "databaseName": "NoReleaseMetadataDatabase",
              "swiftPackageName": "NoReleaseMetadataDatabaseSQLiteNow",
              "swiftTargetName": "NoReleaseMetadataDatabaseSQLiteNow",
              "runtime": "core"
            }
          ]
        }
        """
        let configPath = try writeConfig(config)
        let loaded = try SQLiteNowConfigLoader.load(configPath: configPath)

        XCTAssertThrowsError(
            try SQLiteNowCompilerRequestBuilder.buildInvocations(
                config: loaded,
                configPath: configPath,
                packageRoot: tempRoot,
                compilerVersion: "1.2.3"
            )
        ) { error in
            XCTAssertTrue(error.localizedDescription.contains("requires runtimeArtifact or runtimeXcframeworkDirectory"))
            XCTAssertTrue(error.localizedDescription.contains("release distribution metadata"))
        }
    }

    func testRejectsReleaseDistributionRuntimeModuleMismatch() throws {
        _ = try createSQLDirectory(databaseName: "MismatchDatabase")
        let config = """
        {
          "schemaVersion": 1,
          "databases": [
            {
              "databaseName": "MismatchDatabase",
              "swiftPackageName": "MismatchDatabaseSQLiteNow",
              "swiftTargetName": "MismatchDatabaseSQLiteNow",
              "runtime": "core",
              "runtimeModuleName": "CustomRuntime"
            }
          ]
        }
        """
        let configPath = try writeConfig(config)
        let loaded = try SQLiteNowConfigLoader.load(configPath: configPath)

        XCTAssertThrowsError(
            try SQLiteNowCompilerRequestBuilder.buildInvocations(
                config: loaded,
                configPath: configPath,
                packageRoot: tempRoot,
                compilerVersion: "1.2.3",
                releaseDistribution: releaseDistribution()
            )
        ) { error in
            XCTAssertTrue(error.localizedDescription.contains("runtimeModuleName 'SQLiteNowCoreRuntime' does not match runtimeModuleName 'CustomRuntime'"))
        }
    }

    func testExplicitConfigPathResolvesInputsRelativeToConfigAndEmitsPackageRelativeRequestPaths() throws {
        _ = try createSQLDirectory(
            databaseName: "NestedConfigDatabase",
            relativePath: "DatabaseSQL/NestedConfigDatabase"
        )
        _ = try createRuntimeDirectory()
        let configDir = tempRoot.appendingPathComponent("Config")
        try FileManager.default.createDirectory(at: configDir, withIntermediateDirectories: true)
        let configPath = configDir.appendingPathComponent("SQLiteNow.json")
        try """
        {
          "schemaVersion": 1,
          "databases": [
            {
              "databaseName": "NestedConfigDatabase",
              "sqlDirectory": "../DatabaseSQL/NestedConfigDatabase",
              "swiftPackageName": "NestedConfigDatabaseSQLiteNow",
              "swiftTargetName": "NestedConfigDatabaseSQLiteNow",
              "outputDirectory": "../Generated/NestedConfigDatabaseSQLiteNow",
              "runtime": "core",
              "runtimeXcframeworkDirectory": "../Runtime/SQLiteNowCoreRuntime.xcframework"
            }
          ]
        }
        """.write(to: configPath, atomically: true, encoding: .utf8)

        let loaded = try SQLiteNowConfigLoader.load(configPath: configPath)
        let invocation = try SQLiteNowCompilerRequestBuilder.buildInvocations(
            config: loaded,
            configPath: configPath,
            packageRoot: tempRoot,
            compilerVersion: "1.2.3"
        ).single()

        let request = try jsonObject(invocation.requestJSON)
        XCTAssertEqual(request["sqlDirectory"] as? String, "DatabaseSQL/NestedConfigDatabase")
        XCTAssertEqual(request["swiftPackageOutputDirectory"] as? String, "Generated/NestedConfigDatabaseSQLiteNow")
        XCTAssertEqual(request["runtimeXcframeworkDirectory"] as? String, "Runtime/SQLiteNowCoreRuntime.xcframework")
        XCTAssertEqual(invocation.instructions.relativePackagePath, "Generated/NestedConfigDatabaseSQLiteNow")
    }

    func testNestedConfigDefaultOutputDirectoryUsesPackageRoot() throws {
        _ = try createSQLDirectory(
            databaseName: "NestedDefaultDatabase",
            relativePath: "DatabaseSQL/NestedDefaultDatabase"
        )
        _ = try createRuntimeDirectory()
        let configDir = tempRoot.appendingPathComponent("Config")
        try FileManager.default.createDirectory(at: configDir, withIntermediateDirectories: true)
        let configPath = configDir.appendingPathComponent("SQLiteNow.json")
        try """
        {
          "schemaVersion": 1,
          "databases": [
            {
              "databaseName": "NestedDefaultDatabase",
              "sqlDirectory": "../DatabaseSQL/NestedDefaultDatabase",
              "swiftPackageName": "NestedDefaultDatabaseSQLiteNow",
              "swiftTargetName": "NestedDefaultDatabaseSQLiteNow",
              "runtime": "core",
              "runtimeXcframeworkDirectory": "../Runtime/SQLiteNowCoreRuntime.xcframework"
            }
          ]
        }
        """.write(to: configPath, atomically: true, encoding: .utf8)

        let loaded = try SQLiteNowConfigLoader.load(configPath: configPath)
        let invocation = try SQLiteNowCompilerRequestBuilder.buildInvocations(
            config: loaded,
            configPath: configPath,
            packageRoot: tempRoot,
            compilerVersion: "1.2.3"
        ).single()

        let request = try jsonObject(invocation.requestJSON)
        XCTAssertEqual(request["sqlDirectory"] as? String, "DatabaseSQL/NestedDefaultDatabase")
        XCTAssertEqual(request["swiftPackageOutputDirectory"] as? String, "SQLiteNowGenerated/NestedDefaultDatabaseSQLiteNow")
        XCTAssertEqual(request["runtimeXcframeworkDirectory"] as? String, "Runtime/SQLiteNowCoreRuntime.xcframework")
        XCTAssertEqual(invocation.instructions.relativePackagePath, "SQLiteNowGenerated/NestedDefaultDatabaseSQLiteNow")
    }

    func testRejectsDuplicateDatabaseNames() throws {
        let sqlDir = try createSQLDirectory(databaseName: "DuplicateDatabase")
        let runtimeDir = try createRuntimeDirectory()
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "DuplicateDatabase",
                  "sqlDirectory": "\(sqlDir)",
                  "swiftPackageName": "OneSQLiteNow",
                  "swiftTargetName": "OneSQLiteNow",
                  "runtime": "core",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)"
                },
                {
                  "databaseName": "DuplicateDatabase",
                  "sqlDirectory": "\(sqlDir)",
                  "swiftPackageName": "TwoSQLiteNow",
                  "swiftTargetName": "TwoSQLiteNow",
                  "runtime": "core",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)"
                }
              ]
            }
            """
        )

        XCTAssertThrowsError(try SQLiteNowConfigLoader.load(configPath: configPath)) { error in
            XCTAssertTrue(error.localizedDescription.contains("Duplicate SQLiteNow databaseName"))
        }
    }

    func testRejectsDuplicateSwiftPackageNames() throws {
        let sqlDirOne = try createSQLDirectory(databaseName: "OneDatabase")
        let sqlDirTwo = try createSQLDirectory(databaseName: "TwoDatabase")
        let runtimeDir = try createRuntimeDirectory()
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "OneDatabase",
                  "sqlDirectory": "\(sqlDirOne)",
                  "swiftPackageName": "SharedSQLiteNow",
                  "swiftTargetName": "OneSQLiteNow",
                  "runtime": "core",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)"
                },
                {
                  "databaseName": "TwoDatabase",
                  "sqlDirectory": "\(sqlDirTwo)",
                  "swiftPackageName": "SharedSQLiteNow",
                  "swiftTargetName": "TwoSQLiteNow",
                  "runtime": "core",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)"
                }
              ]
            }
            """
        )

        XCTAssertThrowsError(try SQLiteNowConfigLoader.load(configPath: configPath)) { error in
            XCTAssertTrue(error.localizedDescription.contains("Duplicate SQLiteNow swiftPackageName"))
        }
    }

    func testRejectsUnsupportedRequestedAppleTargets() throws {
        let sqlDir = try createSQLDirectory(databaseName: "UnsupportedTargetDatabase")
        let runtimeDir = try createRuntimeDirectory()
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "UnsupportedTargetDatabase",
                  "sqlDirectory": "\(sqlDir)",
                  "swiftPackageName": "UnsupportedTargetSQLiteNow",
                  "swiftTargetName": "UnsupportedTargetSQLiteNow",
                  "runtime": "core",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)",
                  "requestedAppleTargets": ["macosArm64", "macosX64", "iosX64"]
                }
              ]
            }
            """
        )

        XCTAssertThrowsError(try SQLiteNowConfigLoader.load(configPath: configPath)) { error in
            XCTAssertTrue(error.localizedDescription.contains("Unsupported requestedAppleTargets"))
            XCTAssertTrue(error.localizedDescription.contains("macosX64, iosX64"))
            XCTAssertTrue(error.localizedDescription.contains("x86_64 Apple slices are not supported"))
        }
    }

    func testRejectsDuplicateGeneratedPackageDestinations() throws {
        let sqlDirOne = try createSQLDirectory(databaseName: "OneDatabase")
        let sqlDirTwo = try createSQLDirectory(databaseName: "TwoDatabase")
        let runtimeDir = try createRuntimeDirectory()
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "OneDatabase",
                  "sqlDirectory": "\(sqlDirOne)",
                  "swiftPackageName": "OneSQLiteNow",
                  "swiftTargetName": "OneSQLiteNow",
                  "outputDirectory": "SQLiteNowGenerated/SharedSQLiteNow",
                  "runtime": "core",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)"
                },
                {
                  "databaseName": "TwoDatabase",
                  "sqlDirectory": "\(sqlDirTwo)",
                  "swiftPackageName": "TwoSQLiteNow",
                  "swiftTargetName": "TwoSQLiteNow",
                  "outputDirectory": "SQLiteNowGenerated/SharedSQLiteNow",
                  "runtime": "core",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)"
                }
              ]
            }
            """
        )
        let loaded = try SQLiteNowConfigLoader.load(configPath: configPath)

        XCTAssertThrowsError(
            try SQLiteNowCompilerRequestBuilder.buildInvocations(
                config: loaded,
                configPath: configPath,
                packageRoot: tempRoot,
                compilerVersion: "1.2.3"
            )
        ) { error in
            XCTAssertTrue(error.localizedDescription.contains("Duplicate SQLiteNow generated package output"))
            XCTAssertTrue(error.localizedDescription.contains("SQLiteNowGenerated/SharedSQLiteNow"))
        }
    }

    func testRejectsGeneratedPackageOutputOutsidePackageRoot() throws {
        let sqlDir = try createSQLDirectory(databaseName: "EscapingOutputDatabase")
        let runtimeDir = try createRuntimeDirectory()
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "EscapingOutputDatabase",
                  "sqlDirectory": "\(sqlDir)",
                  "swiftPackageName": "EscapingOutputDatabaseSQLiteNow",
                  "swiftTargetName": "EscapingOutputDatabaseSQLiteNow",
                  "outputDirectory": "../EscapingOutputDatabaseSQLiteNow",
                  "runtime": "core",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)"
                }
              ]
            }
            """
        )
        let loaded = try SQLiteNowConfigLoader.load(configPath: configPath)

        XCTAssertThrowsError(
            try SQLiteNowCompilerRequestBuilder.buildInvocations(
                config: loaded,
                configPath: configPath,
                packageRoot: tempRoot,
                compilerVersion: "1.2.3"
            )
        ) { error in
            XCTAssertTrue(error.localizedDescription.contains("outputDirectory"))
            XCTAssertTrue(error.localizedDescription.contains("must stay inside the package root"))
        }
    }

    func testRejectsGeneratedPackageOutputThroughSymlinkOutsidePackageRoot() throws {
        let sqlDir = try createSQLDirectory(databaseName: "SymlinkEscapingOutputDatabase")
        let runtimeDir = try createRuntimeDirectory()
        let externalOutputRoot = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("sqlitenow-generate-core-external-output-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: externalOutputRoot, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: externalOutputRoot) }
        try FileManager.default.createSymbolicLink(
            at: tempRoot.appendingPathComponent("SQLiteNowGenerated"),
            withDestinationURL: externalOutputRoot
        )
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "SymlinkEscapingOutputDatabase",
                  "sqlDirectory": "\(sqlDir)",
                  "swiftPackageName": "SymlinkEscapingOutputDatabaseSQLiteNow",
                  "swiftTargetName": "SymlinkEscapingOutputDatabaseSQLiteNow",
                  "outputDirectory": "SQLiteNowGenerated/SymlinkEscapingOutputDatabaseSQLiteNow",
                  "runtime": "core",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)"
                }
              ]
            }
            """
        )
        let loaded = try SQLiteNowConfigLoader.load(configPath: configPath)

        XCTAssertThrowsError(
            try SQLiteNowCompilerRequestBuilder.buildInvocations(
                config: loaded,
                configPath: configPath,
                packageRoot: tempRoot,
                compilerVersion: "1.2.3"
            )
        ) { error in
            XCTAssertTrue(error.localizedDescription.contains("outputDirectory"))
            XCTAssertTrue(error.localizedDescription.contains("must stay inside the package root after resolving symlinks"))
            XCTAssertTrue(error.localizedDescription.contains(externalOutputRoot.path))
        }
    }

    func testRejectsMissingRequiredSwiftField() throws {
        let sqlDir = try createSQLDirectory(databaseName: "MissingFieldDatabase")
        let runtimeDir = try createRuntimeDirectory()
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "MissingFieldDatabase",
                  "sqlDirectory": "\(sqlDir)",
                  "swiftPackageName": "MissingFieldDatabaseSQLiteNow",
                  "runtime": "core",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)"
                }
              ]
            }
            """
        )

        XCTAssertThrowsError(try SQLiteNowConfigLoader.load(configPath: configPath)) { error in
            XCTAssertTrue(error.localizedDescription.contains("swiftTargetName"))
        }
    }

    func testRejectsInvalidRuntimeMode() throws {
        let sqlDir = try createSQLDirectory(databaseName: "InvalidRuntimeDatabase")
        let runtimeDir = try createRuntimeDirectory()
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "InvalidRuntimeDatabase",
                  "sqlDirectory": "\(sqlDir)",
                  "swiftPackageName": "InvalidRuntimeDatabaseSQLiteNow",
                  "swiftTargetName": "InvalidRuntimeDatabaseSQLiteNow",
                  "runtime": "desktop",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)"
                }
              ]
            }
            """
        )

        XCTAssertThrowsError(try SQLiteNowConfigLoader.load(configPath: configPath)) { error in
            XCTAssertTrue(error.localizedDescription.contains("Unsupported runtime 'desktop'"))
        }
    }

    func testRejectsLegacyConflictingFields() throws {
        let sqlDir = try createSQLDirectory(databaseName: "ConflictDatabase")
        let runtimeDir = try createRuntimeDirectory()
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "ConflictDatabase",
                  "sqlDirectory": "\(sqlDir)",
                  "swiftPackageName": "ConflictDatabaseSQLiteNow",
                  "swiftTargetName": "ConflictDatabaseSQLiteNow",
                  "runtime": "core",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)",
                  "backend": "dart"
                }
              ]
            }
            """
        )

        XCTAssertThrowsError(try SQLiteNowConfigLoader.load(configPath: configPath)) { error in
            XCTAssertTrue(error.localizedDescription.contains("Unsupported field(s)"))
            XCTAssertTrue(error.localizedDescription.contains("backend"))
        }
    }

    func testRejectsSchemaDatabaseFileConfiguration() throws {
        let sqlDir = try createSQLDirectory(databaseName: "SchemaDebugDatabase")
        let runtimeDir = try createRuntimeDirectory()
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "SchemaDebugDatabase",
                  "sqlDirectory": "\(sqlDir)",
                  "swiftPackageName": "SchemaDebugDatabaseSQLiteNow",
                  "swiftTargetName": "SchemaDebugDatabaseSQLiteNow",
                  "runtime": "core",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)",
                  "schemaDatabaseFile": "tmp/schema.db"
                }
              ]
            }
            """
        )

        XCTAssertThrowsError(try SQLiteNowConfigLoader.load(configPath: configPath)) { error in
            XCTAssertTrue(error.localizedDescription.contains("Unsupported field(s)"))
            XCTAssertTrue(error.localizedDescription.contains("schemaDatabaseFile"))
        }
    }

    func testRejectsUnsupportedMinimumPlatformFields() throws {
        let sqlDir = try createSQLDirectory(databaseName: "UnsupportedPlatformDatabase")
        let runtimeDir = try createRuntimeDirectory()
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "UnsupportedPlatformDatabase",
                  "sqlDirectory": "\(sqlDir)",
                  "swiftPackageName": "UnsupportedPlatformDatabaseSQLiteNow",
                  "swiftTargetName": "UnsupportedPlatformDatabaseSQLiteNow",
                  "runtime": "core",
                  "minimumPlatforms": {
                    "iOS": "15",
                    "watchOS": "10"
                  },
                  "runtimeXcframeworkDirectory": "\(runtimeDir)"
                }
              ]
            }
            """
        )

        XCTAssertThrowsError(try SQLiteNowConfigLoader.load(configPath: configPath)) { error in
            XCTAssertTrue(error.localizedDescription.contains("Unsupported field(s) in minimumPlatforms"))
            XCTAssertTrue(error.localizedDescription.contains("watchOS"))
        }
    }

    func testRejectsMissingSQLDirectory() throws {
        let runtimeDir = try createRuntimeDirectory()
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "MissingSQLDatabase",
                  "swiftPackageName": "MissingSQLDatabaseSQLiteNow",
                  "swiftTargetName": "MissingSQLDatabaseSQLiteNow",
                  "runtime": "core",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)"
                }
              ]
            }
            """
        )
        let loaded = try SQLiteNowConfigLoader.load(configPath: configPath)

        XCTAssertThrowsError(
            try SQLiteNowCompilerRequestBuilder.buildInvocations(
                config: loaded,
                configPath: configPath,
                packageRoot: tempRoot,
                compilerVersion: "1.2.3"
            )
        ) { error in
            XCTAssertTrue(error.localizedDescription.contains("SQL directory not found"))
            XCTAssertTrue(error.localizedDescription.contains("SQLiteNow/databases/MissingSQLDatabase"))
            XCTAssertTrue(error.localizedDescription.contains("set sqlDirectory"))
        }
    }

    func testRejectsBlankSQLDirectoryOverride() throws {
        let runtimeDir = try createRuntimeDirectory()
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "BlankSQLDatabase",
                  "sqlDirectory": "   ",
                  "swiftPackageName": "BlankSQLDatabaseSQLiteNow",
                  "swiftTargetName": "BlankSQLDatabaseSQLiteNow",
                  "runtime": "core",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)"
                }
              ]
            }
            """
        )

        XCTAssertThrowsError(try SQLiteNowConfigLoader.load(configPath: configPath)) { error in
            XCTAssertTrue(error.localizedDescription.contains("sqlDirectory must not be empty"))
        }
    }

    func testRejectsUnavailableRuntimeArtifact() throws {
        let sqlDir = try createSQLDirectory(databaseName: "MissingRuntimeDatabase")
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "MissingRuntimeDatabase",
                  "sqlDirectory": "\(sqlDir)",
                  "swiftPackageName": "MissingRuntimeDatabaseSQLiteNow",
                  "swiftTargetName": "MissingRuntimeDatabaseSQLiteNow",
                  "runtime": "core",
                  "runtimeArtifact": {
                    "kind": "localZip",
                    "path": "Artifacts/Missing.xcframework.zip",
                    "checksum": "abc123",
                    "sqliteNowVersion": "1.2.3"
                  }
                }
              ]
            }
            """
        )
        let loaded = try SQLiteNowConfigLoader.load(configPath: configPath)

        XCTAssertThrowsError(
            try SQLiteNowCompilerRequestBuilder.buildInvocations(
                config: loaded,
                configPath: configPath,
                packageRoot: tempRoot,
                compilerVersion: "1.2.3"
            )
        ) { error in
            XCTAssertTrue(error.localizedDescription.contains("Runtime artifact not found"))
        }
    }

    func testRejectsRemoteRuntimeZipWithoutURL() throws {
        let sqlDir = try createSQLDirectory(databaseName: "RemoteMissingURLDatabase")
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "RemoteMissingURLDatabase",
                  "sqlDirectory": "\(sqlDir)",
                  "swiftPackageName": "RemoteMissingURLDatabaseSQLiteNow",
                  "swiftTargetName": "RemoteMissingURLDatabaseSQLiteNow",
                  "runtime": "core",
                  "runtimeArtifact": {
                    "kind": "remoteZip",
                    "checksum": "abc123",
                    "sqliteNowVersion": "1.2.3"
                  }
                }
              ]
            }
            """
        )

        XCTAssertThrowsError(try SQLiteNowConfigLoader.load(configPath: configPath)) { error in
            XCTAssertTrue(error.localizedDescription.contains("runtimeArtifact.url is required"))
        }
    }

    func testRejectsRemoteRuntimeZipWithoutHTTPSURL() throws {
        let sqlDir = try createSQLDirectory(databaseName: "RemoteHTTPDatabase")
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "RemoteHTTPDatabase",
                  "sqlDirectory": "\(sqlDir)",
                  "swiftPackageName": "RemoteHTTPDatabaseSQLiteNow",
                  "swiftTargetName": "RemoteHTTPDatabaseSQLiteNow",
                  "runtime": "core",
                  "runtimeArtifact": {
                    "kind": "remoteZip",
                    "url": "http://example.com/SQLiteNowCoreRuntime-1.2.3.xcframework.zip",
                    "checksum": "abc123",
                    "sqliteNowVersion": "1.2.3"
                  }
                }
              ]
            }
            """
        )

        XCTAssertThrowsError(try SQLiteNowConfigLoader.load(configPath: configPath)) { error in
            XCTAssertTrue(error.localizedDescription.contains("runtimeArtifact.url must use https"))
        }
    }

    func testRejectsRemoteRuntimeZipPathConflict() throws {
        let sqlDir = try createSQLDirectory(databaseName: "RemotePathConflictDatabase")
        let configPath = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "RemotePathConflictDatabase",
                  "sqlDirectory": "\(sqlDir)",
                  "swiftPackageName": "RemotePathConflictDatabaseSQLiteNow",
                  "swiftTargetName": "RemotePathConflictDatabaseSQLiteNow",
                  "runtime": "core",
                  "runtimeArtifact": {
                    "kind": "remoteZip",
                    "path": "Artifacts/SQLiteNowCoreRuntime-1.2.3.xcframework.zip",
                    "url": "https://example.com/SQLiteNowCoreRuntime-1.2.3.xcframework.zip",
                    "checksum": "abc123",
                    "sqliteNowVersion": "1.2.3"
                  }
                }
              ]
            }
            """
        )

        XCTAssertThrowsError(try SQLiteNowConfigLoader.load(configPath: configPath)) { error in
            XCTAssertTrue(error.localizedDescription.contains("runtimeArtifact.path is not supported for remoteZip"))
        }
    }

    func testCompilerJarResolverPrefersExplicitPath() throws {
        let explicitJar = try createCompilerJar(named: "explicit.jar")
        let environmentJar = try createCompilerJar(named: "environment.jar")
        let bundledJar = try createCompilerJar(named: "bundled.jar")

        let resolved = try CompilerJarResolver.resolve(
            explicitPath: explicitJar.path,
            environment: [CompilerJarResolver.environmentKey: environmentJar.path],
            relativeTo: tempRoot,
            bundledCompilerJar: bundledJar
        )

        XCTAssertEqual(resolved, explicitJar.standardizedFileURL)
    }

    func testCompilerToolResolverPrefersExplicitExecutable() throws {
        let compilerExecutable = try createCompilerExecutable(named: "sqlitenow-compiler")
        let explicitJar = try createCompilerJar(named: "explicit.jar")
        let environmentJar = try createCompilerJar(named: "environment.jar")
        let bundledJar = try createCompilerJar(named: "bundled.jar")

        let resolved = try CompilerToolResolver.resolve(
            explicitExecutablePath: compilerExecutable.path,
            explicitJarPath: explicitJar.path,
            environment: [CompilerJarResolver.environmentKey: environmentJar.path],
            relativeTo: tempRoot,
            bundledCompilerJar: bundledJar
        )

        XCTAssertEqual(resolved, .executable(compilerExecutable.standardizedFileURL))
    }

    func testCompilerToolResolverRejectsNonExecutableTool() throws {
        let nonExecutable = tempRoot.appendingPathComponent("Tools/sqlitenow-compiler")
        try FileManager.default.createDirectory(at: nonExecutable.deletingLastPathComponent(), withIntermediateDirectories: true)
        try "not executable".write(to: nonExecutable, atomically: true, encoding: .utf8)

        XCTAssertThrowsError(
            try CompilerToolResolver.resolve(
                explicitExecutablePath: nonExecutable.path,
                explicitJarPath: nil,
                environment: [:],
                relativeTo: tempRoot,
                bundledCompilerJar: nil
            )
        ) { error in
            XCTAssertTrue(error.localizedDescription.contains("Compiler executable is not executable"))
            XCTAssertTrue(error.localizedDescription.contains(nonExecutable.path))
        }
    }

    func testGeneratorRunsConfiguredCompilerExecutableWithoutJavaWrapper() throws {
        let sqlDir = try createSQLDirectory(databaseName: "ExecutableDatabase")
        let runtimeDir = try createRuntimeDirectory()
        let compilerExecutable = try createCompilerExecutable(named: "sqlitenow-compiler")
        _ = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "ExecutableDatabase",
                  "sqlDirectory": "\(sqlDir)",
                  "swiftPackageName": "ExecutableDatabaseSQLiteNow",
                  "swiftTargetName": "ExecutableDatabaseSQLiteNow",
                  "runtime": "core",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)"
                }
              ]
            }
            """
        )
        let processRunner = RecordingProcessRunner(results: [
            ProcessResult(exitCode: 0, stdout: "1.2.3\n", stderr: ""),
            ProcessResult(exitCode: 0, stdout: #"{"success":true,"failure":null}"#, stderr: ""),
        ])
        let generator = SQLiteNowGenerator(
            processRunner: processRunner,
            environment: [:],
            bundledCompilerJar: nil
        )
        let options = try CommandLineOptions.parse(
            arguments: [
                "--package-root", tempRoot.path,
                "--compiler-executable", compilerExecutable.path,
            ],
            currentDirectory: tempRoot
        )

        try generator.generate(options: options)

        XCTAssertEqual(processRunner.invocations.count, 2)
        XCTAssertEqual(processRunner.invocations[0].executable, compilerExecutable.standardizedFileURL)
        XCTAssertEqual(processRunner.invocations[0].arguments, ["--version"])
        XCTAssertEqual(processRunner.invocations[1].executable, compilerExecutable.standardizedFileURL)
        XCTAssertEqual(processRunner.invocations[1].arguments.count, 2)
        XCTAssertEqual(processRunner.invocations[1].arguments[0], "--request")
        XCTAssertTrue(processRunner.invocations[1].arguments[1].hasSuffix(".build/sqlitenow/requests/ExecutableDatabase.json"))
    }

    func testGeneratorLoadsReleaseDistributionMetadataFromResourceURL() throws {
        _ = try createSQLDirectory(databaseName: "ResourceMetadataDatabase")
        let compilerExecutable = try createCompilerExecutable(named: "sqlitenow-compiler")
        let releaseDistributionURL = try writeReleaseDistributionResource()
        _ = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "ResourceMetadataDatabase",
                  "swiftPackageName": "ResourceMetadataDatabaseSQLiteNow",
                  "swiftTargetName": "ResourceMetadataDatabaseSQLiteNow",
                  "runtime": "core"
                }
              ]
            }
            """
        )
        let processRunner = RecordingProcessRunner(results: [
            ProcessResult(exitCode: 0, stdout: "1.2.3\n", stderr: ""),
            ProcessResult(exitCode: 0, stdout: #"{"success":true,"failure":null}"#, stderr: ""),
        ])
        let generator = SQLiteNowGenerator(
            processRunner: processRunner,
            environment: [:],
            bundledCompilerJar: nil,
            releaseDistributionURL: releaseDistributionURL
        )
        let options = try CommandLineOptions.parse(
            arguments: [
                "--package-root", tempRoot.path,
                "--compiler-executable", compilerExecutable.path,
            ],
            currentDirectory: tempRoot
        )

        try generator.generate(options: options)

        let requestPath = try XCTUnwrap(processRunner.invocations.last?.arguments.last)
        let request = try jsonObject(String(contentsOfFile: requestPath))
        let artifact = try XCTUnwrap(request["runtimeArtifact"] as? [String: Any])
        XCTAssertEqual(artifact["url"] as? String, "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v1.2.3/SQLiteNowCoreRuntime-1.2.3.xcframework.zip")
        XCTAssertEqual(artifact["checksum"] as? String, "corechecksum")
    }

    func testGeneratorReportsCompilerCrashOutputWhenStdoutIsNotJson() throws {
        let sqlDir = try createSQLDirectory(databaseName: "CrashDatabase")
        let runtimeDir = try createRuntimeDirectory()
        let compilerExecutable = try createCompilerExecutable(named: "sqlitenow-compiler")
        _ = try writeConfig(
            """
            {
              "schemaVersion": 1,
              "databases": [
                {
                  "databaseName": "CrashDatabase",
                  "sqlDirectory": "\(sqlDir)",
                  "swiftPackageName": "CrashDatabaseSQLiteNow",
                  "swiftTargetName": "CrashDatabaseSQLiteNow",
                  "runtime": "core",
                  "runtimeXcframeworkDirectory": "\(runtimeDir)"
                }
              ]
            }
            """
        )
        let processRunner = RecordingProcessRunner(results: [
            ProcessResult(exitCode: 0, stdout: "1.2.3\n", stderr: ""),
            ProcessResult(exitCode: 42, stdout: "compiler stdout", stderr: "compiler stderr"),
        ])
        let generator = SQLiteNowGenerator(
            processRunner: processRunner,
            environment: [:],
            bundledCompilerJar: nil
        )
        let options = try CommandLineOptions.parse(
            arguments: [
                "--package-root", tempRoot.path,
                "--compiler-executable", compilerExecutable.path,
            ],
            currentDirectory: tempRoot
        )

        XCTAssertThrowsError(try generator.generate(options: options)) { error in
            let message = error.localizedDescription
            XCTAssertTrue(message.contains("exit code 42"))
            XCTAssertTrue(message.contains("compiler stderr"))
            XCTAssertTrue(message.contains("compiler stdout"))
        }
    }

    func testCompilerJarResolverPrefersEnvironmentBeforeBundledJar() throws {
        let environmentJar = try createCompilerJar(named: "environment.jar")
        let bundledJar = try createCompilerJar(named: "bundled.jar")

        let resolved = try CompilerJarResolver.resolve(
            explicitPath: nil,
            environment: [CompilerJarResolver.environmentKey: environmentJar.path],
            relativeTo: tempRoot,
            bundledCompilerJar: bundledJar
        )

        XCTAssertEqual(resolved, environmentJar.standardizedFileURL)
    }

    func testCompilerJarResolverUsesBundledJarWhenNoOverrideIsConfigured() throws {
        let bundledJar = try createCompilerJar(named: "bundled.jar")

        let resolved = try CompilerJarResolver.resolve(
            explicitPath: nil,
            environment: [:],
            relativeTo: tempRoot,
            bundledCompilerJar: bundledJar
        )

        XCTAssertEqual(resolved, bundledJar.standardizedFileURL)
    }

    func testCompilerJarResolverDoesNotFallBackWhenConfiguredPathIsMissing() throws {
        let bundledJar = try createCompilerJar(named: "bundled.jar")
        let missingJar = tempRoot.appendingPathComponent("missing/compiler.jar")

        XCTAssertThrowsError(
            try CompilerJarResolver.resolve(
                explicitPath: missingJar.path,
                environment: [:],
                relativeTo: tempRoot,
                bundledCompilerJar: bundledJar
            )
        ) { error in
            XCTAssertTrue(error.localizedDescription.contains("Compiler jar not found at"))
            XCTAssertTrue(error.localizedDescription.contains(missingJar.path))
        }
    }

    func testCompilerJarResolverDoesNotFallBackWhenEnvironmentPathIsMissing() throws {
        let bundledJar = try createCompilerJar(named: "bundled.jar")
        let missingJar = tempRoot.appendingPathComponent("missing/environment-compiler.jar")

        XCTAssertThrowsError(
            try CompilerJarResolver.resolve(
                explicitPath: nil,
                environment: [CompilerJarResolver.environmentKey: missingJar.path],
                relativeTo: tempRoot,
                bundledCompilerJar: bundledJar
            )
        ) { error in
            XCTAssertTrue(error.localizedDescription.contains("Compiler jar not found at"))
            XCTAssertTrue(error.localizedDescription.contains(missingJar.path))
        }
    }

    func testRejectsMissingCompilerJar() {
        XCTAssertThrowsError(
            try CompilerJarResolver.resolve(
                explicitPath: nil,
                environment: [:],
                relativeTo: tempRoot,
                bundledCompilerJar: nil
            )
        ) { error in
            XCTAssertTrue(error.localizedDescription.contains(":sqlitenow-compiler:syncSwiftPmPluginCompilerJar"))
            XCTAssertTrue(error.localizedDescription.contains("--compiler-jar"))
            XCTAssertTrue(error.localizedDescription.contains(CompilerJarResolver.environmentKey))
        }
    }

    func testRejectsMissingConfigFile() {
        XCTAssertThrowsError(
            try SQLiteNowConfigLoader.load(configPath: tempRoot.appendingPathComponent("MissingSQLiteNow.json"))
        ) { error in
            XCTAssertTrue(error.localizedDescription.contains("Missing SQLiteNow config"))
        }
    }

    func testRendersDependencyInstructions() {
        let rendered = InstructionRenderer.render([
            GeneratedPackageInstructions(
                packageName: "AppDatabaseSQLiteNow",
                productName: "AppDatabaseSQLiteNow",
                relativePackagePath: "SQLiteNowGenerated/AppDatabaseSQLiteNow"
            ),
        ])

        XCTAssertTrue(rendered.contains(#".package(path: "SQLiteNowGenerated/AppDatabaseSQLiteNow")"#))
        XCTAssertTrue(rendered.contains(#".product(name: "AppDatabaseSQLiteNow", package: "AppDatabaseSQLiteNow")"#))
        XCTAssertTrue(rendered.contains("Add Local Package: SQLiteNowGenerated/AppDatabaseSQLiteNow"))
    }

    private func writeConfig(_ contents: String) throws -> URL {
        let configPath = tempRoot.appendingPathComponent("SQLiteNow.json")
        try contents.write(to: configPath, atomically: true, encoding: .utf8)
        return configPath
    }

    private func createSQLDirectory(databaseName: String, relativePath: String? = nil) throws -> String {
        let path = relativePath ?? "SQLiteNow/databases/\(databaseName)"
        let sqlDirectory = tempRoot
            .appendingPathComponent(path)
        try FileManager.default.createDirectory(
            at: sqlDirectory.appendingPathComponent("schema"),
            withIntermediateDirectories: true
        )
        try """
        CREATE TABLE person (
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL
        );
        """.write(
            to: sqlDirectory.appendingPathComponent("schema/person.sql"),
            atomically: true,
            encoding: .utf8
        )
        return path
    }

    private func createRuntimeDirectory() throws -> String {
        let runtime = tempRoot.appendingPathComponent("Runtime/SQLiteNowCoreRuntime.xcframework")
        try FileManager.default.createDirectory(at: runtime, withIntermediateDirectories: true)
        try "runtime".write(to: runtime.appendingPathComponent("Info.plist"), atomically: true, encoding: .utf8)
        return "Runtime/SQLiteNowCoreRuntime.xcframework"
    }

    private func createRuntimeZip() throws -> String {
        let runtime = tempRoot.appendingPathComponent("Artifacts/SQLiteNowCoreRuntime-1.2.3.xcframework.zip")
        try FileManager.default.createDirectory(at: runtime.deletingLastPathComponent(), withIntermediateDirectories: true)
        try "runtime zip".write(to: runtime, atomically: true, encoding: .utf8)
        return "Artifacts/SQLiteNowCoreRuntime-1.2.3.xcframework.zip"
    }

    private func createCompilerJar(named name: String) throws -> URL {
        let jar = tempRoot.appendingPathComponent("CompilerJars").appendingPathComponent(name)
        try FileManager.default.createDirectory(at: jar.deletingLastPathComponent(), withIntermediateDirectories: true)
        try "compiler jar".write(to: jar, atomically: true, encoding: .utf8)
        return jar
    }

    private func createCompilerExecutable(named name: String) throws -> URL {
        let executable = tempRoot.appendingPathComponent("CompilerTools").appendingPathComponent(name)
        try FileManager.default.createDirectory(at: executable.deletingLastPathComponent(), withIntermediateDirectories: true)
        try "#!/bin/sh\nexit 0\n".write(to: executable, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: executable.path)
        return executable
    }

    private func writeReleaseDistributionResource() throws -> URL {
        let resource = tempRoot.appendingPathComponent("Resources/release-distribution.json")
        try FileManager.default.createDirectory(at: resource.deletingLastPathComponent(), withIntermediateDirectories: true)
        try """
        {
          "manifestVersion": 1,
          "sqliteNowVersion": "1.2.3",
          "compilerBinaryTargetName": "SQLiteNowCompiler",
          "compilerArtifactUrl": "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v1.2.3/SQLiteNowCompiler-1.2.3.artifactbundle.zip",
          "compilerArtifactChecksum": "compilerchecksum",
          "runtimeArtifacts": {
            "core": {
              "kind": "remoteZip",
              "url": "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v1.2.3/SQLiteNowCoreRuntime-1.2.3.xcframework.zip",
              "checksum": "corechecksum",
              "sqliteNowVersion": "1.2.3",
              "runtimeModuleName": "SQLiteNowCoreRuntime"
            },
            "sync": {
              "kind": "remoteZip",
              "url": "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v1.2.3/SQLiteNowSyncRuntime-1.2.3.xcframework.zip",
              "checksum": "syncchecksum",
              "sqliteNowVersion": "1.2.3",
              "runtimeModuleName": "SQLiteNowSyncRuntime"
            }
          }
        }
        """.write(to: resource, atomically: true, encoding: .utf8)
        return resource
    }

    private func releaseDistribution() -> SQLiteNowReleaseDistribution {
        SQLiteNowReleaseDistribution(
            manifestVersion: 1,
            sqliteNowVersion: "1.2.3",
            compilerBinaryTargetName: "SQLiteNowCompiler",
            compilerArtifactUrl: "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v1.2.3/SQLiteNowCompiler-1.2.3.artifactbundle.zip",
            compilerArtifactChecksum: "compilerchecksum",
            runtimeArtifacts: [
                "core": ReleaseRuntimeArtifactConfiguration(
                    kind: "remoteZip",
                    url: "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v1.2.3/SQLiteNowCoreRuntime-1.2.3.xcframework.zip",
                    checksum: "corechecksum",
                    sqliteNowVersion: "1.2.3",
                    runtimeModuleName: "SQLiteNowCoreRuntime"
                ),
                "sync": ReleaseRuntimeArtifactConfiguration(
                    kind: "remoteZip",
                    url: "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v1.2.3/SQLiteNowSyncRuntime-1.2.3.xcframework.zip",
                    checksum: "syncchecksum",
                    sqliteNowVersion: "1.2.3",
                    runtimeModuleName: "SQLiteNowSyncRuntime"
                ),
            ]
        )
    }

    private func jsonObject(_ string: String) throws -> [String: Any] {
        let data = try XCTUnwrap(string.data(using: .utf8))
        return try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: Any])
    }
}

private extension Array {
    func single(file: StaticString = #filePath, line: UInt = #line) throws -> Element {
        XCTAssertEqual(count, 1, file: file, line: line)
        return try XCTUnwrap(first, file: file, line: line)
    }

    func single(
        where predicate: (Element) throws -> Bool,
        file: StaticString = #filePath,
        line: UInt = #line
    ) throws -> Element {
        let matches = try filter(predicate)
        XCTAssertEqual(matches.count, 1, file: file, line: line)
        return try XCTUnwrap(matches.first, file: file, line: line)
    }
}

private final class RecordingProcessRunner: ProcessRunning {
    struct Invocation: Equatable {
        let executable: URL
        let arguments: [String]
        let workingDirectory: URL
    }

    private var results: [ProcessResult]
    private(set) var invocations: [Invocation] = []

    init(results: [ProcessResult]) {
        self.results = results
    }

    func run(executable: URL, arguments: [String], workingDirectory: URL) throws -> ProcessResult {
        invocations.append(Invocation(
            executable: executable.standardizedFileURL,
            arguments: arguments,
            workingDirectory: workingDirectory.standardizedFileURL
        ))
        guard !results.isEmpty else {
            throw SQLiteNowGenerateError("Unexpected process invocation.")
        }
        return results.removeFirst()
    }
}
