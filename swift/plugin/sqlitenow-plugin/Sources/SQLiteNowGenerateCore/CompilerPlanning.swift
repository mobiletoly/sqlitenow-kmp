import Foundation

public struct CompilerInvocation: Equatable {
    public let databaseName: String
    public let requestFile: URL
    public let directoriesToCreate: [URL]
    public let requestJSON: String
    public let instructions: GeneratedPackageInstructions
}

public struct GeneratedPackageInstructions: Equatable {
    public let packageName: String
    public let productName: String
    public let relativePackagePath: String
}

public enum SQLiteNowCompilerRequestBuilder {
    public static func buildInvocations(
        config: SQLiteNowConfiguration,
        configPath: URL,
        packageRoot: URL,
        compilerVersion: String,
        releaseDistribution: SQLiteNowReleaseDistribution? = nil,
        fileManager: FileManager = .default
    ) throws -> [CompilerInvocation] {
        var session = SQLiteNowCompilerInvocationPlanningSession(
            configPath: configPath,
            packageRoot: packageRoot,
            compilerVersion: compilerVersion,
            releaseDistribution: releaseDistribution,
            fileManager: fileManager
        )
        var invocations: [CompilerInvocation] = []
        for database in config.databases {
            invocations.append(try session.buildInvocation(for: database))
        }
        return invocations
    }
}

private struct SQLiteNowCompilerInvocationPlanningSession {
    private let configRoot: URL
    private let packageRoot: URL
    private let compilerVersion: String
    private let releaseDistribution: SQLiteNowReleaseDistribution?
    private let fileManager: FileManager
    private let encoder: JSONEncoder
    private var outputDestinations: [String: String] = [:]

    init(
        configPath: URL,
        packageRoot: URL,
        compilerVersion: String,
        releaseDistribution: SQLiteNowReleaseDistribution?,
        fileManager: FileManager
    ) {
        self.configRoot = configPath.deletingLastPathComponent().standardizedFileURL
        self.packageRoot = packageRoot
        self.compilerVersion = compilerVersion
        self.releaseDistribution = releaseDistribution
        self.fileManager = fileManager
        self.encoder = JSONEncoder()
        self.encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
    }

    mutating func buildInvocation(for database: SQLiteNowDatabaseConfiguration) throws -> CompilerInvocation {
        let sqlDirectoryURL: URL
        if let sqlDirectory = database.sqlDirectory {
            sqlDirectoryURL = SQLiteNowPath.resolve(sqlDirectory, relativeTo: configRoot)
        } else {
            sqlDirectoryURL = packageRoot
                .appendingPathComponent("SQLiteNow/databases")
                .appendingPathComponent(database.databaseName)
                .standardizedFileURL
        }
        guard fileManager.directoryExists(at: sqlDirectoryURL) else {
            if database.sqlDirectory == nil {
                throw SQLiteNowGenerateError(
                    "SQL directory not found for database '\(database.databaseName)': \(sqlDirectoryURL.path). " +
                    "Create SQLiteNow/databases/\(database.databaseName) under the package root or set sqlDirectory in SQLiteNow.json."
                )
            }
            throw SQLiteNowGenerateError("SQL directory not found for database '\(database.databaseName)': \(sqlDirectoryURL.path).")
        }

        let runtimeModuleName = database.runtimeModuleName ?? (database.runtime == "sync" ? "SQLiteNowSyncRuntime" : "SQLiteNowCoreRuntime")
        let outputDirectoryURL: URL
        if let outputDirectory = database.outputDirectory {
            outputDirectoryURL = SQLiteNowPath.resolve(outputDirectory, relativeTo: configRoot)
        } else {
            outputDirectoryURL = packageRoot
                .appendingPathComponent("SQLiteNowGenerated")
                .appendingPathComponent(database.swiftPackageName)
                .standardizedFileURL
        }
        let resolvedPackageRootURL = try packageRoot.resolvingSymlinkComponents(fileManager: fileManager)
        let resolvedOutputDirectoryURL = try outputDirectoryURL.resolvingSymlinkComponents(fileManager: fileManager)
        guard resolvedOutputDirectoryURL.isDescendantOrEqual(to: resolvedPackageRootURL) else {
            throw SQLiteNowGenerateError(
                "outputDirectory for database '\(database.databaseName)' must stay inside the package root after resolving symlinks: " +
                "\(outputDirectoryURL.path) resolves to \(resolvedOutputDirectoryURL.path)."
            )
        }
        let outputDirectoryPath = SQLiteNowPath.requestPath(outputDirectoryURL, relativeTo: packageRoot)
        let outputDestinationKey = resolvedOutputDirectoryURL.path
        if let previousDatabaseName = outputDestinations[outputDestinationKey] {
            throw SQLiteNowGenerateError(
                "Duplicate SQLiteNow generated package output '\(outputDirectoryPath)' for databases '\(previousDatabaseName)' and '\(database.databaseName)'."
            )
        }
        outputDestinations[outputDestinationKey] = database.databaseName
        let requestFile = packageRoot
            .appendingPathComponent(".build/sqlitenow/requests")
            .appendingPathComponent("\(SQLiteNowPath.safeFileName(database.databaseName)).json")
            .standardizedFileURL
        var directoriesToCreate = [requestFile.deletingLastPathComponent()]

        let runtimeXcframeworkDirectory = try database.runtimeXcframeworkDirectory.map { path in
            guard !path.trimmed().isEmpty else {
                throw SQLiteNowGenerateError("runtimeXcframeworkDirectory must not be empty for database '\(database.databaseName)'.")
            }
            let resolved = SQLiteNowPath.resolve(path, relativeTo: configRoot)
            guard fileManager.directoryExists(at: resolved) else {
                throw SQLiteNowGenerateError("Runtime XCFramework not found for database '\(database.databaseName)': \(resolved.path).")
            }
            return SQLiteNowPath.requestPath(resolved, relativeTo: packageRoot)
        }
        let resolvedRuntimeArtifactConfiguration: RuntimeArtifactConfiguration?
        if let runtimeArtifact = database.runtimeArtifact {
            resolvedRuntimeArtifactConfiguration = runtimeArtifact
        } else if runtimeXcframeworkDirectory == nil {
            resolvedRuntimeArtifactConfiguration = try releaseDistribution?.runtimeArtifact(
                for: database.runtime,
                runtimeModuleName: runtimeModuleName,
                databaseName: database.databaseName
            )
        } else {
            resolvedRuntimeArtifactConfiguration = nil
        }
        if runtimeXcframeworkDirectory == nil && resolvedRuntimeArtifactConfiguration == nil {
            throw SQLiteNowGenerateError(
                "Database '\(database.databaseName)' requires runtimeArtifact or runtimeXcframeworkDirectory when no SQLiteNow release distribution metadata is available."
            )
        }
        let runtimeArtifact = try resolvedRuntimeArtifactConfiguration.map { artifact in
            try compilerRuntimeArtifact(artifact, databaseName: database.databaseName)
        }
        let sqliteNowPackage = try compilerSwiftPackageDependency(
            sqliteNowPackageConfiguration(for: database),
            databaseName: database.databaseName,
            generatedPackageRoot: outputDirectoryURL
        )
        let sqliteNowVersion = resolvedRuntimeArtifactConfiguration?.sqliteNowVersion ?? compilerVersion
        let schemaDatabaseFile: String?
        if database.debug ?? false {
            let schemaDirectory = packageRoot
                .appendingPathComponent(".build/sqlitenow/schema")
                .standardizedFileURL
            directoriesToCreate.append(schemaDirectory)
            schemaDatabaseFile = SQLiteNowPath.requestPath(
                schemaDirectory.appendingPathComponent("\(SQLiteNowPath.safeFileName(database.databaseName)).db"),
                relativeTo: packageRoot
            )
        } else {
            schemaDatabaseFile = nil
        }
        let compilerRequest = CompilerRequest(
            databaseName: database.databaseName,
            sqlDirectory: SQLiteNowPath.requestPath(sqlDirectoryURL, relativeTo: packageRoot),
            metadataPackageName: database.metadataPackageName ?? SQLiteNowPath.defaultMetadataPackageName(database.databaseName),
            compilerOutputDirectory: ".build/sqlitenow/compiler/\(SQLiteNowPath.safeFileName(database.databaseName))",
            swiftPackageOutputDirectory: outputDirectoryPath,
            swiftPackageName: database.swiftPackageName,
            swiftTargetName: database.swiftTargetName,
            runtime: database.runtime,
            runtimeModuleName: runtimeModuleName,
            runtimeXcframeworkDirectory: runtimeXcframeworkDirectory,
            runtimeArtifact: runtimeArtifact,
            sqliteNowPackage: sqliteNowPackage,
            sqliteNowVersion: sqliteNowVersion,
            minimumPlatforms: database.minimumPlatforms,
            requestedAppleTargets: database.requestedAppleTargets,
            generatedBy: "swift package plugin --allow-writing-to-package-directory sqlitenow-generate",
            schemaDatabaseFile: schemaDatabaseFile,
            debug: database.debug ?? false
        )
        let data = try encoder.encode(compilerRequest)
        let requestJSON = String(decoding: data, as: UTF8.self) + "\n"
        return CompilerInvocation(
            databaseName: database.databaseName,
            requestFile: requestFile,
            directoriesToCreate: directoriesToCreate,
            requestJSON: requestJSON,
            instructions: GeneratedPackageInstructions(
                packageName: database.swiftPackageName,
                productName: database.swiftPackageName,
                relativePackagePath: outputDirectoryPath
            )
        )
    }

    private func compilerRuntimeArtifact(
        _ artifact: RuntimeArtifactConfiguration,
        databaseName: String
    ) throws -> CompilerRuntimeArtifact {
        if artifact.kind == "remoteZip" {
            return CompilerRuntimeArtifact(
                kind: artifact.kind,
                path: nil,
                url: artifact.url,
                checksum: artifact.checksum,
                sqliteNowVersion: artifact.sqliteNowVersion
            )
        }

        guard let path = artifact.path else {
            throw SQLiteNowGenerateError("runtimeArtifact.path is required for database '\(databaseName)'.")
        }
        let resolved = SQLiteNowPath.resolve(path, relativeTo: configRoot)
        guard fileManager.fileExists(atPath: resolved.path) else {
            throw SQLiteNowGenerateError("Runtime artifact not found for database '\(databaseName)': \(resolved.path).")
        }
        return CompilerRuntimeArtifact(
            kind: artifact.kind,
            path: SQLiteNowPath.requestPath(resolved, relativeTo: packageRoot),
            url: nil,
            checksum: artifact.checksum,
            sqliteNowVersion: artifact.sqliteNowVersion
        )
    }

    private func compilerSwiftPackageDependency(
        _ dependency: SwiftPackageDependencyConfiguration?,
        databaseName: String,
        generatedPackageRoot: URL
    ) throws -> CompilerSwiftPackageDependency? {
        guard let dependency else {
            return nil
        }
        switch dependency.kind {
        case "remoteExact":
            let url = (dependency.url ?? "").trimmed()
            guard !url.isEmpty else {
                throw SQLiteNowGenerateError("sqliteNowPackage.url is required for database '\(databaseName)'.")
            }
            let version = (dependency.version ?? "").trimmed()
            guard !version.isEmpty else {
                throw SQLiteNowGenerateError("sqliteNowPackage.version is required for database '\(databaseName)'.")
            }
            return CompilerSwiftPackageDependency(
                kind: dependency.kind,
                packageIdentity: dependency.packageIdentity,
                path: nil,
                url: url,
                version: version,
                coreRuntimeProduct: dependency.coreRuntimeProduct,
                syncRuntimeProduct: dependency.syncRuntimeProduct,
                coreSupportProduct: dependency.coreSupportProduct,
                syncSupportProduct: dependency.syncSupportProduct
            )
        case "localPath":
            guard let path = dependency.path, !path.trimmed().isEmpty else {
                throw SQLiteNowGenerateError("sqliteNowPackage.path is required for database '\(databaseName)'.")
            }
            let resolved = SQLiteNowPath.resolve(path, relativeTo: configRoot)
            return CompilerSwiftPackageDependency(
                kind: dependency.kind,
                packageIdentity: dependency.packageIdentity,
                path: SQLiteNowPath.localSwiftPackageDependencyPath(
                    resolved,
                    packageRoot: packageRoot,
                    generatedPackageRoot: generatedPackageRoot
                ),
                url: nil,
                version: nil,
                coreRuntimeProduct: dependency.coreRuntimeProduct,
                syncRuntimeProduct: dependency.syncRuntimeProduct,
                coreSupportProduct: dependency.coreSupportProduct,
                syncSupportProduct: dependency.syncSupportProduct
            )
        default:
            throw SQLiteNowGenerateError("Unsupported sqliteNowPackage.kind '\(dependency.kind)' for database '\(databaseName)'.")
        }
    }

    private func sqliteNowPackageConfiguration(
        for database: SQLiteNowDatabaseConfiguration
    ) -> SwiftPackageDependencyConfiguration? {
        if let sqliteNowPackage = database.sqliteNowPackage {
            return sqliteNowPackage
        }
        if database.runtimeArtifact != nil || database.runtimeXcframeworkDirectory != nil {
            return nil
        }
        return releaseDistribution?.sqliteNowPackage
    }
}

private struct CompilerRequest: Encodable {
    let contractVersion = 2
    let generationMode = "swiftProductPackage"
    let databaseName: String
    let sqlDirectory: String
    let metadataPackageName: String
    let compilerOutputDirectory: String
    let swiftPackageOutputDirectory: String
    let swiftPackageName: String
    let swiftTargetName: String
    let runtime: String
    let runtimeModuleName: String
    let runtimeXcframeworkDirectory: String?
    let runtimeArtifact: CompilerRuntimeArtifact?
    let sqliteNowPackage: CompilerSwiftPackageDependency?
    let sqliteNowVersion: String
    let minimumPlatforms: MinimumPlatforms?
    let requestedAppleTargets: [String]?
    let generatedBy: String
    let schemaDatabaseFile: String?
    let debug: Bool
}

private struct CompilerRuntimeArtifact: Encodable, Equatable {
    let kind: String
    let path: String?
    let url: String?
    let checksum: String?
    let sqliteNowVersion: String?
}

private struct CompilerSwiftPackageDependency: Encodable, Equatable {
    let kind: String
    let packageIdentity: String?
    let path: String?
    let url: String?
    let version: String?
    let coreRuntimeProduct: String?
    let syncRuntimeProduct: String?
    let coreSupportProduct: String?
    let syncSupportProduct: String?
}
