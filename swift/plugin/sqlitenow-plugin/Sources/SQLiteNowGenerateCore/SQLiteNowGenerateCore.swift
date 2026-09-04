import Foundation

public enum SQLiteNowGenerateCommand {
    public static func main() -> Never {
        do {
            let options = try CommandLineOptions.parse(arguments: Array(CommandLine.arguments.dropFirst()))
            if options.showHelp {
                print(Self.helpText)
                exit(0)
            }
            try SQLiteNowGenerator().generate(options: options)
            exit(0)
        } catch {
            fputs("SQLiteNow generation failed: \(error.localizedDescription)\n", stderr)
            exit(1)
        }
    }

    public static let helpText = """
    Usage: sqlite-now-generate [--package-root <path>] [--config <path>] [--compiler-executable <path>] [--compiler-jar <path>]

    Options:
      --package-root <path>        Swift package root. Defaults to the current directory.
                                   Xcode command plugins default this to the .xcodeproj directory; pass this explicitly when SQLiteNow.json, SQL, and output live above it.
      --config <path>              SQLiteNow config file. Defaults to <package-root>/SQLiteNow.json.
      --compiler-executable <path> Runnable SQLiteNow compiler executable. Takes precedence over jar options.
      --compiler-jar <path>        Runnable SQLiteNow compiler jar. Defaults to SQLITENOW_COMPILER_JAR.
    """
}

public struct CommandLineOptions: Equatable {
    public var packageRoot: URL
    public var configPath: URL?
    public var compilerExecutablePath: String?
    public var compilerJarPath: String?
    public var showHelp: Bool

    public static func parse(
        arguments: [String],
        currentDirectory: URL = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
    ) throws -> CommandLineOptions {
        var packageRoot = currentDirectory
        var configPath: String?
        var compilerExecutablePath: String?
        var compilerJarPath: String?
        var showHelp = false
        var index = 0

        func requireValue(after flag: String) throws -> String {
            let valueIndex = index + 1
            guard valueIndex < arguments.count else {
                throw SQLiteNowGenerateError("Missing value after \(flag).")
            }
            index = valueIndex
            return arguments[valueIndex]
        }

        while index < arguments.count {
            let argument = arguments[index]
            switch argument {
            case "--help", "-h":
                showHelp = true
            case "--package-root":
                packageRoot = resolveURL(try requireValue(after: argument), relativeTo: currentDirectory)
            case "--config":
                configPath = try requireValue(after: argument)
            case "--compiler-executable":
                compilerExecutablePath = try requireValue(after: argument)
            case "--compiler-jar":
                compilerJarPath = try requireValue(after: argument)
            default:
                throw SQLiteNowGenerateError("Unsupported argument '\(argument)'.")
            }
            index += 1
        }

        let resolvedPackageRoot = packageRoot.standardizedFileURL
        return CommandLineOptions(
            packageRoot: resolvedPackageRoot,
            configPath: configPath.map { resolveURL($0, relativeTo: resolvedPackageRoot).standardizedFileURL },
            compilerExecutablePath: compilerExecutablePath,
            compilerJarPath: compilerJarPath,
            showHelp: showHelp
        )
    }
}

public struct SQLiteNowGenerator {
    private let fileManager: FileManager
    private let processRunner: ProcessRunning
    private let environment: [String: String]
    private let bundledCompilerJar: URL?
    private let releaseDistribution: SQLiteNowReleaseDistribution?
    private let releaseDistributionURL: URL?

    public init(
        fileManager: FileManager = .default,
        processRunner: ProcessRunning = SystemProcessRunner(),
        environment: [String: String] = ProcessInfo.processInfo.environment,
        bundledCompilerJar: URL? = CompilerJarResolver.bundledCompilerJarURL(),
        releaseDistribution: SQLiteNowReleaseDistribution? = nil,
        releaseDistributionURL: URL? = SQLiteNowReleaseDistributionResolver.bundledReleaseDistributionURL()
    ) {
        self.fileManager = fileManager
        self.processRunner = processRunner
        self.environment = environment
        self.bundledCompilerJar = bundledCompilerJar
        self.releaseDistribution = releaseDistribution
        self.releaseDistributionURL = releaseDistributionURL
    }

    public func generate(options: CommandLineOptions) throws {
        let packageRoot = options.packageRoot.standardizedFileURL
        let configPath = (options.configPath ?? packageRoot.appendingPathComponent("SQLiteNow.json")).standardizedFileURL
        let config = try SQLiteNowConfigLoader.load(configPath: configPath, fileManager: fileManager)
        let compilerTool = try CompilerToolResolver.resolve(
            explicitExecutablePath: options.compilerExecutablePath,
            explicitJarPath: options.compilerJarPath,
            environment: environment,
            relativeTo: packageRoot,
            bundledCompilerJar: bundledCompilerJar,
            fileManager: fileManager
        )
        let compilerVersion = try compilerVersion(compilerTool: compilerTool, workingDirectory: packageRoot)
        let effectiveReleaseDistribution: SQLiteNowReleaseDistribution?
        if let releaseDistribution {
            effectiveReleaseDistribution = releaseDistribution
        } else if let releaseDistributionURL {
            effectiveReleaseDistribution = try SQLiteNowReleaseDistributionResolver.load(url: releaseDistributionURL)
        } else {
            effectiveReleaseDistribution = nil
        }
        let invocations = try SQLiteNowCompilerRequestBuilder.buildInvocations(
            config: config,
            configPath: configPath,
            packageRoot: packageRoot,
            compilerVersion: compilerVersion,
            releaseDistribution: effectiveReleaseDistribution,
            fileManager: fileManager
        )

        for invocation in invocations {
            for directory in invocation.directoriesToCreate {
                try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
            }
            try invocation.requestJSON.write(to: invocation.requestFile, atomically: true, encoding: .utf8)
            let response = try runCompilerRequest(
                compilerTool: compilerTool,
                requestFile: invocation.requestFile,
                workingDirectory: packageRoot
            )
            try response.requireSuccess()
        }

        print(InstructionRenderer.render(invocations.map(\.instructions)))
    }

    private func compilerVersion(compilerTool: CompilerTool, workingDirectory: URL) throws -> String {
        let result = try processRunner.run(
            executable: compilerTool.executableURL,
            arguments: compilerTool.arguments(["--version"]),
            workingDirectory: workingDirectory
        )
        guard result.exitCode == 0 else {
            throw SQLiteNowGenerateError(
                "Unable to run SQLiteNow compiler. Ensure Java 17+ is available. stderr: \(result.stderr.trimmed())"
            )
        }
        let version = result.stdout.trimmed()
        guard !version.isEmpty else {
            throw SQLiteNowGenerateError("SQLiteNow compiler --version returned an empty version.")
        }
        return version
    }

    private func runCompilerRequest(
        compilerTool: CompilerTool,
        requestFile: URL,
        workingDirectory: URL
    ) throws -> CompilerResponse {
        let result = try processRunner.run(
            executable: compilerTool.executableURL,
            arguments: compilerTool.arguments(["--request", requestFile.path]),
            workingDirectory: workingDirectory
        )
        if result.exitCode != 0 {
            if
                let data = result.stdout.data(using: .utf8),
                let response = try? JSONDecoder().decode(CompilerResponse.self, from: data)
            {
                try response.requireSuccess()
            }
            throw SQLiteNowGenerateError(
                "SQLiteNow compiler failed with exit code \(result.exitCode). stderr: \(result.stderr.trimmed()) stdout: \(result.stdout.trimmed())"
            )
        }
        guard let data = result.stdout.data(using: .utf8) else {
            throw SQLiteNowGenerateError("SQLiteNow compiler produced non-UTF-8 output.")
        }
        do {
            return try JSONDecoder().decode(CompilerResponse.self, from: data)
        } catch {
            throw SQLiteNowGenerateError(
                "SQLiteNow compiler produced invalid JSON output: \(error.localizedDescription). stdout: \(result.stdout.trimmed()) stderr: \(result.stderr.trimmed())"
            )
        }
    }
}

public struct SQLiteNowConfiguration: Decodable, Equatable {
    public let schemaVersion: Int
    public let databases: [SQLiteNowDatabaseConfiguration]
}

public struct SQLiteNowDatabaseConfiguration: Decodable, Equatable {
    public let databaseName: String
    public let sqlDirectory: String?
    public let swiftPackageName: String
    public let swiftTargetName: String
    public let outputDirectory: String?
    public let runtime: String
    public let minimumPlatforms: MinimumPlatforms?
    public let runtimeModuleName: String?
    public let metadataPackageName: String?
    public let runtimeArtifact: RuntimeArtifactConfiguration?
    public let runtimeXcframeworkDirectory: String?
    public let sqliteNowPackage: SwiftPackageDependencyConfiguration?
    public let requestedAppleTargets: [String]?
    public let debug: Bool?
}

private let supportedAppleTargets = ["macosArm64", "iosArm64", "iosSimulatorArm64"]
private let supportedAppleTargetSet = Set(supportedAppleTargets)

public struct MinimumPlatforms: Codable, Equatable {
    public let iOS: String?
    public let macOS: String?

    public init(iOS: String? = nil, macOS: String? = nil) {
        self.iOS = iOS
        self.macOS = macOS
    }
}

public struct RuntimeArtifactConfiguration: Codable, Equatable {
    public let kind: String
    public let path: String?
    public let url: String?
    public let checksum: String?
    public let sqliteNowVersion: String?

    public init(
        kind: String,
        path: String? = nil,
        url: String? = nil,
        checksum: String? = nil,
        sqliteNowVersion: String? = nil
    ) {
        self.kind = kind
        self.path = path
        self.url = url
        self.checksum = checksum
        self.sqliteNowVersion = sqliteNowVersion
    }
}

public struct SwiftPackageDependencyConfiguration: Codable, Equatable {
    public let kind: String
    public let packageIdentity: String?
    public let path: String?
    public let url: String?
    public let version: String?
    public let coreRuntimeProduct: String?
    public let syncRuntimeProduct: String?
    public let coreSupportProduct: String?
    public let syncSupportProduct: String?

    public init(
        kind: String,
        packageIdentity: String? = nil,
        path: String? = nil,
        url: String? = nil,
        version: String? = nil,
        coreRuntimeProduct: String? = nil,
        syncRuntimeProduct: String? = nil,
        coreSupportProduct: String? = nil,
        syncSupportProduct: String? = nil
    ) {
        self.kind = kind
        self.packageIdentity = packageIdentity
        self.path = path
        self.url = url
        self.version = version
        self.coreRuntimeProduct = coreRuntimeProduct
        self.syncRuntimeProduct = syncRuntimeProduct
        self.coreSupportProduct = coreSupportProduct
        self.syncSupportProduct = syncSupportProduct
    }
}

public struct SQLiteNowReleaseDistribution: Decodable, Equatable {
    public let manifestVersion: Int
    public let sqliteNowVersion: String
    public let compilerBinaryTargetName: String?
    public let compilerArtifactUrl: String?
    public let compilerArtifactChecksum: String?
    public let runtimeArtifacts: [String: ReleaseRuntimeArtifactConfiguration]?
    public let sqliteNowPackage: SwiftPackageDependencyConfiguration?

    public init(
        manifestVersion: Int,
        sqliteNowVersion: String,
        compilerBinaryTargetName: String? = nil,
        compilerArtifactUrl: String? = nil,
        compilerArtifactChecksum: String? = nil,
        runtimeArtifacts: [String: ReleaseRuntimeArtifactConfiguration]? = nil,
        sqliteNowPackage: SwiftPackageDependencyConfiguration? = nil
    ) {
        self.manifestVersion = manifestVersion
        self.sqliteNowVersion = sqliteNowVersion
        self.compilerBinaryTargetName = compilerBinaryTargetName
        self.compilerArtifactUrl = compilerArtifactUrl
        self.compilerArtifactChecksum = compilerArtifactChecksum
        self.runtimeArtifacts = runtimeArtifacts
        self.sqliteNowPackage = sqliteNowPackage
    }

    public func runtimeArtifact(
        for runtime: String,
        runtimeModuleName: String,
        databaseName: String
    ) throws -> RuntimeArtifactConfiguration? {
        guard let artifact = runtimeArtifacts?[runtime] else {
            return nil
        }
        let context = "releaseDistribution.runtimeArtifacts.\(runtime)"
        guard artifact.kind == "remoteZip" else {
            throw SQLiteNowGenerateError("\(context).kind must be 'remoteZip' for database '\(databaseName)'.")
        }
        guard artifact.runtimeModuleName == runtimeModuleName else {
            throw SQLiteNowGenerateError(
                "\(context).runtimeModuleName '\(artifact.runtimeModuleName)' does not match runtimeModuleName '\(runtimeModuleName)' for database '\(databaseName)'."
            )
        }
        let url = artifact.url.trimmed()
        guard !url.isEmpty else {
            throw SQLiteNowGenerateError("\(context).url is required for database '\(databaseName)'.")
        }
        guard URL(string: url)?.scheme == "https" else {
            throw SQLiteNowGenerateError("\(context).url must use https for database '\(databaseName)'.")
        }
        let checksum = artifact.checksum.trimmed()
        guard !checksum.isEmpty else {
            throw SQLiteNowGenerateError("\(context).checksum is required for database '\(databaseName)'.")
        }
        let sqliteNowVersion = artifact.sqliteNowVersion.trimmed()
        guard !sqliteNowVersion.isEmpty else {
            throw SQLiteNowGenerateError("\(context).sqliteNowVersion is required for database '\(databaseName)'.")
        }
        return RuntimeArtifactConfiguration(
            kind: artifact.kind,
            url: url,
            checksum: checksum,
            sqliteNowVersion: sqliteNowVersion
        )
    }
}

public struct ReleaseRuntimeArtifactConfiguration: Decodable, Equatable {
    public let kind: String
    public let url: String
    public let checksum: String
    public let sqliteNowVersion: String
    public let runtimeModuleName: String

    public init(
        kind: String,
        url: String,
        checksum: String,
        sqliteNowVersion: String,
        runtimeModuleName: String
    ) {
        self.kind = kind
        self.url = url
        self.checksum = checksum
        self.sqliteNowVersion = sqliteNowVersion
        self.runtimeModuleName = runtimeModuleName
    }
}

public enum SQLiteNowReleaseDistributionResolver {
    public static func bundledReleaseDistributionURL() -> URL? {
        #if SWIFT_PACKAGE
        return Bundle.module.url(
            forResource: "release-distribution",
            withExtension: "json",
            subdirectory: "Resources"
        ) ?? Bundle.module.url(forResource: "release-distribution", withExtension: "json")
        #else
        return nil
        #endif
    }

    public static func load(url: URL) throws -> SQLiteNowReleaseDistribution {
        do {
            let data = try Data(contentsOf: url)
            return try JSONDecoder().decode(SQLiteNowReleaseDistribution.self, from: data)
        } catch {
            throw SQLiteNowGenerateError(
                "Invalid SQLiteNow release distribution metadata at \(url.path): \(error.localizedDescription)"
            )
        }
    }
}

public enum SQLiteNowConfigLoader {
    public static func load(configPath: URL, fileManager: FileManager = .default) throws -> SQLiteNowConfiguration {
        guard fileManager.fileExists(atPath: configPath.path) else {
            throw SQLiteNowGenerateError("Missing SQLiteNow config at \(configPath.path).")
        }
        let data = try Data(contentsOf: configPath)
        try validateShape(data: data)
        let config: SQLiteNowConfiguration
        do {
            config = try JSONDecoder().decode(SQLiteNowConfiguration.self, from: data)
        } catch DecodingError.keyNotFound(let key, _) {
            throw SQLiteNowGenerateError("Missing required SQLiteNow.json field '\(key.stringValue)'.")
        } catch DecodingError.typeMismatch(_, let context) {
            throw SQLiteNowGenerateError("Invalid SQLiteNow.json value at \(context.codingPath.pathDescription).")
        } catch {
            throw SQLiteNowGenerateError("Invalid SQLiteNow.json: \(error.localizedDescription)")
        }
        try validateConfig(config)
        return config
    }

    private static func validateConfig(_ config: SQLiteNowConfiguration) throws {
        guard config.schemaVersion == 1 else {
            throw SQLiteNowGenerateError("Unsupported SQLiteNow.json schemaVersion \(config.schemaVersion). Expected 1.")
        }
        guard !config.databases.isEmpty else {
            throw SQLiteNowGenerateError("SQLiteNow.json requires at least one database.")
        }

        var names = Set<String>()
        var swiftPackageNames = Set<String>()
        for database in config.databases {
            guard !database.databaseName.trimmed().isEmpty else {
                throw SQLiteNowGenerateError("databaseName must not be empty.")
            }
            if let sqlDirectory = database.sqlDirectory, sqlDirectory.trimmed().isEmpty {
                throw SQLiteNowGenerateError("sqlDirectory must not be empty for database '\(database.databaseName)'.")
            }
            guard !database.swiftPackageName.trimmed().isEmpty else {
                throw SQLiteNowGenerateError("swiftPackageName must not be empty for database '\(database.databaseName)'.")
            }
            guard !database.swiftTargetName.trimmed().isEmpty else {
                throw SQLiteNowGenerateError("swiftTargetName must not be empty for database '\(database.databaseName)'.")
            }
            guard names.insert(database.databaseName).inserted else {
                throw SQLiteNowGenerateError("Duplicate SQLiteNow databaseName '\(database.databaseName)'.")
            }
            guard swiftPackageNames.insert(database.swiftPackageName).inserted else {
                throw SQLiteNowGenerateError("Duplicate SQLiteNow swiftPackageName '\(database.swiftPackageName)'.")
            }
            guard database.runtime == "core" || database.runtime == "sync" else {
                throw SQLiteNowGenerateError("Unsupported runtime '\(database.runtime)' for database '\(database.databaseName)'. Expected 'core' or 'sync'.")
            }
            if let requestedAppleTargets = database.requestedAppleTargets {
                guard !requestedAppleTargets.isEmpty else {
                    throw SQLiteNowGenerateError("requestedAppleTargets must not be empty for database '\(database.databaseName)'.")
                }
                let unsupportedTargets = requestedAppleTargets.filter { !supportedAppleTargetSet.contains($0) }
                guard unsupportedTargets.isEmpty else {
                    throw SQLiteNowGenerateError(
                        "Unsupported requestedAppleTargets for database '\(database.databaseName)': \(unsupportedTargets.joined(separator: ", ")). " +
                        "Supported native Swift runtime targets are \(supportedAppleTargets.joined(separator: ", ")). x86_64 Apple slices are not supported."
                    )
                }
            }
            if database.runtimeArtifact != nil && database.runtimeXcframeworkDirectory != nil {
                throw SQLiteNowGenerateError("Database '\(database.databaseName)' cannot specify both runtimeArtifact and runtimeXcframeworkDirectory.")
            }
            if let sqliteNowPackage = database.sqliteNowPackage {
                try validateSqliteNowPackage(sqliteNowPackage, context: "sqliteNowPackage for database '\(database.databaseName)'")
            }
            if let runtimeArtifact = database.runtimeArtifact {
                switch runtimeArtifact.kind {
                case "localZip":
                    guard !(runtimeArtifact.path ?? "").trimmed().isEmpty else {
                        throw SQLiteNowGenerateError("runtimeArtifact.path is required for database '\(database.databaseName)'.")
                    }
                    guard (runtimeArtifact.url ?? "").trimmed().isEmpty else {
                        throw SQLiteNowGenerateError("runtimeArtifact.url is not supported for localZip runtime artifacts in database '\(database.databaseName)'.")
                    }
                case "remoteZip":
                    guard (runtimeArtifact.path ?? "").trimmed().isEmpty else {
                        throw SQLiteNowGenerateError("runtimeArtifact.path is not supported for remoteZip runtime artifacts in database '\(database.databaseName)'.")
                    }
                    let url = (runtimeArtifact.url ?? "").trimmed()
                    guard !url.isEmpty else {
                        throw SQLiteNowGenerateError("runtimeArtifact.url is required for database '\(database.databaseName)'.")
                    }
                    guard URL(string: url)?.scheme == "https" else {
                        throw SQLiteNowGenerateError("runtimeArtifact.url must use https for database '\(database.databaseName)'.")
                    }
                default:
                    throw SQLiteNowGenerateError("Unsupported runtimeArtifact.kind '\(runtimeArtifact.kind)' for database '\(database.databaseName)'. Expected 'localZip' or 'remoteZip'.")
                }
                guard !(runtimeArtifact.checksum ?? "").trimmed().isEmpty else {
                    throw SQLiteNowGenerateError("runtimeArtifact.checksum is required for database '\(database.databaseName)'.")
                }
                guard !(runtimeArtifact.sqliteNowVersion ?? "").trimmed().isEmpty else {
                    throw SQLiteNowGenerateError("runtimeArtifact.sqliteNowVersion is required for database '\(database.databaseName)'.")
                }
            }
        }
    }

    private static func validateShape(data: Data) throws {
        let raw = try JSONSerialization.jsonObject(with: data)
        guard let root = raw as? [String: Any] else {
            throw SQLiteNowGenerateError("SQLiteNow.json must contain a JSON object.")
        }
        try rejectUnsupportedFields(
            Set(root.keys),
            allowed: ["schemaVersion", "databases"],
            context: "SQLiteNow.json"
        )

        guard let databases = root["databases"] as? [[String: Any]] else {
            return
        }
        for database in databases {
            if let name = database["databaseName"] as? String {
                try rejectUnsupportedFields(
                    Set(database.keys),
                    allowed: [
                        "databaseName",
                        "sqlDirectory",
                        "swiftPackageName",
                        "swiftTargetName",
                        "outputDirectory",
                        "runtime",
                        "minimumPlatforms",
                        "runtimeModuleName",
                        "metadataPackageName",
                        "runtimeArtifact",
                        "runtimeXcframeworkDirectory",
                        "sqliteNowPackage",
                        "requestedAppleTargets",
                        "debug",
                    ],
                    context: "database '\(name)'"
                )
            }
            if let artifact = database["runtimeArtifact"] as? [String: Any] {
                try rejectUnsupportedFields(
                    Set(artifact.keys),
                    allowed: ["kind", "path", "url", "checksum", "sqliteNowVersion"],
                    context: "runtimeArtifact"
                )
            }
            if let sqliteNowPackage = database["sqliteNowPackage"] as? [String: Any] {
                try rejectUnsupportedFields(
                    Set(sqliteNowPackage.keys),
                    allowed: [
                        "kind",
                        "packageIdentity",
                        "path",
                        "url",
                        "version",
                        "coreRuntimeProduct",
                        "syncRuntimeProduct",
                        "coreSupportProduct",
                        "syncSupportProduct",
                    ],
                    context: "sqliteNowPackage"
                )
            }
            if let minimumPlatforms = database["minimumPlatforms"] as? [String: Any] {
                try rejectUnsupportedFields(
                    Set(minimumPlatforms.keys),
                    allowed: ["iOS", "macOS"],
                    context: "minimumPlatforms"
                )
            }
        }
    }

    private static func rejectUnsupportedFields(
        _ fields: Set<String>,
        allowed: Set<String>,
        context: String
    ) throws {
        let unsupported = fields.subtracting(allowed).sorted()
        guard unsupported.isEmpty else {
            throw SQLiteNowGenerateError("Unsupported field(s) in \(context): \(unsupported.joined(separator: ", ")).")
        }
    }

    private static func validateSqliteNowPackage(
        _ sqliteNowPackage: SwiftPackageDependencyConfiguration,
        context: String
    ) throws {
        switch sqliteNowPackage.kind {
        case "localPath":
            guard !(sqliteNowPackage.path ?? "").trimmed().isEmpty else {
                throw SQLiteNowGenerateError("\(context).path is required for localPath dependencies.")
            }
            guard (sqliteNowPackage.url ?? "").trimmed().isEmpty else {
                throw SQLiteNowGenerateError("\(context).url is not supported for localPath dependencies.")
            }
            guard (sqliteNowPackage.version ?? "").trimmed().isEmpty else {
                throw SQLiteNowGenerateError("\(context).version is not supported for localPath dependencies.")
            }
        case "remoteExact":
            guard (sqliteNowPackage.path ?? "").trimmed().isEmpty else {
                throw SQLiteNowGenerateError("\(context).path is not supported for remoteExact dependencies.")
            }
            let url = (sqliteNowPackage.url ?? "").trimmed()
            guard !url.isEmpty else {
                throw SQLiteNowGenerateError("\(context).url is required for remoteExact dependencies.")
            }
            guard URL(string: url)?.scheme == "https" || URL(string: url)?.isFileURL == true else {
                throw SQLiteNowGenerateError("\(context).url must use https or file.")
            }
            guard !(sqliteNowPackage.version ?? "").trimmed().isEmpty else {
                throw SQLiteNowGenerateError("\(context).version is required for remoteExact dependencies.")
            }
        default:
            throw SQLiteNowGenerateError("\(context).kind must be 'localPath' or 'remoteExact'.")
        }
    }
}

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
        let configRoot = configPath.deletingLastPathComponent().standardizedFileURL
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
        var outputDestinations: [String: String] = [:]
        var invocations: [CompilerInvocation] = []

        for database in config.databases {
            let sqlDirectoryURL: URL
            if let sqlDirectory = database.sqlDirectory {
                sqlDirectoryURL = resolveURL(sqlDirectory, relativeTo: configRoot)
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
                outputDirectoryURL = resolveURL(outputDirectory, relativeTo: configRoot)
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
            let outputDirectoryPath = requestPath(outputDirectoryURL, relativeTo: packageRoot)
            let outputDestinationKey = resolvedOutputDirectoryURL.path
            if let previousDatabaseName = outputDestinations[outputDestinationKey] {
                throw SQLiteNowGenerateError(
                    "Duplicate SQLiteNow generated package output '\(outputDirectoryPath)' for databases '\(previousDatabaseName)' and '\(database.databaseName)'."
                )
            }
            outputDestinations[outputDestinationKey] = database.databaseName
            let requestFile = packageRoot
                .appendingPathComponent(".build/sqlitenow/requests")
                .appendingPathComponent("\(safeFileName(database.databaseName)).json")
                .standardizedFileURL
            var directoriesToCreate = [
                requestFile.deletingLastPathComponent()
            ]

            let runtimeXcframeworkDirectory = try database.runtimeXcframeworkDirectory.map { path in
                guard !path.trimmed().isEmpty else {
                    throw SQLiteNowGenerateError("runtimeXcframeworkDirectory must not be empty for database '\(database.databaseName)'.")
                }
                let resolved = resolveURL(path, relativeTo: configRoot)
                guard fileManager.directoryExists(at: resolved) else {
                    throw SQLiteNowGenerateError("Runtime XCFramework not found for database '\(database.databaseName)': \(resolved.path).")
                }
                return requestPath(resolved, relativeTo: packageRoot)
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
                try compilerRuntimeArtifact(
                    artifact,
                    databaseName: database.databaseName,
                    configRoot: configRoot,
                    packageRoot: packageRoot,
                    fileManager: fileManager
                )
            }
            let sqliteNowPackage = try compilerSwiftPackageDependency(
                sqliteNowPackageConfiguration(
                    database: database,
                    releaseDistribution: releaseDistribution
                ),
                databaseName: database.databaseName,
                configRoot: configRoot,
                packageRoot: packageRoot,
                generatedPackageRoot: outputDirectoryURL
            )
            let sqliteNowVersion = resolvedRuntimeArtifactConfiguration?.sqliteNowVersion ?? compilerVersion
            let schemaDatabaseFile: String?
            if database.debug ?? false {
                let schemaDirectory = packageRoot
                    .appendingPathComponent(".build/sqlitenow/schema")
                    .standardizedFileURL
                directoriesToCreate.append(schemaDirectory)
                schemaDatabaseFile = requestPath(
                    schemaDirectory.appendingPathComponent("\(safeFileName(database.databaseName)).db"),
                    relativeTo: packageRoot
                )
            } else {
                schemaDatabaseFile = nil
            }
            let compilerRequest = CompilerRequest(
                databaseName: database.databaseName,
                sqlDirectory: requestPath(sqlDirectoryURL, relativeTo: packageRoot),
                metadataPackageName: database.metadataPackageName ?? defaultMetadataPackageName(database.databaseName),
                compilerOutputDirectory: ".build/sqlitenow/compiler/\(safeFileName(database.databaseName))",
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
            invocations.append(CompilerInvocation(
                databaseName: database.databaseName,
                requestFile: requestFile,
                directoriesToCreate: directoriesToCreate,
                requestJSON: requestJSON,
                instructions: GeneratedPackageInstructions(
                    packageName: database.swiftPackageName,
                    productName: database.swiftPackageName,
                    relativePackagePath: outputDirectoryPath
                )
            ))
        }

        return invocations
    }

    private static func compilerRuntimeArtifact(
        _ artifact: RuntimeArtifactConfiguration,
        databaseName: String,
        configRoot: URL,
        packageRoot: URL,
        fileManager: FileManager
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
        let resolved = resolveURL(path, relativeTo: configRoot)
        guard fileManager.fileExists(atPath: resolved.path) else {
            throw SQLiteNowGenerateError("Runtime artifact not found for database '\(databaseName)': \(resolved.path).")
        }
        return CompilerRuntimeArtifact(
            kind: artifact.kind,
            path: requestPath(resolved, relativeTo: packageRoot),
            url: nil,
            checksum: artifact.checksum,
            sqliteNowVersion: artifact.sqliteNowVersion
        )
    }

    private static func compilerSwiftPackageDependency(
        _ dependency: SwiftPackageDependencyConfiguration?,
        databaseName: String,
        configRoot: URL,
        packageRoot: URL,
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
            let resolved = resolveURL(path, relativeTo: configRoot)
            return CompilerSwiftPackageDependency(
                kind: dependency.kind,
                packageIdentity: dependency.packageIdentity,
                path: localSwiftPackageDependencyPath(
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

    private static func sqliteNowPackageConfiguration(
        database: SQLiteNowDatabaseConfiguration,
        releaseDistribution: SQLiteNowReleaseDistribution?
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

private struct CompilerResponse: Decodable {
    let success: Bool
    let failure: CompilerFailure?

    func requireSuccess() throws {
        guard success else {
            throw SQLiteNowGenerateError(failure?.message ?? "SQLiteNow compiler request failed.")
        }
    }
}

private struct CompilerFailure: Decodable {
    let message: String
}

public enum CompilerTool: Equatable {
    case executable(URL)
    case jar(URL)

    var executableURL: URL {
        switch self {
        case .executable(let url):
            return url
        case .jar:
            return URL(fileURLWithPath: "/usr/bin/env")
        }
    }

    func arguments(_ compilerArguments: [String]) -> [String] {
        switch self {
        case .executable:
            return compilerArguments
        case .jar(let jar):
            return ["java", "-jar", jar.path] + compilerArguments
        }
    }
}

public enum CompilerToolResolver {
    public static func resolve(
        explicitExecutablePath: String?,
        explicitJarPath: String?,
        environment: [String: String],
        relativeTo packageRoot: URL,
        bundledCompilerJar: URL? = CompilerJarResolver.bundledCompilerJarURL(),
        fileManager: FileManager = .default
    ) throws -> CompilerTool {
        if let explicitExecutablePath, !explicitExecutablePath.trimmed().isEmpty {
            return .executable(try resolveConfiguredExecutable(
                explicitExecutablePath,
                relativeTo: packageRoot,
                fileManager: fileManager
            ))
        }

        let jar = try CompilerJarResolver.resolve(
            explicitPath: explicitJarPath,
            environment: environment,
            relativeTo: packageRoot,
            bundledCompilerJar: bundledCompilerJar,
            fileManager: fileManager
        )
        return .jar(jar)
    }

    private static func resolveConfiguredExecutable(
        _ configuredPath: String,
        relativeTo packageRoot: URL,
        fileManager: FileManager
    ) throws -> URL {
        let resolved = resolveURL(configuredPath, relativeTo: packageRoot)
        guard fileManager.fileExists(atPath: resolved.path) else {
            throw SQLiteNowGenerateError("Compiler executable not found at \(resolved.path).")
        }
        guard fileManager.isExecutableFile(atPath: resolved.path) else {
            throw SQLiteNowGenerateError("Compiler executable is not executable at \(resolved.path).")
        }
        return resolved
    }
}

public enum CompilerJarResolver {
    public static let environmentKey = "SQLITENOW_COMPILER_JAR"

    public static func bundledCompilerJarURL() -> URL? {
        #if SWIFT_PACKAGE
        return Bundle.module.url(forResource: "sqlitenow-compiler", withExtension: "jar", subdirectory: "Resources")
            ?? Bundle.module.url(forResource: "sqlitenow-compiler", withExtension: "jar")
        #else
        return nil
        #endif
    }

    public static func resolve(
        explicitPath: String?,
        environment: [String: String],
        relativeTo packageRoot: URL,
        bundledCompilerJar: URL? = bundledCompilerJarURL(),
        fileManager: FileManager = .default
    ) throws -> URL {
        if let explicitPath, !explicitPath.trimmed().isEmpty {
            return try resolveConfiguredJar(
                explicitPath,
                relativeTo: packageRoot,
                fileManager: fileManager
            )
        }

        if let environmentPath = environment[environmentKey], !environmentPath.trimmed().isEmpty {
            return try resolveConfiguredJar(
                environmentPath,
                relativeTo: packageRoot,
                fileManager: fileManager
            )
        }

        if let bundledCompilerJar {
            let resolved = bundledCompilerJar.standardizedFileURL
            guard fileManager.fileExists(atPath: resolved.path) else {
                throw SQLiteNowGenerateError(
                    "Bundled compiler jar not found at \(resolved.path). Run ./gradlew :sqlitenow-compiler:syncSwiftPmPluginCompilerJar, pass --compiler-executable, pass --compiler-jar, or set \(environmentKey)."
                )
            }
            return resolved
        }

        throw SQLiteNowGenerateError(
            "Compiler jar not found. Run ./gradlew :sqlitenow-compiler:syncSwiftPmPluginCompilerJar, pass --compiler-executable, pass --compiler-jar, or set \(environmentKey)."
        )
    }

    private static func resolveConfiguredJar(
        _ configuredPath: String,
        relativeTo packageRoot: URL,
        fileManager: FileManager
    ) throws -> URL {
        let resolved = resolveURL(configuredPath, relativeTo: packageRoot)
        guard fileManager.fileExists(atPath: resolved.path) else {
            throw SQLiteNowGenerateError("Compiler jar not found at \(resolved.path).")
        }
        return resolved
    }
}

public enum InstructionRenderer {
    public static func render(_ instructions: [GeneratedPackageInstructions]) -> String {
        guard !instructions.isEmpty else {
            return "SQLiteNow did not generate any packages."
        }
        let packageLines = instructions.map { instruction in
            """
            SQLiteNow generated \(instruction.packageName) at \(instruction.relativePackagePath)

            SwiftPM dependency:
                .package(path: "\(instruction.relativePackagePath)")

            Target dependency:
                .product(name: "\(instruction.productName)", package: "\(instruction.packageName)")

            Xcode:
                Add Local Package: \(instruction.relativePackagePath)
                Link product: \(instruction.productName)
            """
        }
        return packageLines.joined(separator: "\n\n")
    }
}

public protocol ProcessRunning {
    func run(executable: URL, arguments: [String], workingDirectory: URL) throws -> ProcessResult
}

public struct ProcessResult: Equatable {
    public let exitCode: Int32
    public let stdout: String
    public let stderr: String

    public init(exitCode: Int32, stdout: String, stderr: String) {
        self.exitCode = exitCode
        self.stdout = stdout
        self.stderr = stderr
    }
}

public struct SystemProcessRunner: ProcessRunning {
    public init() {}

    public func run(executable: URL, arguments: [String], workingDirectory: URL) throws -> ProcessResult {
        let fileManager = FileManager.default
        let captureDirectory = fileManager.temporaryDirectory
            .appendingPathComponent("sqlitenow-process-\(UUID().uuidString)", isDirectory: true)
        try fileManager.createDirectory(at: captureDirectory, withIntermediateDirectories: true)
        defer { try? fileManager.removeItem(at: captureDirectory) }

        let stdoutURL = captureDirectory.appendingPathComponent("stdout.txt")
        let stderrURL = captureDirectory.appendingPathComponent("stderr.txt")
        fileManager.createFile(atPath: stdoutURL.path, contents: nil)
        fileManager.createFile(atPath: stderrURL.path, contents: nil)
        let stdoutHandle = try FileHandle(forWritingTo: stdoutURL)
        let stderrHandle = try FileHandle(forWritingTo: stderrURL)
        defer {
            try? stdoutHandle.close()
            try? stderrHandle.close()
        }

        let process = Process()
        process.executableURL = executable
        process.arguments = arguments
        process.currentDirectoryURL = workingDirectory
        process.standardOutput = stdoutHandle
        process.standardError = stderrHandle

        try process.run()
        process.waitUntilExit()

        try stdoutHandle.close()
        try stderrHandle.close()
        let stdoutData = try Data(contentsOf: stdoutURL)
        let stderrData = try Data(contentsOf: stderrURL)
        return ProcessResult(
            exitCode: process.terminationStatus,
            stdout: String(decoding: stdoutData, as: UTF8.self),
            stderr: String(decoding: stderrData, as: UTF8.self)
        )
    }
}

public struct SQLiteNowGenerateError: LocalizedError, Equatable {
    public let message: String

    public init(_ message: String) {
        self.message = message
    }

    public var errorDescription: String? { message }
}

private func resolveURL(_ path: String, relativeTo baseURL: URL) -> URL {
    let expanded = (path as NSString).expandingTildeInPath
    if expanded.hasPrefix("/") {
        return URL(fileURLWithPath: expanded).standardizedFileURL
    }
    return baseURL.appendingPathComponent(expanded).standardizedFileURL
}

private func normalizedRequestPath(_ path: String) -> String {
    if path.hasPrefix("/") {
        return URL(fileURLWithPath: path).standardizedFileURL.path
    }
    var segments: [String] = []
    for segment in path.split(separator: "/", omittingEmptySubsequences: true).map(String.init) {
        switch segment {
        case ".":
            continue
        case "..":
            if let last = segments.last, last != ".." {
                segments.removeLast()
            } else {
                segments.append(segment)
            }
        default:
            segments.append(segment)
        }
    }
    return segments.isEmpty ? "." : segments.joined(separator: "/")
}

private func requestPath(_ url: URL, relativeTo baseURL: URL) -> String {
    let standardizedURL = url.standardizedFileURL
    let standardizedBaseURL = baseURL.standardizedFileURL
    let path = standardizedURL.path
    let basePath = standardizedBaseURL.path

    if path == basePath {
        return "."
    }
    if path.hasPrefix(basePath + "/") {
        let relative = String(path.dropFirst(basePath.count + 1))
        return normalizedRequestPath(relative)
    }
    return path
}

private func localSwiftPackageDependencyPath(
    _ url: URL,
    packageRoot: URL,
    generatedPackageRoot: URL
) -> String {
    let standardizedURL = url.standardizedFileURL
    let standardizedPackageRoot = packageRoot.standardizedFileURL
    guard standardizedURL.isDescendantOrEqual(to: standardizedPackageRoot) else {
        return requestPath(standardizedURL, relativeTo: standardizedPackageRoot)
    }
    return relativeRequestPath(standardizedURL, from: generatedPackageRoot)
}

private func relativeRequestPath(_ url: URL, from baseURL: URL) -> String {
    let targetComponents = url.standardizedFileURL.pathComponents
    let baseComponents = baseURL.standardizedFileURL.pathComponents
    var commonPrefixCount = 0
    while commonPrefixCount < targetComponents.count,
          commonPrefixCount < baseComponents.count,
          targetComponents[commonPrefixCount] == baseComponents[commonPrefixCount] {
        commonPrefixCount += 1
    }

    let parentSegments = Array(repeating: "..", count: baseComponents.count - commonPrefixCount)
    let childSegments = Array(targetComponents.dropFirst(commonPrefixCount))
    return normalizedRequestPath((parentSegments + childSegments).joined(separator: "/"))
}

private extension URL {
    func isDescendantOrEqual(to baseURL: URL) -> Bool {
        path == baseURL.path || path.hasPrefix(baseURL.path + "/")
    }

    func resolvingSymlinkComponents(fileManager: FileManager) throws -> URL {
        var resolvedComponents: [String] = []
        var pendingComponents = Array(standardizedFileURL.pathComponents.dropFirst())
        var symlinkExpansionCount = 0

        while !pendingComponents.isEmpty {
            let component = pendingComponents.removeFirst()
            switch component {
            case ".":
                continue
            case "..":
                if !resolvedComponents.isEmpty {
                    resolvedComponents.removeLast()
                }
                continue
            default:
                break
            }

            let candidatePath = "/" + (resolvedComponents + [component]).joined(separator: "/")
            if let destination = try? fileManager.destinationOfSymbolicLink(atPath: candidatePath) {
                symlinkExpansionCount += 1
                guard symlinkExpansionCount <= 64 else {
                    throw SQLiteNowGenerateError("Too many symbolic links while resolving path: \(path).")
                }
                if destination.hasPrefix("/") {
                    resolvedComponents = []
                }
                pendingComponents = destination.split(separator: "/", omittingEmptySubsequences: true).map(String.init) + pendingComponents
            } else {
                resolvedComponents.append(component)
            }
        }

        return URL(fileURLWithPath: "/" + resolvedComponents.joined(separator: "/"))
    }
}

private func defaultMetadataPackageName(_ databaseName: String) -> String {
    let rawSegments = databaseName
        .lowercased()
        .split { !$0.isLetter && !$0.isNumber }
        .map(String.init)
    let segments = rawSegments.isEmpty ? ["database"] : rawSegments.map { segment in
        guard let first = segment.first, first.isLetter else {
            return "db\(segment)"
        }
        return segment
    }
    return "sqlitenow.generated.\(segments.joined(separator: ".")).metadata"
}

private func safeFileName(_ value: String) -> String {
    let mapped = value.map { character -> Character in
        if character.isLetter || character.isNumber || character == "-" || character == "_" {
            return character
        }
        return "-"
    }
    let result = String(mapped).trimmingCharacters(in: CharacterSet(charactersIn: "-_"))
    return result.isEmpty ? "database" : result
}

private extension FileManager {
    func directoryExists(at url: URL) -> Bool {
        var isDirectory: ObjCBool = false
        return fileExists(atPath: url.path, isDirectory: &isDirectory) && isDirectory.boolValue
    }
}

private extension String {
    func trimmed() -> String {
        trimmingCharacters(in: .whitespacesAndNewlines)
    }
}

private extension Array where Element == CodingKey {
    var pathDescription: String {
        guard !isEmpty else {
            return "<root>"
        }
        return map(\.stringValue).joined(separator: ".")
    }
}
