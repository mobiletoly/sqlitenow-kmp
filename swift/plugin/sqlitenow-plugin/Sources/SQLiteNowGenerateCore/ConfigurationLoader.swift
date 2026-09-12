import Foundation

public enum SQLiteNowConfigLoader {
    public static func load(configPath: URL, fileManager: FileManager = .default) throws -> SQLiteNowConfiguration {
        guard fileManager.fileExists(atPath: configPath.path) else {
            throw SQLiteNowGenerateError("Missing SQLiteNow config at \(configPath.path).")
        }
        let data = try Data(contentsOf: configPath)
        try SQLiteNowConfigValidator.validateShape(data: data)
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
        try SQLiteNowConfigValidator.validate(config)
        return config
    }
}

private enum SQLiteNowConfigValidator {
    private static let supportedAppleTargets = ["macosArm64", "iosArm64", "iosSimulatorArm64"]
    private static let supportedAppleTargetSet = Set(supportedAppleTargets)

    static func validate(_ config: SQLiteNowConfiguration) throws {
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

    static func validateShape(data: Data) throws {
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
