import Foundation

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
