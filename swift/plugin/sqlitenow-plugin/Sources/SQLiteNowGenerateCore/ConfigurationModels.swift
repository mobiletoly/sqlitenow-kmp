import Foundation

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
