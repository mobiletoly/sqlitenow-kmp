public struct SQLiteNowSyncAuth: @unchecked Sendable {
    internal let accessTokenProvider: @Sendable () -> String
    internal let refreshedAccessTokenProvider: (@Sendable () -> String?)?
    internal let runtimeAuth: SQLiteNowSyncRuntimeAuth

    private init(
        accessTokenProvider: @escaping @Sendable () -> String,
        refreshedAccessTokenProvider: (@Sendable () -> String?)?
    ) {
        self.accessTokenProvider = accessTokenProvider
        self.refreshedAccessTokenProvider = refreshedAccessTokenProvider
        self.runtimeAuth = SQLiteNowSyncRuntimeAuth(
            accessTokenProvider: accessTokenProvider,
            refreshedAccessTokenProvider: refreshedAccessTokenProvider
        )
    }

    public static func bearer(token: String) -> SQLiteNowSyncAuth {
        SQLiteNowSyncAuth(
            accessTokenProvider: { token },
            refreshedAccessTokenProvider: nil
        )
    }

    public static func bearer(
        accessToken: @escaping @Sendable () -> String,
        refreshedAccessToken: (@Sendable () -> String?)? = nil
    ) -> SQLiteNowSyncAuth {
        SQLiteNowSyncAuth(
            accessTokenProvider: accessToken,
            refreshedAccessTokenProvider: refreshedAccessToken
        )
    }

}

public struct SQLiteNowTransientRetryPolicy: Equatable, Sendable {
    public var maxAttempts: Int
    public var initialBackoffMillis: Int64
    public var maxBackoffMillis: Int64
    public var jitterRatio: Double

    public init(
        maxAttempts: Int = 3,
        initialBackoffMillis: Int64 = 150,
        maxBackoffMillis: Int64 = 1_500,
        jitterRatio: Double = 0.2
    ) {
        self.maxAttempts = maxAttempts
        self.initialBackoffMillis = initialBackoffMillis
        self.maxBackoffMillis = maxBackoffMillis
        self.jitterRatio = jitterRatio
    }
}

public struct SQLiteNowSnapshotCapacityRetryPolicy: Equatable, Sendable {
    public var enabled: Bool
    public var maxWaitMillis: Int64
    public var fallbackDelayMillis: Int64
    public var jitterRatio: Double

    public init(
        enabled: Bool = true,
        maxWaitMillis: Int64 = 30_000,
        fallbackDelayMillis: Int64 = 1_000,
        jitterRatio: Double = 1.0
    ) {
        self.enabled = enabled
        self.maxWaitMillis = maxWaitMillis
        self.fallbackDelayMillis = fallbackDelayMillis
        self.jitterRatio = jitterRatio
    }
}

public struct SQLiteNowTransferTargets: Equatable, Sendable {
    public var maxPushRows: Int
    public var maxPushDecodedBytes: Int64
    public var maxPullBundles: Int
    public var maxSnapshotChunkRows: Int
    public var maxSnapshotChunkDecodedBytes: Int64

    public init(
        maxPushRows: Int = 200,
        maxPushDecodedBytes: Int64 = 1 * 1_024 * 1_024,
        maxPullBundles: Int = 1_000,
        maxSnapshotChunkRows: Int = 1_000,
        maxSnapshotChunkDecodedBytes: Int64 = 4 * 1_024 * 1_024
    ) {
        self.maxPushRows = maxPushRows
        self.maxPushDecodedBytes = maxPushDecodedBytes
        self.maxPullBundles = maxPullBundles
        self.maxSnapshotChunkRows = maxSnapshotChunkRows
        self.maxSnapshotChunkDecodedBytes = maxSnapshotChunkDecodedBytes
    }
}

public struct SQLiteNowSyncConfig: Equatable, Sendable {
    public var generationId: Int64
    public var databaseId: String
    public var serviceId: String
    public var maxSyncWorkspaceEntries: Int64
    public var maxSyncWorkspaceBytes: Int64
    public var maxSyncWorkUnitsPerInvocation: Int64
    public var transferTargets: SQLiteNowTransferTargets
    public var snapshotApplyBatchRows: Int
    public var snapshotApplyBatchBytes: Int64
    public var verboseLogs: Bool
    public var transientRetryPolicy: SQLiteNowTransientRetryPolicy
    public var snapshotCapacityRetryPolicy: SQLiteNowSnapshotCapacityRetryPolicy

    public init(
        generationId: Int64,
        databaseId: String,
        serviceId: String,
        maxSyncWorkspaceEntries: Int64 = 100_000,
        maxSyncWorkspaceBytes: Int64 = 256 * 1_024 * 1_024,
        maxSyncWorkUnitsPerInvocation: Int64 = 1_000_000,
        transferTargets: SQLiteNowTransferTargets = .init(),
        snapshotApplyBatchRows: Int = 256,
        snapshotApplyBatchBytes: Int64 = 4 * 1_024 * 1_024,
        verboseLogs: Bool = false,
        transientRetryPolicy: SQLiteNowTransientRetryPolicy = .init(),
        snapshotCapacityRetryPolicy: SQLiteNowSnapshotCapacityRetryPolicy = .init()
    ) {
        self.generationId = generationId
        self.databaseId = databaseId
        self.serviceId = serviceId
        self.maxSyncWorkspaceEntries = maxSyncWorkspaceEntries
        self.maxSyncWorkspaceBytes = maxSyncWorkspaceBytes
        self.maxSyncWorkUnitsPerInvocation = maxSyncWorkUnitsPerInvocation
        self.transferTargets = transferTargets
        self.snapshotApplyBatchRows = snapshotApplyBatchRows
        self.snapshotApplyBatchBytes = snapshotApplyBatchBytes
        self.verboseLogs = verboseLogs
        self.transientRetryPolicy = transientRetryPolicy
        self.snapshotCapacityRetryPolicy = snapshotCapacityRetryPolicy
    }

    internal func runtimeConfig(
        syncTables: [SQLiteNowSyncRuntimeTableSpec]
    ) throws -> SQLiteNowSyncRuntimeConfig {
        guard (1...Int64(Int32.max)).contains(generationId) else {
            throw SQLiteNowError.misuse(message: "generationId must be in 1..2147483647")
        }
        guard !databaseId.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
              !serviceId.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw SQLiteNowError.misuse(message: "databaseId and serviceId must be nonblank")
        }
        guard transferTargets.maxPushRows > 0,
              transferTargets.maxPushDecodedBytes > 0,
              transferTargets.maxPullBundles > 0,
              transferTargets.maxSnapshotChunkRows > 0,
              transferTargets.maxSnapshotChunkDecodedBytes > 0 else {
            throw SQLiteNowError.misuse(message: "all transferTargets values must be positive")
        }
        guard maxSyncWorkspaceEntries > 0, maxSyncWorkspaceBytes > 0, maxSyncWorkUnitsPerInvocation > 0 else {
            throw SQLiteNowError.misuse(message: "all sync workspace limits must be positive")
        }
        guard snapshotApplyBatchRows > 0 else {
            throw SQLiteNowError.misuse(message: "snapshotApplyBatchRows must be positive")
        }
        guard snapshotApplyBatchBytes > 0 else {
            throw SQLiteNowError.misuse(message: "snapshotApplyBatchBytes must be positive")
        }
        guard snapshotCapacityRetryPolicy.maxWaitMillis > 0 else {
            throw SQLiteNowError.misuse(message: "snapshotCapacityRetryPolicy.maxWaitMillis must be positive")
        }
        guard snapshotCapacityRetryPolicy.fallbackDelayMillis > 0 else {
            throw SQLiteNowError.misuse(message: "snapshotCapacityRetryPolicy.fallbackDelayMillis must be positive")
        }
        guard (0.0...1.0).contains(snapshotCapacityRetryPolicy.jitterRatio) else {
            throw SQLiteNowError.misuse(message: "snapshotCapacityRetryPolicy.jitterRatio must be within 0...1")
        }

        return SQLiteNowSyncRuntimeConfig(
            generationId: generationId,
            databaseId: databaseId,
            serviceId: serviceId,
            maxSyncWorkspaceEntries: maxSyncWorkspaceEntries,
            maxSyncWorkspaceBytes: maxSyncWorkspaceBytes,
            maxSyncWorkUnitsPerInvocation: maxSyncWorkUnitsPerInvocation,
            syncTables: syncTables,
            transferTargets: SQLiteNowSyncRuntimeTransferTargets(
                maxPushRows: try sqliteNowSyncInt32(transferTargets.maxPushRows, field: "transferTargets.maxPushRows"),
                maxPushDecodedBytes: transferTargets.maxPushDecodedBytes,
                maxPullBundles: try sqliteNowSyncInt32(transferTargets.maxPullBundles, field: "transferTargets.maxPullBundles"),
                maxSnapshotChunkRows: try sqliteNowSyncInt32(transferTargets.maxSnapshotChunkRows, field: "transferTargets.maxSnapshotChunkRows"),
                maxSnapshotChunkDecodedBytes: transferTargets.maxSnapshotChunkDecodedBytes
            ),
            snapshotApplyBatchRows: try sqliteNowSyncInt32(snapshotApplyBatchRows, field: "snapshotApplyBatchRows"),
            snapshotApplyBatchBytes: snapshotApplyBatchBytes,
            verboseLogs: verboseLogs,
            transientRetryPolicy: SQLiteNowSyncRuntimeTransientRetryPolicy(
                maxAttempts: try sqliteNowSyncInt32(transientRetryPolicy.maxAttempts, field: "transientRetryPolicy.maxAttempts"),
                initialBackoffMillis: transientRetryPolicy.initialBackoffMillis,
                maxBackoffMillis: transientRetryPolicy.maxBackoffMillis,
                jitterRatio: transientRetryPolicy.jitterRatio
            ),
            snapshotCapacityRetryPolicy: SQLiteNowSyncRuntimeSnapshotCapacityRetryPolicy(
                enabled: snapshotCapacityRetryPolicy.enabled,
                maxWaitMillis: snapshotCapacityRetryPolicy.maxWaitMillis,
                fallbackDelayMillis: snapshotCapacityRetryPolicy.fallbackDelayMillis,
                jitterRatio: snapshotCapacityRetryPolicy.jitterRatio
            )
        )
    }
}

private func sqliteNowSyncInt32(_ value: Int, field: String) throws -> Int32 {
    guard let converted = Int32(exactly: value) else {
        throw SQLiteNowError.misuse(message: "\(field) is outside the 32-bit signed integer range")
    }
    return converted
}

public enum SQLiteNowAuthorityStatus: Equatable, Sendable {
    case pendingLocalSeed
    case authoritativeEmpty
    case authoritativeMaterialized
    case unknown(String)

    internal init(_ rawValue: String) {
        switch rawValue {
        case "PENDING_LOCAL_SEED": self = .pendingLocalSeed
        case "AUTHORITATIVE_EMPTY": self = .authoritativeEmpty
        case "AUTHORITATIVE_MATERIALIZED": self = .authoritativeMaterialized
        default: self = .unknown(rawValue)
        }
    }
}

public enum SQLiteNowAttachOutcome: Equatable, Sendable {
    case resumedAttachedState
    case usedRemoteState
    case seededFromLocal
    case startedEmpty
    case unknown(String)

    internal init(_ rawValue: String) {
        switch rawValue {
        case "RESUMED_ATTACHED_STATE": self = .resumedAttachedState
        case "USED_REMOTE_STATE": self = .usedRemoteState
        case "SEEDED_FROM_LOCAL": self = .seededFromLocal
        case "STARTED_EMPTY": self = .startedEmpty
        default: self = .unknown(rawValue)
        }
    }
}

public enum SQLiteNowDetachOutcome: Equatable, Sendable {
    case detached
    case blockedUnsyncedData
    case unknown(String)

    internal init(_ rawValue: String) {
        switch rawValue {
        case "DETACHED": self = .detached
        case "BLOCKED_UNSYNCED_DATA": self = .blockedUnsyncedData
        default: self = .unknown(rawValue)
        }
    }
}

public enum SQLiteNowSourceRecoveryReason: Equatable, Sendable {
    case historyPruned
    case sourceSequenceOutOfOrder
    case sourceSequenceChanged
    case sourceRetired
    case unknown(String)

    internal init(_ rawValue: String) {
        switch rawValue {
        case "HISTORY_PRUNED": self = .historyPruned
        case "SOURCE_SEQUENCE_OUT_OF_ORDER": self = .sourceSequenceOutOfOrder
        case "SOURCE_SEQUENCE_CHANGED": self = .sourceSequenceChanged
        case "SOURCE_RETIRED": self = .sourceRetired
        default: self = .unknown(rawValue)
        }
    }
}

public enum SQLiteNowPushOutcome: Equatable, Sendable {
    case noChange
    case committed
    case unknown(String)

    internal init(_ rawValue: String) {
        switch rawValue {
        case "NO_CHANGE": self = .noChange
        case "COMMITTED": self = .committed
        default: self = .unknown(rawValue)
        }
    }
}

public enum SQLiteNowRemoteSyncOutcome: Equatable, Sendable {
    case alreadyAtTarget
    case appliedIncremental
    case appliedSnapshot
    case unknown(String)

    internal init(_ rawValue: String) {
        switch rawValue {
        case "ALREADY_AT_TARGET": self = .alreadyAtTarget
        case "APPLIED_INCREMENTAL": self = .appliedIncremental
        case "APPLIED_SNAPSHOT": self = .appliedSnapshot
        default: self = .unknown(rawValue)
        }
    }
}

public enum SQLiteNowOversqliteOperation: Equatable, Sendable {
    case attach
    case pushPending
    case pullToStable
    case sync
    case rebuildKeepSource
    case rebuildRotateSource
    case unknown(String)

    internal init(_ rawValue: String) {
        switch rawValue {
        case "ATTACH": self = .attach
        case "PUSH_PENDING": self = .pushPending
        case "PULL_TO_STABLE": self = .pullToStable
        case "SYNC": self = .sync
        case "REBUILD_KEEP_SOURCE": self = .rebuildKeepSource
        case "REBUILD_ROTATE_SOURCE": self = .rebuildRotateSource
        default: self = .unknown(rawValue)
        }
    }
}

public enum SQLiteNowOversqlitePhase: Equatable, Sendable {
    case attaching
    case seeding
    case pushing
    case pulling
    case stagingRemoteState
    case applyingRemoteState
    case unknown(String)

    internal init(_ rawValue: String) {
        switch rawValue {
        case "ATTACHING": self = .attaching
        case "SEEDING": self = .seeding
        case "PUSHING": self = .pushing
        case "PULLING": self = .pulling
        case "STAGING_REMOTE_STATE": self = .stagingRemoteState
        case "APPLYING_REMOTE_STATE": self = .applyingRemoteState
        default: self = .unknown(rawValue)
        }
    }
}

public enum SQLiteNowBundleChangeWatchMode: String, Sendable {
    case off = "OFF"
    case auto = "AUTO"
}

public struct SQLiteNowPendingSyncStatus: Equatable, Sendable {
    public let hasPendingSyncData: Bool
    public let pendingRowCount: Int64
    public let blocksDetach: Bool

    internal init(_ runtime: SQLiteNowSyncRuntimePendingSyncStatus) {
        hasPendingSyncData = runtime.hasPendingSyncData
        pendingRowCount = runtime.pendingRowCount
        blocksDetach = runtime.blocksDetach
    }
}

public struct SQLiteNowSyncStatus: Equatable, Sendable {
    public let authority: SQLiteNowAuthorityStatus
    public let pending: SQLiteNowPendingSyncStatus
    public let lastBundleSeqSeen: Int64

    internal init(_ runtime: SQLiteNowSyncRuntimeSyncStatus) {
        authority = SQLiteNowAuthorityStatus(runtime.authority)
        pending = SQLiteNowPendingSyncStatus(runtime.pending)
        lastBundleSeqSeen = runtime.lastBundleSeqSeen
    }
}

public struct SQLiteNowSourceInfo: Equatable, Sendable {
    public let currentSourceId: String
    public let generationId: Int64
    public let contractConfirmed: Bool
    public let rebuildRequired: Bool
    public let sourceRecoveryRequired: Bool
    public let sourceRecoveryReason: SQLiteNowSourceRecoveryReason?

    internal init(_ runtime: SQLiteNowSyncRuntimeSourceInfo) {
        currentSourceId = runtime.currentSourceId
        generationId = runtime.generationId
        contractConfirmed = runtime.contractConfirmed
        rebuildRequired = runtime.rebuildRequired
        sourceRecoveryRequired = runtime.sourceRecoveryRequired
        sourceRecoveryReason = runtime.sourceRecoveryReason.map(SQLiteNowSourceRecoveryReason.init)
    }
}

public struct SQLiteNowRestoreSummary: Equatable, Sendable {
    public let bundleSeq: Int64
    public let rowCount: Int64

    internal init(_ runtime: SQLiteNowSyncRuntimeRestoreSummary) {
        bundleSeq = runtime.bundleSeq
        rowCount = runtime.rowCount
    }
}

public enum SQLiteNowAttachResult: Equatable, Sendable {
    case connected(outcome: SQLiteNowAttachOutcome, status: SQLiteNowSyncStatus, restore: SQLiteNowRestoreSummary?)
    case retryLater(retryAfterSeconds: Int64)
    case unknown(String)

    internal init(_ runtime: SQLiteNowSyncRuntimeAttachResult) {
        switch runtime.kind {
        case "connected":
            guard let outcome = runtime.outcome, let status = runtime.status else {
                self = .unknown(runtime.kind)
                return
            }
            self = .connected(
                outcome: SQLiteNowAttachOutcome(outcome),
                status: SQLiteNowSyncStatus(status),
                restore: runtime.restore.map(SQLiteNowRestoreSummary.init)
            )
        case "retryLater":
            self = .retryLater(retryAfterSeconds: runtime.retryAfterSeconds)
        default:
            self = .unknown(runtime.kind)
        }
    }
}

public struct SQLiteNowPushReport: Equatable, Sendable {
    public let outcome: SQLiteNowPushOutcome
    public let status: SQLiteNowSyncStatus

    internal init(_ runtime: SQLiteNowSyncRuntimePushReport) {
        outcome = SQLiteNowPushOutcome(runtime.outcome)
        status = SQLiteNowSyncStatus(runtime.status)
    }
}

public struct SQLiteNowRemoteSyncReport: Equatable, Sendable {
    public let outcome: SQLiteNowRemoteSyncOutcome
    public let status: SQLiteNowSyncStatus
    public let restore: SQLiteNowRestoreSummary?

    internal init(_ runtime: SQLiteNowSyncRuntimeRemoteSyncReport) {
        outcome = SQLiteNowRemoteSyncOutcome(runtime.outcome)
        status = SQLiteNowSyncStatus(runtime.status)
        restore = runtime.restore.map(SQLiteNowRestoreSummary.init)
    }
}

public struct SQLiteNowSyncReport: Equatable, Sendable {
    public let pushOutcome: SQLiteNowPushOutcome
    public let remoteOutcome: SQLiteNowRemoteSyncOutcome
    public let status: SQLiteNowSyncStatus
    public let restore: SQLiteNowRestoreSummary?

    internal init(_ runtime: SQLiteNowSyncRuntimeSyncReport) {
        pushOutcome = SQLiteNowPushOutcome(runtime.pushOutcome)
        remoteOutcome = SQLiteNowRemoteSyncOutcome(runtime.remoteOutcome)
        status = SQLiteNowSyncStatus(runtime.status)
        restore = runtime.restore.map(SQLiteNowRestoreSummary.init)
    }
}

public struct SQLiteNowSyncThenDetachResult: Equatable, Sendable {
    public let lastSync: SQLiteNowSyncReport
    public let detach: SQLiteNowDetachOutcome
    public let syncRounds: Int32
    public let remainingPendingRowCount: Int64
    public let success: Bool

    internal init(_ runtime: SQLiteNowSyncRuntimeSyncThenDetachResult) {
        lastSync = SQLiteNowSyncReport(runtime.lastSync)
        detach = SQLiteNowDetachOutcome(runtime.detach.outcome)
        syncRounds = runtime.syncRounds
        remainingPendingRowCount = runtime.remainingPendingRowCount
        success = runtime.success
    }
}

public enum SQLiteNowSyncProgress: Equatable, Sendable {
    case idle
    case active(operation: SQLiteNowOversqliteOperation, phase: SQLiteNowOversqlitePhase)
    case unknown(String)

    internal init(_ runtime: SQLiteNowSyncRuntimeProgress) {
        switch runtime.kind {
        case "idle":
            self = .idle
        case "active":
            self = .active(
                operation: SQLiteNowOversqliteOperation(runtime.operation ?? ""),
                phase: SQLiteNowOversqlitePhase(runtime.phase ?? "")
            )
        default:
            self = .unknown(runtime.kind)
        }
    }
}

public struct SQLiteNowAutomaticDownloadConfig: Sendable {
    public var automaticDownloadIntervalMillis: Int64
    public var bundleChangeWatchMode: SQLiteNowBundleChangeWatchMode
    public var bundleChangeWatchReconnectMinMillis: Int64
    public var bundleChangeWatchReconnectMaxMillis: Int64

    public init(
        automaticDownloadIntervalMillis: Int64 = 60_000,
        bundleChangeWatchMode: SQLiteNowBundleChangeWatchMode = .off,
        bundleChangeWatchReconnectMinMillis: Int64 = 1_000,
        bundleChangeWatchReconnectMaxMillis: Int64 = 60_000
    ) {
        self.automaticDownloadIntervalMillis = automaticDownloadIntervalMillis
        self.bundleChangeWatchMode = bundleChangeWatchMode
        self.bundleChangeWatchReconnectMinMillis = bundleChangeWatchReconnectMinMillis
        self.bundleChangeWatchReconnectMaxMillis = bundleChangeWatchReconnectMaxMillis
    }

    internal var runtimeConfig: SQLiteNowSyncRuntimeAutomaticDownloadConfig {
        SQLiteNowSyncRuntimeAutomaticDownloadConfig(
            automaticDownloadIntervalMillis: automaticDownloadIntervalMillis,
            bundleChangeWatchMode: bundleChangeWatchMode.rawValue,
            bundleChangeWatchReconnectMinMillis: bundleChangeWatchReconnectMinMillis,
            bundleChangeWatchReconnectMaxMillis: bundleChangeWatchReconnectMaxMillis
        )
    }
}

public struct SQLiteNowSyncConflict: Equatable, Sendable {
    public let schema: String
    public let table: String
    public let keyJson: String
    public let localOp: String
    public let localPayloadJson: String?
    public let baseRowVersion: Int64
    public let serverRowVersion: Int64
    public let serverRowDeleted: Bool
    public let serverRowJson: String?

    public init(
        schema: String,
        table: String,
        keyJson: String,
        localOp: String,
        localPayloadJson: String?,
        baseRowVersion: Int64,
        serverRowVersion: Int64,
        serverRowDeleted: Bool,
        serverRowJson: String?
    ) {
        self.schema = schema
        self.table = table
        self.keyJson = keyJson
        self.localOp = localOp
        self.localPayloadJson = localPayloadJson
        self.baseRowVersion = baseRowVersion
        self.serverRowVersion = serverRowVersion
        self.serverRowDeleted = serverRowDeleted
        self.serverRowJson = serverRowJson
    }

    internal init(_ runtime: SQLiteNowSyncRuntimeConflict) {
        self.init(
            schema: runtime.schema,
            table: runtime.table,
            keyJson: runtime.keyJson,
            localOp: runtime.localOp,
            localPayloadJson: runtime.localPayloadJson,
            baseRowVersion: runtime.baseRowVersion,
            serverRowVersion: runtime.serverRowVersion,
            serverRowDeleted: runtime.serverRowDeleted,
            serverRowJson: runtime.serverRowJson
        )
    }

    internal var runtimeConflict: SQLiteNowSyncRuntimeConflict {
        SQLiteNowSyncRuntimeConflict(
            schema: schema,
            table: table,
            keyJson: keyJson,
            localOp: localOp,
            localPayloadJson: localPayloadJson,
            baseRowVersion: baseRowVersion,
            serverRowVersion: serverRowVersion,
            serverRowDeleted: serverRowDeleted,
            serverRowJson: serverRowJson
        )
    }
}

private enum SQLiteNowSyncSupportError: Error, Equatable {
    case unsupportedResolverResult(String)
}

public enum SQLiteNowSyncResolverResult: Equatable, Sendable {
    case deferConflict
    case acceptServer
    case keepLocal
    case keepMerged(payloadJson: String)

    internal init(_ runtime: SQLiteNowSyncRuntimeResolverResult) throws {
        switch runtime.kind {
        case "deferConflict": self = .deferConflict
        case "acceptServer": self = .acceptServer
        case "keepLocal": self = .keepLocal
        case "keepMerged": self = .keepMerged(payloadJson: runtime.mergedPayloadJson ?? "{}")
        default: throw SQLiteNowSyncSupportError.unsupportedResolverResult(runtime.kind)
        }
    }

    internal var runtimeResult: SQLiteNowSyncRuntimeResolverResult {
        switch self {
        case .deferConflict:
            return SQLiteNowSyncRuntimeResolverResult(kind: "deferConflict", mergedPayloadJson: nil)
        case .acceptServer:
            return SQLiteNowSyncRuntimeResolverResult(kind: "acceptServer", mergedPayloadJson: nil)
        case .keepLocal:
            return SQLiteNowSyncRuntimeResolverResult(kind: "keepLocal", mergedPayloadJson: nil)
        case let .keepMerged(payloadJson):
            return SQLiteNowSyncRuntimeResolverResult(kind: "keepMerged", mergedPayloadJson: payloadJson)
        }
    }
}

public struct SQLiteNowSyncResolver: @unchecked Sendable {
    internal let runtimeResolver: SQLiteNowSyncRuntimeResolver

    public init(resolve: @escaping @Sendable (SQLiteNowSyncConflict) -> SQLiteNowSyncResolverResult) {
        self.runtimeResolver = SQLiteNowSyncRuntimeResolver { conflict in
            resolve(SQLiteNowSyncConflict(conflict)).runtimeResult
        }
    }

    public static let serverWins = SQLiteNowSyncResolver { _ in .acceptServer }
    public static let clientWins = SQLiteNowSyncResolver { _ in .keepLocal }
    public static let deferConflict = SQLiteNowSyncResolver { _ in .deferConflict }

    func resolveForRuntimeSmoke(_ conflict: SQLiteNowSyncConflict) throws -> SQLiteNowSyncResolverResult {
        try SQLiteNowSyncResolverResult(runtimeResolver.resolveForRuntimeSmoke(conflict: conflict.runtimeConflict))
    }
}

public struct SQLiteNowPendingConflict: Equatable, Sendable {
    public let revision: Int64
    public let sourceId: String
    public let sourceBundleId: Int64
    public let canonicalRequestHash: String
    public let generationId: Int64
    public let generationFingerprint: String
    public let conflict: SQLiteNowSyncConflict

    internal init(_ runtime: SQLiteNowSyncRuntimePendingConflict) {
        revision = runtime.revision
        sourceId = runtime.sourceId
        sourceBundleId = runtime.sourceBundleId
        canonicalRequestHash = runtime.canonicalRequestHash
        generationId = runtime.generationId
        generationFingerprint = runtime.generationFingerprint
        conflict = SQLiteNowSyncConflict(runtime.conflict)
    }

    internal var runtimePendingConflict: SQLiteNowSyncRuntimePendingConflict {
        SQLiteNowSyncRuntimePendingConflict(
            revision: revision, sourceId: sourceId, sourceBundleId: sourceBundleId,
            canonicalRequestHash: canonicalRequestHash, generationId: generationId,
            generationFingerprint: generationFingerprint, conflict: conflict.runtimeConflict
        )
    }
}

public struct SQLiteNowSyncUpgradePlan: Equatable, Sendable {
    public let migrationId: String
    public let fromGenerationId: Int64
    public let toGenerationId: Int64
    public let fromLocalVersion: Int32
    public let toLocalVersion: Int32
    public let preservedLocalTables: Set<String>

    public init(
        migrationId: String,
        fromGenerationId: Int64,
        toGenerationId: Int64,
        fromLocalVersion: Int32,
        toLocalVersion: Int32,
        preservedLocalTables: Set<String> = []
    ) {
        self.migrationId = migrationId
        self.fromGenerationId = fromGenerationId
        self.toGenerationId = toGenerationId
        self.fromLocalVersion = fromLocalVersion
        self.toLocalVersion = toLocalVersion
        self.preservedLocalTables = preservedLocalTables
    }

    internal var runtimePlan: SQLiteNowSyncRuntimeUpgradePlan {
        SQLiteNowSyncRuntimeUpgradePlan(
            migrationId: migrationId,
            fromGenerationId: fromGenerationId,
            toGenerationId: toGenerationId,
            fromLocalVersion: fromLocalVersion,
            toLocalVersion: toLocalVersion,
            preservedLocalTables: preservedLocalTables.sorted()
        )
    }
}

public enum SQLiteNowSyncOpeningMode: String, Equatable, Sendable {
    case fresh
    case unchanged
    case localOnly = "local_only"
    case generationChange = "generation_change"
}

public enum SQLiteNowSyncOpeningOutcome: String, Equatable, Sendable {
    case ready
    case waitingForNetwork = "waiting_for_network"
    case needsResolution = "needs_resolution"
    case cancelled
    case upgradeBusy = "upgrade_busy"
    case upgradePreconditionFailed = "upgrade_precondition_failed"
    case upgradePlanMismatch = "upgrade_plan_mismatch"
    case oldGenerationUnavailable = "old_generation_unavailable"
    case targetGenerationUnavailable = "target_generation_unavailable"
    case localMigrationFailed = "local_migration_failed"
    case localContractMismatch = "local_contract_mismatch"
    case localPreservationFailed = "local_preservation_failed"
    case upgradeIntegrityFailed = "upgrade_integrity_failed"
    case replacementDiverged = "replacement_diverged"
}

public enum SQLiteNowSyncOpeningReplacementState: String, Equatable, Sendable {
    case none
    case reserved
    case confirmed
}

public enum SQLiteNowSyncOpeningNextAction: String, Equatable, Sendable {
    case advance
    case resolveConflict = "resolve_conflict"
    case resolveRejection = "resolve_rejection"
    case finishOldAttachment = "finish_old_attachment"
    case repairConfiguration = "repair_configuration"
    case restoreSupport = "restore_support"
    case inspectIntegrity = "inspect_integrity"
    case useReady = "use_ready"
    case close
}

public struct SQLiteNowSyncOpeningStatus: Equatable, Sendable {
    public let mode: SQLiteNowSyncOpeningMode?
    public let outcome: SQLiteNowSyncOpeningOutcome?
    public let phase: String?
    public let revision: Int64
    public let installedGenerationId: Int64?
    public let targetGenerationId: Int64
    public let installedLocalVersion: Int32?
    public let targetLocalVersion: Int32
    public let contractConfirmed: Bool?
    public let pendingDirtyCount: Int64?
    public let pendingFrozenCount: Int64?
    public let pendingConflictCount: Int64?
    public let drainComplete: Bool?
    public let replacementState: SQLiteNowSyncOpeningReplacementState?
    public let stagedRowCount: Int64?
    public let stagedByteCount: Int64?
    public let targetReadsAvailable: Bool?
    public let writesAvailable: Bool?
    public let nextAction: SQLiteNowSyncOpeningNextAction
    public let diagnosticCode: String?
    public let diagnosticMessage: String?

    internal init(_ runtime: SQLiteNowSyncRuntimeOpeningStatus) throws {
        let mode = runtime.mode.flatMap(SQLiteNowSyncOpeningMode.init(rawValue:))
        let replacementState = runtime.replacementState.flatMap(SQLiteNowSyncOpeningReplacementState.init(rawValue:))
        guard (runtime.mode == nil || mode != nil),
              (runtime.replacementState == nil || replacementState != nil),
              let nextAction = SQLiteNowSyncOpeningNextAction(rawValue: runtime.nextAction) else {
            throw SQLiteNowError.sync(message: "KMP returned an unknown coordinated-opening status value.")
        }
        if let rawOutcome = runtime.outcome,
           SQLiteNowSyncOpeningOutcome(rawValue: rawOutcome) == nil {
            throw SQLiteNowError.sync(message: "KMP returned an unknown coordinated-opening outcome.")
        }
        self.mode = mode
        self.outcome = runtime.outcome.flatMap(SQLiteNowSyncOpeningOutcome.init(rawValue:))
        self.phase = runtime.phase
        self.revision = runtime.revision
        self.installedGenerationId = runtime.installedGenerationId?.int64Value
        self.targetGenerationId = runtime.targetGenerationId
        self.installedLocalVersion = runtime.installedLocalVersion?.int32Value
        self.targetLocalVersion = runtime.targetLocalVersion
        self.contractConfirmed = runtime.contractConfirmed?.boolValue
        self.pendingDirtyCount = runtime.pendingDirtyCount?.int64Value
        self.pendingFrozenCount = runtime.pendingFrozenCount?.int64Value
        self.pendingConflictCount = runtime.pendingConflictCount?.int64Value
        self.drainComplete = runtime.drainComplete?.boolValue
        self.replacementState = replacementState
        self.stagedRowCount = runtime.stagedRowCount?.int64Value
        self.stagedByteCount = runtime.stagedByteCount?.int64Value
        self.targetReadsAvailable = runtime.targetReadsAvailable?.boolValue
        self.writesAvailable = runtime.writesAvailable?.boolValue
        self.nextAction = nextAction
        self.diagnosticCode = runtime.diagnosticCode
        self.diagnosticMessage = runtime.diagnosticMessage
    }
}

public struct SQLiteNowSyncPendingRejection: Equatable, Sendable {
    public let revision: Int64
    public let sourceId: String
    public let sourceBundleId: Int64
    public let canonicalRequestHash: String
    public let generationId: Int64
    public let rejectionCode: String

    internal init(_ runtime: SQLiteNowSyncRuntimePendingRejection) {
        revision = runtime.revision
        sourceId = runtime.sourceId
        sourceBundleId = runtime.sourceBundleId
        canonicalRequestHash = runtime.canonicalRequestHash
        generationId = runtime.generationId
        rejectionCode = runtime.rejectionCode
    }

    internal var runtimeRejection: SQLiteNowSyncRuntimePendingRejection {
        SQLiteNowSyncRuntimePendingRejection(
            revision: revision,
            sourceId: sourceId,
            sourceBundleId: sourceBundleId,
            canonicalRequestHash: canonicalRequestHash,
            generationId: generationId,
            rejectionCode: rejectionCode
        )
    }
}

public struct SQLiteNowSyncReady<Database>: @unchecked Sendable {
    public let database: Database
    public let client: SQLiteNowSyncClient
}

internal struct SQLiteNowSyncClientBinding: @unchecked Sendable {
    let baseURL: String
    let runtimeAuth: SQLiteNowSyncRuntimeAuth
    let config: SQLiteNowSyncConfig
    let runtimeResolver: SQLiteNowSyncRuntimeResolver?

    init(baseURL: URL, auth: SQLiteNowSyncAuth, config: SQLiteNowSyncConfig, resolver: SQLiteNowSyncResolver?) {
        self.baseURL = baseURL.absoluteString
        self.runtimeAuth = auth.runtimeAuth
        self.config = config
        self.runtimeResolver = resolver?.runtimeResolver
    }

    func matches(_ other: SQLiteNowSyncClientBinding) -> Bool {
        baseURL == other.baseURL &&
            runtimeAuth === other.runtimeAuth &&
            config == other.config &&
            ((runtimeResolver == nil && other.runtimeResolver == nil) ||
                (runtimeResolver != nil && runtimeResolver === other.runtimeResolver))
    }
}

public final class SQLiteNowSyncOpening<Database>: @unchecked Sendable {
    private let runtime: SQLiteNowSyncRuntimeOpening
    private let database: Database
    private let lease: SQLiteNowSyncClientLease
    private let binding: SQLiteNowSyncClientBinding?
    private let lock = NSLock()
    private var ready: SQLiteNowSyncReady<Database>?
    private var isClosed = false

    internal init(
        runtime: SQLiteNowSyncRuntimeOpening,
        database: Database,
        lease: SQLiteNowSyncClientLease,
        binding: SQLiteNowSyncClientBinding? = nil
    ) {
        self.runtime = runtime
        self.database = database
        self.lease = lease
        self.binding = binding
    }

    public func status() async throws -> SQLiteNowSyncOpeningStatus {
        try await mapRuntimeErrors {
            let result = try await awaitSyncOpeningOperation { self.runtime.startStatus(completion: $0) }
            guard let status = result.status else {
                throw SQLiteNowError.sync(message: "KMP omitted the coordinated-opening status result.")
            }
            try bindReadyIfAvailable()
            return try SQLiteNowSyncOpeningStatus(status)
        }
    }

    public func advance() async throws -> SQLiteNowSyncOpeningStatus {
        try await mapRuntimeErrors {
            let result = try await awaitSyncOpeningOperation { self.runtime.startAdvance(completion: $0) }
            guard let status = result.status else {
                throw SQLiteNowError.sync(message: "KMP omitted the coordinated-opening advance result.")
            }
            try bindReadyIfAvailable()
            return try SQLiteNowSyncOpeningStatus(status)
        }
    }

    public func pendingConflict() async throws -> SQLiteNowPendingConflict? {
        try await mapRuntimeErrors {
            try await awaitSyncOpeningOperation { self.runtime.startPendingConflict(completion: $0) }
                .conflict.map(SQLiteNowPendingConflict.init)
        }
    }

    public func resolveConflict(
        _ selected: SQLiteNowPendingConflict,
        result: SQLiteNowSyncResolverResult
    ) async throws {
        _ = try await mapRuntimeErrors {
            try await awaitSyncOpeningOperation {
                self.runtime.startResolveConflict(
                    selected: selected.runtimePendingConflict,
                    result: result.runtimeResult,
                    completion: $0
                )
            }
        }
    }

    public func retryConflict(_ selected: SQLiteNowPendingConflict) async throws {
        _ = try await mapRuntimeErrors {
            try await awaitSyncOpeningOperation {
                self.runtime.startRetryConflict(selected: selected.runtimePendingConflict, completion: $0)
            }
        }
    }

    public func pendingRejection() async throws -> SQLiteNowSyncPendingRejection? {
        try await mapRuntimeErrors {
            try await awaitSyncOpeningOperation { self.runtime.startPendingRejection(completion: $0) }
                .rejection.map(SQLiteNowSyncPendingRejection.init)
        }
    }

    public func discardRejected(_ selected: SQLiteNowSyncPendingRejection) async throws -> SQLiteNowSyncOpeningStatus {
        try await mapRuntimeErrors {
            let result = try await awaitSyncOpeningOperation {
                self.runtime.startDiscardRejected(selected: selected.runtimeRejection, completion: $0)
            }
            guard let status = result.status else {
                throw SQLiteNowError.sync(message: "KMP omitted the discard-rejected status result.")
            }
            return try SQLiteNowSyncOpeningStatus(status)
        }
    }

    public func retryRejected(_ selected: SQLiteNowSyncPendingRejection) async throws -> SQLiteNowSyncOpeningStatus {
        try await mapRuntimeErrors {
            let result = try await awaitSyncOpeningOperation {
                self.runtime.startRetryRejected(selected: selected.runtimeRejection, completion: $0)
            }
            guard let status = result.status else {
                throw SQLiteNowError.sync(message: "KMP omitted the retry-rejected status result.")
            }
            return try SQLiteNowSyncOpeningStatus(status)
        }
    }

    public func cancel() async throws {
        _ = try await mapRuntimeErrors {
            try await awaitSyncOpeningOperation { self.runtime.startCancel(completion: $0) }
        }
    }

    public func close() async throws {
        lock.withLock {
            isClosed = true
            ready = nil
        }
        lease.close()
        _ = try await mapRuntimeErrors {
            try await awaitSyncOpeningOperation { self.runtime.startClose(completion: $0) }
        }
    }

    public func readyOrNil() -> SQLiteNowSyncReady<Database>? {
        guard runtime.readyClientOrNull() != nil else {
            lock.lock()
            ready = nil
            lock.unlock()
            return nil
        }
        lock.lock()
        defer { lock.unlock() }
        return isClosed ? nil : ready
    }

    private func bindReadyIfAvailable() throws {
        lock.lock()
        if isClosed || ready != nil {
            lock.unlock()
            return
        }
        lock.unlock()
        guard let runtimeClient = runtime.readyClientOrNull() else { return }
        let client = try lease.bind(runtime: runtimeClient, binding: binding) { [runtime] in
            runtime.readyClientOrNull() === runtimeClient
        }
        guard runtime.readyClientOrNull() === runtimeClient else { return }
        lock.lock()
        if !isClosed && ready == nil {
            ready = SQLiteNowSyncReady(database: database, client: client)
            lock.unlock()
            return
        }
        lock.unlock()
    }
}

public func awaitSyncOpeningOperation(
    _ start: @escaping (SQLiteNowSyncRuntimeOpeningOperationCompletion) throws -> SQLiteNowSyncRuntimeCancelHandle
) async throws -> SQLiteNowSyncRuntimeOpeningOperationResult {
    try Task.checkCancellation()
    let state = ProductSyncOpeningOperationState()
    return try await withTaskCancellationHandler {
        try await withCheckedThrowingContinuation { continuation in
            let completion = ProductSyncOpeningOperationCompletion(continuation: continuation, state: state)
            do { state.install(try start(completion)) }
            catch { completion.onStartFailure(error) }
        }
    } onCancel: {
        state.cancel()
    }
}

private final class ProductSyncOpeningOperationState: @unchecked Sendable {
    private let lock = NSLock()
    private var handle: SQLiteNowSyncRuntimeCancelHandle?
    private var cancelled = false
    private var completed = false

    func install(_ handle: SQLiteNowSyncRuntimeCancelHandle) {
        lock.lock()
        if completed { lock.unlock(); return }
        if cancelled {
            lock.unlock()
            handle.cancel()
            return
        }
        self.handle = handle
        lock.unlock()
    }

    func cancel() {
        lock.lock()
        guard !completed, !cancelled else { lock.unlock(); return }
        cancelled = true
        let handle = handle
        self.handle = nil
        lock.unlock()
        handle?.cancel()
    }

    func finish() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard !completed else { return false }
        completed = true
        handle = nil
        return true
    }

}

private final class ProductSyncOpeningOperationCompletion: SQLiteNowSyncRuntimeOpeningOperationCompletion, @unchecked Sendable {
    private let state: ProductSyncOpeningOperationState
    private let continuation: CheckedContinuation<SQLiteNowSyncRuntimeOpeningOperationResult, Error>

    init(continuation: CheckedContinuation<SQLiteNowSyncRuntimeOpeningOperationResult, Error>, state: ProductSyncOpeningOperationState) {
        self.state = state
        self.continuation = continuation
    }

    func onStartFailure(_ error: Error) { finish(result: nil, error: error) }

    func onSuccess(result: SQLiteNowSyncRuntimeOpeningOperationResult) {
        finish(result: result, error: nil)
    }

    func onFailure(payload_ payload: SQLiteNowSyncRuntimeErrorPayload) {
        finish(
            result: nil,
            error: payload.category == "cancelled" ? CancellationError() : SQLiteNowError.from(payload)
        )
    }

    private func finish(result: SQLiteNowSyncRuntimeOpeningOperationResult?, error: Error?) {
        guard state.finish() else { return }
        if let result {
            continuation.resume(returning: result)
        } else {
            continuation.resume(throwing: error ?? SQLiteNowError.sync(message: "KMP omitted an opening result."))
        }
    }
}


public final class SQLiteNowAutomaticDownloads {
    private let handle: SQLiteNowSyncRuntimeCancelHandle
    private let observer: RuntimeSyncErrorObserver
    private let lock = NSLock()
    private var isCancelled = false

    fileprivate init(
        handle: SQLiteNowSyncRuntimeCancelHandle,
        observer: RuntimeSyncErrorObserver
    ) {
        self.handle = handle
        self.observer = observer
    }

    public func cancel() {
        lock.lock()
        if isCancelled {
            lock.unlock()
            return
        }
        isCancelled = true
        lock.unlock()
        handle.cancel()
        observer.cancel()
    }

    deinit {
        cancel()
    }
}

public final class SQLiteNowSyncClient {
    private let runtime: SQLiteNowSyncRuntimeClient
    private let lock = NSLock()
    private var isClosed = false
    private var onClose: (() -> Void)?

    internal init(
        runtime: SQLiteNowSyncRuntimeClient,
        onClose: (() -> Void)? = nil
    ) {
        self.runtime = runtime
        self.onClose = onClose
    }

    public func open() async throws {
        _ = try await mapRuntimeErrors {
            try await runtime.open()
        }
    }

    public func attach(userId: String) async throws -> SQLiteNowAttachResult {
        try await mapRuntimeErrors {
            let result = try await runtime.attach(userId: userId)
            return SQLiteNowAttachResult(result)
        }
    }

    public func pauseUploads() async throws {
        _ = try await mapRuntimeErrors { try await runtime.pauseUploads() }
    }

    public func resumeUploads() async throws {
        _ = try await mapRuntimeErrors { try await runtime.resumeUploads() }
    }

    public func pauseDownloads() async throws {
        _ = try await mapRuntimeErrors { try await runtime.pauseDownloads() }
    }

    public func resumeDownloads() async throws {
        _ = try await mapRuntimeErrors { try await runtime.resumeDownloads() }
    }

    public func sourceInfo() async throws -> SQLiteNowSourceInfo {
        try await mapRuntimeErrors {
            let info = try await runtime.sourceInfo()
            return SQLiteNowSourceInfo(info)
        }
    }

    public func syncStatus() async throws -> SQLiteNowSyncStatus {
        try await mapRuntimeErrors {
            let status = try await runtime.syncStatus()
            return SQLiteNowSyncStatus(status)
        }
    }

    public func detach() async throws -> SQLiteNowDetachOutcome {
        try await mapRuntimeErrors {
            let outcome = try await runtime.detach()
            return SQLiteNowDetachOutcome(outcome.outcome)
        }
    }

    public func pushPending() async throws -> SQLiteNowPushReport {
        try await mapRuntimeErrors {
            let report = try await runtime.pushPending()
            return SQLiteNowPushReport(report)
        }
    }

    public func pendingConflict() async throws -> SQLiteNowPendingConflict? {
        try await mapRuntimeErrors {
            try await runtime.pendingConflict().map(SQLiteNowPendingConflict.init)
        }
    }

    public func resolveConflict(
        _ selected: SQLiteNowPendingConflict,
        result: SQLiteNowSyncResolverResult
    ) async throws {
        _ = try await mapRuntimeErrors {
            try await runtime.resolveConflict(
                selected: selected.runtimePendingConflict,
                result: result.runtimeResult
            )
        }
    }

    public func retryConflict(_ selected: SQLiteNowPendingConflict) async throws {
        _ = try await mapRuntimeErrors {
            try await runtime.retryConflict(selected: selected.runtimePendingConflict)
        }
    }

    public func pullToStable() async throws -> SQLiteNowRemoteSyncReport {
        try await mapRuntimeErrors {
            let report = try await runtime.pullToStable()
            return SQLiteNowRemoteSyncReport(report)
        }
    }

    public func sync() async throws -> SQLiteNowSyncReport {
        try await mapRuntimeErrors {
            let report = try await runtime.sync()
            return SQLiteNowSyncReport(report)
        }
    }

    public func syncThenDetach() async throws -> SQLiteNowSyncThenDetachResult {
        try await mapRuntimeErrors {
            let result = try await runtime.syncThenDetach()
            return SQLiteNowSyncThenDetachResult(result)
        }
    }

    public func rebuild() async throws -> SQLiteNowRemoteSyncReport {
        try await mapRuntimeErrors {
            let report = try await runtime.rebuild()
            return SQLiteNowRemoteSyncReport(report)
        }
    }

    public func progress() -> AsyncThrowingStream<SQLiteNowSyncProgress, Error> {
        AsyncThrowingStream { continuation in
            let observer = RuntimeSyncProgressObserver(
                onProgress: { progress in
                    continuation.yield(SQLiteNowSyncProgress(progress))
                },
                onError: { payload in
                    continuation.finish(throwing: SQLiteNowError.from(payload))
                },
                onComplete: {
                    continuation.finish()
                }
            )
            let handle = RuntimeCancelHandleBox(runtime.observeProgress(observer: observer))
            continuation.onTermination = { _ in
                handle.cancel()
                observer.cancel()
            }
        }
    }

    public func startAutomaticDownloads(
        _ config: SQLiteNowAutomaticDownloadConfig = .init(),
        onError: @escaping @Sendable (SQLiteNowError) -> Void = { _ in }
    ) -> SQLiteNowAutomaticDownloads {
        let observer = RuntimeSyncErrorObserver(
            onError: { payload in
                onError(SQLiteNowError.from(payload))
            },
            onComplete: {}
        )
        let handle = runtime.startAutomaticDownloads(
            config: config.runtimeConfig,
            observer: observer
        )
        observer.retainUntilCancel()
        return SQLiteNowAutomaticDownloads(handle: handle, observer: observer)
    }

    func resolveForRuntimeSmoke(_ conflict: SQLiteNowSyncConflict) throws -> SQLiteNowSyncResolverResult {
        try SQLiteNowSyncResolverResult(runtime.resolveForRuntimeSmoke(conflict: conflict.runtimeConflict))
    }

    public func close() {
        lock.lock()
        if isClosed {
            lock.unlock()
            return
        }
        isClosed = true
        let onClose = onClose
        self.onClose = nil
        lock.unlock()
        runtime.close()
        onClose?()
    }
}

internal final class SQLiteNowSyncClientLease: @unchecked Sendable {
    private final class LeaseToken: @unchecked Sendable {
        private let lock = NSLock()
        private var cancelled = false

        func cancel() { lock.withLock { cancelled = true } }
        var isCancelled: Bool { lock.withLock { cancelled } }
    }

    private let lock = NSCondition()
    private var activeClient: SQLiteNowSyncClient?
    private var activeLeaseToken: LeaseToken?
    private var activeRuntime: SQLiteNowSyncRuntimeClient?
    private var activeBinding: SQLiteNowSyncClientBinding?
    private var activeReadiness: (() -> Bool)?
    private var reservedRuntime: SQLiteNowSyncRuntimeClient?
    private var reservedBinding: SQLiteNowSyncClientBinding?
    private var isClosed = false
    internal var beforeReadyPublicationForTest: (() -> Void)?
    internal var afterClientConstructionForTest: ((SQLiteNowSyncClient) -> Void)?

    func bind(
        runtime: SQLiteNowSyncRuntimeClient,
        binding: SQLiteNowSyncClientBinding? = nil,
        isReady: @escaping () -> Bool = { true }
    ) throws -> SQLiteNowSyncClient {
        guard isReady() else { throw SQLiteNowError.misuse(message: "upgrade_busy") }
        lock.lock()
        while let reservedRuntime {
            guard reservedRuntime === runtime && sameBinding(reservedBinding, binding) else {
                lock.unlock()
                throw SQLiteNowError.misuse(message: "upgrade_busy")
            }
            lock.wait()
        }
        guard !isClosed else {
            lock.unlock()
            throw SQLiteNowError.misuse(message: "upgrade_busy")
        }
        if let activeClient {
            guard activeRuntime === runtime && sameBinding(activeBinding, binding) else {
                lock.unlock()
                throw SQLiteNowError.misuse(message: "upgrade_busy")
            }
            lock.unlock()
            return activeClient
        }
        reservedRuntime = runtime
        reservedBinding = binding
        lock.unlock()

        let leaseToken = LeaseToken()
        let leasedClient = SQLiteNowSyncClient(runtime: runtime) { [weak self, leaseToken] in
            leaseToken.cancel()
            self?.release(leaseToken)
        }
        afterClientConstructionForTest?(leasedClient)

        beforeReadyPublicationForTest?()
        let stillReady = isReady()
        lock.lock()
        reservedRuntime = nil
        reservedBinding = nil
        lock.broadcast()
        guard !isClosed, !leaseToken.isCancelled, stillReady, activeClient == nil else {
            lock.unlock()
            throw SQLiteNowError.misuse(message: "upgrade_busy")
        }
        activeClient = leasedClient
        activeLeaseToken = leaseToken
        activeRuntime = runtime
        activeBinding = binding
        activeReadiness = isReady
        lock.unlock()
        return leasedClient
    }

    func readyClient(matching binding: SQLiteNowSyncClientBinding) throws -> SQLiteNowSyncClient? {
        lock.lock()
        let client = activeClient
        let readiness = activeReadiness
        lock.unlock()
        guard let client else { return nil }
        let ready = readiness?() == true
        lock.lock()
        defer { lock.unlock() }
        guard activeClient === client else { return nil }
        guard !isClosed, ready else {
            activeClient = nil
            activeLeaseToken = nil
            activeRuntime = nil
            activeBinding = nil
            activeReadiness = nil
            return nil
        }
        guard activeBinding?.matches(binding) == true else {
            throw SQLiteNowError.misuse(message: "makeSyncClient must use the exact URL, auth, resolver, and configuration supplied to openForSync.")
        }
        return client
    }

    func closeActiveClient() {
        lock.lock()
        let client = activeClient
        activeClient = nil
        activeLeaseToken = nil
        activeRuntime = nil
        activeBinding = nil
        activeReadiness = nil
        lock.unlock()
        client?.close()
    }

    func close() {
        lock.lock()
        isClosed = true
        let client = activeClient
        activeClient = nil
        activeLeaseToken = nil
        activeRuntime = nil
        activeBinding = nil
        activeReadiness = nil
        lock.unlock()
        client?.close()
    }

    private func sameBinding(
        _ left: SQLiteNowSyncClientBinding?,
        _ right: SQLiteNowSyncClientBinding?
    ) -> Bool {
        switch (left, right) {
        case (nil, nil): return true
        case let (left?, right?): return left.matches(right)
        default: return false
        }
    }

    private func release(_ token: LeaseToken) {
        lock.lock()
        if activeLeaseToken === token {
            activeClient = nil
            activeLeaseToken = nil
            activeRuntime = nil
            activeBinding = nil
            activeReadiness = nil
        }
        lock.unlock()
    }
}

private final class RuntimeSyncProgressObserver: SQLiteNowSyncRuntimeProgressObserver, @unchecked Sendable {
    private let onProgressBlock: @Sendable (SQLiteNowSyncRuntimeProgress) -> Void
    private let onErrorBlock: @Sendable (SQLiteNowSyncRuntimeErrorPayload) -> Void
    private let onCompleteBlock: @Sendable () -> Void
    private var retainedSelf: RuntimeSyncProgressObserver?

    init(
        onProgress: @escaping @Sendable (SQLiteNowSyncRuntimeProgress) -> Void,
        onError: @escaping @Sendable (SQLiteNowSyncRuntimeErrorPayload) -> Void,
        onComplete: @escaping @Sendable () -> Void
    ) {
        self.onProgressBlock = onProgress
        self.onErrorBlock = onError
        self.onCompleteBlock = onComplete
        self.retainedSelf = self
    }

    func onProgress(progress: SQLiteNowSyncRuntimeProgress) {
        onProgressBlock(progress)
    }

    func onError(payload_ payload: SQLiteNowSyncRuntimeErrorPayload) {
        onErrorBlock(payload)
        cancel()
    }

    func onComplete() {
        onCompleteBlock()
        cancel()
    }

    func cancel() {
        retainedSelf = nil
    }
}

private final class RuntimeCancelHandleBox: @unchecked Sendable {
    private let handle: SQLiteNowSyncRuntimeCancelHandle

    init(_ handle: SQLiteNowSyncRuntimeCancelHandle) {
        self.handle = handle
    }

    func cancel() {
        handle.cancel()
    }
}

private final class RuntimeSyncErrorObserver: SQLiteNowSyncRuntimeErrorObserver, @unchecked Sendable {
    private let onErrorBlock: @Sendable (SQLiteNowSyncRuntimeErrorPayload) -> Void
    private let onCompleteBlock: @Sendable () -> Void
    private var retainedSelf: RuntimeSyncErrorObserver?

    init(
        onError: @escaping @Sendable (SQLiteNowSyncRuntimeErrorPayload) -> Void,
        onComplete: @escaping @Sendable () -> Void
    ) {
        self.onErrorBlock = onError
        self.onCompleteBlock = onComplete
    }

    func retainUntilCancel() {
        retainedSelf = self
    }

    func onError(payload_ payload: SQLiteNowSyncRuntimeErrorPayload) {
        onErrorBlock(payload)
        cancel()
    }

    func onComplete() {
        onCompleteBlock()
        cancel()
    }

    func cancel() {
        retainedSelf = nil
    }
}
