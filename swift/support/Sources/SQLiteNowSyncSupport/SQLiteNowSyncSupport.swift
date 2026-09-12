@_exported import SQLiteNowSyncRuntime
import Foundation

public extension SQLiteNowError {
    static func from(_ payload: SQLiteNowSyncRuntimeErrorPayload) -> SQLiteNowError {
        if payload.category == "cancelled" { return .cancelled(message: payload.message) }
        if let local = payload.localPlanningJson {
            guard let data = local.data(using: .utf8) else { return .protocol(message: "invalid local planning result") }
            let decoder = JSONDecoder()
            do {
                switch payload.code {
                case "PushDependencyGroupExceedsAdmittedLimitException":
                    return .pushDependencyGroupExceedsAdmittedLimit(try decoder.decode(SQLiteNowPushDependencyGroupLimit.self, from: data))
                case "FKReconciliationRequiredException":
                    let result = try decoder.decode(SQLiteNowFKReconciliationRequired.self, from: data)
                    guard payload.category == "state", !payload.remoteContract,
                          result.reason == "fk_reconciliation_required",
                          ["relationship_unknown", "dependency_unavailable", "historical_repair_required"].contains(result.detail),
                          validPlanningUnit(result.unit), validPlanningProgress(result.progress),
                          validPlanningSample(result.sample),
                          validPlanningFields(data, ["reason", "detail", "unit", "sample", "hasMore", "progress"])
                    else { return .protocol(message: "invalid local planning result") }
                    return .fkReconciliationRequired(result)
                case "ResolutionBaseUnavailableException":
                    let result = try decoder.decode(SQLiteNowResolutionBaseUnavailable.self, from: data)
                    guard result.reason == "resolution_base_unavailable", validPlanningUnit(result.unit),
                          validPlanningProgress(result.progress), validPlanningSample(result.sample)
                    else { return .protocol(message: "invalid local planning result") }
                    return .resolutionBaseUnavailable(result)
                case "SyncWorkLimitException":
                    let result = try decoder.decode(SQLiteNowSyncWorkLimit.self, from: data)
                    guard payload.category == "state", !payload.remoteContract,
                          result.reason == "sync_work_limit", validPlanningUnit(result.unit),
                          result.limit > 0, result.used >= 0, validPlanningProgress(result.progress),
                          validPlanningFields(data, ["reason", "unit", "limit", "used", "progress"])
                    else { return .protocol(message: "invalid local planning result") }
                    return .syncWorkLimit(result)
                case "SyncWorkspaceLimitException":
                    let result = try decoder.decode(SQLiteNowSyncWorkspaceLimit.self, from: data)
                    guard payload.category == "state", !payload.remoteContract,
                          result.reason == "sync_workspace_limit", validPlanningUnit(result.unit),
                          ["entries", "bytes", "atomic_apply_work"].contains(result.dimension),
                          result.limit > 0, result.used >= 0, result.reserved >= 0, validPlanningProgress(result.progress),
                          validPlanningFields(data, ["reason", "unit", "dimension", "limit", "used", "reserved", "resumable", "progress"])
                    else { return .protocol(message: "invalid local planning result") }
                    return .syncWorkspaceLimit(result)
                case "StalePushDependencyDiagnosticException":
                    return .stalePushDependencyDiagnostic(try decoder.decode(SQLiteNowStalePushDependencyDiagnostic.self, from: data))
                default: return .protocol(message: "unknown local planning result")
                }
            } catch { return .protocol(message: "invalid local planning result") }
        }
        if payload.code == "SyncIncompleteException" {
            return .syncIncomplete(
                reason: SQLiteNowSyncIncompleteReason(payload.incompleteReason ?? ""),
                dirtyCount: payload.dirtyCount,
                outboundCount: payload.outboundCount,
                confirmedBundleCount: payload.confirmedBundleCount,
                pushAttempts: payload.pushAttempts
            )
        }
        if payload.code == "ConflictNeedsResolutionException" {
            return .conflictNeedsResolution(message: payload.message)
        }
        if payload.code == "StaleConflictResolutionException" {
            return .staleConflictResolution(message: payload.message)
        }
        if payload.remoteContract {
            let generationId: Int64?
            if let rawGenerationId = payload.generationId {
                guard let parsedGenerationId = Int64(rawGenerationId),
                      (1...Int64(Int32.max)).contains(parsedGenerationId),
                      String(parsedGenerationId) == rawGenerationId else {
                    return .protocol(message: "remote contract error contained an invalid generation ID")
                }
                generationId = parsedGenerationId
            } else {
                generationId = nil
            }
            return .remote(
                code: payload.code,
                httpStatus: Int(payload.httpStatus),
                operation: payload.operation,
                remediation: payload.remediation,
                retryable: payload.retryable,
                generationId: generationId,
                message: payload.message
            )
        }
        switch payload.category {
        case "cancelled": return .cancelled(message: payload.message)
        case "state": return .misuse(message: payload.message)
        case "network", "auth", "conflict": return .sync(message: payload.message)
        case "protocol": return .protocol(message: payload.message)
        default: return .unknown(message: payload.message)
        }
    }
}

private func validPlanningUnit(_ value: String) -> Bool {
    value.utf8.count == 64 && value.utf8.allSatisfy { (48...57).contains($0) || (97...102).contains($0) }
}

private func validPlanningProgress(_ value: SQLiteNowSyncPendingProgress) -> Bool {
    value.dirtyRows >= 0 && value.outboundRows >= 0 && value.committedBundles >= 0
}

private func validPlanningSample(_ sample: [SQLiteNowPushDependencyRecordRef]) -> Bool {
    guard sample.count <= 16, sample.allSatisfy({ !$0.schema.isEmpty && !$0.table.isEmpty && !$0.key.isEmpty }) else { return false }
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
    guard let encoded = try? encoder.encode(sample) else { return false }
    return encoded.count <= 4096
}

private func validPlanningFields(_ data: Data, _ fields: Set<String>) -> Bool {
    guard let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any], Set(object.keys) == fields,
          let progress = object["progress"] as? [String: Any],
          Set(progress.keys) == ["dirtyRows", "outboundRows", "committedBundles"] else { return false }
    let counters = fields.intersection(["limit", "used", "reserved"])
    for value in counters.compactMap({ object[$0] }) + Array(progress.values) {
        guard let number = value as? NSNumber,
              ["i", "q", "Q"].contains(String(cString: number.objCType)) else { return false }
    }
    return true
}

public struct SQLiteNowSyncPendingProgress: Codable, Equatable, Sendable {
    public let dirtyRows: Int64
    public let outboundRows: Int64
    public let committedBundles: Int64
}

public struct SQLiteNowPushDependencyRecordRef: Codable, Equatable, Sendable {
    public let schema: String
    public let table: String
    public let key: [String: String]
}

public struct SQLiteNowPushDependencyDiagnosticPage: Equatable, Sendable {
    public let diagnosticId: String
    public let identities: [SQLiteNowPushDependencyRecordRef]
    public let nextCursor: String?
}

public struct SQLiteNowPushDependencyGroupLimit: Codable, Equatable, Sendable {
    public let reason: String
    public let dimension: String
    public let limit: Int64
    public let actual: Int64
    public let componentRows: Int64
    public let componentDecodedBytes: Int64
    public let diagnosticId: String
    public let sample: [SQLiteNowPushDependencyRecordRef]
    public let hasMore: Bool
    public let progress: SQLiteNowSyncPendingProgress
}

public struct SQLiteNowFKReconciliationRequired: Codable, Equatable, Sendable {
    public let reason: String
    public let detail: String
    public let unit: String
    public let sample: [SQLiteNowPushDependencyRecordRef]
    public let hasMore: Bool
    public let progress: SQLiteNowSyncPendingProgress
}

public struct SQLiteNowResolutionBaseUnavailable: Codable, Equatable, Sendable {
    public let reason: String
    public let unit: String
    public let sample: [SQLiteNowPushDependencyRecordRef]
    public let hasMore: Bool
    public let progress: SQLiteNowSyncPendingProgress
}

public struct SQLiteNowSyncWorkLimit: Codable, Equatable, Sendable {
    public let reason: String
    public let unit: String
    public let limit: Int64
    public let used: Int64
    public let progress: SQLiteNowSyncPendingProgress
}

public struct SQLiteNowSyncWorkspaceLimit: Codable, Equatable, Sendable {
    public let reason: String
    public let unit: String
    public let dimension: String
    public let limit: Int64
    public let used: Int64
    public let reserved: Int64
    public let resumable: Bool
    public let progress: SQLiteNowSyncPendingProgress
}

public struct SQLiteNowStalePushDependencyDiagnostic: Codable, Equatable, Sendable {
    public let reason: String
    public let diagnosticId: String
}

public struct SQLiteNowSyncAuth: @unchecked Sendable {
    internal let accessTokenProvider: @Sendable () -> String
    internal let refreshedAccessTokenProvider: (@Sendable () -> String?)?
    public let runtimeAuth: SQLiteNowSyncRuntimeAuth

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

    public func runtimeConfig(
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
    public let lastSync: SQLiteNowSyncReport?
    public let detach: SQLiteNowDetachOutcome
    public let syncRounds: Int32
    public let remainingPendingRowCount: Int64
    public let blockedReason: SQLiteNowSyncThenDetachBlockedReason?
    public let success: Bool

    internal init(_ runtime: SQLiteNowSyncRuntimeSyncThenDetachResult) {
        lastSync = runtime.lastSync.map(SQLiteNowSyncReport.init)
        detach = SQLiteNowDetachOutcome(runtime.detach.outcome)
        syncRounds = runtime.syncRounds
        remainingPendingRowCount = runtime.remainingPendingRowCount
        blockedReason = runtime.blockedReason.map(SQLiteNowSyncThenDetachBlockedReason.init)
        success = runtime.success
    }
}

public enum SQLiteNowSyncIncompleteReason: Equatable, Sendable {
    case pushAttemptLimit
    case noProgress
    case unknown(String)

    internal init(_ raw: String) {
        switch raw {
        case "PUSH_ATTEMPT_LIMIT": self = .pushAttemptLimit
        case "NO_PROGRESS": self = .noProgress
        default: self = .unknown(raw)
        }
    }
}

public enum SQLiteNowSyncThenDetachBlockedReason: Equatable, Sendable {
    case pushAttemptLimit
    case noProgress
    case syncRoundLimit
    case unknown(String)

    internal init(_ raw: String) {
        switch raw {
        case "PUSH_ATTEMPT_LIMIT": self = .pushAttemptLimit
        case "NO_PROGRESS": self = .noProgress
        case "SYNC_ROUND_LIMIT": self = .syncRoundLimit
        default: self = .unknown(raw)
        }
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
    public let runtimeResolver: SQLiteNowSyncRuntimeResolver

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
    public let baselineRevision: Int64
    public let sourceId: String
    public let sourceBundleId: Int64
    public let canonicalRequestHash: String
    public let generationId: Int64
    public let generationFingerprint: String
    public let conflict: SQLiteNowSyncConflict

    internal init(_ runtime: SQLiteNowSyncRuntimePendingConflict) {
        revision = runtime.revision
        baselineRevision = runtime.baselineRevision
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
            generationFingerprint: generationFingerprint, conflict: conflict.runtimeConflict,
            baselineRevision: baselineRevision
        )
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

    public init(
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
            let operation = try await awaitSyncOpeningOperation { completion in
                self.runtime.startPushPending(completion: completion)
            }
            guard let report = operation.pushReport else {
                throw SQLiteNowError.sync(message: "KMP omitted the push report.")
            }
            return SQLiteNowPushReport(report)
        }
    }

    public func readPushDependencyDiagnostic(
        _ diagnosticId: String, cursor: String? = nil, pageSize: Int = 64
    ) async throws -> SQLiteNowPushDependencyDiagnosticPage {
        guard (1...128).contains(pageSize) else { throw SQLiteNowError.misuse(message: "diagnostic page size must be between 1 and 128") }
        return try await mapRuntimeErrors {
            let page = try await runtime.readPushDependencyDiagnostic(diagnosticId: diagnosticId, cursor: cursor, pageSize: Int32(pageSize))
            guard let data = page.identitiesJson.data(using: .utf8) else { throw SQLiteNowError.protocol(message: "invalid local diagnostic page") }
            let identities = try JSONDecoder().decode([SQLiteNowPushDependencyRecordRef].self, from: data)
            return SQLiteNowPushDependencyDiagnosticPage(diagnosticId: page.diagnosticId, identities: identities, nextCursor: page.nextCursor)
        }
    }

    public func releasePushDependencyDiagnostic(_ diagnosticId: String) async throws {
        _ = try await mapRuntimeErrors { try await runtime.releasePushDependencyDiagnostic(diagnosticId: diagnosticId) }
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
            try await awaitSyncOpeningOperation { completion in
                self.runtime.startResolveConflict(
                    selected: selected.runtimePendingConflict,
                    result: result.runtimeResult,
                    completion: completion
                )
            }
        }
    }

    public func retryConflict(_ selected: SQLiteNowPendingConflict) async throws {
        _ = try await mapRuntimeErrors {
            try await awaitSyncOpeningOperation { completion in
                self.runtime.startRetryConflict(selected: selected.runtimePendingConflict, completion: completion)
            }
        }
    }

    public func pullToStable() async throws -> SQLiteNowRemoteSyncReport {
        try await mapRuntimeErrors {
            let operation = try await awaitSyncOpeningOperation { completion in
                self.runtime.startPullToStable(completion: completion)
            }
            guard let report = operation.remoteReport else {
                throw SQLiteNowError.sync(message: "KMP omitted the pull report.")
            }
            return SQLiteNowRemoteSyncReport(report)
        }
    }

    public func sync() async throws -> SQLiteNowSyncReport {
        try await mapRuntimeErrors {
            let operation = try await awaitSyncOpeningOperation { completion in
                self.runtime.startSync(completion: completion)
            }
            guard let report = operation.syncReport else {
                throw SQLiteNowError.sync(message: "KMP omitted the sync report.")
            }
            return SQLiteNowSyncReport(report)
        }
    }

    public func syncThenDetach() async throws -> SQLiteNowSyncThenDetachResult {
        try await mapRuntimeErrors {
            let operation = try await awaitSyncOpeningOperation { completion in
                self.runtime.startSyncThenDetach(completion: completion)
            }
            guard let result = operation.syncThenDetachResult else {
                throw SQLiteNowError.sync(message: "KMP omitted the sync then detach result.")
            }
            return SQLiteNowSyncThenDetachResult(result)
        }
    }

    public func rebuild() async throws -> SQLiteNowRemoteSyncReport {
        try await mapRuntimeErrors {
            let operation = try await awaitSyncOpeningOperation { completion in
                self.runtime.startRebuild(completion: completion)
            }
            guard let report = operation.remoteReport else {
                throw SQLiteNowError.sync(message: "KMP omitted the rebuild report.")
            }
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

public final class SQLiteNowSyncClientLease: @unchecked Sendable {
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

    public init() {}

    public func bind(
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

    public func readyClient(matching binding: SQLiteNowSyncClientBinding) throws -> SQLiteNowSyncClient? {
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

    public func closeActiveClient() {
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

    public func close() {
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
