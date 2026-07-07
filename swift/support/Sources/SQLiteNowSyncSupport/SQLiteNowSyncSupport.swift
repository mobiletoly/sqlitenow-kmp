@_exported import SQLiteNowSyncRuntime
import Foundation

public extension SQLiteNowError {
    static func from(_ payload: SQLiteNowSyncRuntimeErrorPayload) -> SQLiteNowError {
        switch payload.category {
        case "cancelled": return .cancelled(message: payload.message)
        case "state": return .misuse(message: payload.message)
        case "network", "auth", "conflict": return .sync(message: payload.message)
        default: return .unknown(message: payload.message)
        }
    }
}

public struct SQLiteNowSyncAuth: Sendable {
    internal let accessTokenProvider: @Sendable () -> String
    internal let refreshedAccessTokenProvider: (@Sendable () -> String?)?

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

    public var runtimeAuth: SQLiteNowSyncRuntimeAuth {
        SQLiteNowSyncRuntimeAuth(
            accessTokenProvider: accessTokenProvider,
            refreshedAccessTokenProvider: refreshedAccessTokenProvider
        )
    }
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
    public let rebuildRequired: Bool
    public let sourceRecoveryRequired: Bool
    public let sourceRecoveryReason: SQLiteNowSourceRecoveryReason?

    internal init(_ runtime: SQLiteNowSyncRuntimeSourceInfo) {
        currentSourceId = runtime.currentSourceId
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

    internal init(_ runtime: SQLiteNowSyncRuntimeProgress) {
        switch runtime.kind {
        case "active":
            self = .active(
                operation: SQLiteNowOversqliteOperation(runtime.operation ?? ""),
                phase: SQLiteNowOversqlitePhase(runtime.phase ?? "")
            )
        default:
            self = .idle
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

public enum SQLiteNowSyncResolverResult: Equatable, Sendable {
    case acceptServer
    case keepLocal
    case keepMerged(payloadJson: String)

    internal init(_ runtime: SQLiteNowSyncRuntimeResolverResult) {
        switch runtime.kind {
        case "keepLocal": self = .keepLocal
        case "keepMerged": self = .keepMerged(payloadJson: runtime.mergedPayloadJson ?? "{}")
        default: self = .acceptServer
        }
    }

    internal var runtimeResult: SQLiteNowSyncRuntimeResolverResult {
        switch self {
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

    func resolveForRuntimeSmoke(_ conflict: SQLiteNowSyncConflict) -> SQLiteNowSyncResolverResult {
        SQLiteNowSyncResolverResult(runtimeResolver.resolveForRuntimeSmoke(conflict: conflict.runtimeConflict))
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

    func resolveForRuntimeSmoke(_ conflict: SQLiteNowSyncConflict) -> SQLiteNowSyncResolverResult {
        SQLiteNowSyncResolverResult(runtime.resolveForRuntimeSmoke(conflict: conflict.runtimeConflict))
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
    private let lock = NSLock()
    private var activeClient: SQLiteNowSyncClient?

    public init() {}

    public func bind(runtime: SQLiteNowSyncRuntimeClient) throws -> SQLiteNowSyncClient {
        lock.lock()
        guard activeClient == nil else {
            lock.unlock()
            runtime.close()
            throw SQLiteNowError.misuse(message: "Only one active SQLiteNowSyncClient is allowed per database. Close the existing client before creating another one.")
        }
        lock.unlock()

        var leasedClient: SQLiteNowSyncClient!
        leasedClient = SQLiteNowSyncClient(runtime: runtime) { [weak self] in
            self?.release(leasedClient)
        }

        lock.lock()
        guard activeClient == nil else {
            lock.unlock()
            leasedClient.close()
            throw SQLiteNowError.misuse(message: "Only one active SQLiteNowSyncClient is allowed per database. Close the existing client before creating another one.")
        }
        activeClient = leasedClient
        lock.unlock()
        return leasedClient
    }

    public func closeActiveClient() {
        lock.lock()
        let client = activeClient
        activeClient = nil
        lock.unlock()
        client?.close()
    }

    private func release(_ client: SQLiteNowSyncClient) {
        lock.lock()
        if activeClient === client {
            activeClient = nil
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
