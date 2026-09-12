@preconcurrency import SQLiteNowSyncRuntime
import Foundation

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

    public var runtimePlan: SQLiteNowSyncRuntimeUpgradePlan {
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

    public init(_ runtime: SQLiteNowSyncRuntimeOpeningStatus) throws {
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

    public init(_ runtime: SQLiteNowSyncRuntimePendingRejection) {
        revision = runtime.revision
        sourceId = runtime.sourceId
        sourceBundleId = runtime.sourceBundleId
        canonicalRequestHash = runtime.canonicalRequestHash
        generationId = runtime.generationId
        rejectionCode = runtime.rejectionCode
    }

    public var runtimeRejection: SQLiteNowSyncRuntimePendingRejection {
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

    public init(database: Database, client: SQLiteNowSyncClient) {
        self.database = database
        self.client = client
    }
}

public struct SQLiteNowSyncClientBinding: @unchecked Sendable {
    fileprivate let baseURL: String
    fileprivate let runtimeAuth: SQLiteNowSyncRuntimeAuth
    fileprivate let config: SQLiteNowSyncConfig
    fileprivate let runtimeResolver: SQLiteNowSyncRuntimeResolver?

    public init(
        baseURL: URL,
        auth: SQLiteNowSyncAuth,
        config: SQLiteNowSyncConfig,
        resolver: SQLiteNowSyncResolver?
    ) {
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

    public init(
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
            let result = try await awaitSyncOpeningOperation { completion in
                self.runtime.startStatus(completion: completion)
            }
            guard let status = result.status else {
                throw SQLiteNowError.sync(message: "KMP omitted the coordinated-opening status result.")
            }
            try bindReadyIfAvailable()
            return try SQLiteNowSyncOpeningStatus(status)
        }
    }

    public func advance() async throws -> SQLiteNowSyncOpeningStatus {
        try await mapRuntimeErrors {
            let result = try await awaitSyncOpeningOperation { completion in
                self.runtime.startAdvance(completion: completion)
            }
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
            lock.withLock { ready = nil }
            return nil
        }
        return lock.withLock { isClosed ? nil : ready }
    }

    private func bindReadyIfAvailable() throws {
        if lock.withLock({ isClosed || ready != nil }) { return }
        guard let runtimeClient = runtime.readyClientOrNull() else { return }
        let client = try lease.bind(runtime: runtimeClient, binding: binding) { [runtime] in
            runtime.readyClientOrNull() === runtimeClient
        }
        guard runtime.readyClientOrNull() === runtimeClient else { return }
        _ = lock.withLock {
            if !isClosed && ready == nil {
                ready = SQLiteNowSyncReady(database: database, client: client)
                return true
            }
            return false
        }
    }
}

public func awaitSyncOpeningOperation(
    _ start: @escaping (SQLiteNowSyncRuntimeOpeningOperationCompletion) throws -> SQLiteNowSyncRuntimeCancelHandle
) async throws -> SQLiteNowSyncRuntimeOpeningOperationResult {
    try Task.checkCancellation()
    let state = SyncOpeningOperationState()
    return try await withTaskCancellationHandler {
        try await withCheckedThrowingContinuation { continuation in
            let completion = SyncOpeningOperationCompletion(continuation: continuation, state: state)
            do { state.install(try start(completion)) }
            catch { completion.onStartFailure(error) }
        }
    } onCancel: {
        state.cancel()
    }
}

private final class SyncOpeningOperationState: @unchecked Sendable {
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

private final class SyncOpeningOperationCompletion: SQLiteNowSyncRuntimeOpeningOperationCompletion, @unchecked Sendable {
    private let state: SyncOpeningOperationState
    private let continuation: CheckedContinuation<SQLiteNowSyncRuntimeOpeningOperationResult, Error>

    init(continuation: CheckedContinuation<SQLiteNowSyncRuntimeOpeningOperationResult, Error>, state: SyncOpeningOperationState) {
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
