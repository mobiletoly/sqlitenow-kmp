/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.gradle.swift

import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement


internal class SwiftOverlayEmitter(
    private val databaseName: String,
    private val config: SqliteNowSwiftExportConfig,
    private val context: GeneratorContext,
    private val model: SwiftKmpExportModel,
    private val swiftOversqliteEnabled: Boolean,
) {

    fun emit(): String =
        SwiftWriter().apply {
            line("@preconcurrency import ${config.frameworkModuleName}")
            line("import Foundation")
            line()
            emitSwiftErrorType()
            emitSwiftDataHelpers()
            emitSwiftAdapters()
            emitSwiftSelectQuery()
            emitSwiftExecuteReturningQuery()
            emitSwiftSyncTypes()
            emitSwiftDatabase()
            emitSwiftResultModels()
            emitSwiftParams()
            emitSwiftNamespaces()
            emitSwiftTransactions()
            emitSwiftStreams()
        }.toString()

    private fun SwiftWriter.emitSwiftErrorType() {
        line("public enum SQLiteNowError: Error, CustomStringConvertible {")
        indent {
            line("case sqlite(message: String, underlying: Error?)")
            line("case migration(message: String, underlying: Error?)")
            line("case misuse(message: String, underlying: Error?)")
            line("case cancelled(underlying: Error?)")
            line("case adapter(message: String, underlying: Error?)")
            line("case observation(message: String, underlying: Error?)")
            if (swiftOversqliteEnabled) {
                line("case sync(message: String, underlying: Error?)")
                line("case `protocol`(message: String, underlying: Error?)")
            }
            line("case unknown(message: String, underlying: Error?)")
            line()
            line("public var description: String {")
            indent {
                line("switch self {")
                line("case let .sqlite(message, _): return message")
                line("case let .migration(message, _): return message")
                line("case let .misuse(message, _): return message")
                line("case .cancelled: return \"Operation cancelled\"")
                line("case let .adapter(message, _): return message")
                line("case let .observation(message, _): return message")
                if (swiftOversqliteEnabled) {
                    line("case let .sync(message, _): return message")
                    line("case let .protocol(message, _): return message")
                }
                line("case let .unknown(message, _): return message")
                line("}")
            }
            line("}")
            line()
            line("fileprivate static func from(_ error: Error) -> SQLiteNowError {")
            indent {
                line("if let sqliteNowError = error as? SQLiteNowError {")
                indent {
                    line("return sqliteNowError")
                }
                line("}")
                line("if let overlayError = error as? ${databaseName}SwiftOverlayException {")
                indent {
                    line("return from(overlayError.payload, underlying: error)")
                }
                line("}")
                line("if let overlayError = (error as NSError).userInfo[\"K\" + \"otlinException\"] as? ${databaseName}SwiftOverlayException {")
                indent {
                    line("return from(overlayError.payload, underlying: error)")
                }
                line("}")
                line("if error is CancellationError {")
                indent {
                    line("return .cancelled(underlying: error)")
                }
                line("}")
                line("return .unknown(message: String(describing: error), underlying: error)")
            }
            line("}")
            line()
            line("fileprivate static func from(_ payload: ${databaseName}SwiftOverlayErrorPayload, underlying: Error?) -> SQLiteNowError {")
            indent {
                line("switch payload.category {")
                line("case \"sqlite\": return .sqlite(message: payload.message, underlying: underlying)")
                line("case \"migration\": return .migration(message: payload.message, underlying: underlying)")
                line("case \"misuse\", \"state\": return .misuse(message: payload.message, underlying: underlying)")
                line("case \"cancelled\": return .cancelled(underlying: underlying)")
                line("case \"adapter\": return .adapter(message: payload.message, underlying: underlying)")
                if (swiftOversqliteEnabled) {
                    line("case \"network\", \"auth\", \"conflict\": return .sync(message: payload.message, underlying: underlying)")
                    line("case \"protocol\": return .protocol(message: payload.message, underlying: underlying)")
                }
                line("default: return .unknown(message: payload.message, underlying: underlying)")
                line("}")
            }
            line("}")
        }
        line("}")
        line()
        line("private func mapSQLiteNowErrors<T>(_ operation: () async throws -> T) async throws -> T {")
        indent {
            line("do {")
            indent {
                line("return try await operation()")
            }
            line("} catch {")
            indent {
                line("throw SQLiteNowError.from(error)")
            }
            line("}")
        }
        line("}")
        line()
        line("private func mapAdapterErrors<T>(_ operation: () throws -> T) throws -> T {")
        indent {
            line("do {")
            indent {
                line("return try operation()")
            }
            line("} catch {")
            indent {
                line("throw SQLiteNowError.adapter(message: String(describing: error), underlying: error)")
            }
            line("}")
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitSwiftDataHelpers() {
        line("private func sqliteNowData(from value: KotlinByteArray) -> Data {")
        indent {
            line("var bytes = [UInt8]()")
            line("bytes.reserveCapacity(Int(value.size))")
            line("for index in 0..<Int(value.size) {")
            indent {
                line("bytes.append(UInt8(bitPattern: value.get(index: Int32(index))))")
            }
            line("}")
            line("return Data(bytes)")
        }
        line("}")
        line()
        line("private func sqliteNowByteArray(from data: Data) -> KotlinByteArray {")
        indent {
            line("let bytes = KotlinByteArray(size: Int32(data.count))")
            line("for index in 0..<data.count {")
            indent {
                line("bytes.set(index: Int32(index), value: Int8(bitPattern: data[index]))")
            }
            line("}")
            line("return bytes")
        }
        line("}")
        line()
        line("private func sqliteNowInt64(from value: KotlinLong?) -> Int64? {")
        indent {
            line("value?.int64Value")
        }
        line("}")
        line()
        line("private func sqliteNowKotlinLong(from value: Int64?) -> KotlinLong? {")
        indent {
            line("value.map { KotlinLong(longLong: \$0) }")
        }
        line("}")
        line()
        line("private func sqliteNowDouble(from value: KotlinDouble?) -> Double? {")
        indent {
            line("value?.doubleValue")
        }
        line("}")
        line()
        line("private func sqliteNowKotlinDouble(from value: Double?) -> KotlinDouble? {")
        indent {
            line("value.map { KotlinDouble(double: \$0) }")
        }
        line("}")
        line()
        line("private func sqliteNowBool(from value: KotlinBoolean?) -> Bool? {")
        indent {
            line("value?.boolValue")
        }
        line("}")
        line()
        line("private func sqliteNowKotlinBoolean(from value: Bool?) -> KotlinBoolean? {")
        indent {
            line("value.map { KotlinBoolean(bool: \$0) }")
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitSwiftAdapters() {
        val adapters = adapterDescriptors()
        line("public struct ${databaseName}Adapters {")
        indent {
            if (adapters.isEmpty()) {
                line("public init() {}")
            } else {
                adapters.forEach { adapter ->
                    line("public let ${adapter.name}: (${adapter.inputSwiftType}) throws -> ${adapter.outputSwiftType}")
                }
                line()
                line("public init(")
                indent {
                    adapters.forEachIndexed { index, adapter ->
                        val suffix = if (index == adapters.lastIndex) "" else ","
                        line("${adapter.name}: @escaping (${adapter.inputSwiftType}) throws -> ${adapter.outputSwiftType}$suffix")
                    }
                }
                line(") {")
                indent {
                    adapters.forEach { adapter ->
                        line("self.${adapter.name} = ${adapter.name}")
                    }
                }
                line("}")
            }
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitSwiftSelectQuery() {
        line("public final class SQLiteNowSelectQuery<Row: Sendable> {")
        indent {
            line("private let listBlock: () async throws -> [Row]")
            line("private let oneBlock: () async throws -> Row")
            line("private let oneOrNullBlock: () async throws -> Row?")
            line("private let streamBlock: () -> AsyncThrowingStream<[Row], Error>")
            line()
            line("fileprivate init(")
            indent {
                line("list: @escaping () async throws -> [Row],")
                line("one: @escaping () async throws -> Row,")
                line("oneOrNull: @escaping () async throws -> Row?,")
                line("stream: @escaping () -> AsyncThrowingStream<[Row], Error>")
            }
            line(") {")
            indent {
                line("self.listBlock = list")
                line("self.oneBlock = one")
                line("self.oneOrNullBlock = oneOrNull")
                line("self.streamBlock = stream")
            }
            line("}")
            line()
            line("public func list() async throws -> [Row] {")
            indent {
                line("try await listBlock()")
            }
            line("}")
            line()
            line("public func one() async throws -> Row {")
            indent {
                line("try await oneBlock()")
            }
            line("}")
            line()
            line("public func oneOrNull() async throws -> Row? {")
            indent {
                line("try await oneOrNullBlock()")
            }
            line("}")
            line()
            line("public func stream() -> AsyncThrowingStream<[Row], Error> {")
            indent {
                line("streamBlock()")
            }
            line("}")
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitSwiftExecuteReturningQuery() {
        line("public final class SQLiteNowExecuteReturningQuery<Row: Sendable> {")
        indent {
            line("private let listBlock: () async throws -> [Row]")
            line("private let oneBlock: () async throws -> Row")
            line("private let oneOrNullBlock: () async throws -> Row?")
            line()
            line("fileprivate init(")
            indent {
                line("list: @escaping () async throws -> [Row],")
                line("one: @escaping () async throws -> Row,")
                line("oneOrNull: @escaping () async throws -> Row?")
            }
            line(") {")
            indent {
                line("self.listBlock = list")
                line("self.oneBlock = one")
                line("self.oneOrNullBlock = oneOrNull")
            }
            line("}")
            line()
            line("public func list() async throws -> [Row] {")
            indent {
                line("try await listBlock()")
            }
            line("}")
            line()
            line("public func one() async throws -> Row {")
            indent {
                line("try await oneBlock()")
            }
            line("}")
            line()
            line("public func oneOrNull() async throws -> Row? {")
            indent {
                line("try await oneOrNullBlock()")
            }
            line("}")
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitSwiftSyncTypes() {
        if (!swiftOversqliteEnabled) return

        line("public struct SQLiteNowSyncAuth: Sendable {")
        indent {
            line("fileprivate let accessTokenProvider: @Sendable () -> String")
            line("fileprivate let refreshedAccessTokenProvider: (@Sendable () -> String?)?")
            line()
            line("public static func bearer(token: String) -> SQLiteNowSyncAuth {")
            indent {
                line("SQLiteNowSyncAuth(")
                indent {
                    line("accessTokenProvider: { token },")
                    line("refreshedAccessTokenProvider: nil")
                }
                line(")")
            }
            line("}")
            line()
            line("public static func bearer(")
            indent {
                line("accessToken: @escaping @Sendable () -> String,")
                line("refreshedAccessToken: (@Sendable () -> String?)? = nil")
            }
            line(") -> SQLiteNowSyncAuth {")
            indent {
                line("SQLiteNowSyncAuth(")
                indent {
                    line("accessTokenProvider: accessToken,")
                    line("refreshedAccessTokenProvider: refreshedAccessToken")
                }
                line(")")
            }
            line("}")
        }
        line("}")
        line()
        line("public struct SQLiteNowTransientRetryPolicy: Equatable, Sendable {")
        indent {
            line("public var maxAttempts: Int")
            line("public var initialBackoffMillis: Int64")
            line("public var maxBackoffMillis: Int64")
            line("public var jitterRatio: Double")
            line()
            line("public init(")
            indent {
                line("maxAttempts: Int = 3,")
                line("initialBackoffMillis: Int64 = 150,")
                line("maxBackoffMillis: Int64 = 1_500,")
                line("jitterRatio: Double = 0.2")
            }
            line(") {")
            indent {
                line("self.maxAttempts = maxAttempts")
                line("self.initialBackoffMillis = initialBackoffMillis")
                line("self.maxBackoffMillis = maxBackoffMillis")
                line("self.jitterRatio = jitterRatio")
            }
            line("}")
        }
        line("}")
        line()
        line("public struct SQLiteNowSnapshotCapacityRetryPolicy: Equatable, Sendable {")
        indent {
            line("public var enabled: Bool")
            line("public var maxWaitMillis: Int64")
            line("public var fallbackDelayMillis: Int64")
            line("public var jitterRatio: Double")
            line()
            line("public init(")
            indent {
                line("enabled: Bool = true,")
                line("maxWaitMillis: Int64 = 30_000,")
                line("fallbackDelayMillis: Int64 = 1_000,")
                line("jitterRatio: Double = 1.0")
            }
            line(") {")
            indent {
                line("self.enabled = enabled")
                line("self.maxWaitMillis = maxWaitMillis")
                line("self.fallbackDelayMillis = fallbackDelayMillis")
                line("self.jitterRatio = jitterRatio")
            }
            line("}")
        }
        line("}")
        line()
        line("public struct SQLiteNowSyncConfig: Equatable, Sendable {")
        indent {
            line("public var schema: String")
            line("public var uploadLimit: Int")
            line("public var downloadLimit: Int")
            line("public var snapshotChunkRows: Int")
            line("public var snapshotChunkBytes: Int64")
            line("public var snapshotApplyBatchRows: Int")
            line("public var snapshotApplyBatchBytes: Int64")
            line("public var verboseLogs: Bool")
            line("public var transientRetryPolicy: SQLiteNowTransientRetryPolicy")
            line("public var snapshotCapacityRetryPolicy: SQLiteNowSnapshotCapacityRetryPolicy")
            line()
            line("public init(")
            indent {
                line("schema: String = \"main\",")
                line("uploadLimit: Int = 200,")
                line("downloadLimit: Int = 1_000,")
                line("snapshotChunkRows: Int = 1_000,")
                line("snapshotChunkBytes: Int64 = 4 * 1_024 * 1_024,")
                line("snapshotApplyBatchRows: Int = 256,")
                line("snapshotApplyBatchBytes: Int64 = 4 * 1_024 * 1_024,")
                line("verboseLogs: Bool = false,")
                line("transientRetryPolicy: SQLiteNowTransientRetryPolicy = .init(),")
                line("snapshotCapacityRetryPolicy: SQLiteNowSnapshotCapacityRetryPolicy = .init()")
            }
            line(") {")
            indent {
                line("self.schema = schema")
                line("self.uploadLimit = uploadLimit")
                line("self.downloadLimit = downloadLimit")
                line("self.snapshotChunkRows = snapshotChunkRows")
                line("self.snapshotChunkBytes = snapshotChunkBytes")
                line("self.snapshotApplyBatchRows = snapshotApplyBatchRows")
                line("self.snapshotApplyBatchBytes = snapshotApplyBatchBytes")
                line("self.verboseLogs = verboseLogs")
                line("self.transientRetryPolicy = transientRetryPolicy")
                line("self.snapshotCapacityRetryPolicy = snapshotCapacityRetryPolicy")
            }
            line("}")
            line()
            line("fileprivate func validate() throws {")
            indent {
                line("guard snapshotChunkRows > 0 else { throw SQLiteNowError.misuse(message: \"snapshotChunkRows must be positive\", underlying: nil) }")
                line("guard snapshotChunkBytes > 0 else { throw SQLiteNowError.misuse(message: \"snapshotChunkBytes must be positive\", underlying: nil) }")
                line("guard snapshotApplyBatchRows > 0 else { throw SQLiteNowError.misuse(message: \"snapshotApplyBatchRows must be positive\", underlying: nil) }")
                line("guard snapshotApplyBatchBytes > 0 else { throw SQLiteNowError.misuse(message: \"snapshotApplyBatchBytes must be positive\", underlying: nil) }")
                line("guard snapshotCapacityRetryPolicy.maxWaitMillis > 0 else { throw SQLiteNowError.misuse(message: \"snapshotCapacityRetryPolicy.maxWaitMillis must be positive\", underlying: nil) }")
                line("guard snapshotCapacityRetryPolicy.fallbackDelayMillis > 0 else { throw SQLiteNowError.misuse(message: \"snapshotCapacityRetryPolicy.fallbackDelayMillis must be positive\", underlying: nil) }")
                line("guard (0.0...1.0).contains(snapshotCapacityRetryPolicy.jitterRatio) else { throw SQLiteNowError.misuse(message: \"snapshotCapacityRetryPolicy.jitterRatio must be within 0...1\", underlying: nil) }")
                line("_ = try sqliteNowSyncInt32(uploadLimit, field: \"uploadLimit\")")
                line("_ = try sqliteNowSyncInt32(downloadLimit, field: \"downloadLimit\")")
                line("_ = try sqliteNowSyncInt32(snapshotChunkRows, field: \"snapshotChunkRows\")")
                line("_ = try sqliteNowSyncInt32(snapshotApplyBatchRows, field: \"snapshotApplyBatchRows\")")
                line("_ = try sqliteNowSyncInt32(transientRetryPolicy.maxAttempts, field: \"transientRetryPolicy.maxAttempts\")")
            }
            line("}")
        }
        line("}")
        line()
        line("private func sqliteNowSyncInt32(_ value: Int, field: String) throws -> Int32 {")
        indent {
            line("guard let converted = Int32(exactly: value) else {")
            indent {
                line("throw SQLiteNowError.misuse(message: \"\\(field) is outside the 32-bit signed integer range\", underlying: nil)")
            }
            line("}")
            line("return converted")
        }
        line("}")
        line()
        emitSwiftBridgeEnum(
            name = "SQLiteNowAuthorityStatus",
            cases = listOf(
                "PENDING_LOCAL_SEED" to "pendingLocalSeed",
                "AUTHORITATIVE_EMPTY" to "authoritativeEmpty",
                "AUTHORITATIVE_MATERIALIZED" to "authoritativeMaterialized",
            )
        )
        emitSwiftBridgeEnum(
            name = "SQLiteNowAttachOutcome",
            cases = listOf(
                "RESUMED_ATTACHED_STATE" to "resumedAttachedState",
                "USED_REMOTE_STATE" to "usedRemoteState",
                "SEEDED_FROM_LOCAL" to "seededFromLocal",
                "STARTED_EMPTY" to "startedEmpty",
            )
        )
        emitSwiftBridgeEnum(
            name = "SQLiteNowSourceRecoveryReason",
            cases = listOf(
                "HISTORY_PRUNED" to "historyPruned",
                "SOURCE_SEQUENCE_OUT_OF_ORDER" to "sourceSequenceOutOfOrder",
                "SOURCE_SEQUENCE_CHANGED" to "sourceSequenceChanged",
                "SOURCE_RETIRED" to "sourceRetired",
            )
        )
        emitSwiftBridgeEnum(
            name = "SQLiteNowDetachOutcome",
            cases = listOf(
                "DETACHED" to "detached",
                "BLOCKED_UNSYNCED_DATA" to "blockedUnsyncedData",
            )
        )
        emitSwiftBridgeEnum(
            name = "SQLiteNowPushOutcome",
            cases = listOf(
                "NO_CHANGE" to "noChange",
                "COMMITTED" to "committed",
            )
        )
        emitSwiftBridgeEnum(
            name = "SQLiteNowRemoteSyncOutcome",
            cases = listOf(
                "ALREADY_AT_TARGET" to "alreadyAtTarget",
                "APPLIED_INCREMENTAL" to "appliedIncremental",
                "APPLIED_SNAPSHOT" to "appliedSnapshot",
            )
        )
        emitSwiftBridgeEnum(
            name = "SQLiteNowOversqliteOperation",
            cases = listOf(
                "ATTACH" to "attach",
                "PUSH_PENDING" to "pushPending",
                "PULL_TO_STABLE" to "pullToStable",
                "SYNC" to "sync",
                "REBUILD_KEEP_SOURCE" to "rebuildKeepSource",
                "REBUILD_ROTATE_SOURCE" to "rebuildRotateSource",
            )
        )
        emitSwiftBridgeEnum(
            name = "SQLiteNowOversqlitePhase",
            cases = listOf(
                "ATTACHING" to "attaching",
                "SEEDING" to "seeding",
                "PUSHING" to "pushing",
                "PULLING" to "pulling",
                "STAGING_REMOTE_STATE" to "stagingRemoteState",
                "APPLYING_REMOTE_STATE" to "applyingRemoteState",
            )
        )
        line("public enum SQLiteNowBundleChangeWatchMode: String, Sendable {")
        indent {
            line("case off = \"OFF\"")
            line("case auto = \"AUTO\"")
        }
        line("}")
        line()
        line("public struct SQLiteNowPendingSyncStatus: Equatable, Sendable {")
        indent {
            line("public let hasPendingSyncData: Bool")
            line("public let pendingRowCount: Int64")
            line("public let blocksDetach: Bool")
            line()
            line("fileprivate init(_ bridge: ${databaseName}PendingSyncStatusBridge) {")
            indent {
                line("hasPendingSyncData = bridge.hasPendingSyncData")
                line("pendingRowCount = bridge.pendingRowCount")
                line("blocksDetach = bridge.blocksDetach")
            }
            line("}")
        }
        line("}")
        line()
        line("public struct SQLiteNowSyncStatus: Equatable, Sendable {")
        indent {
            line("public let authority: SQLiteNowAuthorityStatus")
            line("public let pending: SQLiteNowPendingSyncStatus")
            line("public let lastBundleSeqSeen: Int64")
            line()
            line("fileprivate init(_ bridge: ${databaseName}SyncStatusBridge) {")
            indent {
                line("authority = SQLiteNowAuthorityStatus(bridge.authority)")
                line("pending = SQLiteNowPendingSyncStatus(bridge.pending)")
                line("lastBundleSeqSeen = bridge.lastBundleSeqSeen")
            }
            line("}")
        }
        line("}")
        line()
        line("public struct SQLiteNowSourceInfo: Equatable, Sendable {")
        indent {
            line("public let currentSourceId: String")
            line("public let rebuildRequired: Bool")
            line("public let sourceRecoveryRequired: Bool")
            line("public let sourceRecoveryReason: SQLiteNowSourceRecoveryReason?")
            line()
            line("fileprivate init(_ bridge: ${databaseName}SourceInfoBridge) {")
            indent {
                line("currentSourceId = bridge.currentSourceId")
                line("rebuildRequired = bridge.rebuildRequired")
                line("sourceRecoveryRequired = bridge.sourceRecoveryRequired")
                line("sourceRecoveryReason = bridge.sourceRecoveryReason.map(SQLiteNowSourceRecoveryReason.init)")
            }
            line("}")
        }
        line("}")
        line()
        line("public struct SQLiteNowRestoreSummary: Equatable, Sendable {")
        indent {
            line("public let bundleSeq: Int64")
            line("public let rowCount: Int64")
            line()
            line("fileprivate init(_ bridge: ${databaseName}RestoreSummaryBridge) {")
            indent {
                line("bundleSeq = bridge.bundleSeq")
                line("rowCount = bridge.rowCount")
            }
            line("}")
        }
        line("}")
        line()
        line("public enum SQLiteNowAttachResult: Equatable, Sendable {")
        indent {
            line("case connected(outcome: SQLiteNowAttachOutcome, status: SQLiteNowSyncStatus, restore: SQLiteNowRestoreSummary?)")
            line("case retryLater(retryAfterSeconds: Int64)")
            line("case unknown(String)")
            line()
            line("fileprivate init(_ bridge: ${databaseName}AttachResultBridge) {")
            indent {
                line("switch bridge.kind {")
                line("case \"connected\":")
                indent {
                    line("guard let outcome = bridge.outcome, let status = bridge.status else {")
                    indent {
                        line("self = .unknown(bridge.kind)")
                        line("return")
                    }
                    line("}")
                    line("self = .connected(")
                    indent {
                        line("outcome: SQLiteNowAttachOutcome(outcome),")
                        line("status: SQLiteNowSyncStatus(status),")
                        line("restore: bridge.restore.map(SQLiteNowRestoreSummary.init)")
                    }
                    line(")")
                }
                line("case \"retryLater\":")
                indent {
                    line("self = .retryLater(retryAfterSeconds: bridge.retryAfterSeconds)")
                }
                line("default:")
                indent {
                    line("self = .unknown(bridge.kind)")
                }
                line("}")
            }
            line("}")
        }
        line("}")
        line()
        line("public struct SQLiteNowPushReport: Equatable, Sendable {")
        indent {
            line("public let outcome: SQLiteNowPushOutcome")
            line("public let status: SQLiteNowSyncStatus")
            line()
            line("fileprivate init(_ bridge: ${databaseName}PushReportBridge) {")
            indent {
                line("outcome = SQLiteNowPushOutcome(bridge.outcome)")
                line("status = SQLiteNowSyncStatus(bridge.status)")
            }
            line("}")
        }
        line("}")
        line()
        line("public struct SQLiteNowRemoteSyncReport: Equatable, Sendable {")
        indent {
            line("public let outcome: SQLiteNowRemoteSyncOutcome")
            line("public let status: SQLiteNowSyncStatus")
            line("public let restore: SQLiteNowRestoreSummary?")
            line()
            line("fileprivate init(_ bridge: ${databaseName}RemoteSyncReportBridge) {")
            indent {
                line("outcome = SQLiteNowRemoteSyncOutcome(bridge.outcome)")
                line("status = SQLiteNowSyncStatus(bridge.status)")
                line("restore = bridge.restore.map(SQLiteNowRestoreSummary.init)")
            }
            line("}")
        }
        line("}")
        line()
        line("public struct SQLiteNowSyncReport: Equatable, Sendable {")
        indent {
            line("public let pushOutcome: SQLiteNowPushOutcome")
            line("public let remoteOutcome: SQLiteNowRemoteSyncOutcome")
            line("public let status: SQLiteNowSyncStatus")
            line("public let restore: SQLiteNowRestoreSummary?")
            line()
            line("fileprivate init(_ bridge: ${databaseName}SyncReportBridge) {")
            indent {
                line("pushOutcome = SQLiteNowPushOutcome(bridge.pushOutcome)")
                line("remoteOutcome = SQLiteNowRemoteSyncOutcome(bridge.remoteOutcome)")
                line("status = SQLiteNowSyncStatus(bridge.status)")
                line("restore = bridge.restore.map(SQLiteNowRestoreSummary.init)")
            }
            line("}")
        }
        line("}")
        line()
        line("public struct SQLiteNowSyncThenDetachResult: Equatable, Sendable {")
        indent {
            line("public let lastSync: SQLiteNowSyncReport")
            line("public let detach: SQLiteNowDetachOutcome")
            line("public let syncRounds: Int32")
            line("public let remainingPendingRowCount: Int64")
            line("public let success: Bool")
            line()
            line("fileprivate init(_ bridge: ${databaseName}SyncThenDetachResultBridge) {")
            indent {
                line("lastSync = SQLiteNowSyncReport(bridge.lastSync)")
                line("detach = SQLiteNowDetachOutcome(bridge.detach.outcome)")
                line("syncRounds = bridge.syncRounds")
                line("remainingPendingRowCount = bridge.remainingPendingRowCount")
                line("success = bridge.success")
            }
            line("}")
        }
        line("}")
        line()
        line("public enum SQLiteNowSyncProgress: Equatable, Sendable {")
        indent {
            line("case idle")
            line("case active(operation: SQLiteNowOversqliteOperation, phase: SQLiteNowOversqlitePhase)")
            line()
            line("fileprivate init(_ bridge: ${databaseName}ProgressBridge) {")
            indent {
                line("switch bridge.kind {")
                line("case \"active\":")
                indent {
                    line("self = .active(")
                    indent {
                        line("operation: SQLiteNowOversqliteOperation(bridge.operation ?? \"\"),")
                        line("phase: SQLiteNowOversqlitePhase(bridge.phase ?? \"\")")
                    }
                    line(")")
                }
                line("default:")
                indent {
                    line("self = .idle")
                }
                line("}")
            }
            line("}")
        }
        line("}")
        line()
        line("public struct SQLiteNowAutomaticDownloadConfig: Sendable {")
        indent {
            line("public var automaticDownloadIntervalMillis: Int64")
            line("public var bundleChangeWatchMode: SQLiteNowBundleChangeWatchMode")
            line("public var bundleChangeWatchReconnectMinMillis: Int64")
            line("public var bundleChangeWatchReconnectMaxMillis: Int64")
            line()
            line("public init(")
            indent {
                line("automaticDownloadIntervalMillis: Int64 = 60_000,")
                line("bundleChangeWatchMode: SQLiteNowBundleChangeWatchMode = .off,")
                line("bundleChangeWatchReconnectMinMillis: Int64 = 1_000,")
                line("bundleChangeWatchReconnectMaxMillis: Int64 = 60_000")
            }
            line(") {")
            indent {
                line("self.automaticDownloadIntervalMillis = automaticDownloadIntervalMillis")
                line("self.bundleChangeWatchMode = bundleChangeWatchMode")
                line("self.bundleChangeWatchReconnectMinMillis = bundleChangeWatchReconnectMinMillis")
                line("self.bundleChangeWatchReconnectMaxMillis = bundleChangeWatchReconnectMaxMillis")
            }
            line("}")
            line()
            line("fileprivate var bridgeConfig: ${databaseName}AutomaticDownloadConfigBridge {")
            indent {
                line("${databaseName}AutomaticDownloadConfigBridge(")
                indent {
                    line("automaticDownloadIntervalMillis: automaticDownloadIntervalMillis,")
                    line("bundleChangeWatchMode: bundleChangeWatchMode.rawValue,")
                    line("bundleChangeWatchReconnectMinMillis: bundleChangeWatchReconnectMinMillis,")
                    line("bundleChangeWatchReconnectMaxMillis: bundleChangeWatchReconnectMaxMillis")
                }
                line(")")
            }
            line("}")
        }
        line("}")
        line()
        line("public struct SQLiteNowSyncConflict: Equatable, Sendable {")
        indent {
            line("public let schema: String")
            line("public let table: String")
            line("public let keyJson: String")
            line("public let localOp: String")
            line("public let localPayloadJson: String?")
            line("public let baseRowVersion: Int64")
            line("public let serverRowVersion: Int64")
            line("public let serverRowDeleted: Bool")
            line("public let serverRowJson: String?")
            line()
            line("fileprivate init(_ bridge: ${databaseName}SyncConflictBridge) {")
            indent {
                line("schema = bridge.schema")
                line("table = bridge.table")
                line("keyJson = bridge.keyJson")
                line("localOp = bridge.localOp")
                line("localPayloadJson = bridge.localPayloadJson")
                line("baseRowVersion = bridge.baseRowVersion")
                line("serverRowVersion = bridge.serverRowVersion")
                line("serverRowDeleted = bridge.serverRowDeleted")
                line("serverRowJson = bridge.serverRowJson")
            }
            line("}")
        }
        line("}")
        line()
        line("public enum SQLiteNowSyncResolverResult: Equatable, Sendable {")
        indent {
            line("case acceptServer")
            line("case keepLocal")
            line("case keepMerged(payloadJson: String)")
            line()
            line("fileprivate var bridgeResult: ${databaseName}SyncResolverResultBridge {")
            indent {
                line("switch self {")
                line("case .acceptServer:")
                indent { line("return ${databaseName}SyncResolverResultBridge(kind: \"acceptServer\", mergedPayloadJson: nil)") }
                line("case .keepLocal:")
                indent { line("return ${databaseName}SyncResolverResultBridge(kind: \"keepLocal\", mergedPayloadJson: nil)") }
                line("case let .keepMerged(payloadJson):")
                indent { line("return ${databaseName}SyncResolverResultBridge(kind: \"keepMerged\", mergedPayloadJson: payloadJson)") }
                line("}")
            }
            line("}")
        }
        line("}")
        line()
        line("public struct SQLiteNowSyncResolver: @unchecked Sendable {")
        indent {
            line("fileprivate let resolveBlock: @Sendable (SQLiteNowSyncConflict) -> SQLiteNowSyncResolverResult")
            line()
            line("public init(resolve: @escaping @Sendable (SQLiteNowSyncConflict) -> SQLiteNowSyncResolverResult) {")
            indent { line("resolveBlock = resolve") }
            line("}")
            line()
            line("public static let serverWins = SQLiteNowSyncResolver { _ in .acceptServer }")
            line("public static let clientWins = SQLiteNowSyncResolver { _ in .keepLocal }")
            line()
            line("fileprivate func resolve(_ bridge: ${databaseName}SyncConflictBridge) -> ${databaseName}SyncResolverResultBridge {")
            indent { line("resolveBlock(SQLiteNowSyncConflict(bridge)).bridgeResult") }
            line("}")
        }
        line("}")
        line()
        line("public final class SQLiteNowAutomaticDownloads {")
        indent {
            line("private let observation: ${databaseName}Observation")
            line()
            line("fileprivate init(observation: ${databaseName}Observation) {")
            indent {
                line("self.observation = observation")
            }
            line("}")
            line()
            line("public func cancel() {")
            indent {
                line("observation.cancel()")
            }
            line("}")
        }
        line("}")
        line()
        line("public final class SQLiteNowSyncClient {")
        indent {
            line("private let bridge: ${databaseName}SyncBridge")
            line()
            line("fileprivate init(bridge: ${databaseName}SyncBridge) {")
            indent {
                line("self.bridge = bridge")
            }
            line("}")
            line()
            line("public func open() async throws {")
            indent {
                line("try await mapSQLiteNowErrors {")
                indent {
                    line("try await bridge.open()")
                }
                line("}")
            }
            line("}")
            line()
            line("public func attach(userId: String) async throws -> SQLiteNowAttachResult {")
            indent {
                line("try await mapSQLiteNowErrors {")
                indent {
                    line("let result = try await bridge.attach(userId: userId)")
                    line("return SQLiteNowAttachResult(result)")
                }
                line("}")
            }
            line("}")
            line()
            line("public func pauseUploads() async throws {")
            indent {
                line("try await mapSQLiteNowErrors { try await bridge.pauseUploads() }")
            }
            line("}")
            line()
            line("public func resumeUploads() async throws {")
            indent {
                line("try await mapSQLiteNowErrors { try await bridge.resumeUploads() }")
            }
            line("}")
            line()
            line("public func pauseDownloads() async throws {")
            indent {
                line("try await mapSQLiteNowErrors { try await bridge.pauseDownloads() }")
            }
            line("}")
            line()
            line("public func resumeDownloads() async throws {")
            indent {
                line("try await mapSQLiteNowErrors { try await bridge.resumeDownloads() }")
            }
            line("}")
            line()
            line("public func sourceInfo() async throws -> SQLiteNowSourceInfo {")
            indent {
                line("try await mapSQLiteNowErrors {")
                indent {
                    line("let info = try await bridge.sourceInfo()")
                    line("return SQLiteNowSourceInfo(info)")
                }
                line("}")
            }
            line("}")
            line()
            line("public func syncStatus() async throws -> SQLiteNowSyncStatus {")
            indent {
                line("try await mapSQLiteNowErrors {")
                indent {
                    line("let status = try await bridge.syncStatus()")
                    line("return SQLiteNowSyncStatus(status)")
                }
                line("}")
            }
            line("}")
            line()
            line("public func detach() async throws -> SQLiteNowDetachOutcome {")
            indent {
                line("try await mapSQLiteNowErrors {")
                indent {
                    line("let outcome = try await bridge.detach()")
                    line("return SQLiteNowDetachOutcome(outcome.outcome)")
                }
                line("}")
            }
            line("}")
            line()
            line("public func pushPending() async throws -> SQLiteNowPushReport {")
            indent {
                line("try await mapSQLiteNowErrors {")
                indent {
                    line("let report = try await bridge.pushPending()")
                    line("return SQLiteNowPushReport(report)")
                }
                line("}")
            }
            line("}")
            line()
            line("public func pullToStable() async throws -> SQLiteNowRemoteSyncReport {")
            indent {
                line("try await mapSQLiteNowErrors {")
                indent {
                    line("let report = try await bridge.pullToStable()")
                    line("return SQLiteNowRemoteSyncReport(report)")
                }
                line("}")
            }
            line("}")
            line()
            line("public func sync() async throws -> SQLiteNowSyncReport {")
            indent {
                line("try await mapSQLiteNowErrors {")
                indent {
                    line("let report = try await bridge.sync()")
                    line("return SQLiteNowSyncReport(report)")
                }
                line("}")
            }
            line("}")
            line()
            line("public func syncThenDetach() async throws -> SQLiteNowSyncThenDetachResult {")
            indent {
                line("try await mapSQLiteNowErrors {")
                indent {
                    line("let result = try await bridge.syncThenDetach()")
                    line("return SQLiteNowSyncThenDetachResult(result)")
                }
                line("}")
            }
            line("}")
            line()
            line("public func rebuild() async throws -> SQLiteNowRemoteSyncReport {")
            indent {
                line("try await mapSQLiteNowErrors {")
                indent {
                    line("let report = try await bridge.rebuild()")
                    line("return SQLiteNowRemoteSyncReport(report)")
                }
                line("}")
            }
            line("}")
            line()
            line("public func progress() -> AsyncThrowingStream<SQLiteNowSyncProgress, Error> {")
            indent {
                line("AsyncThrowingStream { continuation in")
                indent {
                    line("let observation = bridge.observeProgress(")
                    indent {
                        line("onProgress: { progress in")
                        indent {
                            line("continuation.yield(SQLiteNowSyncProgress(progress))")
                        }
                        line("},")
                        line("onError: { failure in")
                        indent {
                            line("continuation.finish(throwing: SQLiteNowError.from(failure, underlying: nil))")
                        }
                        line("},")
                        line("onComplete: {")
                        indent {
                            line("continuation.finish()")
                        }
                        line("}")
                    }
                    line(")")
                    line("continuation.onTermination = { _ in")
                    indent {
                        line("observation.cancel()")
                    }
                    line("}")
                }
                line("}")
            }
            line("}")
            line()
            line("@discardableResult")
            line("public func startAutomaticDownloads(")
            indent {
                line("_ config: SQLiteNowAutomaticDownloadConfig = .init(),")
                line("onError: @escaping @Sendable (SQLiteNowError) -> Void = { _ in }")
            }
            line(") -> SQLiteNowAutomaticDownloads {")
            indent {
                line("let observation = bridge.startAutomaticDownloads(")
                indent {
                    line("config: config.bridgeConfig,")
                    line("onError: { failure in")
                    indent {
                        line("onError(SQLiteNowError.from(failure, underlying: nil))")
                    }
                    line("}")
                }
                line(")")
                line("return SQLiteNowAutomaticDownloads(observation: observation)")
            }
            line("}")
            line()
            line("public func close() {")
            indent {
                line("bridge.close()")
            }
            line("}")
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitSwiftBridgeEnum(name: String, cases: List<Pair<String, String>>) {
        line("public enum $name: Equatable, Sendable {")
        indent {
            cases.forEach { (_, caseName) ->
                line("case $caseName")
            }
            line("case unknown(String)")
            line()
            line("fileprivate init(_ rawValue: String) {")
            indent {
                line("switch rawValue {")
                cases.forEach { (rawValue, caseName) ->
                    line("case \"$rawValue\": self = .$caseName")
                }
                line("default: self = .unknown(rawValue)")
                line("}")
            }
            line("}")
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitSwiftDatabase() {
        val adapterDefault = if (adapterDescriptors().isEmpty()) " = .init()" else ""
        val bridgeArgs = listOf("path: path.path") + adapterDescriptors().map { adapter ->
            """
                ${adapter.name}: { value in
                    do {
                        return ${adapter.resultClassName()}.companion.success(value: try adapters.${adapter.name}(value))
                    } catch {
                        return ${adapter.resultClassName()}.companion.failure(message: String(describing: error))
                    }
                }
            """.trimIndent()
        }
        line("public final class $databaseName {")
        indent {
            line("private let bridge: ${databaseName}Bridge")
            line("private let adapters: ${databaseName}Adapters")
            line()
            namespaces().forEach { namespace ->
                line("public let ${namespace.swiftNamespacePropertyName()}: ${namespace.swiftNamespaceTypeName("Queries")}")
            }
            line()
            line("public init(path: URL, adapters: ${databaseName}Adapters$adapterDefault) {")
            indent {
                line("self.bridge = ${databaseName}Bridge(")
                indent {
                    bridgeArgs.forEachIndexed { index, argument ->
                        val suffix = if (index == bridgeArgs.lastIndex) "" else ","
                        line("$argument$suffix")
                    }
                }
                line(")")
                line("self.adapters = adapters")
                namespaces().forEach { namespace ->
                    line("self.${namespace.swiftNamespacePropertyName()} = ${namespace.swiftNamespaceTypeName("Queries")}(bridge: bridge, adapters: adapters)")
                }
            }
            line("}")
            line()
            line("public func open() async throws {")
            indent {
                line("try await mapSQLiteNowErrors {")
                indent {
                    line("try await bridge.open()")
                }
                line("}")
            }
            line("}")
            line()
            line("public func close() async throws {")
            indent {
                line("try await mapSQLiteNowErrors {")
                indent {
                    line("try await bridge.close()")
                }
                line("}")
            }
            line("}")
            line()
            line("public func transaction(_ block: (${databaseName}Transaction) throws -> Void) async throws {")
            indent {
                line("let batch = ${databaseName}MutationBatch()")
                line("try block(${databaseName}Transaction(batch: batch))")
                line("try await mapSQLiteNowErrors {")
                indent {
                    line("try await bridge.transaction(batch: batch)")
                }
                line("}")
            }
            line("}")
            if (swiftOversqliteEnabled) {
                line()
                line("public func makeSyncClient(")
                indent {
                    line("baseURL: URL,")
                    line("auth: SQLiteNowSyncAuth,")
                    line("config: SQLiteNowSyncConfig = .init(),")
                    line("resolver: SQLiteNowSyncResolver? = nil")
                }
                line(") throws -> SQLiteNowSyncClient {")
                indent {
                    line("try config.validate()")
                    line("let syncBridge = bridge.makeSyncClient(")
                    indent {
                        line("baseUrl: baseURL.absoluteString,")
                        line("accessTokenProvider: auth.accessTokenProvider,")
                        line("refreshedAccessTokenProvider: auth.refreshedAccessTokenProvider,")
                        line("schema: config.schema,")
                        line("uploadLimit: try sqliteNowSyncInt32(config.uploadLimit, field: \"uploadLimit\"),")
                        line("downloadLimit: try sqliteNowSyncInt32(config.downloadLimit, field: \"downloadLimit\"),")
                        line("snapshotChunkRows: try sqliteNowSyncInt32(config.snapshotChunkRows, field: \"snapshotChunkRows\"),")
                        line("snapshotChunkBytes: config.snapshotChunkBytes,")
                        line("snapshotApplyBatchRows: try sqliteNowSyncInt32(config.snapshotApplyBatchRows, field: \"snapshotApplyBatchRows\"),")
                        line("snapshotApplyBatchBytes: config.snapshotApplyBatchBytes,")
                        line("verboseLogs: config.verboseLogs,")
                        line("transientMaxAttempts: try sqliteNowSyncInt32(config.transientRetryPolicy.maxAttempts, field: \"transientRetryPolicy.maxAttempts\"),")
                        line("transientInitialBackoffMillis: config.transientRetryPolicy.initialBackoffMillis,")
                        line("transientMaxBackoffMillis: config.transientRetryPolicy.maxBackoffMillis,")
                        line("transientJitterRatio: config.transientRetryPolicy.jitterRatio,")
                        line("capacityRetryEnabled: config.snapshotCapacityRetryPolicy.enabled,")
                        line("capacityRetryMaxWaitMillis: config.snapshotCapacityRetryPolicy.maxWaitMillis,")
                        line("capacityRetryFallbackDelayMillis: config.snapshotCapacityRetryPolicy.fallbackDelayMillis,")
                        line("capacityRetryJitterRatio: config.snapshotCapacityRetryPolicy.jitterRatio,")
                        line("resolver: resolver.map { resolver in { conflict in resolver.resolve(conflict) } }")
                    }
                    line(")")
                    line("return SQLiteNowSyncClient(bridge: syncBridge)")
                }
                line("}")
            }
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitSwiftResultModels() {
        resultStatements().forEach { result ->
            val resultName = result.name
            val fields = result.fields
            val conformances = buildList {
                if (fields.all { it.supportsSwiftEquatable() }) add("Equatable")
                add(if (fields.all { it.supportsSwiftSendable() }) "Sendable" else "@unchecked Sendable")
            }
            val conformanceSuffix = conformances.takeIf { it.isNotEmpty() }
                ?.joinToString(prefix = ": ")
                .orEmpty()
            line("public struct $resultName$conformanceSuffix {")
            indent {
                fields.forEach { field ->
                    line("public let ${field.propertyName.swiftIdentifier()}: ${field.swiftType}")
                }
                line()
                line("fileprivate init(_ row: ${resultName.swiftBridgeResultName()}) {")
                indent {
                    fields.forEach { field ->
                        line("${field.propertyName.swiftIdentifier()} = ${field.bridgeToSwiftExpression("row.${field.propertyName}")}")
                    }
                }
                line("}")
            }
            line("}")
            line()
        }
    }

    private fun SwiftWriter.emitSwiftParams() {
        statements().forEach { (namespace, statement) ->
            val params = parameterDescriptors(statement)
            if (params.isEmpty()) return@forEach
            val paramsName = statement.swiftParamsName(namespace)
            val bridgeName = paramsName.swiftBridgeParamsName()
            line("public struct $paramsName {")
            indent {
                params.forEach { param ->
                    line("public let ${param.propertyName.swiftIdentifier()}: ${param.swiftType}")
                }
                line()
                line("public init(")
                indent {
                    params.forEachIndexed { index, param ->
                        val suffix = if (index == params.lastIndex) "" else ","
                        line("${param.propertyName}: ${param.swiftType}$suffix")
                    }
                }
                line(") {")
                indent {
                    params.forEach { param ->
                        line("self.${param.propertyName.swiftIdentifier()} = ${param.propertyName}")
                    }
                }
                line("}")
                line()
                line("fileprivate var bridgeParams: $bridgeName {")
                indent {
                    line("$bridgeName(")
                    indent {
                        params.forEach { param ->
                            line("${param.propertyName}: ${param.swiftToBridgeExpression(param.propertyName.swiftIdentifier())},")
                        }
                    }
                    line(")")
                }
                line("}")
            }
            line("}")
            line()
        }
    }

    private fun SwiftWriter.emitSwiftNamespaces() {
        namespaces().forEach { namespace ->
            line("public final class ${namespace.swiftNamespaceTypeName("Queries")} {")
            indent {
                line("private let bridge: ${databaseName}Bridge")
                line("private let adapters: ${databaseName}Adapters")
                line()
                line("fileprivate init(bridge: ${databaseName}Bridge, adapters: ${databaseName}Adapters) {")
                indent {
                    line("self.bridge = bridge")
                    line("self.adapters = adapters")
                }
                line("}")
                line()
                context.nsWithStatements[namespace].orEmpty().forEach { statement ->
                    when (statement) {
                        is AnnotatedSelectStatement -> emitSwiftSelectFunction(namespace, statement)
                        is AnnotatedExecuteStatement -> emitSwiftExecuteFunction(namespace, statement)
                        else -> Unit
                    }
                    line()
                }
            }
            line("}")
            line()
        }
    }

    private fun SwiftWriter.emitSwiftSelectFunction(namespace: String, statement: AnnotatedSelectStatement) {
        val params = parameterDescriptors(statement)
        val resultName = resultClassName(statement)
        val functionName = statement.swiftFunctionName()
        val bridgeMethod = bridgeSelectMethodName(namespace, statement)
        val bridgeQuery = if (params.isEmpty()) {
            "bridge.$bridgeMethod()"
        } else {
            "bridge.$bridgeMethod(params: params.bridgeParams)"
        }
        val signature = if (params.isEmpty()) {
            "public func $functionName() -> SQLiteNowSelectQuery<$resultName>"
        } else {
            "public func $functionName(_ params: ${statement.swiftParamsName(namespace)}) -> SQLiteNowSelectQuery<$resultName>"
        }
        line("$signature {")
        indent {
            line("let query = $bridgeQuery")
            line("return SQLiteNowSelectQuery<$resultName>(")
            indent {
                line("list: {")
                indent {
                    line("try await mapSQLiteNowErrors {")
                    indent {
                        line("let rows = try await query.list()")
                        line("return rows.map($resultName.init)")
                    }
                    line("}")
                }
                line("},")
                line("one: {")
                indent {
                    line("try await mapSQLiteNowErrors {")
                    indent {
                        line("let row = try await query.one()")
                        line("return $resultName(row)")
                    }
                    line("}")
                }
                line("},")
                line("oneOrNull: {")
                indent {
                    line("try await mapSQLiteNowErrors {")
                    indent {
                        line("let row = try await query.oneOrNull()")
                        line("return row.map($resultName.init)")
                    }
                    line("}")
                }
                line("},")
                line("stream: {")
                indent {
                    line("sqliteNowStream(")
                    indent {
                        line("observe: query.observe,")
                        line("map: $resultName.init")
                    }
                    line(")")
                }
                line("}")
            }
            line(")")
        }
        line("}")
    }

    private fun SwiftWriter.emitSwiftExecuteFunction(namespace: String, statement: AnnotatedExecuteStatement) {
        if (statement.hasReturningClause()) {
            emitSwiftExecuteReturningFunction(namespace, statement)
            return
        }
        val params = parameterDescriptors(statement)
        val functionName = statement.swiftFunctionName()
        val bridgeMethod = bridgeExecuteMethodName(namespace, statement)
        val signature = if (params.isEmpty()) {
            "public func $functionName() async throws"
        } else {
            "public func $functionName(_ params: ${statement.swiftParamsName(namespace)}) async throws"
        }
        line("$signature {")
        indent {
            line("try await mapSQLiteNowErrors {")
            indent {
                if (params.isEmpty()) {
                    line("try await bridge.$bridgeMethod()")
                } else {
                    line("try await bridge.$bridgeMethod(params: params.bridgeParams)")
                }
            }
            line("}")
        }
        line("}")
    }

    private fun SwiftWriter.emitSwiftExecuteReturningFunction(namespace: String, statement: AnnotatedExecuteStatement) {
        val params = parameterDescriptors(statement)
        val resultName = resultClassName(statement)
        val functionName = statement.swiftFunctionName()
        val bridgeMethod = bridgeExecuteMethodName(namespace, statement)
        val bridgeQuery = if (params.isEmpty()) {
            "bridge.$bridgeMethod()"
        } else {
            "bridge.$bridgeMethod(params: params.bridgeParams)"
        }
        val signature = if (params.isEmpty()) {
            "public func $functionName() -> SQLiteNowExecuteReturningQuery<$resultName>"
        } else {
            "public func $functionName(_ params: ${statement.swiftParamsName(namespace)}) -> SQLiteNowExecuteReturningQuery<$resultName>"
        }
        line("$signature {")
        indent {
            line("let query = $bridgeQuery")
            line("return SQLiteNowExecuteReturningQuery<$resultName>(")
            indent {
                line("list: {")
                indent {
                    line("try await mapSQLiteNowErrors {")
                    indent {
                        line("let rows = try await query.list()")
                        line("return rows.map($resultName.init)")
                    }
                    line("}")
                }
                line("},")
                line("one: {")
                indent {
                    line("try await mapSQLiteNowErrors {")
                    indent {
                        line("let row = try await query.one()")
                        line("return $resultName(row)")
                    }
                    line("}")
                }
                line("},")
                line("oneOrNull: {")
                indent {
                    line("try await mapSQLiteNowErrors {")
                    indent {
                        line("let row = try await query.oneOrNull()")
                        line("return row.map($resultName.init)")
                    }
                    line("}")
                }
                line("}")
            }
            line(")")
        }
        line("}")
    }

    private fun SwiftWriter.emitSwiftTransactions() {
        line("public final class ${databaseName}Transaction {")
        indent {
            line("private let batch: ${databaseName}MutationBatch")
            namespaces().forEach { namespace ->
                line("public let ${namespace.swiftNamespacePropertyName()}: ${namespace.swiftNamespaceTypeName("TransactionQueries")}")
            }
            line()
            line("fileprivate init(batch: ${databaseName}MutationBatch) {")
            indent {
                line("self.batch = batch")
                namespaces().forEach { namespace ->
                    line("self.${namespace.swiftNamespacePropertyName()} = ${namespace.swiftNamespaceTypeName("TransactionQueries")}(batch: batch.${namespace.swiftNamespacePropertyName()})")
                }
            }
            line("}")
        }
        line("}")
        line()
        namespaces().forEach { namespace ->
            val executeStatements = context.nsWithStatements[namespace].orEmpty()
                .filterIsInstance<AnnotatedExecuteStatement>()
                .filterNot { it.hasReturningClause() }
            line("public final class ${namespace.swiftNamespaceTypeName("TransactionQueries")} {")
            indent {
                line("private let batch: ${namespace.swiftNamespaceTypeName("MutationBatch")}")
                line()
                line("fileprivate init(batch: ${namespace.swiftNamespaceTypeName("MutationBatch")}) {")
                indent {
                    line("self.batch = batch")
                }
                line("}")
                line()
                executeStatements.forEach { statement ->
                    val params = parameterDescriptors(statement)
                    val signature = if (params.isEmpty()) {
                        "public func ${statement.swiftFunctionName()}()"
                    } else {
                        "public func ${statement.swiftFunctionName()}(_ params: ${statement.swiftParamsName(namespace)})"
                    }
                    line("$signature {")
                    indent {
                        if (params.isEmpty()) {
                            line("batch.${statement.swiftFunctionName()}()")
                        } else {
                            line("batch.${statement.swiftFunctionName()}(params: params.bridgeParams)")
                        }
                    }
                    line("}")
                    line()
                }
            }
            line("}")
            line()
        }
    }

    private fun SwiftWriter.emitSwiftStreams() {
        line("private func sqliteNowStream<BridgeRow, Row: Sendable>(")
        indent {
            line("observe: (@escaping ([BridgeRow]) -> Void, @escaping (${databaseName}SwiftOverlayErrorPayload) -> Void) -> ${databaseName}Observation,")
            line("map: @escaping (BridgeRow) -> Row")
        }
        line(") -> AsyncThrowingStream<[Row], Error> {")
        indent {
            line("AsyncThrowingStream { continuation in")
            indent {
                line("let observation = observe(")
                indent {
                    line("{ rows in")
                    indent {
                        line("continuation.yield(rows.map(map))")
                    }
                    line("},")
                    line("{ failure in")
                    indent {
                        line("continuation.finish(throwing: SQLiteNowError.from(failure, underlying: nil))")
                    }
                    line("}")
                }
                line(")")
                line("continuation.onTermination = { _ in")
                indent {
                    line("observation.cancel()")
                }
                line("}")
            }
            line("}")
        }
        line("}")
        line()
    }


    private fun parameterDescriptors(statement: AnnotatedStatement): List<SwiftKmpParameter> =
        model.parameterDescriptors(statement)

    private fun adapterDescriptors(): List<SwiftAdapterDescriptor> =
        model.adapterDescriptors()

    private fun namespaces(): List<String> = model.namespaces()

    private fun resultStatements(): List<SwiftKmpResult> = model.resultStatements()

    private fun statements(): List<Pair<String, AnnotatedStatement>> = model.statements()

    private fun resultClassName(statement: AnnotatedStatement): String = model.resultClassName(statement)

    private fun bridgeSelectMethodName(namespace: String, statement: AnnotatedSelectStatement): String =
        model.bridgeSelectMethodName(namespace, statement)

    private fun bridgeExecuteMethodName(namespace: String, statement: AnnotatedExecuteStatement): String =
        model.bridgeExecuteMethodName(namespace, statement)

}
