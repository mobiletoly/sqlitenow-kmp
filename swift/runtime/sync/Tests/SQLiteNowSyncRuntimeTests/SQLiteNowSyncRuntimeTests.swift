import Foundation
@preconcurrency import SQLiteNowSyncRuntime
import XCTest

final class SQLiteNowSyncRuntimeTests: XCTestCase {
    func testRemoteContractErrorPayloadExportsStructuredFields() {
        let payload = SQLiteNowSyncRuntimeErrorPayload(
            category: "protocol",
            code: "retired_schema_generation",
            message: "upgrade",
            remoteContract: true,
            httpStatus: 426,
            operation: "generation discovery",
            remediation: "upgrade_application",
            retryable: false,
            generationId: "1",
            incompleteReason: nil, dirtyCount: 0, outboundCount: 0, confirmedBundleCount: 0, pushAttempts: 0
        )

        XCTAssertTrue(payload.remoteContract)
        XCTAssertEqual(payload.code, "retired_schema_generation")
        XCTAssertEqual(payload.httpStatus, 426)
        XCTAssertEqual(payload.operation, "generation discovery")
        XCTAssertEqual(payload.remediation, "upgrade_application")
        XCTAssertFalse(payload.retryable)
        XCTAssertEqual(payload.generationId, "1")
    }

    func testSyncRuntimeUsesExportedCoreRuntimeSurface() async throws {
        let core = SQLiteNowCoreRuntimeDatabase(
            path: temporaryDatabaseURL().path,
            migrationPlan: Self.migrationPlan(),
            debug: false,
            onMigrationStep: nil,
            syncManaged: true
        )
        let opening = try await SQLiteNowSyncRuntimeKt.sqliteNowSyncRuntimeOpenForSync(
            coreDatabase: core,
            baseUrl: "http://127.0.0.1:1",
            auth: SQLiteNowSyncRuntimeAuth(
                accessTokenProvider: { "runtime-smoke-token" },
                refreshedAccessTokenProvider: nil
            ),
            config: SQLiteNowSyncRuntimeConfig(
                generationId: 1, databaseId: "swift-runtime-fixture", serviceId: "fixture-service",
                maxSyncWorkspaceEntries: 100_000, maxSyncWorkspaceBytes: 268_435_456, maxSyncWorkUnitsPerInvocation: 1_000_000,
                syncTables: [
                    SQLiteNowSyncRuntimeTableSpec(tableName: "docs", syncKeyColumnName: "doc_id")
                ],
                transferTargets: SQLiteNowSyncRuntimeTransferTargets(
                    maxPushRows: 200,
                    maxPushDecodedBytes: 1 * 1024 * 1024,
                    maxPullBundles: 1000,
                    maxSnapshotChunkRows: 1000,
                    maxSnapshotChunkDecodedBytes: 4 * 1024 * 1024
                ),
                snapshotApplyBatchRows: 256,
                snapshotApplyBatchBytes: 4 * 1024 * 1024,
                verboseLogs: false,
                transientRetryPolicy: SQLiteNowSyncRuntimeTransientRetryPolicy(
                    maxAttempts: 3,
                    initialBackoffMillis: 150,
                    maxBackoffMillis: 1500,
                    jitterRatio: 0.2
                ),
                snapshotCapacityRetryPolicy: SQLiteNowSyncRuntimeSnapshotCapacityRetryPolicy(
                    enabled: true,
                    maxWaitMillis: 30_000,
                    fallbackDelayMillis: 1_000,
                    jitterRatio: 1.0
                )
            ),
            resolver: nil,
            upgradePlan: nil
        )

        do {
            let status = try await opening.advance()
            XCTAssertEqual(status.outcome, "ready")
            let client = try XCTUnwrap(opening.readyClientOrNull())

            try await core.execute(
                sql: "INSERT OR REPLACE INTO docs (doc_id, title) VALUES (?, ?)",
                bindValues: [
                    SQLiteNowCoreRuntimeBindValue(textValue: "doc-1"),
                    SQLiteNowCoreRuntimeBindValue(textValue: "First"),
                ],
                affectedTables: ["docs"]
            )

            let docs = try await core.query(
                sql: "SELECT doc_id, title FROM docs ORDER BY doc_id",
                bindValues: [],
                columnTypes: ["text", "text"]
            )
            XCTAssertEqual(docs.count, 1)
            XCTAssertEqual(docs.rowAt(index: 0).cellAt(index: 0).textValue, "doc-1")

            try await client.pauseUploads()
            try await client.resumeUploads()
            try await client.pauseDownloads()
            try await client.resumeDownloads()

            let sourceInfo = try await client.sourceInfo()
            XCTAssertFalse(sourceInfo.currentSourceId.isEmpty)

            do {
                _ = try await client.syncStatus()
                XCTFail("Expected syncStatus() to require attach")
            } catch {
                guard let runtimeError = Self.syncRuntimeException(from: error) else {
                    let nsError = error as NSError
                    XCTFail("Expected SQLiteNowSyncRuntimeException payload, got \(type(of: error)): \(nsError)")
                    try await opening.close()
                    return
                }
                XCTAssertEqual(runtimeError.payload.category, "state")
                XCTAssertFalse(runtimeError.payload.code.isEmpty)
                XCTAssertFalse(runtimeError.payload.message.isEmpty)
            }
            try await opening.close()
        } catch {
            try? await opening.close()
            throw error
        }
    }

    func testRuntimeConfigurationExportsEveryOversqliteField() {
        let transient = SQLiteNowSyncRuntimeTransientRetryPolicy(
            maxAttempts: 7,
            initialBackoffMillis: 11,
            maxBackoffMillis: 22,
            jitterRatio: 0.4
        )
        let capacity = SQLiteNowSyncRuntimeSnapshotCapacityRetryPolicy(
            enabled: false,
            maxWaitMillis: 33,
            fallbackDelayMillis: 44,
            jitterRatio: 0.5
        )
        let config = SQLiteNowSyncRuntimeConfig(
            generationId: 1, databaseId: "swift-runtime-fixture", serviceId: "fixture-service",
            maxSyncWorkspaceEntries: 100_000, maxSyncWorkspaceBytes: 268_435_456, maxSyncWorkUnitsPerInvocation: 1_000_000,
            syncTables: [],
            transferTargets: SQLiteNowSyncRuntimeTransferTargets(
                maxPushRows: 5,
                maxPushDecodedBytes: 6,
                maxPullBundles: 7,
                maxSnapshotChunkRows: 8,
                maxSnapshotChunkDecodedBytes: 9
            ),
            snapshotApplyBatchRows: 10,
            snapshotApplyBatchBytes: 11,
            verboseLogs: true,
            transientRetryPolicy: transient,
            snapshotCapacityRetryPolicy: capacity
        )

        XCTAssertEqual(config.generationId, 1)
        XCTAssertEqual(config.databaseId, "swift-runtime-fixture")
        XCTAssertEqual(config.serviceId, "fixture-service")
        XCTAssertEqual(config.maxSyncWorkspaceEntries, 100_000)
        XCTAssertEqual(config.maxSyncWorkspaceBytes, 268_435_456)
        XCTAssertEqual(config.maxSyncWorkUnitsPerInvocation, 1_000_000)
        XCTAssertEqual(config.transferTargets.maxPushRows, 5)
        XCTAssertEqual(config.transferTargets.maxPushDecodedBytes, 6)
        XCTAssertEqual(config.transferTargets.maxPullBundles, 7)
        XCTAssertEqual(config.transferTargets.maxSnapshotChunkRows, 8)
        XCTAssertEqual(config.transferTargets.maxSnapshotChunkDecodedBytes, 9)
        XCTAssertEqual(config.snapshotApplyBatchRows, 10)
        XCTAssertEqual(config.snapshotApplyBatchBytes, 11)
        XCTAssertTrue(config.verboseLogs)
        XCTAssertEqual(config.transientRetryPolicy.maxAttempts, 7)
        XCTAssertEqual(config.snapshotCapacityRetryPolicy.maxWaitMillis, 33)
    }

    func testCoordinatedOpeningPublishesFreshSyncManagedCore() async throws {
        let path = temporaryDatabaseURL().path
        let core = SQLiteNowCoreRuntimeDatabase(
            path: path,
            migrationPlan: Self.migrationPlan(),
            debug: false,
            onMigrationStep: nil,
            syncManaged: true
        )
        let opening = try await SQLiteNowSyncRuntimeKt.sqliteNowSyncRuntimeOpenForSync(
            coreDatabase: core,
            baseUrl: "http://127.0.0.1:1",
            auth: SQLiteNowSyncRuntimeAuth(
                accessTokenProvider: { "runtime-smoke-token" },
                refreshedAccessTokenProvider: nil
            ),
            config: Self.syncConfig(),
            resolver: nil,
            upgradePlan: nil
        )

        do {
            let initial = try await opening.status()
            XCTAssertNil(initial.mode)
            XCTAssertNil(initial.outcome)
            XCTAssertEqual(initial.nextAction, "advance")
            XCTAssertNil(opening.readyClientOrNull())

            let ready = try await opening.advance()
            XCTAssertEqual(ready.outcome, "ready")
            XCTAssertNotNil(opening.readyClientOrNull())

            let rows = try await core.query(
                sql: "SELECT COUNT(*) FROM docs",
                bindValues: [],
                columnTypes: ["int64"]
            )
            XCTAssertEqual(rows.rowAt(index: 0).cellAt(index: 0).int64Value, 0)
            try await core.execute(
                sql: "INSERT INTO docs(doc_id,title) VALUES('old-1','visible before publication')",
                bindValues: [],
                affectedTables: ["docs"]
            )
            try await opening.close()
        } catch {
            try? await opening.close()
            throw error
        }

        let reopenedCore = SQLiteNowCoreRuntimeDatabase(
            path: path,
            migrationPlan: Self.migrationPlan(),
            debug: false,
            onMigrationStep: nil,
            syncManaged: true
        )
        let reopened = try await SQLiteNowSyncRuntimeKt.sqliteNowSyncRuntimeOpenForSync(
            coreDatabase: reopenedCore,
            baseUrl: "http://127.0.0.1:1",
            auth: SQLiteNowSyncRuntimeAuth(
                accessTokenProvider: { "runtime-smoke-token" },
                refreshedAccessTokenProvider: nil
            ),
            config: Self.syncConfig(),
            resolver: nil,
            upgradePlan: nil
        )
        do {
            let reopenedStatus = try await reopened.status()
            XCTAssertEqual(reopenedStatus.mode, "unchanged")
            XCTAssertNil(reopened.readyClientOrNull())
            let ready = try await reopened.advance()
            XCTAssertEqual(ready.outcome, "ready")
            XCTAssertNotNil(reopened.readyClientOrNull())
            let rows = try await reopenedCore.query(
                sql: "SELECT title FROM docs WHERE doc_id=?",
                bindValues: [SQLiteNowCoreRuntimeBindValue(textValue: "old-1")],
                columnTypes: ["text"]
            )
            XCTAssertEqual(rows.count, 1)
            XCTAssertEqual(rows.rowAt(index: 0).cellAt(index: 0).textValue, "visible before publication")
            try await reopened.close()
        } catch {
            try? await reopened.close()
            throw error
        }
    }

    func testResolverCallbackUsesRuntimeJsonPayloads() {
        let resolver = SQLiteNowSyncRuntimeResolver { conflict in
            XCTAssertEqual(conflict.schema, "main")
            XCTAssertEqual(conflict.table, "docs")
            XCTAssertEqual(conflict.keyJson, #"{"doc_id":"doc-1"}"#)
            XCTAssertEqual(conflict.serverRowJson, #"{"title":"Remote"}"#)
            return SQLiteNowSyncRuntimeResolverResult(
                kind: "keepMerged",
                mergedPayloadJson: #"{"title":"Merged"}"#
            )
        }

        let result = resolver.resolveForRuntimeSmoke(
            conflict: SQLiteNowSyncRuntimeConflict(
                schema: "main",
                table: "docs",
                keyJson: #"{"doc_id":"doc-1"}"#,
                localOp: "upsert",
                localPayloadJson: #"{"title":"Local"}"#,
                baseRowVersion: 1,
                serverRowVersion: 2,
                serverRowDeleted: false,
                serverRowJson: #"{"title":"Remote"}"#
            )
        )

        XCTAssertEqual(result.kind, "keepMerged")
        XCTAssertEqual(result.mergedPayloadJson, #"{"title":"Merged"}"#)
    }

    private static func migrationPlan() -> SQLiteNowCoreRuntimeMigrationPlan {
        SQLiteNowCoreRuntimeMigrationPlan(
            latestVersion: 1,
            schemaSql: [
                """
                CREATE TABLE docs (
                    doc_id TEXT PRIMARY KEY NOT NULL,
                    title TEXT NOT NULL
                );
                """
            ],
            initSql: [],
            migrationSteps: []
        )
    }

    private static func syncConfig() -> SQLiteNowSyncRuntimeConfig {
        SQLiteNowSyncRuntimeConfig(
            generationId: 1,
            databaseId: "swift-runtime-fixture",
            serviceId: "fixture-service",
            maxSyncWorkspaceEntries: 100_000, maxSyncWorkspaceBytes: 268_435_456, maxSyncWorkUnitsPerInvocation: 1_000_000,
            syncTables: [
                SQLiteNowSyncRuntimeTableSpec(tableName: "docs", syncKeyColumnName: "doc_id")
            ],
            transferTargets: SQLiteNowSyncRuntimeTransferTargets(
                maxPushRows: 200,
                maxPushDecodedBytes: 1 * 1_024 * 1_024,
                maxPullBundles: 1_000,
                maxSnapshotChunkRows: 1_000,
                maxSnapshotChunkDecodedBytes: 4 * 1_024 * 1_024
            ),
            snapshotApplyBatchRows: 256,
            snapshotApplyBatchBytes: 4 * 1_024 * 1_024,
            verboseLogs: false,
            transientRetryPolicy: SQLiteNowSyncRuntimeTransientRetryPolicy(
                maxAttempts: 3,
                initialBackoffMillis: 150,
                maxBackoffMillis: 1_500,
                jitterRatio: 0.2
            ),
            snapshotCapacityRetryPolicy: SQLiteNowSyncRuntimeSnapshotCapacityRetryPolicy(
                enabled: true,
                maxWaitMillis: 30_000,
                fallbackDelayMillis: 1_000,
                jitterRatio: 1.0
            )
        )
    }

    private func temporaryDatabaseURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlitenow-sync-runtime-smoke-\(UUID().uuidString).db")
    }

    private static func syncRuntimeException(from error: Error) -> SQLiteNowSyncRuntimeException? {
        if let runtimeError = error as? SQLiteNowSyncRuntimeException {
            return runtimeError
        }
        return (error as NSError).userInfo["K" + "otlinException"] as? SQLiteNowSyncRuntimeException
    }
}
