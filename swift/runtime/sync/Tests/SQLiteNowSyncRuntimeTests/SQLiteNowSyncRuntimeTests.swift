import Foundation
@preconcurrency import SQLiteNowSyncRuntime
import XCTest

final class SQLiteNowSyncRuntimeTests: XCTestCase {
    func testSyncRuntimeUsesExportedCoreRuntimeSurface() async throws {
        let core = SQLiteNowCoreRuntimeDatabase(
            path: temporaryDatabaseURL().path,
            migrationPlan: Self.migrationPlan(),
            debug: false
        )

        try await core.open()

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

        let client = SQLiteNowSyncRuntimeClient(
            coreDatabase: core,
            baseUrl: "http://127.0.0.1:1",
            auth: SQLiteNowSyncRuntimeAuth(
                accessTokenProvider: { "runtime-smoke-token" },
                refreshedAccessTokenProvider: nil
            ),
            config: SQLiteNowSyncRuntimeConfig(
                schema: "main",
                syncTables: [
                    SQLiteNowSyncRuntimeTableSpec(tableName: "docs", syncKeyColumnName: "doc_id")
                ],
                uploadLimit: 200,
                downloadLimit: 1000,
                snapshotChunkRows: 1000,
                snapshotChunkBytes: 4 * 1024 * 1024,
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
            resolver: nil
        )

        try await client.open()
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
                client.close()
                try await core.close()
                return
            }
            XCTAssertEqual(runtimeError.payload.category, "state")
            XCTAssertFalse(runtimeError.payload.code.isEmpty)
            XCTAssertFalse(runtimeError.payload.message.isEmpty)
        }

        client.close()
        try await core.close()
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
            schema: "business",
            syncTables: [],
            uploadLimit: 5,
            downloadLimit: 6,
            snapshotChunkRows: 7,
            snapshotChunkBytes: 8,
            snapshotApplyBatchRows: 9,
            snapshotApplyBatchBytes: 10,
            verboseLogs: true,
            transientRetryPolicy: transient,
            snapshotCapacityRetryPolicy: capacity
        )

        XCTAssertEqual(config.schema, "business")
        XCTAssertEqual(config.uploadLimit, 5)
        XCTAssertEqual(config.downloadLimit, 6)
        XCTAssertEqual(config.snapshotChunkRows, 7)
        XCTAssertEqual(config.snapshotChunkBytes, 8)
        XCTAssertEqual(config.snapshotApplyBatchRows, 9)
        XCTAssertEqual(config.snapshotApplyBatchBytes, 10)
        XCTAssertTrue(config.verboseLogs)
        XCTAssertEqual(config.transientRetryPolicy.maxAttempts, 7)
        XCTAssertEqual(config.snapshotCapacityRetryPolicy.maxWaitMillis, 33)
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
