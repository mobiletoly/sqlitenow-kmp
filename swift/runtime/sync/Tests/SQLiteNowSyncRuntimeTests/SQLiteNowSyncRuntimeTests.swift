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
                verboseLogs: false
            ),
            resolver: nil
        )

        try await client.open()

        let sourceInfo = try await client.sourceInfo()
        XCTAssertFalse(sourceInfo.currentSourceId.isEmpty)

        do {
            _ = try await client.syncStatus()
            XCTFail("Expected syncStatus() to require attach")
        } catch {
            XCTAssertTrue(String(describing: error).lowercased().contains("attach"))
        }

        client.close()
        try await core.close()
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
}
