import Foundation
import SQLiteNowSyncRuntime
@testable import SyncFixtureDatabaseSQLiteNow
import XCTest

final class SyncFixtureTests: XCTestCase {
    func testSyncConfigurationDefaultsAndCustomValues() {
        let defaults = SQLiteNowSyncConfig()
        XCTAssertEqual(defaults.schema, "main")
        XCTAssertEqual(defaults.uploadLimit, 200)
        XCTAssertEqual(defaults.downloadLimit, 1_000)
        XCTAssertEqual(defaults.snapshotChunkRows, 1_000)
        XCTAssertEqual(defaults.snapshotChunkBytes, 4 * 1_024 * 1_024)
        XCTAssertEqual(defaults.snapshotApplyBatchRows, 256)
        XCTAssertEqual(defaults.snapshotApplyBatchBytes, 4 * 1_024 * 1_024)
        XCTAssertFalse(defaults.verboseLogs)
        XCTAssertEqual(defaults.transientRetryPolicy, .init())
        XCTAssertEqual(defaults.snapshotCapacityRetryPolicy, .init())

        let custom = SQLiteNowSyncConfig(
            schema: "business",
            uploadLimit: 11,
            downloadLimit: 12,
            snapshotChunkRows: 13,
            snapshotChunkBytes: 14,
            snapshotApplyBatchRows: 15,
            snapshotApplyBatchBytes: 16,
            verboseLogs: true,
            transientRetryPolicy: .init(maxAttempts: 17, initialBackoffMillis: 18, maxBackoffMillis: 19, jitterRatio: 0.3),
            snapshotCapacityRetryPolicy: .init(enabled: false, maxWaitMillis: 20, fallbackDelayMillis: 21, jitterRatio: 0.4)
        )
        XCTAssertEqual(custom.schema, "business")
        XCTAssertEqual(custom.snapshotChunkRows, 13)
        XCTAssertEqual(custom.transientRetryPolicy.maxAttempts, 17)
        XCTAssertEqual(custom.snapshotCapacityRetryPolicy.fallbackDelayMillis, 21)
    }

    func testSyncConfigurationRejectsInvalidBoundaryValues() async throws {
        try await withOpenDatabase { db in
            let invalidCases: [(String, SQLiteNowSyncConfig)] = [
                ("upload Int overflow", .init(uploadLimit: Int.max)),
                ("download Int overflow", .init(downloadLimit: Int.max)),
                ("snapshot chunk rows nonpositive", .init(snapshotChunkRows: 0)),
                ("snapshot chunk bytes nonpositive", .init(snapshotChunkBytes: 0)),
                ("snapshot apply rows nonpositive", .init(snapshotApplyBatchRows: 0)),
                ("snapshot apply bytes nonpositive", .init(snapshotApplyBatchBytes: 0)),
                ("transient attempts Int overflow", .init(transientRetryPolicy: .init(maxAttempts: Int.max))),
                ("capacity max wait nonpositive", .init(snapshotCapacityRetryPolicy: .init(maxWaitMillis: 0))),
                ("capacity fallback nonpositive", .init(snapshotCapacityRetryPolicy: .init(fallbackDelayMillis: 0))),
                ("capacity jitter below range", .init(snapshotCapacityRetryPolicy: .init(jitterRatio: -0.1))),
                ("capacity jitter above range", .init(snapshotCapacityRetryPolicy: .init(jitterRatio: 1.1)))
            ]

            for (name, config) in invalidCases {
                XCTAssertThrowsError(
                    try db.makeSyncClient(
                        baseURL: URL(string: "http://127.0.0.1:1")!,
                        auth: .bearer(token: "fixture-token"),
                        config: config
                    ),
                    name
                ) { error in
                    guard let sqliteNowError = error as? SQLiteNowError,
                          case .misuse = sqliteNowError else {
                        return XCTFail("Expected misuse for \(name), got \(error)")
                    }
                }
            }
        }
    }

    func testProtocolPayloadMapsDirectly() {
        let error = SQLiteNowError.from(
            SQLiteNowSyncRuntimeErrorPayload(category: "protocol", code: "wire", message: "bad payload")
        )
        XCTAssertEqual(error, .protocol(message: "bad payload"))
    }

    func testDocsCrudUsesCoreRuntime() async throws {
        try await withOpenDatabase { db in
            let emptyRows = try await db.docs.selectAll().list()
            XCTAssertEqual(emptyRows, [])

            try await db.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "doc-1", title: "First"))
            try await db.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "doc-1", title: "First replaced"))
            try await db.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "doc-2", title: "Second"))
            let insertedRows = try await db.docs.selectAll().list()
            XCTAssertEqual(insertedRows, [
                DocRow(docId: "doc-1", title: "First replaced"),
                DocRow(docId: "doc-2", title: "Second")
            ])

            try await db.docs.updateTitle(DocsUpdateTitleParams(title: "Updated", docId: "doc-1"))
            let updatedRows = try await db.docs.selectAll().list()
            XCTAssertEqual(updatedRows.first?.title, "Updated")

            try await db.docs.deleteById(DocsDeleteByIdParams(docId: "doc-2"))
            let remainingRows = try await db.docs.selectAll().list()
            XCTAssertEqual(remainingRows, [
                DocRow(docId: "doc-1", title: "Updated")
            ])
        }
    }

    func testSyncOpenSourceInfoAndExpectedAttachedError() async throws {
        try await withOpenDatabase { db in
            let sync = try db.makeSyncClient(
                baseURL: URL(string: "http://127.0.0.1:1")!,
                auth: .bearer(token: "fixture-token")
            )
            try await sync.open()
            try await sync.pauseUploads()
            try await sync.resumeUploads()
            try await sync.pauseDownloads()
            try await sync.resumeDownloads()
            defer {
                sync.close()
            }

            let sourceInfo = try await sync.sourceInfo()
            XCTAssertFalse(sourceInfo.currentSourceId.isEmpty)

            do {
                _ = try await sync.syncStatus()
                XCTFail("Expected syncStatus() to require attach")
            } catch {
                XCTAssertTrue(String(describing: error).lowercased().contains("attach"))
            }
        }
    }

    func testProgressStreamEmitsIdle() async throws {
        try await withOpenDatabase { db in
            let sync = try db.makeSyncClient(
                baseURL: URL(string: "http://127.0.0.1:1")!,
                auth: .bearer(token: "fixture-token")
            )
            defer {
                sync.close()
            }

            var iterator = sync.progress().makeAsyncIterator()
            let first = try await iterator.next()
            XCTAssertEqual(first, .idle)
        }
    }

    func testRejectsSecondSyncClientWhileFirstIsActive() async throws {
        try await withOpenDatabase { db in
            let sync = try db.makeSyncClient(
                baseURL: URL(string: "http://127.0.0.1:1")!,
                auth: .bearer(token: "fixture-token")
            )
            defer {
                sync.close()
            }

            do {
                _ = try db.makeSyncClient(
                    baseURL: URL(string: "http://127.0.0.1:1")!,
                    auth: .bearer(token: "fixture-token")
                )
                XCTFail("Expected makeSyncClient to reject a second active client")
            } catch {
                XCTAssertTrue(String(describing: error).contains("Only one active SQLiteNowSyncClient"))
            }
        }
    }

    func testAutomaticDownloadsHandleCanCancel() async throws {
        try await withOpenDatabase { db in
            let sync = try db.makeSyncClient(
                baseURL: URL(string: "http://127.0.0.1:1")!,
                auth: .bearer(token: "fixture-token")
            )
            try await sync.open()
            defer {
                sync.close()
            }

            let handle = sync.startAutomaticDownloads(
                SQLiteNowAutomaticDownloadConfig(automaticDownloadIntervalMillis: 1)
            )
            handle.cancel()
        }
    }

    func testResolverCallbackUsesJsonPayloads() async throws {
        try await withOpenDatabase { db in
            let resolver = SQLiteNowSyncResolver { conflict in
                XCTAssertEqual(conflict.schema, "main")
                XCTAssertEqual(conflict.table, "docs")
                XCTAssertEqual(conflict.keyJson, #"{"doc_id":"doc-1"}"#)
                XCTAssertEqual(conflict.serverRowJson, #"{"title":"Remote"}"#)
                return .keepMerged(payloadJson: #"{"title":"Merged"}"#)
            }

            let sync = try db.makeSyncClient(
                baseURL: URL(string: "http://127.0.0.1:1")!,
                auth: .bearer(token: "fixture-token"),
                resolver: resolver
            )
            defer {
                sync.close()
            }

            let result = sync.resolveForRuntimeSmoke(
                SQLiteNowSyncConflict(
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
            XCTAssertEqual(result, .keepMerged(payloadJson: #"{"title":"Merged"}"#))
        }
    }

    private func temporaryDatabaseURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlitenow-sync-fixture-\(UUID().uuidString).db")
    }

    private func withOpenDatabase<T>(
        _ body: (SyncFixtureDatabase) async throws -> T
    ) async throws -> T {
        let db = SyncFixtureDatabase(path: temporaryDatabaseURL())
        try await db.open()
        do {
            let result = try await body(db)
            try await db.close()
            return result
        } catch {
            try? await db.close()
            throw error
        }
    }
}
