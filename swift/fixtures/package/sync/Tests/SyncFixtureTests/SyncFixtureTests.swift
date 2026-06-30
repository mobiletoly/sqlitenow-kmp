import Foundation
@testable import SyncFixtureDatabaseSQLiteNow
import XCTest

final class SyncFixtureTests: XCTestCase {
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
