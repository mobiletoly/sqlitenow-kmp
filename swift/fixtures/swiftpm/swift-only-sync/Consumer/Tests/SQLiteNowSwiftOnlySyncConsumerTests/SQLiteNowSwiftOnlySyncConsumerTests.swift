import Foundation
@testable import SwiftOnlySyncDatabaseSQLiteNow
import XCTest

final class SQLiteNowSwiftOnlySyncConsumerTests: XCTestCase {
    func testGeneratedSyncDatabaseCrudUsesLocalRuntime() async throws {
        let db = SwiftOnlySyncDatabase(path: temporaryDatabaseURL())
        let opening = try await openForSync(db)
        try await withAsyncCleanup({ try await opening.close() }) {
            let emptyRows = try await db.docs.selectAll().list()
            XCTAssertEqual(emptyRows, [])

            try await db.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "doc-1", title: "First"))
            try await db.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "doc-1", title: "First replaced"))
            try await db.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "doc-2", title: "Second"))
            let insertedRows = try await db.docs.selectAll().list()
            XCTAssertEqual(insertedRows, [
                DocRow(docId: "doc-1", title: "First replaced"),
                DocRow(docId: "doc-2", title: "Second"),
            ])

            try await db.docs.updateTitle(DocsUpdateTitleParams(title: "Updated", docId: "doc-1"))
            let updatedRows = try await db.docs.selectAll().list()
            XCTAssertEqual(updatedRows.first?.title, "Updated")

            try await db.docs.deleteById(DocsDeleteByIdParams(docId: "doc-2"))
            let remainingRows = try await db.docs.selectAll().list()
            XCTAssertEqual(remainingRows, [
                DocRow(docId: "doc-1", title: "Updated"),
            ])
        }
    }

    func testGeneratedSyncClientOpensLocally() async throws {
        let db = SwiftOnlySyncDatabase(path: temporaryDatabaseURL())
        let opening = try await openForSync(db)
        try await withAsyncCleanup({ try await opening.close() }) {
            let sync = try XCTUnwrap(opening.readyOrNil()?.client)

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
        let db = SwiftOnlySyncDatabase(path: temporaryDatabaseURL())
        let opening = try await openForSync(db)
        try await withAsyncCleanup({ try await opening.close() }) {
            let sync = try XCTUnwrap(opening.readyOrNil()?.client)

            var iterator = sync.progress().makeAsyncIterator()
            let first = try await iterator.next()
            XCTAssertEqual(first, .idle)
        }
    }

    private func openForSync(
        _ db: SwiftOnlySyncDatabase
    ) async throws -> SQLiteNowSyncOpening<SwiftOnlySyncDatabase> {
        let opening = try await db.openForSync(
            baseURL: URL(string: "http://127.0.0.1:1")!,
            auth: .bearer(token: "fixture-token"),
            config: .init(
                generationId: 1,
                databaseId: "swift-sync-fixture",
                serviceId: "nethttp-server-example"
            )
        )
        let status = try await opening.advance()
        XCTAssertEqual(status.outcome, .ready)
        XCTAssertNotNil(opening.readyOrNil())
        return opening
    }

    private func temporaryDatabaseURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlitenow-swift-only-sync-\(UUID().uuidString).db")
    }

    private func withAsyncCleanup(
        _ cleanup: () async throws -> Void,
        operation: () async throws -> Void
    ) async throws {
        do {
            try await operation()
            try await cleanup()
        } catch {
            try? await cleanup()
            throw error
        }
    }
}
