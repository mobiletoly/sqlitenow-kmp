import Foundation
import CleanConsumerCoreDatabaseSQLiteNow
import CleanConsumerSyncDatabaseSQLiteNow
import XCTest

final class SQLiteNowCleanConsumerTests: XCTestCase {
    func testGeneratedPackagesOpenAndExecuteQueries() async throws {
        let coreDb = CleanConsumerCoreDatabase(path: temporaryDatabaseURL(prefix: "core"))
        try await coreDb.open()
        try await withAsyncCleanup({
            try await coreDb.close()
        }) {
            let emptyPeople = try await coreDb.person.selectAll().list()
            XCTAssertEqual(emptyPeople, [])

            try await coreDb.person.insert(PersonInsertParams(id: 1, name: "Ada"))
            let people = try await coreDb.person.selectAll().list()
            XCTAssertEqual(people, [
                PersonSelectAllResult(id: 1, name: "Ada"),
            ])
        }

        let syncDb = SwiftOnlySyncDatabase(path: temporaryDatabaseURL(prefix: "sync"))
        try await syncDb.open()
        try await withAsyncCleanup({
            try await syncDb.close()
        }) {
            let emptyDocs = try await syncDb.docs.selectAll().list()
            XCTAssertEqual(emptyDocs, [])

            try await syncDb.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "doc-1", title: "First"))
            try await syncDb.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "doc-1", title: "First replaced"))
            try await syncDb.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "doc-2", title: "Second"))
            let docs = try await syncDb.docs.selectAll().list()
            XCTAssertEqual(docs, [
                DocRow(docId: "doc-1", title: "First replaced"),
                DocRow(docId: "doc-2", title: "Second"),
            ])

            try await syncDb.docs.updateTitle(DocsUpdateTitleParams(title: "Updated", docId: "doc-1"))
            let updatedDocs = try await syncDb.docs.selectAll().list()
            XCTAssertEqual(updatedDocs.first?.title, "Updated")

            try await syncDb.docs.deleteById(DocsDeleteByIdParams(docId: "doc-2"))
            let remainingDocs = try await syncDb.docs.selectAll().list()
            XCTAssertEqual(remainingDocs, [
                DocRow(docId: "doc-1", title: "Updated"),
            ])

            let sync = try syncDb.makeSyncClient(
                baseURL: URL(string: "http://127.0.0.1:1")!,
                auth: .bearer(token: "fixture-token")
            )
            try await sync.open()
            defer {
                sync.close()
            }

            let sourceInfo = try await sync.sourceInfo()
            XCTAssertFalse(sourceInfo.currentSourceId.isEmpty)

            var progressIterator = sync.progress().makeAsyncIterator()
            let firstProgress = try await progressIterator.next()
            XCTAssertEqual(firstProgress, .idle)
        }
    }

    private func temporaryDatabaseURL(prefix: String) -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlitenow-clean-consumer-\(prefix)-\(UUID().uuidString).db")
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
