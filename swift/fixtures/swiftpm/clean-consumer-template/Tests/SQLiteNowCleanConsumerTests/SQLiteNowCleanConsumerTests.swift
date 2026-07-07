import Foundation
import CleanConsumerCoreDatabaseSQLiteNow
import CleanConsumerSyncDatabaseSQLiteNow
import SQLiteNowCoreSupport
import SQLiteNowSyncSupport
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
            await assertCoreSQLiteError {
                try await coreDb.person.insert(PersonInsertParams(id: 1, name: "Duplicate"))
            }
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
            await assertSyncMisuseError {
                _ = try await sync.syncStatus()
            }

            var progressIterator = sync.progress().makeAsyncIterator()
            let firstProgress = try await progressIterator.next()
            XCTAssertEqual(firstProgress, .idle)
        }

        let generic = MessageOnlyError(description: "sqlite constraint auth open")
        XCTAssertEqual(
            SQLiteNowCoreSupport.SQLiteNowError.from(generic),
            SQLiteNowCoreSupport.SQLiteNowError.unknown(message: generic.description)
        )
        XCTAssertEqual(
            SQLiteNowSyncSupport.SQLiteNowError.from(generic),
            SQLiteNowSyncSupport.SQLiteNowError.unknown(message: generic.description)
        )
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

    private func assertCoreSQLiteError(_ operation: () async throws -> Void) async {
        do {
            try await operation()
            XCTFail("Expected SQLiteNowCoreSupport.SQLiteNowError.sqlite")
        } catch let error as SQLiteNowCoreSupport.SQLiteNowError {
            guard case .sqlite = error else {
                XCTFail("Expected SQLiteNowCoreSupport.SQLiteNowError.sqlite, got \(error)")
                return
            }
        } catch {
            XCTFail("Expected SQLiteNowCoreSupport.SQLiteNowError.sqlite, got \(type(of: error)): \(error)")
        }
    }

    private func assertSyncMisuseError(_ operation: () async throws -> Void) async {
        do {
            try await operation()
            XCTFail("Expected SQLiteNowSyncSupport.SQLiteNowError.misuse")
        } catch let error as SQLiteNowSyncSupport.SQLiteNowError {
            guard case .misuse = error else {
                XCTFail("Expected SQLiteNowSyncSupport.SQLiteNowError.misuse, got \(error)")
                return
            }
        } catch {
            XCTFail("Expected SQLiteNowSyncSupport.SQLiteNowError.misuse, got \(type(of: error)): \(error)")
        }
    }
}

private struct MessageOnlyError: Error, CustomStringConvertible {
    let description: String
}
