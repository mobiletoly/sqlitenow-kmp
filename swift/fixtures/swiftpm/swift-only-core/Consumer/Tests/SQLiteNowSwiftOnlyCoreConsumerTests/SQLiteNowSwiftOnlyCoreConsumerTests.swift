import Foundation
@testable import SwiftOnlyCoreDatabaseSQLiteNow
import XCTest

final class SQLiteNowSwiftOnlyCoreConsumerTests: XCTestCase {
    func testGeneratedCoreDatabaseCrud() async throws {
        let db = SwiftOnlyCoreDatabase(path: temporaryDatabaseURL())
        try await db.open()
        try await withAsyncCleanup({ try await db.close() }) {
            let seeded = try await db.person.selectAll().list()
            XCTAssertEqual(seeded.map(\.name), ["Ada"])

            try await db.person.insert(PersonInsertParams(id: 2, name: "Grace"))

            let allRows = try await db.person.selectAll().list()
            XCTAssertEqual(allRows.map(\.name), ["Ada", "Grace"])
        }
    }

    private func temporaryDatabaseURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlitenow-swift-only-core-\(UUID().uuidString).db")
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
