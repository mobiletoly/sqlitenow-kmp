import Foundation
@preconcurrency import SQLiteNowCoreRuntime
import XCTest

final class SQLiteNowCoreRuntimeTests: XCTestCase {
    func testOpenExecuteQueryTransactionAndBlobRoundTrip() async throws {
        let db = SQLiteNowCoreRuntimeDatabase(
            path: temporaryDatabaseURL().path,
            migrationPlan: Self.migrationPlan(),
            debug: false
        )

        try await db.open()

        let initial = try await db.query(
            sql: "SELECT id, title, is_done, payload FROM runtime_item ORDER BY id",
            bindValues: [],
            columnTypes: Self.columnTypes
        )
        XCTAssertEqual(initial.count, 1)
        XCTAssertEqual(initial.rowAt(index: 0).cellAt(index: 1).textValue, "seed")

        let payload = Data([0x01, 0x02, 0xA0])
        try await db.execute(
            sql: "INSERT INTO runtime_item (id, title, is_done, payload) VALUES (?, ?, ?, ?)",
            bindValues: [
                SQLiteNowCoreRuntimeBindValue(int64Value: 2),
                SQLiteNowCoreRuntimeBindValue(textValue: "inserted"),
                SQLiteNowCoreRuntimeBindValue(boolValue: true),
                SQLiteNowCoreRuntimeBindValue(dataValue: payload),
            ],
            affectedTables: ["runtime_item"]
        )

        let inserted = try await db.query(
            sql: "SELECT id, title, is_done, payload FROM runtime_item WHERE id = ?",
            bindValues: [SQLiteNowCoreRuntimeBindValue(int64Value: 2)],
            columnTypes: Self.columnTypes
        )
        XCTAssertEqual(inserted.count, 1)
        let insertedRow = inserted.rowAt(index: 0)
        XCTAssertEqual(insertedRow.cellAt(index: 0).int64Value, 2)
        XCTAssertEqual(insertedRow.cellAt(index: 1).textValue, "inserted")
        XCTAssertEqual(insertedRow.cellAt(index: 2).boolValue, true)
        XCTAssertEqual(insertedRow.cellAt(index: 3).dataValue, payload)

        let batch = SQLiteNowCoreRuntimeMutationBatch()
        batch.add(
            sql: "UPDATE runtime_item SET title = ? WHERE id = ?",
            bindValues: [
                SQLiteNowCoreRuntimeBindValue(textValue: "updated"),
                SQLiteNowCoreRuntimeBindValue(int64Value: 2),
            ],
            affectedTables: ["runtime_item"]
        )
        try await db.transaction(batch: batch)

        let updated = try await db.query(
            sql: "SELECT id, title, is_done, payload FROM runtime_item WHERE id = ?",
            bindValues: [SQLiteNowCoreRuntimeBindValue(int64Value: 2)],
            columnTypes: Self.columnTypes
        )
        XCTAssertEqual(updated.rowAt(index: 0).cellAt(index: 1).textValue, "updated")

        try await db.close()
    }

    func testCancelingTableObservationDoesNotReportError() async throws {
        let db = SQLiteNowCoreRuntimeDatabase(
            path: temporaryDatabaseURL().path,
            migrationPlan: Self.migrationPlan(),
            debug: false
        )
        try await db.open()

        let observer = RecordingTableObserver()
        let handle = db.observeTables(tableNames: ["runtime_item"], observer: observer)
        handle.cancel()

        try await Task.sleep(nanoseconds: 100_000_000)

        XCTAssertEqual(observer.errorCount, 0)
        try await db.close()
    }

    private static let columnTypes = ["int64", "text", "bool", "blob"]

    private static func migrationPlan() -> SQLiteNowCoreRuntimeMigrationPlan {
        SQLiteNowCoreRuntimeMigrationPlan(
            latestVersion: 1,
            schemaSql: [
                """
                CREATE TABLE runtime_item (
                    id INTEGER PRIMARY KEY NOT NULL,
                    title TEXT NOT NULL,
                    is_done INTEGER NOT NULL DEFAULT 0,
                    payload BLOB
                );
                """
            ],
            initSql: [
                """
                INSERT INTO runtime_item (id, title, is_done, payload)
                VALUES (1, 'seed', 0, NULL);
                """
            ],
            migrationSteps: []
        )
    }

    private func temporaryDatabaseURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlitenow-core-runtime-smoke-\(UUID().uuidString).db")
    }
}

private final class RecordingTableObserver: SQLiteNowCoreRuntimeTableObserver, @unchecked Sendable {
    private let lock = NSLock()
    private var changedCount = 0
    private var errors: [SQLiteNowCoreRuntimeErrorPayload] = []

    var errorCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return errors.count
    }

    func onChanged() {
        lock.lock()
        changedCount += 1
        lock.unlock()
    }

    func onError(payload: SQLiteNowCoreRuntimeErrorPayload) {
        lock.lock()
        errors.append(payload)
        lock.unlock()
    }
}
