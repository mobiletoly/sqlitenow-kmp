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

    func testTableObservationIsActiveWhenObserveReturns() async throws {
        let db = SQLiteNowCoreRuntimeDatabase(
            path: temporaryDatabaseURL().path,
            migrationPlan: Self.migrationPlan(),
            debug: false
        )
        try await db.open()

        let changed = expectation(description: "table observer receives immediate invalidation")
        let observer = RecordingTableObserver(onChanged: {
            changed.fulfill()
        })
        let handle = db.observeTables(
            tableNames: ["runtime_item", "unrelated_table"],
            observer: observer
        )

        try await db.execute(
            sql: "UPDATE runtime_item SET is_done = 1 WHERE id = 1",
            bindValues: [],
            affectedTables: ["runtime_item"]
        )

        let result = await XCTWaiter.fulfillment(of: [changed], timeout: 5)
        XCTAssertEqual(result, .completed)
        handle.cancel()
        try await db.close()
    }

    func testRuntimeExceptionPayloadIsReachableFromSwift() async throws {
        let db = SQLiteNowCoreRuntimeDatabase(
            path: temporaryDatabaseURL().path,
            migrationPlan: Self.migrationPlan(),
            debug: false
        )
        try await db.open()

        do {
            try await db.execute(
                sql: "INSERT INTO runtime_item (id, title, is_done, payload) VALUES (?, ?, ?, ?)",
                bindValues: [
                    SQLiteNowCoreRuntimeBindValue(int64Value: 1),
                    SQLiteNowCoreRuntimeBindValue(textValue: "duplicate"),
                    SQLiteNowCoreRuntimeBindValue(boolValue: false),
                    SQLiteNowCoreRuntimeBindValue(),
                ],
                affectedTables: ["runtime_item"]
            )
            XCTFail("Expected duplicate primary key insert to throw SQLiteNowCoreRuntimeException")
        } catch {
            guard let runtimeError = Self.coreRuntimeException(from: error) else {
                let nsError = error as NSError
                XCTFail("Expected SQLiteNowCoreRuntimeException payload, got \(type(of: error)): \(nsError)")
                try await db.close()
                return
            }
            XCTAssertEqual(runtimeError.payload.category, "sqlite")
            XCTAssertFalse(runtimeError.payload.code.isEmpty)
            XCTAssertFalse(runtimeError.payload.message.isEmpty)
        }

        try await db.close()
    }

    func testMigrationFailurePayloadIsReachableFromSwift() async throws {
        let db = SQLiteNowCoreRuntimeDatabase(
            path: temporaryDatabaseURL().path,
            migrationPlan: SQLiteNowCoreRuntimeMigrationPlan(
                latestVersion: 1,
                schemaSql: ["CREATE TABLE broken_schema ("],
                initSql: [],
                migrationSteps: []
            ),
            debug: false
        )

        do {
            try await db.open()
            XCTFail("Expected invalid schema migration to throw SQLiteNowCoreRuntimeException")
        } catch {
            guard let runtimeError = Self.coreRuntimeException(from: error) else {
                let nsError = error as NSError
                XCTFail("Expected SQLiteNowCoreRuntimeException payload, got \(type(of: error)): \(nsError)")
                return
            }
            XCTAssertEqual(runtimeError.payload.category, "migration")
            XCTAssertFalse(runtimeError.payload.code.isEmpty)
            XCTAssertFalse(runtimeError.payload.message.isEmpty)
        }
    }

    func testAsyncMigrationCallbackTransformsRowsAtEveryBoundary() async throws {
        let url = temporaryDatabaseURL()
        try await bootstrapVersionOnePersonDatabase(at: url)
        let boundaries = LockedStrings()
        let callback = RuntimeMigrationCallback { scope in
            boundaries.append("\(scope.fromVersion)->\(scope.toVersion)")
            guard scope.toVersion == 2 else { return }
            let rows = try await scope.query(
                sql: "SELECT id, full_name FROM migration_person",
                bindValues: [],
                columnTypes: ["int64", "text"]
            )
            let row = rows.rowAt(index: 0)
            let names = try XCTUnwrap(row.cellAt(index: 1).textValue).split(separator: " ", maxSplits: 1)
            try await scope.execute(
                sql: "UPDATE migration_person SET first_name = ?, last_name = ? WHERE id = ?",
                bindValues: [
                    SQLiteNowCoreRuntimeBindValue(textValue: String(names[0])),
                    SQLiteNowCoreRuntimeBindValue(textValue: String(names[1])),
                    SQLiteNowCoreRuntimeBindValue(int64Value: row.cellAt(index: 0).int64Value),
                ]
            )
        }
        let db = SQLiteNowCoreRuntimeDatabase(
            path: url.path,
            migrationPlan: Self.personMigrationPlan(),
            debug: false,
            onMigrationStep: callback
        )

        try await db.open()
        XCTAssertEqual(boundaries.values, ["1->2", "2->3", "3->4", "4->5"])
        let names = try await db.query(
            sql: "SELECT first_name, last_name FROM migration_person",
            bindValues: [],
            columnTypes: ["text", "text"]
        )
        XCTAssertEqual(names.rowAt(index: 0).cellAt(index: 0).textValue, "Ada")
        XCTAssertEqual(names.rowAt(index: 0).cellAt(index: 1).textValue, "Lovelace")
        try await db.close()

        let reopening = SQLiteNowCoreRuntimeDatabase(
            path: url.path,
            migrationPlan: Self.personMigrationPlan(),
            debug: false,
            onMigrationStep: RuntimeMigrationCallback { _ in throw TestMigrationError.unexpectedCallback }
        )
        try await reopening.open()
        try await reopening.close()
    }

    func testDatabaseNewerThanRuntimeTargetIsNotDowngradedOrMigrated() async throws {
        let url = temporaryDatabaseURL()
        let newestSupportedSQLiteVersion: Int64 = 2_147_483_647
        let bootstrap = SQLiteNowCoreRuntimeDatabase(
            path: url.path,
            migrationPlan: SQLiteNowCoreRuntimeMigrationPlan(
                latestVersion: newestSupportedSQLiteVersion,
                schemaSql: ["CREATE TABLE version_probe (id INTEGER PRIMARY KEY NOT NULL)"],
                initSql: [],
                migrationSteps: []
            ),
            debug: false
        )
        try await bootstrap.open()
        try await bootstrap.close()

        let reopening = SQLiteNowCoreRuntimeDatabase(
            path: url.path,
            migrationPlan: Self.personMigrationPlan(),
            debug: false,
            onMigrationStep: RuntimeMigrationCallback { _ in
                throw TestMigrationError.unexpectedCallback
            }
        )

        try await reopening.open()
        let version = try await reopening.query(
            sql: "PRAGMA user_version",
            bindValues: [],
            columnTypes: ["int64"]
        )
        XCTAssertEqual(
            version.rowAt(index: 0).cellAt(index: 0).int64Value,
            newestSupportedSQLiteVersion
        )
        try await reopening.close()
    }

    func testSwiftCallbackFailureAndCancellationRollBackOwningTransaction() async throws {
        for failure in [TestMigrationError.failed, CancellationError()] as [any Error] {
            let url = temporaryDatabaseURL()
            try await bootstrapVersionOnePersonDatabase(at: url)
            let db = SQLiteNowCoreRuntimeDatabase(
                path: url.path,
                migrationPlan: Self.personMigrationPlan(),
                debug: false,
                onMigrationStep: RuntimeMigrationCallback { scope in
                    if scope.toVersion == 2 {
                        try await scope.execute(
                            sql: "UPDATE migration_person SET first_name = 'Ada', last_name = 'Lovelace'",
                            bindValues: []
                        )
                        throw failure
                    }
                }
            )

            do {
                try await db.open()
                XCTFail("Expected Swift callback failure")
            } catch {
                if failure is CancellationError {
                    XCTAssertEqual(Self.coreRuntimeException(from: error)?.payload.category, "cancelled")
                }
            }

            let verifier = SQLiteNowCoreRuntimeDatabase(
                path: url.path,
                migrationPlan: Self.versionOnePersonPlan(),
                debug: false
            )
            try await verifier.open()
            let version = try await verifier.query(
                sql: "PRAGMA user_version",
                bindValues: [],
                columnTypes: ["int64"]
            )
            XCTAssertEqual(version.rowAt(index: 0).cellAt(index: 0).int64Value, 1)
            let columns = try await verifier.query(
                sql: "PRAGMA table_info(migration_person)",
                bindValues: [],
                columnTypes: ["int64", "text", "text", "int64", "text", "int64"]
            )
            XCTAssertEqual(columns.count, 2)
            try await verifier.close()
        }
    }

    func testMigrationCompletionIgnoresDuplicateTerminalSignals() async throws {
        let url = temporaryDatabaseURL()
        try await bootstrapVersionOnePersonDatabase(at: url)
        let db = SQLiteNowCoreRuntimeDatabase(
            path: url.path,
            migrationPlan: Self.personMigrationPlan(),
            debug: false,
            onMigrationStep: DuplicateCompletionMigrationCallback()
        )

        try await db.open()
        let version = try await db.query(
            sql: "PRAGMA user_version",
            bindValues: [],
            columnTypes: ["int64"]
        )
        XCTAssertEqual(version.rowAt(index: 0).cellAt(index: 0).int64Value, 5)
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

    private static func versionOnePersonPlan() -> SQLiteNowCoreRuntimeMigrationPlan {
        SQLiteNowCoreRuntimeMigrationPlan(
            latestVersion: 1,
            schemaSql: [
                "CREATE TABLE migration_person (id INTEGER PRIMARY KEY NOT NULL, full_name TEXT NOT NULL)"
            ],
            initSql: ["INSERT INTO migration_person(id, full_name) VALUES (1, 'Ada Lovelace')"],
            migrationSteps: []
        )
    }

    private static func personMigrationPlan() -> SQLiteNowCoreRuntimeMigrationPlan {
        SQLiteNowCoreRuntimeMigrationPlan(
            latestVersion: 5,
            schemaSql: [
                "CREATE TABLE migration_person (id INTEGER PRIMARY KEY NOT NULL, first_name TEXT NOT NULL, last_name TEXT NOT NULL)"
            ],
            initSql: [],
            migrationSteps: [
                SQLiteNowCoreRuntimeMigrationStep(
                    version: 2,
                    sql: [
                        "ALTER TABLE migration_person ADD COLUMN first_name TEXT",
                        "ALTER TABLE migration_person ADD COLUMN last_name TEXT",
                    ]
                ),
                SQLiteNowCoreRuntimeMigrationStep(
                    version: 3,
                    sql: ["ALTER TABLE migration_person DROP COLUMN full_name"]
                ),
                SQLiteNowCoreRuntimeMigrationStep(version: 5, sql: []),
            ]
        )
    }

    private func bootstrapVersionOnePersonDatabase(at url: URL) async throws {
        let db = SQLiteNowCoreRuntimeDatabase(
            path: url.path,
            migrationPlan: Self.versionOnePersonPlan(),
            debug: false
        )
        try await db.open()
        try await db.close()
    }

    private func temporaryDatabaseURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlitenow-core-runtime-smoke-\(UUID().uuidString).db")
    }

    private static func coreRuntimeException(from error: Error) -> SQLiteNowCoreRuntimeException? {
        if let runtimeError = error as? SQLiteNowCoreRuntimeException {
            return runtimeError
        }
        return (error as NSError).userInfo["K" + "otlinException"] as? SQLiteNowCoreRuntimeException
    }
}

private enum TestMigrationError: Error {
    case failed
    case unexpectedCallback
}

private final class RuntimeMigrationCallback: SQLiteNowCoreRuntimeMigrationCallback, @unchecked Sendable {
    private let callback: @Sendable (SQLiteNowCoreRuntimeMigrationScope) async throws -> Void

    init(callback: @escaping @Sendable (SQLiteNowCoreRuntimeMigrationScope) async throws -> Void) {
        self.callback = callback
    }

    func onMigrationStep(
        scope: SQLiteNowCoreRuntimeMigrationScope,
        completion: SQLiteNowCoreRuntimeMigrationCompletion
    ) -> SQLiteNowCoreRuntimeMigrationTask {
        let task = Task {
            do {
                try await callback(scope)
                completion.success()
            } catch is CancellationError {
                completion.cancel()
            } catch {
                completion.failure(message: String(describing: error))
            }
        }
        return RuntimeMigrationTask(task: task)
    }
}

private final class RuntimeMigrationTask: SQLiteNowCoreRuntimeMigrationTask, @unchecked Sendable {
    private let task: Task<Void, Never>

    init(task: Task<Void, Never>) {
        self.task = task
    }

    func cancel() {
        task.cancel()
    }
}

private final class DuplicateCompletionMigrationCallback: SQLiteNowCoreRuntimeMigrationCallback, @unchecked Sendable {
    func onMigrationStep(
        scope: SQLiteNowCoreRuntimeMigrationScope,
        completion: SQLiteNowCoreRuntimeMigrationCompletion
    ) -> SQLiteNowCoreRuntimeMigrationTask {
        completion.success()
        completion.failure(message: "late failure")
        return RuntimeMigrationTask(task: Task {})
    }
}

private final class LockedStrings: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [String] = []

    func append(_ value: String) {
        lock.lock()
        storage.append(value)
        lock.unlock()
    }

    var values: [String] {
        lock.lock()
        defer { lock.unlock() }
        return storage
    }
}

private final class RecordingTableObserver: SQLiteNowCoreRuntimeTableObserver, @unchecked Sendable {
    private let lock = NSLock()
    private let onChangedBlock: @Sendable () -> Void
    private var changedCount = 0
    private var errors: [SQLiteNowCoreRuntimeErrorPayload] = []

    init(onChanged: @escaping @Sendable () -> Void = {}) {
        self.onChangedBlock = onChanged
    }

    var errorCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return errors.count
    }

    func onChanged() {
        lock.lock()
        changedCount += 1
        lock.unlock()
        onChangedBlock()
    }

    func onError(payload: SQLiteNowCoreRuntimeErrorPayload) {
        lock.lock()
        errors.append(payload)
        lock.unlock()
    }
}
