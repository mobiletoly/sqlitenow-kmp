@preconcurrency import SQLiteNowCoreRuntime
import Foundation
@testable import CoreFixtureDatabaseSQLiteNow
import XCTest

final class CoreFixtureTests: XCTestCase {
    func testFreshOpenSeedAndCrud() async throws {
        try await withOpenDatabase { db in
            let initialRows = try await db.task.selectAll().list()
            XCTAssertEqual(initialRows, [
                TaskRow(
                    id: 1,
                    title: "Seed task",
                    isDone: false,
                    createdAt: "2026-07-01T00:00:00Z",
                    payload: nil,
                    priority: 0
                )
            ])

            let payload = Data([0x01, 0x02, 0xA0])
            let inserted = try await db.task.insertReturning(
                TaskInsertReturningParams(
                    id: 2,
                    title: "Write fixture",
                    isDone: false,
                    createdAt: "2026-07-01T01:00:00Z",
                    payload: payload,
                    priority: 7
                )
            ).one()
            XCTAssertEqual(inserted.payload, payload)
            XCTAssertEqual(inserted.priority, 7)

            try await db.task.updateDone(TaskUpdateDoneParams(isDone: true, id: 2))
            let updatedTask = try await db.task.selectById(TaskSelectByIdParams(id: 2)).oneOrNull()
            XCTAssertEqual(updatedTask?.isDone, true)

            try await db.task.deleteByIds(TaskDeleteByIdsParams(ids: [2]))
            let deletedTask = try await db.task.selectById(TaskSelectByIdParams(id: 2)).oneOrNull()
            XCTAssertNil(deletedTask)
        }
    }

    func testUpgradeFromVersionOneAddsPriority() async throws {
        let url = temporaryDatabaseURL()
        try await Self.createVersionOneDatabaseForFixture(at: url)

        try await withOpenDatabase(path: url) { db in
            let seed = try await db.task.selectById(TaskSelectByIdParams(id: 1)).one()
            XCTAssertEqual(seed.priority, 0)

            let inserted = try await db.task.insertReturning(
                TaskInsertReturningParams(
                    id: 3,
                    title: "After upgrade",
                    isDone: false,
                    createdAt: "2026-07-01T02:00:00Z",
                    payload: nil,
                    priority: 9
                )
            ).one()
            XCTAssertEqual(inserted.priority, 9)
        }
    }

    func testTransactionRollsBackOnDuplicateFailure() async throws {
        try await withOpenDatabase { db in
            do {
                try await db.transaction { tx in
                    try tx.task.insert(
                        TaskInsertParams(
                            id: 20,
                            title: "Will roll back",
                            isDone: false,
                            createdAt: "2026-07-01T03:00:00Z",
                            payload: nil,
                            priority: 1
                        )
                    )
                    try tx.task.insert(
                        TaskInsertParams(
                            id: 1,
                            title: "Duplicate seed",
                            isDone: false,
                            createdAt: "2026-07-01T04:00:00Z",
                            payload: nil,
                            priority: 1
                        )
                    )
                }
                XCTFail("Expected duplicate primary-key failure")
            } catch {
                let rolledBackTask = try await db.task.selectById(TaskSelectByIdParams(id: 20)).oneOrNull()
                XCTAssertNil(rolledBackTask)
            }
        }
    }

    func testStreamReceivesInvalidation() async throws {
        try await withOpenDatabase { db in
            var iterator = db.task.selectAll().stream().makeAsyncIterator()
            let initial = try await iterator.next()
            XCTAssertEqual(initial?.first?.isDone, false)

            let next = Task {
                try await iterator.next()
            }
            try await db.task.updateDone(TaskUpdateDoneParams(isDone: true, id: 1))
            let updated = try await next.value
            XCTAssertEqual(updated?.first?.isDone, true)
        }
    }

    func testExpectedErrors() async throws {
        let db = makeDatabase()

        do {
            _ = try await db.task.selectAll().list()
            XCTFail("Expected closed database error")
        } catch {
            XCTAssertTrue(String(describing: error).lowercased().contains("open"))
        }

        try await withOpenDatabase(db) { db in
            do {
                _ = try await db.task.selectById(TaskSelectByIdParams(id: 404)).one()
                XCTFail("Expected one() misuse error")
            } catch SQLiteNowError.misuse {
            } catch {
                XCTFail("Expected misuse error, got \(error)")
            }
        }
    }

    func testDynamicCollectionHydration() async throws {
        try await withOpenDatabase { db in
            try await db.person.insert(PersonInsertParams(id: 10, firstName: "Ada"))
            try await db.person.insert(PersonInsertParams(id: 20, firstName: "Grace"))
            try await db.personAddress.insert(
                PersonAddressInsertParams(id: 101, personId: 10, street: "1 Analytical Engine Way", isPrimary: true)
            )
            try await db.personAddress.insert(
                PersonAddressInsertParams(id: 102, personId: 10, street: "2 Compiler Ave", isPrimary: false)
            )
            try await db.comment.insert(CommentInsertParams(id: 201, personId: 10, body: "first"))
            try await db.comment.insert(CommentInsertParams(id: 202, personId: 10, body: "second"))

            let rows = try await db.person.selectAllWithAddresses().list()

            XCTAssertEqual(rows.count, 2)
            XCTAssertEqual(rows[0].personId, 10)
            XCTAssertEqual(rows[0].personFirstName, "Ada")
            XCTAssertEqual(rows[0].addresses.map(\.id), [101, 102])
            XCTAssertEqual(rows[0].addresses.map(\.isPrimary), [true, false])
            XCTAssertEqual(rows[0].comments.map(\.body), ["first", "second"])
            XCTAssertEqual(rows[1].personId, 20)
            XCTAssertEqual(rows[1].personFirstName, "Grace")
            XCTAssertTrue(rows[1].addresses.isEmpty)
            XCTAssertTrue(rows[1].comments.isEmpty)
        }
    }

    func testAdapterBackedParametersAndResults() async throws {
        let recorder = AdapterRecorder()
        let adapters = CoreFixtureDatabaseAdapters(
            sqlValueToStatus: { value in
                recorder.recordFromSql(value)
                return value.lowercased()
            },
            statusToSqlValue: { value in
                recorder.recordToSql(value)
                return value.uppercased()
            }
        )

        try await withOpenDatabase(adapters: adapters) { db in
            try await db.doc.insert(DocInsertParams(id: 1, status: "draft"))
            try await db.doc.insert(DocInsertParams(id: 2, status: "published"))

            let allRows = try await db.doc.selectAll().list()
            XCTAssertEqual(allRows, [
                DocRow(id: 1, status: "draft"),
                DocRow(id: 2, status: "published")
            ])

            let filteredRows = try await db.doc.selectByStatuses(
                DocSelectByStatusesParams(statuses: ["published"])
            ).list()
            XCTAssertEqual(filteredRows, [
                DocRow(id: 2, status: "published")
            ])
        }

        XCTAssertEqual(recorder.toSqlValues, ["draft", "published", "published"])
        XCTAssertEqual(recorder.fromSqlValues, ["DRAFT", "PUBLISHED", "PUBLISHED"])
    }

    func testPackageManifestUsesCoreRuntimeContract() throws {
        let data = try Data(contentsOf: Self.packageManifestURL())
        let manifest = try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: Any])

        XCTAssertEqual(manifest["manifestVersion"] as? Int, 3)
        XCTAssertEqual(manifest["databaseName"] as? String, "CoreFixtureDatabase")
        XCTAssertEqual(manifest["packageName"] as? String, "CoreFixtureDatabaseSQLiteNow")
        XCTAssertEqual(manifest["swiftTargetName"] as? String, "CoreFixtureDatabaseSQLiteNow")
        XCTAssertEqual(manifest["runtimeMode"] as? String, "core")
        XCTAssertEqual(manifest["runtimeBinaryTargets"] as? [String], ["SQLiteNowCoreRuntime"])
        XCTAssertEqual(manifest["requestedAppleTargets"] as? [String], ["macosArm64", "iosArm64", "iosSimulatorArm64"])
        XCTAssertEqual(manifest["frameworkMode"] as? String, "dynamic")

        let minimumPlatforms = try XCTUnwrap(manifest["minimumPlatforms"] as? [String: String])
        XCTAssertEqual(minimumPlatforms["iOS"], "15")
        XCTAssertEqual(minimumPlatforms["macOS"], "14")

        let generatorInputs = try XCTUnwrap(manifest["generatorInputs"] as? [String])
        XCTAssertTrue(generatorInputs.contains("src/commonMain/sql/CoreFixtureDatabase/schema/task.sql"))
        XCTAssertTrue(generatorInputs.contains("runtimeMode=core"))
        XCTAssertTrue(generatorInputs.contains("swiftTargetName=CoreFixtureDatabaseSQLiteNow"))
    }

    private func temporaryDatabaseURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlitenow-core-fixture-\(UUID().uuidString).db")
    }

    private func makeDatabase(
        path: URL? = nil,
        adapters: CoreFixtureDatabaseAdapters = passthroughAdapters()
    ) -> CoreFixtureDatabase {
        CoreFixtureDatabase(path: path ?? temporaryDatabaseURL(), adapters: adapters)
    }

    private func withOpenDatabase<T>(
        path: URL? = nil,
        adapters: CoreFixtureDatabaseAdapters = passthroughAdapters(),
        _ body: (CoreFixtureDatabase) async throws -> T
    ) async throws -> T {
        try await withOpenDatabase(makeDatabase(path: path, adapters: adapters), body)
    }

    private func withOpenDatabase<T>(
        _ db: CoreFixtureDatabase,
        _ body: (CoreFixtureDatabase) async throws -> T
    ) async throws -> T {
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

    private static func passthroughAdapters() -> CoreFixtureDatabaseAdapters {
        CoreFixtureDatabaseAdapters(
            sqlValueToStatus: { $0 },
            statusToSqlValue: { $0 }
        )
    }

    private static func packageManifestURL() -> URL {
        let testFileURL = URL(fileURLWithPath: #filePath)
        let corePackageRoot = testFileURL
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let generatedPackageManifest = corePackageRoot
            .appendingPathComponent(".sqlitenow/package-manifest.json")
        if FileManager.default.fileExists(atPath: generatedPackageManifest.path) {
            return generatedPackageManifest
        }
        return corePackageRoot
            .deletingLastPathComponent()
            .appendingPathComponent("build/swift-package/CoreFixtureDatabaseSQLiteNow/.sqlitenow/package-manifest.json")
    }

    private static func createVersionOneDatabaseForFixture(at path: URL) async throws {
        let runtime = SQLiteNowCoreRuntimeDatabase(
            path: path.path,
            migrationPlan: SQLiteNowCoreRuntimeMigrationPlan(
                latestVersion: 1,
                schemaSql: [
                    """
                    CREATE TABLE task (
                        id INTEGER PRIMARY KEY NOT NULL,
                        title TEXT NOT NULL,
                        is_done INTEGER NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL,
                        payload BLOB
                    );
                    """,
                    "CREATE INDEX idx_task_created_at ON task (created_at, id);"
                ],
                initSql: [
                    """
                    INSERT INTO task (id, title, is_done, created_at, payload)
                    VALUES (1, 'Seed task', 0, '2026-07-01T00:00:00Z', NULL);
                    """
                ],
                migrationSteps: []
            ),
            debug: false
        )
        _ = try await runtime.open()
        _ = try await runtime.close()
    }
}

private final class AdapterRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var toSqlValuesStorage: [String] = []
    private var fromSqlValuesStorage: [String] = []

    var toSqlValues: [String] {
        lock.lock()
        defer { lock.unlock() }
        return toSqlValuesStorage
    }

    var fromSqlValues: [String] {
        lock.lock()
        defer { lock.unlock() }
        return fromSqlValuesStorage
    }

    func recordToSql(_ value: String) {
        lock.lock()
        defer { lock.unlock() }
        toSqlValuesStorage.append(value)
    }

    func recordFromSql(_ value: String) {
        lock.lock()
        defer { lock.unlock() }
        fromSqlValuesStorage.append(value)
    }
}
