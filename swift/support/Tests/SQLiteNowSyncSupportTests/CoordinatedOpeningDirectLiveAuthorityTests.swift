import Foundation
@preconcurrency import SQLiteNowSyncRuntime
@testable import SQLiteNowSyncSupport
import XCTest

final class CoordinatedOpeningDirectLiveAuthorityTests: XCTestCase {
    func testDirectSwiftTwoDeviceGenerationOneToTwoJourney() async throws {
        guard let rawBaseURL = ProcessInfo.processInfo.environment["COORDINATED_OPENING_AUTHORITY_BASE_URL"]?
            .trimmingCharacters(in: .whitespacesAndNewlines),
            !rawBaseURL.isEmpty,
            let baseURL = URL(string: rawBaseURL)
        else {
            throw XCTSkip("Set COORDINATED_OPENING_AUTHORITY_BASE_URL to run the direct Swift live-authority pilot.")
        }

        let userId = "coordinatedopening-direct-swift-\(UUID().uuidString.lowercased())"
        let token = try await issueToken(baseURL: baseURL, userId: userId)
        let auth = SQLiteNowSyncAuth.bearer(token: token)
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlitenow-coordinatedopening-direct-swift-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let pathA = directory.appendingPathComponent("device-a.sqlite3")
        let pathB = directory.appendingPathComponent("device-b.sqlite3")
        let coreA = makeCore(path: pathA, generation: 1)
        let coreB = makeCore(path: pathB, generation: 1)
        var openingA: SQLiteNowSyncOpening<SQLiteNowCoreRuntimeDatabase>?
        var openingB: SQLiteNowSyncOpening<SQLiteNowCoreRuntimeDatabase>?
        var targetOpening: SQLiteNowSyncOpening<SQLiteNowCoreRuntimeDatabase>?

        do {
            openingA = try await open(coreA, baseURL: baseURL, auth: auth, generation: 1)
            let readyA = try await XCTUnwrap(openingA).advance()
            XCTAssertEqual(readyA.outcome, .ready)
            let clientA = try XCTUnwrap(openingA?.readyOrNil()?.client)
            try requireConnected(try await clientA.attach(userId: userId))

            try await execute(coreA, "INSERT INTO drafts(id,body) VALUES(?,?)", [
                .init(textValue: "draft-1"), .init(dataValue: Data([0, 1, 2, 255])),
            ], affected: ["drafts"])
            try await insertNote(coreA, id: Self.conflictId, title: "base-conflict")
            try await insertNote(coreA, id: Self.deleteId, title: "base-delete")
            _ = try await clientA.sync()

            openingB = try await open(coreB, baseURL: baseURL, auth: auth, generation: 1)
            let readyB = try await XCTUnwrap(openingB).advance()
            XCTAssertEqual(readyB.outcome, .ready)
            let clientB = try XCTUnwrap(openingB?.readyOrNil()?.client)
            try requireConnected(try await clientB.attach(userId: userId))
            let restoredRows = try await noteRows(coreB)
            XCTAssertEqual(restoredRows.count, 2)

            try await updateNote(coreB, id: Self.conflictId, title: "server-wins")
            _ = try await clientB.sync()

            try await updateNote(coreA, id: Self.conflictId, title: "client-conflict")
            try await execute(coreA, "DELETE FROM notes WHERE id=?", [
                .init(textValue: Self.deleteId),
            ], affected: ["notes"])
            try await insertNote(coreA, id: Self.insertId, title: "client-insert")
            try await openingA?.close()
            openingA = nil

            let callbackCalls = DirectLockedCounter()
            let targetCore = makeCore(
                path: pathA,
                generation: 2,
                onMigrationStep: SQLiteNowMigrationCallbackAdapter { _ in callbackCalls.increment() }
            )
            targetOpening = try await open(
                targetCore,
                baseURL: baseURL,
                auth: auth,
                generation: 2,
                upgradePlan: .init(
                    migrationId: "coordinatedopening-notes-v1-v2",
                    fromGenerationId: 1,
                    toGenerationId: 2,
                    fromLocalVersion: 1,
                    toLocalVersion: 2,
                    preservedLocalTables: ["drafts"]
                )
            )
            XCTAssertNil(targetOpening?.readyOrNil())
            let paused = try await XCTUnwrap(targetOpening).advance()
            XCTAssertEqual(paused.outcome, .needsResolution)
            let pendingConflict = try await targetOpening?.pendingConflict()
            let conflict = try XCTUnwrap(pendingConflict)
            XCTAssertEqual(conflict.conflict.table, "notes")

            try await insertNote(coreB, id: Self.oldDeviceId, title: "old-device-live")
            _ = try await clientB.sync()
            let oldDeviceRows = try await noteRows(coreB)
            XCTAssertTrue(oldDeviceRows.contains { $0.id == Self.oldDeviceId })

            try await targetOpening?.resolveConflict(conflict, result: .acceptServer)
            let targetReady = try await XCTUnwrap(targetOpening).advance()
            XCTAssertEqual(targetReady.outcome, .ready)
            XCTAssertEqual(callbackCalls.value, 1)

            let targetRows = try await noteRows(targetCore, includeDetails: true)
            let rows = Dictionary(uniqueKeysWithValues: targetRows.map { ($0.id, $0) })
            XCTAssertEqual(rows[Self.conflictId]?.title, "server-wins")
            XCTAssertNil(rows[Self.deleteId])
            XCTAssertEqual(rows[Self.insertId]?.title, "client-insert")
            XCTAssertNil(rows[Self.oldDeviceId]?.details)
            let drafts = try await targetCore.query(
                sql: "SELECT id, body FROM drafts ORDER BY id",
                bindValues: [],
                columnTypes: ["text", "blob"]
            )
            XCTAssertEqual(drafts.count, 1)
            XCTAssertEqual(drafts.rowAt(index: 0).cellAt(index: 0).textValue, "draft-1")
            XCTAssertEqual(
                drafts.rowAt(index: 0).cellAt(index: 1).dataValue.map { $0 as Data },
                Data([0, 1, 2, 255])
            )
            print("COORDINATED_OPENING-PILOT: direct Swift generation 1 to 2 journey ready with exact preservation")

            try await targetOpening?.close()
            targetOpening = nil
            try await openingB?.close()
            openingB = nil
            try FileManager.default.removeItem(at: directory)
        } catch {
            try? await targetOpening?.close()
            try? await openingA?.close()
            try? await openingB?.close()
            try? FileManager.default.removeItem(at: directory)
            throw error
        }
    }

    private func open(
        _ core: SQLiteNowCoreRuntimeDatabase,
        baseURL: URL,
        auth: SQLiteNowSyncAuth,
        generation: Int64,
        upgradePlan: SQLiteNowSyncUpgradePlan? = nil
    ) async throws -> SQLiteNowSyncOpening<SQLiteNowCoreRuntimeDatabase> {
        let config = SQLiteNowSyncConfig(
            generationId: generation,
            databaseId: Self.databaseId,
            serviceId: Self.serviceId,
            transferTargets: .init(maxPushRows: 8, maxPullBundles: 8, maxSnapshotChunkRows: 8)
        )
        let runtime = try await SQLiteNowSyncRuntimeKt.sqliteNowSyncRuntimeOpenForSync(
            coreDatabase: core,
            baseUrl: baseURL.absoluteString,
            auth: auth.runtimeAuth,
            config: try config.runtimeConfig(syncTables: [
                .init(tableName: "notes", syncKeyColumnName: "id"),
            ]),
            resolver: nil,
            upgradePlan: upgradePlan?.runtimePlan
        )
        return SQLiteNowSyncOpening(
            runtime: runtime,
            database: core,
            lease: SQLiteNowSyncClientLease()
        )
    }

    private func makeCore(
        path: URL,
        generation: Int,
        onMigrationStep: SQLiteNowCoreRuntimeMigrationCallback? = nil
    ) -> SQLiteNowCoreRuntimeDatabase {
        let notes = generation == 1
            ? "CREATE TABLE notes (id TEXT PRIMARY KEY NOT NULL, title TEXT NOT NULL)"
            : "CREATE TABLE notes (id TEXT PRIMARY KEY NOT NULL, title TEXT NOT NULL, details TEXT)"
        var steps = [
            SQLiteNowCoreRuntimeMigrationStep(
                version: 1,
                sql: [
                    "CREATE TABLE notes (id TEXT PRIMARY KEY NOT NULL, title TEXT NOT NULL)",
                    "CREATE TABLE drafts (id TEXT PRIMARY KEY NOT NULL, body BLOB NOT NULL)",
                ]
            ),
        ]
        if generation == 2 {
            steps.append(.init(version: 2, sql: ["ALTER TABLE notes ADD COLUMN details TEXT"]))
        }
        return SQLiteNowCoreRuntimeDatabase(
            path: path.path,
            migrationPlan: .init(
                latestVersion: Int64(generation),
                schemaSql: [
                    notes,
                    "CREATE TABLE drafts (id TEXT PRIMARY KEY NOT NULL, body BLOB NOT NULL)",
                ],
                initSql: [],
                migrationSteps: steps
            ),
            debug: false,
            onMigrationStep: onMigrationStep,
            syncManaged: true
        )
    }

    private func insertNote(_ core: SQLiteNowCoreRuntimeDatabase, id: String, title: String) async throws {
        try await execute(core, "INSERT INTO notes(id,title) VALUES(?,?)", [
            .init(textValue: id), .init(textValue: title),
        ], affected: ["notes"])
    }

    private func updateNote(_ core: SQLiteNowCoreRuntimeDatabase, id: String, title: String) async throws {
        try await execute(core, "UPDATE notes SET title=? WHERE id=?", [
            .init(textValue: title), .init(textValue: id),
        ], affected: ["notes"])
    }

    private func execute(
        _ core: SQLiteNowCoreRuntimeDatabase,
        _ sql: String,
        _ values: [SQLiteNowCoreRuntimeBindValue],
        affected: [String]
    ) async throws {
        try await core.execute(sql: sql, bindValues: values, affectedTables: affected)
    }

    private func noteRows(
        _ core: SQLiteNowCoreRuntimeDatabase,
        includeDetails: Bool = false
    ) async throws -> [DirectNoteRow] {
        let sql = includeDetails
            ? "SELECT id, title, details FROM notes ORDER BY id"
            : "SELECT id, title FROM notes ORDER BY id"
        let columnTypes = includeDetails ? ["text", "text", "text"] : ["text", "text"]
        let rows = try await core.query(
            sql: sql,
            bindValues: [],
            columnTypes: columnTypes
        )
        return (0..<rows.count).map { index in
            let row = rows.rowAt(index: index)
            return DirectNoteRow(
                id: row.cellAt(index: 0).textValue ?? "",
                title: row.cellAt(index: 1).textValue ?? "",
                details: includeDetails ? row.cellAt(index: 2).textValue : nil
            )
        }
    }

    private func issueToken(baseURL: URL, userId: String) async throws -> String {
        var request = URLRequest(url: baseURL.appendingPathComponent("dummy-signin"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONSerialization.data(withJSONObject: [
            "user": userId, "password": "anything", "device": "direct-swift-pilot",
        ])
        let (data, response) = try await URLSession.shared.data(for: request)
        XCTAssertEqual(try XCTUnwrap(response as? HTTPURLResponse).statusCode, 200)
        let payload = try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: Any])
        return try XCTUnwrap(payload["token"] as? String)
    }

    private func requireConnected(_ result: SQLiteNowAttachResult) throws {
        guard case .connected = result else {
            throw DirectPilotError.unexpectedAttach
        }
    }

    private static let databaseId = "coordinatedopening-live-pilot"
    private static let serviceId = "coordinatedopening-authority"
    private static let conflictId = "11111111-1111-4111-8111-111111111111"
    private static let deleteId = "22222222-2222-4222-8222-222222222222"
    private static let insertId = "33333333-3333-4333-8333-333333333333"
    private static let oldDeviceId = "44444444-4444-4444-8444-444444444444"
}

private struct DirectNoteRow {
    let id: String
    let title: String
    let details: String?
}

private final class DirectLockedCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var storage = 0
    func increment() { lock.withLock { storage += 1 } }
    var value: Int { lock.withLock { storage } }
}

private enum DirectPilotError: Error {
    case unexpectedAttach
}
