import Foundation
import CoordinatedOpeningGenerationOneDatabaseSQLiteNow
import CoordinatedOpeningGenerationTwoDatabaseSQLiteNow
import XCTest

private typealias GenerationOne = CoordinatedOpeningGenerationOneDatabaseSQLiteNow.CoordinatedOpeningGenerationOneDatabase
private typealias GenerationTwo = CoordinatedOpeningGenerationTwoDatabaseSQLiteNow.CoordinatedOpeningGenerationTwoDatabase

final class CoordinatedOpeningGeneratedLiveAuthorityTests: XCTestCase {
    func testGeneratedSwiftTwoDeviceGenerationOneToTwoJourney() async throws {
        guard let rawBaseURL = ProcessInfo.processInfo.environment["COORDINATED_OPENING_AUTHORITY_BASE_URL"]?
            .trimmingCharacters(in: .whitespacesAndNewlines),
            !rawBaseURL.isEmpty,
            let baseURL = URL(string: rawBaseURL)
        else {
            throw XCTSkip("Set COORDINATED_OPENING_AUTHORITY_BASE_URL to run the generated Swift live-authority pilot.")
        }

        let userId = "coordinatedopening-generated-swift-\(UUID().uuidString.lowercased())"
        let token = try await issueToken(baseURL: baseURL, userId: userId)
        let authOne = CoordinatedOpeningGenerationOneDatabaseSQLiteNow.SQLiteNowSyncAuth.bearer(token: token)
        let authTwo = CoordinatedOpeningGenerationTwoDatabaseSQLiteNow.SQLiteNowSyncAuth.bearer(token: token)
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlitenow-coordinatedopening-generated-swift-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let pathA = directory.appendingPathComponent("device-a.sqlite3")
        let pathB = directory.appendingPathComponent("device-b.sqlite3")
        let generationOneA = GenerationOne(path: pathA)
        let generationOneB = GenerationOne(path: pathB)
        var openingA: CoordinatedOpeningGenerationOneDatabaseSQLiteNow.SQLiteNowSyncOpening<GenerationOne>?
        var openingB: CoordinatedOpeningGenerationOneDatabaseSQLiteNow.SQLiteNowSyncOpening<GenerationOne>?
        var targetOpening: CoordinatedOpeningGenerationTwoDatabaseSQLiteNow.SQLiteNowSyncOpening<GenerationTwo>?

        do {
            let configOne = CoordinatedOpeningGenerationOneDatabaseSQLiteNow.SQLiteNowSyncConfig(
                generationId: 1,
                databaseId: Self.databaseId,
                serviceId: Self.serviceId,
                transferTargets: .init(maxPushRows: 8, maxPullBundles: 8, maxSnapshotChunkRows: 8)
            )
            openingA = try await generationOneA.openForSync(
                baseURL: baseURL,
                auth: authOne,
                config: configOne
            )
            XCTAssertNil(openingA?.readyOrNil())
            let openingAHandle = try XCTUnwrap(openingA)
            let readyA = try await openingAHandle.advance()
            XCTAssertEqual(readyA.outcome, .ready)
            let clientA = try generationOneA.makeSyncClient(
                baseURL: baseURL,
                auth: authOne,
                config: configOne
            )
            try requireConnected(try await clientA.attach(userId: userId))
            print("COORDINATED_OPENING-PILOT: generated Swift device A generation 1 ready")

            try await generationOneA.drafts.insert(
                CoordinatedOpeningGenerationOneDatabaseSQLiteNow.DraftsInsertParams(
                    id: "draft-1",
                    body: Data([0, 1, 2, 255])
                )
            )
            try await generationOneA.notes.insert(
                CoordinatedOpeningGenerationOneDatabaseSQLiteNow.NotesInsertParams(
                    id: Self.conflictId,
                    title: "base-conflict"
                )
            )
            try await generationOneA.notes.insert(
                CoordinatedOpeningGenerationOneDatabaseSQLiteNow.NotesInsertParams(
                    id: Self.deleteId,
                    title: "base-delete"
                )
            )
            _ = try await clientA.sync()
            print("COORDINATED_OPENING-PILOT: generated Swift device A seed synchronized")

            openingB = try await generationOneB.openForSync(
                baseURL: baseURL,
                auth: authOne,
                config: configOne
            )
            let openingBHandle = try XCTUnwrap(openingB)
            let readyB = try await openingBHandle.advance()
            XCTAssertEqual(readyB.outcome, .ready)
            let clientB = try generationOneB.makeSyncClient(
                baseURL: baseURL,
                auth: authOne,
                config: configOne
            )
            try requireConnected(try await clientB.attach(userId: userId))
            let restoredRows = try await generationOneB.notes.selectAll().list()
            XCTAssertEqual(restoredRows.count, 2)
            print("COORDINATED_OPENING-PILOT: generated Swift device B generation 1 restored")

            try await generationOneB.notes.updateTitle(
                CoordinatedOpeningGenerationOneDatabaseSQLiteNow.NotesUpdateTitleParams(
                    title: "server-wins",
                    id: Self.conflictId
                )
            )
            _ = try await clientB.sync()

            try await generationOneA.notes.updateTitle(
                CoordinatedOpeningGenerationOneDatabaseSQLiteNow.NotesUpdateTitleParams(
                    title: "client-conflict",
                    id: Self.conflictId
                )
            )
            try await generationOneA.notes.deleteById(
                CoordinatedOpeningGenerationOneDatabaseSQLiteNow.NotesDeleteByIdParams(id: Self.deleteId)
            )
            try await generationOneA.notes.insert(
                CoordinatedOpeningGenerationOneDatabaseSQLiteNow.NotesInsertParams(
                    id: Self.insertId,
                    title: "client-insert"
                )
            )
            try await openingA?.close()
            openingA = nil

            let callbackCalls = LockedCounter()
            let generationTwoA = GenerationTwo(
                path: pathA,
                onMigrationStep: { _ in callbackCalls.increment() }
            )
            let configTwo = CoordinatedOpeningGenerationTwoDatabaseSQLiteNow.SQLiteNowSyncConfig(
                generationId: 2,
                databaseId: Self.databaseId,
                serviceId: Self.serviceId,
                transferTargets: .init(maxPushRows: 8, maxPullBundles: 8, maxSnapshotChunkRows: 8)
            )
            targetOpening = try await generationTwoA.openForSync(
                baseURL: baseURL,
                auth: authTwo,
                config: configTwo,
                upgradePlan: CoordinatedOpeningGenerationTwoDatabaseSQLiteNow.SQLiteNowSyncUpgradePlan(
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
            print("COORDINATED_OPENING-PILOT: generated Swift target returned \(paused.phase ?? "nil")/\(String(describing: paused.outcome))")
            let pendingConflict = try await targetOpening?.pendingConflict()
            let conflict = try XCTUnwrap(pendingConflict)
            XCTAssertEqual(conflict.conflict.table, "notes")

            try await generationOneB.notes.insert(
                CoordinatedOpeningGenerationOneDatabaseSQLiteNow.NotesInsertParams(
                    id: Self.oldDeviceId,
                    title: "old-device-live"
                )
            )
            _ = try await clientB.sync()
            let oldDeviceRows = try await generationOneB.notes.selectAll().list()
            XCTAssertTrue(oldDeviceRows.contains { $0.id == Self.oldDeviceId })
            print("COORDINATED_OPENING-PILOT: generated Swift old device remained writable during target pause")

            try await targetOpening?.resolveConflict(conflict, result: .acceptServer)
            let ready = try await XCTUnwrap(targetOpening).advance()
            XCTAssertEqual(ready.outcome, .ready)
            XCTAssertEqual(ready.phase, "ready")
            _ = try generationTwoA.makeSyncClient(
                baseURL: baseURL,
                auth: authTwo,
                config: configTwo
            )
            XCTAssertEqual(callbackCalls.value, 1)

            let rows = Dictionary(
                uniqueKeysWithValues: try await generationTwoA.notes.selectAll().list().map { ($0.id, $0) }
            )
            XCTAssertEqual(rows[Self.conflictId]?.title, "server-wins")
            XCTAssertNil(rows[Self.deleteId])
            XCTAssertEqual(rows[Self.insertId]?.title, "client-insert")
            XCTAssertNil(try XCTUnwrap(rows[Self.oldDeviceId]).details)
            let preservedDrafts = try await generationTwoA.drafts.selectAll().list()
            XCTAssertEqual(
                preservedDrafts,
                [
                    CoordinatedOpeningGenerationTwoDatabaseSQLiteNow.DraftsSelectAllResult(
                        id: "draft-1",
                        body: Data([0, 1, 2, 255])
                    )
                ]
            )
            print("COORDINATED_OPENING-PILOT: generated Swift generation 2 ready with exact preservation")

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

    private func issueToken(baseURL: URL, userId: String) async throws -> String {
        var request = URLRequest(url: baseURL.appendingPathComponent("dummy-signin"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONEncoder().encode(
            SigninRequest(user: userId, password: "anything", device: "generated-swift-pilot")
        )
        let (data, response) = try await URLSession.shared.data(for: request)
        let status = try XCTUnwrap(response as? HTTPURLResponse).statusCode
        XCTAssertEqual(status, 200)
        return try JSONDecoder().decode(SigninResponse.self, from: data).token
    }

    private func requireConnected(
        _ result: CoordinatedOpeningGenerationOneDatabaseSQLiteNow.SQLiteNowAttachResult,
        file: StaticString = #filePath,
        line: UInt = #line
    ) throws {
        guard case .connected = result else {
            XCTFail("Expected connected attach result, got \(result)", file: file, line: line)
            throw PilotError.unexpectedAttach
        }
    }

    private static let databaseId = "coordinatedopening-live-pilot"
    private static let serviceId = "coordinatedopening-authority"
    private static let conflictId = "11111111-1111-4111-8111-111111111111"
    private static let deleteId = "22222222-2222-4222-8222-222222222222"
    private static let insertId = "33333333-3333-4333-8333-333333333333"
    private static let oldDeviceId = "44444444-4444-4444-8444-444444444444"
}

private final class LockedCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var storage = 0

    func increment() {
        lock.withLock { storage += 1 }
    }

    var value: Int {
        lock.withLock { storage }
    }
}

private struct SigninRequest: Encodable {
    let user: String
    let password: String
    let device: String
}

private struct SigninResponse: Decodable {
    let token: String
}

private enum PilotError: Error {
    case unexpectedAttach
}
