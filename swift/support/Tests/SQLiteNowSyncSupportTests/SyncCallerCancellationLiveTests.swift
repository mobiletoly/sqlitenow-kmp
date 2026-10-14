import Foundation
@preconcurrency import SQLiteNowSyncRuntime
@testable import SQLiteNowSyncSupport
import XCTest

final class SyncCallerCancellationLiveTests: XCTestCase, @unchecked Sendable {
    func testCancellingCallerDuringSecondPushJoinsOwnedOperation() async throws {
        guard let rawURL = ProcessInfo.processInfo.environment["OVERSQLITE_SWIFT_CONVERGENT_BASE_URL"],
              let baseURL = URL(string: rawURL) else {
            throw XCTSkip("Set OVERSQLITE_SWIFT_CONVERGENT_BASE_URL for the isolated second-push cancellation server.")
        }
        let userID = "swift-cancel-\(UUID().uuidString.lowercased())"
        let token = try await issueToken(baseURL: baseURL, userID: userID)
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("swift-sync-cancel-\(UUID().uuidString).sqlite3")
        let schema = "CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, email TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
        let core = SQLiteNowCoreRuntimeDatabase(
            path: path.path,
            migrationPlan: .init(latestVersion: 1, schemaSql: [schema], initSql: [],
                                 migrationSteps: [.init(version: 1, sql: [schema])]),
            debug: false, onMigrationStep: nil, syncManaged: true
        )
        let config = SQLiteNowSyncConfig(
            generationId: 1, databaseId: "fixture", serviceId: "business",
            maxSyncWorkUnitsPerInvocation: 1,
            transferTargets: .init(maxPushRows: 1, maxPullBundles: 8, maxSnapshotChunkRows: 8)
        )
        let runtime = try await SQLiteNowSyncRuntimeKt.sqliteNowSyncRuntimeOpenForSync(
            coreDatabase: core, baseUrl: baseURL.absoluteString,
            auth: SQLiteNowSyncAuth.bearer(token: token).runtimeAuth,
            config: try config.runtimeConfig(syncTables: [.init(tableName: "users", syncKeyColumnName: "id")]),
            resolver: nil, upgradePlan: nil
        )
        let opening = SQLiteNowSyncOpening(runtime: runtime, database: core, lease: SQLiteNowSyncClientLease())
        defer { try? FileManager.default.removeItem(at: path) }
        do {
            let ready = try await opening.advance()
            XCTAssertEqual(ready.outcome, .ready)
            let client = try XCTUnwrap(opening.readyOrNil()?.client)
            let clientBox = SyncClientBox(client)
            guard case .connected = try await client.attach(userId: userID) else {
                XCTFail("Attach did not connect")
                return
            }
            for index in 0..<3 {
                try await core.execute(
                    sql: "INSERT INTO users(id,name,email,created_at,updated_at) VALUES(?,?,?,?,?)",
                    bindValues: [
                        .init(textValue: UUID().uuidString.lowercased()),
                        .init(textValue: "Swift \(index)"),
                        .init(textValue: "swift\(index)@example.com"),
                        .init(textValue: "2026-09-24T00:00:00Z"),
                        .init(textValue: "2026-09-24T00:00:00Z"),
                    ], affectedTables: ["users"]
                )
            }

            let initialPushCount = try await pushCount(baseURL: baseURL)
            XCTAssertEqual(initialPushCount, 0, "This test requires a fresh isolated authority.")

            let operation = Task { try await clientBox.client.sync() }
            let deadline = Date().addingTimeInterval(15)
            while try await pushCount(baseURL: baseURL) < 2 && Date() < deadline {
                try await Task.sleep(nanoseconds: 20_000_000)
            }
            let blockedPushCount = try await pushCount(baseURL: baseURL)
            XCTAssertEqual(blockedPushCount, 2)
            operation.cancel()
            do {
                _ = try await operation.value
                XCTFail("Cancelled sync returned success")
            } catch is CancellationError {
            }
            let finalPushCount = try await pushCount(baseURL: baseURL)
            XCTAssertEqual(finalPushCount, 2)
            let status = try await clientBox.client.syncStatus()
            let pending = status.pending
            XCTAssertTrue(pending.hasPendingSyncData)
            XCTAssertGreaterThan(pending.pendingRowCount, 0)
            try await opening.close()
        } catch {
            try? await opening.close()
            throw error
        }
    }

    private func issueToken(baseURL: URL, userID: String) async throws -> String {
        var request = URLRequest(url: baseURL.appendingPathComponent("dummy-signin"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONSerialization.data(withJSONObject: [
            "user": userID, "password": "anything", "device": "swift-cancellation",
        ])
        let (data, response) = try await URLSession.shared.data(for: request)
        XCTAssertEqual(try XCTUnwrap(response as? HTTPURLResponse).statusCode, 200)
        let payload = try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: Any])
        return try XCTUnwrap(payload["token"] as? String)
    }

    private func pushCount(baseURL: URL) async throws -> Int {
        let (data, _) = try await URLSession.shared.data(from: baseURL.appendingPathComponent("__test/pushes"))
        let payload = try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: Int])
        return try XCTUnwrap(payload["pushes"])
    }
}

private final class SyncClientBox: @unchecked Sendable {
    let client: SQLiteNowSyncClient
    init(_ client: SQLiteNowSyncClient) { self.client = client }
}
