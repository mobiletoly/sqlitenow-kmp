import Foundation
@testable import SyncFixtureDatabaseSQLiteNow
import XCTest

final class SyncFixtureRealserverTests: XCTestCase {
    func testFreshReaderRestoresTypedSnapshotExactly() async throws {
        let realserver = try await requireRealserverConfig()
        try await resetRealserver(baseURL: realserver.baseURL)

        let userId = "swift-typed-snapshot-\(UUID().uuidString)"
        let rowId = UUID().uuidString.lowercased()
        let writer = SyncFixtureDatabase(path: temporaryDatabaseURL(prefix: "typed-writer"))
        let reader = SyncFixtureDatabase(path: temporaryDatabaseURL(prefix: "typed-reader"))
        let smallSnapshotConfig = SQLiteNowSyncConfig(
            generationId: 1, databaseId: "swift-sync-fixture", serviceId: "nethttp-server-example",
            transferTargets: .init(
                maxSnapshotChunkRows: 1,
                // The reference server advertises a 4 MiB maximum row, which is also
                // the minimum valid client chunk/apply byte budget.
                maxSnapshotChunkDecodedBytes: 4 * 1_024 * 1_024
            ),
            snapshotApplyBatchRows: 1,
            snapshotApplyBatchBytes: 4 * 1_024 * 1_024
        )
        let writerTokens = MutableTokenStore()
        let readerTokens = MutableTokenStore()
        let writerOpening = try await openForSync(writer, baseURL: realserver.baseURL, config: smallSnapshotConfig, tokens: writerTokens)
        let readerOpening = try await openForSync(reader, baseURL: realserver.baseURL, config: smallSnapshotConfig, tokens: readerTokens)

        try await withOpenings([writerOpening, readerOpening]) {
            let writerSync = try XCTUnwrap(writerOpening.readyOrNil()?.client)
            let readerSync = try XCTUnwrap(readerOpening.readyOrNil()?.client)
            let writerSourceId = try await writerSync.sourceInfo().currentSourceId
            let readerSourceId = try await readerSync.sourceInfo().currentSourceId
            let writerToken = try await issueDummySigninToken(baseURL: realserver.baseURL, userId: userId, sourceId: writerSourceId)
            let readerToken = try await issueDummySigninToken(baseURL: realserver.baseURL, userId: userId, sourceId: readerSourceId)
            writerTokens.set(writerToken)
            readerTokens.set(readerToken)
            try requireConnected(try await writerSync.attach(userId: userId))
            try await writer.typedRows.insert(
                TypedRowsInsertParams(
                    id: rowId,
                    name: "Typed Rich",
                    note: "swift-snapshot",
                    countValue: 9_007_199_254_740_993,
                    smallCount: -32_768,
                    mediumCount: 2_147_483_647,
                    exactAmount: "12345678901234567890.1234567890",
                    enabledFlag: true,
                    rating: 1.25,
                    float4Value: 3.5,
                    data: Data([0xca, 0xfe, 0xba, 0xbe]),
                    createdAt: "2026-03-24T18:42:11Z"
                )
            )
            try await assertPushCommitted(writerSync)

            // The reader remains unattached until the writer's authoritative state is committed.
            let restore = try requireUsedRemoteState(try await readerSync.attach(userId: userId))
            XCTAssertGreaterThan(restore.rowCount, 0)

            let rows = try await reader.typedRows.selectAll().list()
            XCTAssertEqual(rows.count, 1)
            let row = try XCTUnwrap(rows.first)
            XCTAssertEqual(row.id, rowId)
            XCTAssertEqual(row.name, "Typed Rich")
            XCTAssertEqual(row.note, "swift-snapshot")
            XCTAssertEqual(row.countValue, 9_007_199_254_740_993)
            XCTAssertEqual(row.smallCount, -32_768)
            XCTAssertEqual(row.mediumCount, 2_147_483_647)
            XCTAssertEqual(row.exactAmount, "12345678901234567890.1234567890")
            XCTAssertTrue(row.enabledFlag)
            XCTAssertEqual(row.rating, 1.25)
            XCTAssertEqual(row.float4Value, 3.5)
            XCTAssertEqual(row.data, Data([0xca, 0xfe, 0xba, 0xbe]))

            let formatter = ISO8601DateFormatter()
            XCTAssertEqual(
                formatter.date(from: try XCTUnwrap(row.createdAt)),
                formatter.date(from: "2026-03-24T18:42:11Z")
            )
        }
    }

    func testGeneratedSyncPackagePushesAndPullsAgainstRealserver() async throws {
        let config = try await requireRealserverConfig()
        try await resetRealserver(baseURL: config.baseURL)

        let userId = "swift-realserver-user-\(UUID().uuidString)"
        let rowUserId = UUID().uuidString.lowercased()
        let rowPostId = UUID().uuidString.lowercased()
        let rowName = "Swift Realserver User"
        let rowEmail = "swift-realserver-\(UUID().uuidString)@example.com"
        let rowTitle = "Swift Realserver Post"
        let rowContent = "Generated Swift realserver content"
        let updatedRowContent = "Generated Swift realserver content after update"
        let writer = SyncFixtureDatabase(path: temporaryDatabaseURL(prefix: "writer"))
        let reader = SyncFixtureDatabase(path: temporaryDatabaseURL(prefix: "reader"))
        let syncConfig = SQLiteNowSyncConfig(
            generationId: 1,
            databaseId: "swift-sync-fixture",
            serviceId: "nethttp-server-example"
        )
        let writerTokens = MutableTokenStore()
        let readerTokens = MutableTokenStore()
        let writerOpening = try await openForSync(writer, baseURL: config.baseURL, config: syncConfig, tokens: writerTokens)
        let readerOpening = try await openForSync(reader, baseURL: config.baseURL, config: syncConfig, tokens: readerTokens)

        try await withOpenings([writerOpening, readerOpening]) {
            let writerSync = try XCTUnwrap(writerOpening.readyOrNil()?.client)
            let readerSync = try XCTUnwrap(readerOpening.readyOrNil()?.client)
            let writerSourceId = try await writerSync.sourceInfo().currentSourceId
            let readerSourceId = try await readerSync.sourceInfo().currentSourceId
            let writerToken = try await issueDummySigninToken(baseURL: config.baseURL, userId: userId, sourceId: writerSourceId)
            let readerToken = try await issueDummySigninToken(baseURL: config.baseURL, userId: userId, sourceId: readerSourceId)
            writerTokens.set(writerToken)
            readerTokens.set(readerToken)
            try requireConnected(try await writerSync.attach(userId: userId))
            try requireConnected(try await readerSync.attach(userId: userId))

            try await writer.users.insert(UsersInsertParams(id: rowUserId, name: rowName, email: rowEmail))
            try await writer.posts.insert(PostsInsertParams(id: rowPostId, title: rowTitle, content: rowContent, authorId: rowUserId))
            try await assertPushCommitted(writerSync)

            try await assertPullApplied(readerSync)

            let readerUsers = try await reader.users.selectAll().list()
            let insertedReaderPosts = try await reader.posts.selectAll().list()
            XCTAssertEqual(readerUsers, [
                UserRow(id: rowUserId, name: rowName, email: rowEmail)
            ])
            XCTAssertEqual(insertedReaderPosts, [
                PostRow(id: rowPostId, title: rowTitle, content: rowContent, authorId: rowUserId)
            ])

            try await writer.posts.updateContent(PostsUpdateContentParams(content: updatedRowContent, id: rowPostId))
            try await assertPushCommitted(writerSync)
            try await assertPullApplied(readerSync)

            let updatedReaderPosts = try await reader.posts.selectAll().list()
            XCTAssertEqual(updatedReaderPosts, [
                PostRow(id: rowPostId, title: rowTitle, content: updatedRowContent, authorId: rowUserId)
            ])

            try await writer.posts.deleteById(PostsDeleteByIdParams(id: rowPostId))
            try await assertPushCommitted(writerSync)
            try await assertPullApplied(readerSync)

            let deletedReaderPosts = try await reader.posts.selectAll().list()
            let remainingReaderUsers = try await reader.users.selectAll().list()
            XCTAssertEqual(deletedReaderPosts, [])
            XCTAssertEqual(remainingReaderUsers, [
                UserRow(id: rowUserId, name: rowName, email: rowEmail)
            ])

            let writerStatus = try await writerSync.syncStatus()
            let readerStatus = try await readerSync.syncStatus()
            XCTAssertFalse(writerStatus.pending.hasPendingSyncData)
            XCTAssertFalse(readerStatus.pending.hasPendingSyncData)
        }
    }

    private func requireRealserverConfig() async throws -> RealserverConfig {
        guard realserverFlagEnabled("OVERSQLITE_REALSERVER_TESTS") else {
            throw XCTSkip("Set OVERSQLITE_REALSERVER_TESTS=true to run Swift realserver smoke.")
        }

        let baseURLText = ProcessInfo.processInfo.environment["OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL"]?
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let resolvedBaseURLText = baseURLText?.isEmpty == false ? baseURLText! : "http://localhost:8080"
        guard let baseURL = URL(string: resolvedBaseURLText) else {
            throw RealserverSmokeError.invalidURL(resolvedBaseURLText)
        }

        _ = try await request(baseURL: baseURL, path: "/syncx/health")
        let status: RealserverStatusResponse = try await decodedRequest(baseURL: baseURL, path: "/syncx/status")
        guard status.appName == "nethttp-server-example" else {
            throw RealserverSmokeError.unexpectedServerAppName(status.appName)
        }
        return RealserverConfig(baseURL: baseURL)
    }

    private func resetRealserver(baseURL: URL) async throws {
        _ = try await request(baseURL: baseURL, path: "/syncx/status")
    }

    private func openForSync(
        _ database: SyncFixtureDatabase,
        baseURL: URL,
        config: SQLiteNowSyncConfig,
        tokens: MutableTokenStore
    ) async throws -> SQLiteNowSyncOpening<SyncFixtureDatabase> {
        let opening = try await database.openForSync(
            baseURL: baseURL,
            auth: tokens.auth,
            config: config
        )
        let status = try await opening.advance()
        guard status.outcome == .ready, opening.readyOrNil() != nil else {
            try? await opening.close()
            throw RealserverSmokeError.unexpectedAttachResult("coordinated opening did not publish ready")
        }
        return opening
    }

    private func issueDummySigninToken(baseURL: URL, userId: String, sourceId: String) async throws -> String {
        let response: DummySigninResponse = try await jsonRequest(
            baseURL: baseURL,
            path: "/dummy-signin",
            body: DummySigninRequest(user: userId, password: "anything", device: sourceId)
        )
        XCTAssertFalse(response.token.isEmpty)
        return response.token
    }

    private func requireConnected(
        _ result: SQLiteNowAttachResult,
        file: StaticString = #filePath,
        line: UInt = #line
    ) throws {
        switch result {
        case .connected:
            return
        case let .retryLater(retryAfterSeconds):
            XCTFail("Expected connected attach result, got retryLater(\(retryAfterSeconds)).", file: file, line: line)
            throw RealserverSmokeError.unexpectedAttachResult("retryLater(\(retryAfterSeconds))")
        case let .unknown(kind):
            XCTFail("Expected connected attach result, got unknown(\(kind)).", file: file, line: line)
            throw RealserverSmokeError.unexpectedAttachResult("unknown(\(kind))")
        }
    }

    private func requireUsedRemoteState(
        _ result: SQLiteNowAttachResult,
        file: StaticString = #filePath,
        line: UInt = #line
    ) throws -> SQLiteNowRestoreSummary {
        switch result {
        case let .connected(outcome, _, restore):
            XCTAssertEqual(outcome, .usedRemoteState, file: file, line: line)
            guard let restore else {
                XCTFail("Expected fresh-reader attach to include a restore summary.", file: file, line: line)
                throw RealserverSmokeError.unexpectedAttachResult("missing restore summary")
            }
            return restore
        case let .retryLater(retryAfterSeconds):
            throw RealserverSmokeError.unexpectedAttachResult("retryLater(\(retryAfterSeconds))")
        case let .unknown(kind):
            throw RealserverSmokeError.unexpectedAttachResult("unknown(\(kind))")
        }
    }

    private func assertPullApplied(
        _ sync: SQLiteNowSyncClient,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        let pullReport = try await sync.pullToStable()
        switch pullReport.outcome {
        case .appliedIncremental, .appliedSnapshot:
            break
        default:
            XCTFail("Expected reader pull to apply remote state, got \(pullReport.outcome)", file: file, line: line)
        }
    }

    private func assertPushCommitted(
        _ sync: SQLiteNowSyncClient,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        let pushReport = try await sync.pushPending()
        XCTAssertEqual(pushReport.outcome, .committed, file: file, line: line)
    }

    private func decodedRequest<Response: Decodable>(
        baseURL: URL,
        path: String
    ) async throws -> Response {
        let data = try await request(baseURL: baseURL, path: path)
        return try JSONDecoder().decode(Response.self, from: data)
    }

    private func jsonRequest<Request: Encodable, Response: Decodable>(
        baseURL: URL,
        path: String,
        body: Request
    ) async throws -> Response {
        let bodyData = try JSONEncoder().encode(body)
        let data = try await request(
            baseURL: baseURL,
            path: path,
            method: "POST",
            body: bodyData,
            headers: ["Content-Type": "application/json"]
        )
        return try JSONDecoder().decode(Response.self, from: data)
    }

    private func request(
        baseURL: URL,
        path: String,
        method: String = "GET",
        body: Data? = nil,
        headers: [String: String] = [:]
    ) async throws -> Data {
        guard let url = URL(string: path, relativeTo: baseURL)?.absoluteURL else {
            throw RealserverSmokeError.invalidURL(path)
        }
        var request = URLRequest(url: url)
        request.httpMethod = method
        request.httpBody = body
        request.timeoutInterval = 10
        headers.forEach { request.setValue($0.value, forHTTPHeaderField: $0.key) }

        let (data, response) = try await URLSession.shared.data(for: request)
        guard let httpResponse = response as? HTTPURLResponse else {
            throw RealserverSmokeError.nonHTTPResponse(path)
        }
        guard (200..<300).contains(httpResponse.statusCode) else {
            let responseBody = String(data: data, encoding: .utf8) ?? ""
            throw RealserverSmokeError.httpFailure(path: path, statusCode: httpResponse.statusCode, body: responseBody)
        }
        return data
    }

    private func realserverFlagEnabled(_ name: String) -> Bool {
        switch ProcessInfo.processInfo.environment[name]?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
        case "1", "true", "yes", "on":
            return true
        default:
            return false
        }
    }

    private func temporaryDatabaseURL(prefix: String) -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlitenow-sync-fixture-realserver-\(prefix)-\(UUID().uuidString).db")
    }

    private func withOpenings<T>(
        _ openings: [SQLiteNowSyncOpening<SyncFixtureDatabase>],
        _ body: () async throws -> T
    ) async throws -> T {
        do {
            let result = try await body()
            for opening in openings.reversed() {
                try await opening.close()
            }
            return result
        } catch {
            for opening in openings.reversed() {
                try? await opening.close()
            }
            throw error
        }
    }
}

private struct RealserverConfig {
    let baseURL: URL
}

private final class MutableTokenStore: @unchecked Sendable {
    private let lock = NSLock()
    private var token = "bootstrap-\(UUID().uuidString)"

    var auth: SQLiteNowSyncAuth {
        .bearer(accessToken: { [weak self] in self?.value() ?? "" })
    }

    func set(_ value: String) {
        lock.withLock { token = value }
    }

    private func value() -> String {
        lock.withLock { token }
    }
}


private struct DummySigninRequest: Encodable {
    let user: String
    let password: String
    let device: String
}

private struct DummySigninResponse: Decodable {
    let token: String
}

private struct RealserverStatusResponse: Decodable {
    let appName: String

    private enum CodingKeys: String, CodingKey {
        case appName = "app_name"
    }
}

private enum RealserverSmokeError: Error, CustomStringConvertible {
    case invalidURL(String)
    case nonHTTPResponse(String)
    case httpFailure(path: String, statusCode: Int, body: String)
    case unexpectedAttachResult(String)
    case unexpectedServerAppName(String)

    var description: String {
        switch self {
        case let .invalidURL(path):
            return "Invalid realserver URL path: \(path)"
        case let .nonHTTPResponse(path):
            return "Realserver request did not return an HTTP response: \(path)"
        case let .httpFailure(path, statusCode, body):
            return "Realserver request failed: \(path) returned HTTP \(statusCode): \(body)"
        case let .unexpectedAttachResult(result):
            return "Expected connected realserver attach result, got \(result)"
        case let .unexpectedServerAppName(appName):
            return "Swift realserver smoke requires go-oversync/examples/nethttp_server, but /syncx/status reported app_name='\(appName)'"
        }
    }
}
