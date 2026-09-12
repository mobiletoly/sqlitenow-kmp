import Foundation
import SQLiteNowSyncRuntime
@testable import SyncFixtureDatabaseSQLiteNow
import XCTest

final class SyncFixtureTests: XCTestCase {
    func testSyncConfigurationDefaultsAndCustomValues() {
        let defaults = SQLiteNowSyncConfig(generationId: 1, databaseId: "swift-sync-fixture", serviceId: "nethttp-server-example")
        XCTAssertEqual(defaults.generationId, 1)
        XCTAssertEqual(defaults.transferTargets.maxPushRows, 200)
        XCTAssertEqual(defaults.transferTargets.maxPushDecodedBytes, 1 * 1_024 * 1_024)
        XCTAssertEqual(defaults.transferTargets.maxPullBundles, 1_000)
        XCTAssertEqual(defaults.transferTargets.maxSnapshotChunkRows, 1_000)
        XCTAssertEqual(defaults.transferTargets.maxSnapshotChunkDecodedBytes, 4 * 1_024 * 1_024)
        XCTAssertEqual(defaults.snapshotApplyBatchRows, 256)
        XCTAssertEqual(defaults.snapshotApplyBatchBytes, 4 * 1_024 * 1_024)
        XCTAssertFalse(defaults.verboseLogs)
        XCTAssertEqual(defaults.transientRetryPolicy, .init())
        XCTAssertEqual(defaults.snapshotCapacityRetryPolicy, .init())

        let custom = SQLiteNowSyncConfig(
            generationId: 1, databaseId: "swift-sync-fixture", serviceId: "nethttp-server-example",
            transferTargets: .init(
                maxPushRows: 11,
                maxPushDecodedBytes: 12,
                maxPullBundles: 13,
                maxSnapshotChunkRows: 14,
                maxSnapshotChunkDecodedBytes: 15
            ),
            snapshotApplyBatchRows: 16,
            snapshotApplyBatchBytes: 17,
            verboseLogs: true,
            transientRetryPolicy: .init(maxAttempts: 17, initialBackoffMillis: 18, maxBackoffMillis: 19, jitterRatio: 0.3),
            snapshotCapacityRetryPolicy: .init(enabled: false, maxWaitMillis: 20, fallbackDelayMillis: 21, jitterRatio: 0.4)
        )
        XCTAssertEqual(custom.databaseId, "swift-sync-fixture")
        XCTAssertEqual(custom.transferTargets.maxSnapshotChunkRows, 14)
        XCTAssertEqual(custom.transientRetryPolicy.maxAttempts, 17)
        XCTAssertEqual(custom.snapshotCapacityRetryPolicy.fallbackDelayMillis, 21)
    }

    func testSyncConfigurationRejectsInvalidBoundaryValues() async throws {
        try await withOpenDatabase { db in
            let invalidCases: [(String, SQLiteNowSyncConfig)] = [
                ("generation zero", .init(generationId: 0, databaseId: "fixture", serviceId: "fixture")),
                ("generation overflow", .init(generationId: Int64(Int32.max) + 1, databaseId: "fixture", serviceId: "fixture")),
                ("push rows Int overflow", .init(generationId: 1, databaseId: "swift-sync-fixture", serviceId: "nethttp-server-example", transferTargets: .init(maxPushRows: Int.max))),
                ("pull bundles Int overflow", .init(generationId: 1, databaseId: "swift-sync-fixture", serviceId: "nethttp-server-example", transferTargets: .init(maxPullBundles: Int.max))),
                ("snapshot rows nonpositive", .init(generationId: 1, databaseId: "swift-sync-fixture", serviceId: "nethttp-server-example", transferTargets: .init(maxSnapshotChunkRows: 0))),
                ("snapshot bytes nonpositive", .init(generationId: 1, databaseId: "swift-sync-fixture", serviceId: "nethttp-server-example", transferTargets: .init(maxSnapshotChunkDecodedBytes: 0))),
                ("snapshot apply rows nonpositive", .init(generationId: 1, databaseId: "swift-sync-fixture", serviceId: "nethttp-server-example", snapshotApplyBatchRows: 0)),
                ("snapshot apply bytes nonpositive", .init(generationId: 1, databaseId: "swift-sync-fixture", serviceId: "nethttp-server-example", snapshotApplyBatchBytes: 0)),
                ("transient attempts Int overflow", .init(generationId: 1, databaseId: "swift-sync-fixture", serviceId: "nethttp-server-example", transientRetryPolicy: .init(maxAttempts: Int.max))),
                ("capacity max wait nonpositive", .init(generationId: 1, databaseId: "swift-sync-fixture", serviceId: "nethttp-server-example", snapshotCapacityRetryPolicy: .init(maxWaitMillis: 0))),
                ("capacity fallback nonpositive", .init(generationId: 1, databaseId: "swift-sync-fixture", serviceId: "nethttp-server-example", snapshotCapacityRetryPolicy: .init(fallbackDelayMillis: 0))),
                ("capacity jitter below range", .init(generationId: 1, databaseId: "swift-sync-fixture", serviceId: "nethttp-server-example", snapshotCapacityRetryPolicy: .init(jitterRatio: -0.1))),
                ("capacity jitter above range", .init(generationId: 1, databaseId: "swift-sync-fixture", serviceId: "nethttp-server-example", snapshotCapacityRetryPolicy: .init(jitterRatio: 1.1)))
            ]

            for (name, config) in invalidCases {
                XCTAssertThrowsError(
                    try db.makeSyncClient(
                        baseURL: URL(string: "http://127.0.0.1:1")!,
                        auth: .bearer(token: "fixture-token"),
                        config: config
                    ),
                    name
                ) { error in
                    guard let sqliteNowError = error as? SQLiteNowError,
                          case .misuse = sqliteNowError else {
                        return XCTFail("Expected misuse for \(name), got \(error)")
                    }
                }
            }
        }
    }

    func testProtocolPayloadMapsDirectly() {
        let error = SQLiteNowError.from(
            SQLiteNowSyncRuntimeErrorPayload(
                category: "protocol",
                code: "wire",
                message: "bad payload",
                remoteContract: false,
                httpStatus: 0,
                operation: "",
                remediation: nil,
                retryable: false,
                generationId: nil
            )
        )
        XCTAssertEqual(error, .protocol(message: "bad payload"))
    }

    func testRemoteContractPayloadPreservesStructuredFields() {
        let error = SQLiteNowError.from(
            SQLiteNowSyncRuntimeErrorPayload(
                category: "protocol",
                code: "retired_schema_generation",
                message: "upgrade",
                remoteContract: true,
                httpStatus: 426,
                operation: "generation discovery",
                remediation: "upgrade_application",
                retryable: false,
                generationId: "1"
            )
        )
        XCTAssertEqual(
            error,
            .remote(
                code: "retired_schema_generation",
                httpStatus: 426,
                operation: "generation discovery",
                remediation: "upgrade_application",
                retryable: false,
                generationId: 1,
                message: "upgrade"
            )
        )
    }

    func testRemoteContractPayloadRejectsNoncanonicalGenerationId() {
        let error = SQLiteNowError.from(
            SQLiteNowSyncRuntimeErrorPayload(
                category: "protocol",
                code: "retired_schema_generation",
                message: "upgrade",
                remoteContract: true,
                httpStatus: 426,
                operation: "generation discovery",
                remediation: "upgrade_application",
                retryable: false,
                generationId: "01"
            )
        )
        XCTAssertEqual(
            error,
            .protocol(message: "remote contract error contained an invalid generation ID")
        )
    }

    func testDocsCrudUsesCoreRuntime() async throws {
        try await withOpenDatabase { db in
            let emptyRows = try await db.docs.selectAll().list()
            XCTAssertEqual(emptyRows, [])

            try await db.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "doc-1", title: "First"))
            try await db.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "doc-1", title: "First replaced"))
            try await db.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "doc-2", title: "Second"))
            let insertedRows = try await db.docs.selectAll().list()
            XCTAssertEqual(insertedRows, [
                DocRow(docId: "doc-1", title: "First replaced"),
                DocRow(docId: "doc-2", title: "Second")
            ])

            try await db.docs.updateTitle(DocsUpdateTitleParams(title: "Updated", docId: "doc-1"))
            let updatedRows = try await db.docs.selectAll().list()
            XCTAssertEqual(updatedRows.first?.title, "Updated")

            try await db.docs.deleteById(DocsDeleteByIdParams(docId: "doc-2"))
            let remainingRows = try await db.docs.selectAll().list()
            XCTAssertEqual(remainingRows, [
                DocRow(docId: "doc-1", title: "Updated")
            ])
        }
    }

    func testCoordinatedOpeningPublishesFreshDatabaseWithoutNetwork() async throws {
        let db = SyncFixtureDatabase(path: temporaryDatabaseURL())
        let opening = try await db.openForSync(
            baseURL: URL(string: "http://127.0.0.1:1")!,
            auth: .bearer(token: "fixture-token"),
            config: .init(
                generationId: 1,
                databaseId: "swift-sync-fixture",
                serviceId: "nethttp-server-example"
            )
        )
        do {
            let initial = try await opening.status()
            XCTAssertNil(initial.mode)
            XCTAssertNil(initial.outcome)
            XCTAssertEqual(initial.nextAction, .advance)
            XCTAssertNil(opening.readyOrNil())

            let readyStatus = try await opening.advance()
            XCTAssertEqual(readyStatus.outcome, .ready)
            XCTAssertEqual(readyStatus.nextAction, .useReady)
            XCTAssertNotNil(opening.readyOrNil())

            try await db.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "fresh", title: "Published"))
            let published = try await db.docs.selectAll().one()
            XCTAssertEqual(published.title, "Published")
            try await opening.close()
        } catch {
            try? await opening.close()
            throw error
        }
    }

    func testCoordinatedReopenPublishesOnlyAfterAdvance() async throws {
        let path = temporaryDatabaseURL()
        let baseURL = URL(string: "http://127.0.0.1:1")!
        let auth = SQLiteNowSyncAuth.bearer(token: "fixture-token")
        let config = SQLiteNowSyncConfig(
            generationId: 1,
            databaseId: "swift-sync-fixture",
            serviceId: "nethttp-server-example"
        )
        let initial = SyncFixtureDatabase(path: path)
        let initialOpening = try await initial.openForSync(baseURL: baseURL, auth: auth, config: config)
        _ = try await initialOpening.advance()
        try await initial.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "doc-1", title: "First"))
        try await initial.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "doc-2", title: "Second"))
        try await initialOpening.close()

        let reopened = SyncFixtureDatabase(path: path)
        let opening = try await reopened.openForSync(baseURL: baseURL, auth: auth, config: config)
        XCTAssertNil(opening.readyOrNil())
        let ready = try await opening.advance()
        XCTAssertEqual(ready.outcome, .ready)
        let rows = try await reopened.docs.selectAll().list()
        XCTAssertEqual(rows.map(\.title), ["First", "Second"])
        try await opening.close()
    }

    func testSyncOpenSourceInfoAndExpectedAttachedError() async throws {
        try await withReadyDatabase { _, sync in
            try await sync.pauseUploads()
            try await sync.resumeUploads()
            try await sync.pauseDownloads()
            try await sync.resumeDownloads()

            let sourceInfo = try await sync.sourceInfo()
            XCTAssertFalse(sourceInfo.currentSourceId.isEmpty)

            do {
                _ = try await sync.syncStatus()
                XCTFail("Expected syncStatus() to require attach")
            } catch {
                XCTAssertTrue(String(describing: error).lowercased().contains("attach"))
            }
        }
    }

    func testProgressStreamEmitsIdle() async throws {
        try await withReadyDatabase { _, sync in
            var iterator = sync.progress().makeAsyncIterator()
            let first = try await iterator.next()
            XCTAssertEqual(first, .idle)
        }
    }

    func testMakeSyncClientReturnsReadyIdentityAndRejectsMismatchedBinding() async throws {
        let db = SyncFixtureDatabase(path: temporaryDatabaseURL())
        let baseURL = URL(string: "http://127.0.0.1:1")!
        let auth = SQLiteNowSyncAuth.bearer(token: "fixture-token")
        let config = SQLiteNowSyncConfig(
            generationId: 1,
            databaseId: "swift-sync-fixture",
            serviceId: "nethttp-server-example"
        )
        let opening = try await db.openForSync(baseURL: baseURL, auth: auth, config: config)
        _ = try await opening.advance()
        let ready = try XCTUnwrap(opening.readyOrNil())

        let sameClient = try db.makeSyncClient(baseURL: baseURL, auth: auth, config: config)
        XCTAssertTrue(sameClient === ready.client)

        XCTAssertThrowsError(
            try db.makeSyncClient(
                baseURL: baseURL,
                auth: auth,
                config: .init(
                    generationId: 1,
                    databaseId: "mismatched-database",
                    serviceId: "nethttp-server-example"
                )
            )
        ) { error in
            XCTAssertTrue(String(describing: error).contains("exact URL, auth, resolver, and configuration"))
        }
        try await opening.close()
    }

    func testAutomaticDownloadsHandleCanCancel() async throws {
        try await withReadyDatabase { _, sync in
            let handle = sync.startAutomaticDownloads(
                SQLiteNowAutomaticDownloadConfig(automaticDownloadIntervalMillis: 1)
            )
            handle.cancel()
        }
    }

    func testResolverCallbackUsesJsonPayloads() async throws {
        let resolver = SQLiteNowSyncResolver { conflict in
            XCTAssertEqual(conflict.schema, "main")
            XCTAssertEqual(conflict.table, "docs")
            XCTAssertEqual(conflict.keyJson, #"{"doc_id":"doc-1"}"#)
            XCTAssertEqual(conflict.serverRowJson, #"{"title":"Remote"}"#)
            return .keepMerged(payloadJson: #"{"title":"Merged"}"#)
        }
        try await withReadyDatabase(resolver: resolver) { _, sync in
            let result = try sync.resolveForRuntimeSmoke(
                SQLiteNowSyncConflict(
                    schema: "main",
                    table: "docs",
                    keyJson: #"{"doc_id":"doc-1"}"#,
                    localOp: "upsert",
                    localPayloadJson: #"{"title":"Local"}"#,
                    baseRowVersion: 1,
                    serverRowVersion: 2,
                    serverRowDeleted: false,
                    serverRowJson: #"{"title":"Remote"}"#
                )
            )
            XCTAssertEqual(result, .keepMerged(payloadJson: #"{"title":"Merged"}"#))
        }
    }

    private func temporaryDatabaseURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlitenow-sync-fixture-\(UUID().uuidString).db")
    }

    private func withOpenDatabase<T>(
        _ body: (SyncFixtureDatabase) async throws -> T
    ) async throws -> T {
        let db = SyncFixtureDatabase(path: temporaryDatabaseURL())
        let opening = try await db.openForSync(
            baseURL: URL(string: "http://127.0.0.1:1")!,
            auth: .bearer(token: "fixture-token"),
            config: .init(generationId: 1, databaseId: "swift-sync-fixture", serviceId: "nethttp-server-example")
        )
        _ = try await opening.advance()
        do {
            let result = try await body(db)
            try await opening.close()
            return result
        } catch {
            try? await opening.close()
            throw error
        }
    }

    private func withReadyDatabase<T>(
        resolver: SQLiteNowSyncResolver? = nil,
        _ body: (SyncFixtureDatabase, SQLiteNowSyncClient) async throws -> T
    ) async throws -> T {
        let db = SyncFixtureDatabase(path: temporaryDatabaseURL())
        let opening = try await db.openForSync(
            baseURL: URL(string: "http://127.0.0.1:1")!,
            auth: .bearer(token: "fixture-token"),
            config: .init(generationId: 1, databaseId: "swift-sync-fixture", serviceId: "nethttp-server-example"),
            resolver: resolver
        )
        _ = try await opening.advance()
        guard let ready = opening.readyOrNil() else {
            try? await opening.close()
            throw NSError(
                domain: "SyncFixtureTests",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "Expected coordinated opening to publish ready"]
            )
        }
        do {
            let result = try await body(db, ready.client)
            try await opening.close()
            return result
        } catch {
            try? await opening.close()
            throw error
        }
    }
}
