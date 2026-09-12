import Foundation
@preconcurrency import SQLiteNowSyncRuntime
#if canImport(SyncFixtureDatabaseSQLiteNow)
@testable import SyncFixtureDatabaseSQLiteNow
#else
@testable import SQLiteNowSyncSupport
#endif
import XCTest

// The generated product target compiles this same file through a source symlink.
#if canImport(SyncFixtureDatabaseSQLiteNow)
final class GeneratedSyncOwnershipScenarios: XCTestCase, @unchecked Sendable {
    func testGeneratedPendingCountsPreserveInt64AboveIntMax() {
        let expected = Int64(Int32.max) + 9_001
        let runtime = SQLiteNowSyncRuntimePendingSyncStatus(
            hasPendingSyncData: true,
            pendingRowCount: expected,
            blocksDetach: true
        )
        XCTAssertEqual(SQLiteNowPendingSyncStatus(runtime).pendingRowCount, expected)
    }

    func testGeneratedPublicationRacesEveryPublicCloseOwner() async throws {
        for owner in ["opening", "database", "client"] {
            let fixture = try await makeGeneratedFixture()
            let barrier = PublicationBarrier()
            let entered = expectation(description: "publication / \(owner) close")
            let observedClient = WeakClientBox()
            fixture.database.syncClientLeaseForTest.afterClientConstructionForTest = { observedClient.store($0) }
            fixture.database.syncClientLeaseForTest.beforeReadyPublicationForTest = {
                if barrier.claim() {
                    entered.fulfill()
                    XCTAssertTrue(barrier.wait(), owner)
                }
            }

            let advance = Task { try await fixture.opening.advance() }
            await fulfillment(of: [entered], timeout: 10)
            let close: Task<Void, Error>?
            if owner == "client" {
                observedClient.load()?.close()
                close = nil
            } else {
                close = Task { () throws -> Void in
                    switch owner {
                    case "opening": try await fixture.opening.close()
                    case "database": try await fixture.database.close()
                    default: XCTFail("Unknown close owner: \(owner)")
                    }
                }
            }
            barrier.release()
            do {
                _ = try await advance.value
                XCTFail("Publication won after \(owner) close")
            } catch {}
            if let close { _ = try? await close.value }
            if owner == "client" { try? await fixture.opening.close() }

            XCTAssertNil(fixture.opening.readyOrNil(), owner)
            XCTAssertNil(observedClient.load(), owner)
            do {
                _ = try await fixture.database.docs.selectAll().list()
                XCTFail("Generated query admitted after \(owner) close")
            } catch {}
        }
    }

    func testGeneratedConcurrentStatusAndAdvanceUseOnePublishedClient() async throws {
        for scenario in ["status/status", "status/advance"] {
            let fixture = try await makeGeneratedFixture()
            async let first = fixture.opening.status()
            if scenario == "status/status" {
                async let second = fixture.opening.status()
                _ = try await (first, second)
            } else {
                async let second = fixture.opening.advance()
                _ = try await (first, second)
            }
            if fixture.opening.readyOrNil() == nil { _ = try await fixture.opening.advance() }
            let ready = try XCTUnwrap(fixture.opening.readyOrNil(), scenario)
            let resolved = try fixture.database.makeSyncClient(
                baseURL: fixture.baseURL,
                auth: fixture.auth,
                config: fixture.config
            )
            XCTAssertTrue(ready.client === resolved, scenario)
            try await fixture.opening.close()
        }
    }

    func testGeneratedAlreadyCancelledOperationRequiresCancellationError() async throws {
        let fixture = try await makeGeneratedFixture()
        let begin = AsyncSignal()
        let operation = Task {
            await begin.wait()
            _ = try await fixture.opening.status()
        }
        operation.cancel()
        await begin.release()
        do {
            try await operation.value
            XCTFail("Already-cancelled generated operation succeeded")
        } catch is CancellationError {
        } catch {
            XCTFail("Expected CancellationError, got \(error)")
        }
        XCTAssertNil(fixture.opening.readyOrNil())
        try await fixture.opening.close()
    }

    func testGeneratedStreamsTerminateAndQueriesRejectAfterEveryCloseOwner() async throws {
        for owner in ["opening", "database", "client"] {
            let fixture = try await makeGeneratedFixture()
            _ = try await fixture.opening.advance()
            let client = try XCTUnwrap(fixture.opening.readyOrNil()?.client)
            let streamStarted = expectation(description: "generated stream / \(owner) first rows")
            let streamEnded = expectation(description: "generated stream / \(owner) close")
            let stream = Task {
                var receivedFirstRows = false
                do {
                    for try await _ in fixture.database.docs.selectAll().stream() {
                        if !receivedFirstRows {
                            receivedFirstRows = true
                            streamStarted.fulfill()
                        }
                    }
                } catch {}
                streamEnded.fulfill()
            }
            await fulfillment(of: [streamStarted], timeout: 10)
            switch owner {
            case "opening": try await fixture.opening.close()
            case "database": try await fixture.database.close()
            case "client":
                client.close()
                await fulfillment(of: [streamEnded], timeout: 10)
                try await fixture.opening.close()
            default: XCTFail("Unknown close owner: \(owner)")
            }
            if owner != "client" {
                await fulfillment(of: [streamEnded], timeout: 10)
            }
            stream.cancel()
            XCTAssertNil(fixture.opening.readyOrNil(), owner)
            do {
                _ = try await fixture.database.docs.selectAll().list()
                XCTFail("Generated query admitted after \(owner) close")
            } catch {}
        }
    }

    private func makeGeneratedFixture() async throws -> GeneratedOwnershipFixture {
        let database = SyncFixtureDatabase(
            path: FileManager.default.temporaryDirectory
                .appendingPathComponent("coordinatedopening-generated-swift-\(UUID()).sqlite")
        )
        let baseURL = URL(string: "http://127.0.0.1:1")!
        let auth = SQLiteNowSyncAuth.bearer(token: "offline")
        let config = SQLiteNowSyncConfig(
            generationId: 1,
            databaseId: "swift-sync-fixture",
            serviceId: "nethttp-server-example"
        )
        let opening = try await database.openForSync(baseURL: baseURL, auth: auth, config: config)
        return GeneratedOwnershipFixture(
            database: database,
            opening: opening,
            baseURL: baseURL,
            auth: auth,
            config: config
        )
    }
}

private struct GeneratedOwnershipFixture: @unchecked Sendable {
    let database: SyncFixtureDatabase
    let opening: SQLiteNowSyncOpening<SyncFixtureDatabase>
    let baseURL: URL
    let auth: SQLiteNowSyncAuth
    let config: SQLiteNowSyncConfig
}

private final class WeakClientBox: @unchecked Sendable {
    private let lock = NSLock()
    private weak var client: SQLiteNowSyncClient?
    func store(_ client: SQLiteNowSyncClient) { lock.withLock { self.client = client } }
    func load() -> SQLiteNowSyncClient? { lock.withLock { client } }
}
#else
final class SyncOwnershipScenarios: XCTestCase, @unchecked Sendable {
    func testLeaseDoesNotRetainPublishedOrRejectedClient() async throws {
        for scenario in ["losing publication", "winning publication", "client close", "lease close"] {
            let fixture = try await makeFixture()
            weak var observedClient: SQLiteNowSyncClient?
            fixture.lease.afterClientConstructionForTest = { client in
                observedClient = client
            }

            var ready = true
            if scenario == "losing publication" {
                fixture.lease.beforeReadyPublicationForTest = { ready = false }
                XCTAssertThrowsError(
                    try fixture.lease.bind(
                        runtime: fixture.runtimeClient,
                        binding: fixture.binding,
                        isReady: { ready }
                    ),
                    scenario
                )
                XCTAssertNil(observedClient, scenario)
            } else {
                var client: SQLiteNowSyncClient? = try fixture.lease.bind(
                    runtime: fixture.runtimeClient,
                    binding: fixture.binding
                )
                XCTAssertTrue(observedClient === client, scenario)
                switch scenario {
                case "winning publication": fixture.lease.closeActiveClient()
                case "client close": client?.close()
                case "lease close": fixture.lease.close()
                default: XCTFail("Unknown scenario: \(scenario)")
                }
                client = nil
                XCTAssertNil(observedClient, scenario)
            }

            try await fixture.opening.close()
        }
    }

    func testConcurrentReadyObserversAndEveryCloseEntry() async throws {
        for scenario in ["status/status", "status/advance", "publication/opening close", "publication/database close", "publication/client close"] {
            let fixture = try await makeFixture()
            let barrier = PublicationBarrier()
            let entered = expectation(description: scenario)
            fixture.lease.beforeReadyPublicationForTest = {
                if barrier.claim() {
                    entered.fulfill()
                    XCTAssertTrue(barrier.wait(), scenario)
                }
            }
            let first = Task { () -> Bool in
                do { _ = try await fixture.opening.status(); return true }
                catch { return false }
            }
            await fulfillment(of: [entered], timeout: 10)
            if scenario.hasPrefix("status/") {
                let second = Task {
                    if scenario == "status/status" { _ = try await fixture.opening.status() }
                    else { _ = try await fixture.opening.advance() }
                }
                barrier.release()
                let firstSucceeded = await first.value
                XCTAssertTrue(firstSucceeded, scenario)
                try await second.value
                let ready = try XCTUnwrap(fixture.opening.readyOrNil(), scenario)
                XCTAssertTrue(try fixture.lease.readyClient(matching: fixture.binding) === ready.client, scenario)
                _ = try await fixture.opening.status()
                XCTAssertTrue(fixture.opening.readyOrNil()?.client === ready.client, scenario)
                XCTAssertThrowsError(try fixture.lease.bind(runtime: fixture.runtimeClient, binding: fixture.otherBinding))
                try await fixture.core.execute(sql: "INSERT INTO docs VALUES ('retained', 'usable')", bindValues: [], affectedTables: ["docs"])
                try await fixture.opening.close()
            } else {
                switch scenario {
                case "publication/opening close": try await fixture.opening.close()
                case "publication/database close": try await fixture.core.close()
                default: fixture.runtimeClient.close()
                }
                let rejectedObserver = RevokedTableObserver()
                let rejectedHandle = fixture.core.observeTables(tableNames: ["docs"], observer: rejectedObserver)
                XCTAssertEqual(rejectedObserver.failures, 1, scenario)
                XCTAssertEqual(rejectedObserver.changes, 0, scenario)
                rejectedHandle.cancel()
                rejectedHandle.cancel()
                XCTAssertNil(fixture.runtime.readyClientOrNull(), scenario)
                XCTAssertNil(fixture.opening.readyOrNil(), scenario)
                XCTAssertNil(try fixture.lease.readyClient(matching: fixture.binding), scenario)
                barrier.release()
                _ = await first.value
                XCTAssertNil(fixture.opening.readyOrNil(), scenario)
                XCTAssertNil(try fixture.lease.readyClient(matching: fixture.binding), scenario)
                do {
                    _ = try await fixture.core.query(sql: "SELECT title FROM docs", bindValues: [], columnTypes: ["text"])
                    XCTFail("Revoked Core query admitted: \(scenario)")
                } catch {}
                try await fixture.opening.close()
            }
            try await fixture.opening.close()
        }
    }

    func testAlreadyCancelledTaskAndHandleInstallationRace() async throws {
        for scenario in ["already cancelled", "completion before handle installation"] {
            let fixture = try await makeFixture()
            if scenario == "already cancelled" {
                let begin = AsyncSignal()
                let operation = Task {
                    await begin.wait()
                    _ = try await fixture.opening.status()
                }
                operation.cancel()
                await begin.release()
                do {
                    try await operation.value
                    XCTFail("Already-cancelled task returned success")
                } catch is CancellationError {} catch {
                    XCTFail("Unexpected cancellation error: \(error)")
                }
            } else {
                // Force a synchronous successful completion before start returns its actual runtime handle.
                let result = try await awaitSyncOpeningOperation { completion in
                    completion.onSuccess(result: SQLiteNowSyncRuntimeOpeningOperationResult(opening: nil, status: nil, conflict: nil, rejection: nil, syncReport: nil, syncThenDetachResult: nil, pushReport: nil, remoteReport: nil))
                    return fixture.runtime.startStatus(completion: completion)
                }
                XCTAssertNil(result.status)
            }
            // Neither cancellation nor a losing completion shuts down the facade.
            _ = try await fixture.opening.status()
            XCTAssertNotNil(fixture.opening.readyOrNil())
            try await fixture.opening.close()
        }
    }

    private func makeFixture() async throws -> OwnershipFixture {
        let core = SQLiteNowCoreRuntimeDatabase(
            path: FileManager.default.temporaryDirectory.appendingPathComponent("coordinatedopening-swift-\(UUID()).sqlite").path,
            migrationPlan: SQLiteNowCoreRuntimeMigrationPlan(latestVersion: 1,
                schemaSql: ["CREATE TABLE docs (doc_id TEXT PRIMARY KEY NOT NULL, title TEXT NOT NULL)"],
                initSql: [], migrationSteps: []),
            debug: false, onMigrationStep: nil, syncManaged: true)
        let auth = SQLiteNowSyncAuth.bearer(token: "offline")
        let config = SQLiteNowSyncConfig(generationId: 1, databaseId: "fixture", serviceId: "fixture")
        let url = URL(string: "http://127.0.0.1:1")!
        let binding = SQLiteNowSyncClientBinding(baseURL: url, auth: auth, config: config, resolver: nil)
        let otherBinding = SQLiteNowSyncClientBinding(baseURL: url, auth: .bearer(token: "other"), config: config, resolver: nil)
        let runtime = try await SQLiteNowSyncRuntimeKt.sqliteNowSyncRuntimeOpenForSync(
            coreDatabase: core, baseUrl: url.absoluteString, auth: auth.runtimeAuth,
            config: try config.runtimeConfig(syncTables: [.init(tableName: "docs", syncKeyColumnName: "doc_id")]),
            resolver: nil, upgradePlan: nil)
        _ = try await runtime.advance()
        let client = try XCTUnwrap(runtime.readyClientOrNull())
        let lease = SQLiteNowSyncClientLease()
        let opening = SQLiteNowSyncOpening(runtime: runtime, database: core, lease: lease, binding: binding)
        return OwnershipFixture(core: core, runtime: runtime, runtimeClient: client, lease: lease,
            opening: opening, binding: binding, otherBinding: otherBinding)
    }
}
#endif

private struct OwnershipFixture: @unchecked Sendable {
    let core: SQLiteNowCoreRuntimeDatabase
    let runtime: SQLiteNowSyncRuntimeOpening
    let runtimeClient: SQLiteNowSyncRuntimeClient
    let lease: SQLiteNowSyncClientLease
    let opening: SQLiteNowSyncOpening<SQLiteNowCoreRuntimeDatabase>
    let binding: SQLiteNowSyncClientBinding
    let otherBinding: SQLiteNowSyncClientBinding
}
private final class PublicationBarrier: @unchecked Sendable {
    private let lock = NSLock()
    private var claimed = false
    private let signal = DispatchSemaphore(value: 0)
    func claim() -> Bool { lock.withLock { if claimed { return false }; claimed = true; return true } }
    func wait() -> Bool { signal.wait(timeout: .now() + 10) == .success }
    func release() { signal.signal() }
}
private actor AsyncSignal {
    private var continuation: CheckedContinuation<Void, Never>?
    private var released = false
    func wait() async { if !released { await withCheckedContinuation { continuation = $0 } } }
    func release() { released = true; continuation?.resume(); continuation = nil }
}

private final class RevokedTableObserver: SQLiteNowCoreRuntimeTableObserver, @unchecked Sendable {
    private let lock = NSLock()
    private var failureCount = 0
    private var changeCount = 0
    var failures: Int { lock.withLock { failureCount } }
    var changes: Int { lock.withLock { changeCount } }
    func onChanged() { lock.withLock { changeCount += 1 } }
    func onError(payload: SQLiteNowCoreRuntimeErrorPayload) { lock.withLock { failureCount += 1 } }
}
