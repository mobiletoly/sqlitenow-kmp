import Foundation
@preconcurrency import SQLiteNowSyncRuntime
@testable import SQLiteNowSyncSupport
import XCTest

final class SyncCancellationStateTests: XCTestCase, @unchecked Sendable {
    func testPersistentVectorPreservesTypesAndIgnoresInsertionOrder() async throws {
        let schema = "CREATE TABLE witness(id TEXT NOT NULL, \"select\" BLOB)"
        let core = SQLiteNowCoreRuntimeDatabase(path: ":memory:",
            migrationPlan: .init(latestVersion: 1, schemaSql: [schema], initSql: [],
                migrationSteps: [.init(version: 1, sql: [schema])]),
            debug: false, onMigrationStep: nil, syncManaged: false)
        try await core.open()
        do {
            let rows = ["('integer',9007199254740993)", "('text','9007199254740993')",
                        "('blob',X'00FF')", "('null',NULL)", "('real',1.0000000000000002)"]
            try await core.execute(sql: "INSERT INTO witness VALUES \(rows.joined(separator: ","))",
                bindValues: [], affectedTables: ["witness"])
            let original = try await persistentVector(core, tables: ["witness"])
            XCTAssertEqual(original["witness"]?.count, 5)
            try await core.execute(sql: "DELETE FROM witness", bindValues: [], affectedTables: ["witness"])
            try await core.execute(sql: "INSERT INTO witness VALUES \(rows.reversed().joined(separator: ","))",
                bindValues: [], affectedTables: ["witness"])
            let reordered = try await persistentVector(core, tables: ["witness"])
            XCTAssertEqual(original, reordered)
            try await core.execute(sql: "UPDATE witness SET \"select\"=1.0 WHERE id='real'",
                bindValues: [], affectedTables: ["witness"])
            let changedReal = try await persistentVector(core, tables: ["witness"])
            XCTAssertNotEqual(original, changedReal, "state comparison must retain SQLite REAL precision")
            try await core.execute(sql: "UPDATE witness SET \"select\"='9007199254740993' WHERE id='integer'",
                bindValues: [], affectedTables: ["witness"])
            let changedType = try await persistentVector(core, tables: ["witness"])
            XCTAssertNotEqual(changedReal, changedType, "equal text rendering must not hide a storage-type change")
            try await core.close()
        } catch { try? await core.close(); throw error }
    }

    func testSnapshotAdmissionAndPredispatchCancellationPreserveOwnedState() async throws {
        try await withLiveFixture { fixture in
            try await withConnection(fixture, work: 1000) { core, client in
                let before = try await client.syncStatus()
                do { _ = try await client.rebuild(); XCTFail("Snapshot should refuse atomic admission") }
                catch let error as SQLiteNowError {
                    guard case let .syncWorkspaceLimit(limit) = error else { throw error }
                    XCTAssertEqual(limit.dimension,"atomic_apply_work")
                    XCTAssertGreaterThan(limit.reserved,limit.limit)
                    XCTAssertEqual(limit.progress.dirtyRows,0)
                    XCTAssertEqual(limit.progress.outboundRows,0)
                    XCTAssertEqual(limit.progress.committedBundles,0)
                }
                let box = CancellationClientBox(client)
                let gate = CancellationStartGate()
                let operation = Task { await gate.wait(); _ = try await box.client.rebuild() }
                operation.cancel()
                await gate.release()
                do { _ = try await operation.value; XCTFail("Cancelled snapshot returned success") }
                catch is CancellationError {}
                let after = try await client.syncStatus()
                XCTAssertEqual(before.pending.pendingRowCount,after.pending.pendingRowCount)
                let rows = try await core.query(sql:"SELECT name FROM users WHERE id='11111111-1111-4111-8111-111111111111'",bindValues:[],columnTypes:["text"])
                XCTAssertEqual(rows.rowAt(index:0).cellAt(index:0).textValue,"original")
            }
        }
    }

    func testConflictAdmissionAndPredispatchCancellationPreservePendingDecision() async throws {
        try await withLiveFixture { fixture in
            try await prepareConflict(fixture)
            try await withConnection(fixture, work: 1000) { limitedCore, limited in
                let retained = try await limited.pendingConflict();let pending = try XCTUnwrap(retained)
                let before = try await persistentVector(limitedCore)
                do { try await limited.resolveConflict(pending,result:.acceptServer);XCTFail("Conflict should refuse atomic admission") }
                catch let error as SQLiteNowError {
                    guard case let .syncWorkspaceLimit(limit) = error else { throw error }
                    XCTAssertEqual(limit.dimension,"atomic_apply_work")
                    XCTAssertEqual(limit.progress.outboundRows,1)
                }
                let current = try await persistentVector(limitedCore);XCTAssertEqual(before,current)
                let gate = CancellationStartGate()
                let box = CancellationClientBox(limited)
                let decision = Task { await gate.wait();try await box.client.resolveConflict(pending,result:.acceptServer) }
                decision.cancel()
                await gate.release()
                do { try await decision.value;XCTFail("Cancelled conflict returned success") } catch is CancellationError {}
                let cancelledPending = try await limited.pendingConflict();XCTAssertEqual(pending,cancelledPending)
                let cancelledVector = try await persistentVector(limitedCore);XCTAssertEqual(before,cancelledVector)
            }
        }
    }

    func testReachedConflictCancellationRollsBackOwnedState() async throws {
#if A3_TEST_BRIDGE
        try await withLiveFixture { fixture in
            try await prepareConflict(fixture)
            try await withConnection(fixture) { finishCore, finish in
                let retained = try await finish.pendingConflict()
                let pending = try XCTUnwrap(retained)
                let beforeConflict = try await persistentVector(finishCore, includeApplyOwners: true)
                XCTAssertEqual(1, SwiftA3ConflictApplyProbeKt.armA3ConflictApplyForSwiftTest(requestHash: pending.canonicalRequestHash))
                let conflictBox = CancellationClientBox(finish)
                let inFlightConflict = Task { try await conflictBox.client.resolveConflict(pending,result:.acceptServer) }
                do {
                    try await waitForConflictApplyMarker()
                    inFlightConflict.cancel()
                    do { try await inFlightConflict.value;XCTFail("Reached conflict cancellation returned success") }
                    catch is CancellationError {}
                    let afterConflict = try await persistentVector(finishCore, includeApplyOwners: true)
                    let afterPending = try await finish.pendingConflict()
                    let reservation = try await text(finishCore,"SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state")
                    let applyMode = try await text(finishCore,"SELECT apply_mode FROM _sync_apply_state")
                    let foreignKeyFailures = try await text(finishCore,"SELECT count(*) FROM pragma_foreign_key_check")
                    XCTAssertEqual(beforeConflict,afterConflict,"in-flight conflict rollback preserves complete state")
                    XCTAssertEqual(pending,afterPending)
                    XCTAssertEqual("0",reservation)
                    XCTAssertEqual("0",applyMode)
                    XCTAssertEqual("0",foreignKeyFailures)
                } catch {
                    inFlightConflict.cancel()
                    _ = SwiftA3ConflictApplyProbeKt.releaseA3ConflictApplyForSwiftTest()
                    _ = try? await inFlightConflict.value
                    throw error
                }
                XCTAssertEqual(1,SwiftA3ConflictApplyProbeKt.releaseA3ConflictApplyForSwiftTest())
                // Prove the cancelled decision remains usable by the same owner.
                try await finish.resolveConflict(pending, result: .keepLocal)
                _ = try await finish.pushPending()
                let remaining = try await text(finishCore, "SELECT count(*) FROM _sync_outbox_rows")
                XCTAssertEqual(remaining, "0")
            }
        }
#else
        throw XCTSkip("Reached-conflict cancellation requires the isolated A3 test bridge")
#endif
    }

    func testPushCancellationRetainsFrozenRequestAcrossRetry() async throws {
        try await withLiveFixture { fixture in
            try await prepareConflict(fixture)
            try await withConnection(fixture) { finishCore, finish in
                let baseURL = fixture.baseURL
                let retained = try await finish.pendingConflict()
                let pending = try XCTUnwrap(retained)
                try await finish.resolveConflict(pending,result:.keepLocal)
                let box = CancellationClientBox(finish)
                let firstBlocked = try await blockedCount(baseURL)
                try await setBarrier(baseURL,path:"push")
                let preserving = Task { try await box.client.pushPending() }
                defer { preserving.cancel() }
                try await waitForBarrier(baseURL,after:firstBlocked)
                preserving.cancel()
                do {_ = try await preserving.value;XCTFail("Cancelled preservation returned success")} catch is CancellationError {}
                let frozen = try await finish.syncStatus()
                XCTAssertEqual(frozen.pending.pendingRowCount,1)
                let frozenHash = try await text(finishCore,"SELECT canonical_request_hash FROM _sync_outbox_bundle")
                XCTAssertEqual(frozenHash.count,64)
                let frozenRequest = try await persistentVector(finishCore,
                    tables: ["_sync_outbox_bundle", "_sync_outbox_rows", "_sync_source_state"])
                // The fixture exposes arrival counts, not HTTP bodies. Prove exact
                // retained request/source state across a second cancelled attempt;
                // do not label eventual drain as a wire-byte replay witness.
                let replayBlocked = try await blockedCount(baseURL)
                let replay = Task { try await box.client.pushPending() }
                defer { replay.cancel() }
                try await waitForBarrier(baseURL,after:replayBlocked)
                replay.cancel()
                do { _ = try await replay.value; XCTFail("Cancelled replay returned success") }
                catch is CancellationError {}
                let retainedRequest = try await persistentVector(finishCore,
                    tables: ["_sync_outbox_bundle", "_sync_outbox_rows", "_sync_source_state"])
                XCTAssertEqual(frozenRequest, retainedRequest, "retry retains the complete frozen request and source state")
                try await setBarrier(baseURL,path:"none")
                _ = try await finish.pushPending() // replay the original retained request
                let remaining = try await text(finishCore,"SELECT count(*) FROM _sync_outbox_rows");XCTAssertEqual(remaining,"0")
            }
        }
    }

    func testSnapshotCancellationPreservesStateAndResumes() async throws {
        try await withLiveFixture { fixture in
            try await prepareConflict(fixture)
            try await withConnection(fixture) { finishCore, finish in
                let baseURL = fixture.baseURL
                let retained = try await finish.pendingConflict()
                let pending = try XCTUnwrap(retained)
                try await finish.resolveConflict(pending, result: .keepLocal)
                _ = try await finish.pushPending()
                let box = CancellationClientBox(finish)
                // Existing snapshot admission retains rebuild_required until resume.
                // Compare every other binding, checkpoint and authority field exactly.
                let beforeSnapshot = try await persistentVector(finishCore, snapshotAdmission: true)
                let secondBlocked = try await blockedCount(baseURL)
                try await setBarrier(baseURL,path:"snapshot")
                let snapshot = Task { try await box.client.rebuild() }
                defer { snapshot.cancel() }
                try await waitForBarrier(baseURL,after:secondBlocked)
                snapshot.cancel()
                do {_ = try await snapshot.value;XCTFail("Cancelled snapshot returned success")} catch is CancellationError {}
                let cancelledSnapshot = try await persistentVector(finishCore, snapshotAdmission: true);XCTAssertEqual(beforeSnapshot,cancelledSnapshot)
                let rebuildRequired = try await text(finishCore,"SELECT rebuild_required FROM _sync_attachment_state");XCTAssertEqual(rebuildRequired,"1")
                try await setBarrier(baseURL,path:"none")
                _ = try await finish.rebuild()
                let finalName = try await text(finishCore,"SELECT name FROM users WHERE id='11111111-1111-4111-8111-111111111111'");XCTAssertEqual(finalName,"local intent")
            }
        }
    }

    private func withLiveFixture(_ body: (CancellationFixture) async throws -> Void) async throws {
        guard let rawURL = ProcessInfo.processInfo.environment["OVERSQLITE_SWIFT_CONVERGENT_BASE_URL"],
              let baseURL = URL(string: rawURL) else {
            throw XCTSkip("Requires a fresh isolated cancellation live authority (users fixture mode)")
        }
        let userID = "swift-cancellation-\(UUID().uuidString.lowercased())"
        var request = URLRequest(url: baseURL.appendingPathComponent("dummy-signin"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONSerialization.data(withJSONObject: ["user": userID, "password": "anything", "device": "swift-cancellation"])
        let (data, _) = try await URLSession.shared.data(for: request)
        let token = try XCTUnwrap((JSONSerialization.jsonObject(with: data) as? [String: Any])?["token"] as? String)
        let fixture = CancellationFixture(baseURL: baseURL, token: token, userID: userID,
            path: FileManager.default.temporaryDirectory.appendingPathComponent("\(userID).sqlite3"),
            peerPath: FileManager.default.temporaryDirectory.appendingPathComponent("\(userID)-peer.sqlite3"))
        defer {
            for path in [fixture.path, fixture.peerPath] {
                for suffix in ["", "-wal", "-shm"] { try? FileManager.default.removeItem(atPath: path.path + suffix) }
            }
        }
        do {
            try await withConnection(fixture) { core, client in
                try await core.execute(sql: "INSERT INTO users VALUES('11111111-1111-4111-8111-111111111111','original','a@example.com','2026-09-28','2026-09-28')", bindValues: [], affectedTables: ["users"])
                _ = try await client.pushPending()
            }
            try await body(fixture)
        } catch {
            try? await setBarrier(baseURL, path: "none")
            throw error
        }
    }

    private func withConnection(_ fixture: CancellationFixture, work: Int64 = 1_000_000,
                                path: URL? = nil,
                                body: (SQLiteNowCoreRuntimeDatabase, SQLiteNowSyncClient) async throws -> Void) async throws {
        let schema = "CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,email TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        let core = SQLiteNowCoreRuntimeDatabase(path: (path ?? fixture.path).path,
            migrationPlan: .init(latestVersion: 1, schemaSql: [schema], initSql: [], migrationSteps: [.init(version: 1, sql: [schema])]),
            debug: false, onMigrationStep: nil, syncManaged: true)
        var opening: SQLiteNowSyncOpening<SQLiteNowCoreRuntimeDatabase>?
        do {
            let config = SQLiteNowSyncConfig(generationId: 1, databaseId: "fixture", serviceId: "business", maxSyncWorkUnitsPerInvocation: work)
            let runtime = try await SQLiteNowSyncRuntimeKt.sqliteNowSyncRuntimeOpenForSync(coreDatabase: core,
                baseUrl: fixture.baseURL.absoluteString, auth: SQLiteNowSyncAuth.bearer(token: fixture.token).runtimeAuth,
                config: try config.runtimeConfig(syncTables: [.init(tableName: "users", syncKeyColumnName: "id")]), resolver: nil, upgradePlan: nil)
            let owned = SQLiteNowSyncOpening(runtime: runtime, database: core, lease: SQLiteNowSyncClientLease())
            opening = owned
            _ = try await owned.advance()
            let client = try XCTUnwrap(owned.readyOrNil()?.client)
            _ = try await client.attach(userId: fixture.userID)
            try await body(core, client)
            try await owned.close()
        } catch {
            if let opening { try? await opening.close() } else { try? await core.close() }
            throw error
        }
    }

    private func prepareConflict(_ fixture: CancellationFixture) async throws {
        // A separate owner observes S before its independently committed update.
        try await withConnection(fixture) { ownerCore, owner in
            _ = try await owner.rebuild()
            try await withConnection(fixture, path: fixture.peerPath) { peerCore, peer in
                _ = try await peer.rebuild()
                try await peerCore.execute(sql: "UPDATE users SET name='server update' WHERE id='11111111-1111-4111-8111-111111111111'", bindValues: [], affectedTables: ["users"])
                _ = try await peer.pushPending()
            }
            try await ownerCore.execute(sql: "UPDATE users SET name='local intent' WHERE id='11111111-1111-4111-8111-111111111111'", bindValues: [], affectedTables: ["users"])
            do { _ = try await owner.pushPending(); XCTFail("Expected deferred conflict") }
            catch let error as SQLiteNowError {
                guard case .conflictNeedsResolution = error else { throw error }
            }
            let pending = try await owner.pendingConflict()
            _ = try XCTUnwrap(pending)
        }
    }

    private func text(_ core: SQLiteNowCoreRuntimeDatabase,_ sql: String) async throws -> String {
        let rows = try await core.query(sql:sql,bindValues:[],columnTypes:["text"])
        return rows.rowAt(index:0).cellAt(index:0).textValue ?? ""
    }
    private func persistentVector(_ core: SQLiteNowCoreRuntimeDatabase, snapshotAdmission: Bool = false,
                                  includeApplyOwners: Bool = false,
                                  tables selectedTables: [String]? = nil) async throws -> [String: [String]] {
        var vector: [String: [String]] = [:]
        let tables = selectedTables ?? (["users","_sync_row_state","_sync_dirty_rows","_sync_outbox_rows","_sync_outbox_bundle","_sync_source_state","_sync_attachment_state","_sync_relationships","_sync_conflict_state",
            "_sync_opening_state","_sync_contract_identity","_sync_admitted_contracts","_sync_managed_tables"] +
            (includeApplyOwners ? ["_sync_apply_plan","_sync_apply_rows","_sync_apply_edges","_sync_apply_state","_sync_operation_state","_sync_snapshot_stage","_sync_push_plan","_sync_push_nodes","_sync_push_edges"] : []))
        for table in tables {
            let columns = try await core.query(sql:"SELECT name FROM pragma_table_info('\(table)') ORDER BY cid",bindValues:[],columnTypes:["text"])
            let names = (0..<columns.count).map { columns.rowAt(index:$0).cellAt(index:0).textValue ?? "" }
            guard !names.isEmpty else { throw SQLiteNowError.protocol(message: "State witness requires table \(table)") }
            let values = names.map { name in
                let quoted = "\"\(name.replacingOccurrences(of: "\"", with: "\"\""))\""
                let value = snapshotAdmission && table == "_sync_attachment_state" && name == "rebuild_required" ? "1" : quoted
                return "json_object('storage',typeof(\(value)),'value',CASE typeof(\(value)) WHEN 'blob' THEN hex(\(value)) WHEN 'real' THEN printf('%!.26g',\(value)) ELSE CAST(\(value) AS TEXT) END)"
            }.joined(separator:",")
            let rows = try await core.query(sql:"SELECT json_array(\(values)) AS row_value FROM \"\(table)\" ORDER BY row_value COLLATE BINARY",bindValues:[],columnTypes:["text"])
            vector[table] = try (0..<rows.count).map { index in
                try XCTUnwrap(rows.rowAt(index:index).cellAt(index:0).textValue)
            }
        }
        return vector
    }
    private func setBarrier(_ baseURL: URL,path: String) async throws {
        var components = URLComponents(url:baseURL.appendingPathComponent("__test/block"),resolvingAgainstBaseURL:false)!
        components.queryItems=[.init(name:"path",value:path)]
        let (_,response) = try await URLSession.shared.data(from:components.url!)
        XCTAssertEqual((response as? HTTPURLResponse)?.statusCode,204)
    }
    private func blockedCount(_ baseURL: URL) async throws -> Int {
        let (data,_) = try await URLSession.shared.data(from:baseURL.appendingPathComponent("__test/pushes"))
        return try XCTUnwrap((JSONSerialization.jsonObject(with:data) as? [String:Int])?["blocked"])
    }
    private func waitForBarrier(_ baseURL: URL,after count: Int) async throws {
        let deadline=Date().addingTimeInterval(15)
        while Date()<deadline {
            if try await blockedCount(baseURL)>count {return}
            try await Task.sleep(nanoseconds:20_000_000)
        }
        throw SQLiteNowError.protocol(message:"Cancellation fixture barrier timed out")
    }

#if A3_TEST_BRIDGE
    private func waitForConflictApplyMarker() async throws {
        let deadline = Date().addingTimeInterval(15)
        while Date() < deadline {
            if SwiftA3ConflictApplyProbeKt.a3ConflictApplyEnteredForSwiftTest() == 1 { return }
            try await Task.sleep(nanoseconds:20_000_000)
        }
        throw SQLiteNowError.protocol(message:"Reached-conflict apply marker timed out")
    }
#endif

}

private final class CancellationClientBox: @unchecked Sendable {
    let client: SQLiteNowSyncClient
    init(_ client: SQLiteNowSyncClient) { self.client=client }
}

private actor CancellationStartGate {
    private var released=false
    private var continuation: CheckedContinuation<Void,Never>?
    func wait() async {if !released {await withCheckedContinuation {continuation=$0}}}
    func release() {released=true;continuation?.resume();continuation=nil}
}

private struct CancellationFixture {
    let baseURL: URL
    let token: String
    let userID: String
    let path: URL
    let peerPath: URL
}
