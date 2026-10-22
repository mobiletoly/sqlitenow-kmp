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
                let box = CancellationClientBox(client)
                let gate = CancellationStartGate()
                let operation = Task { await gate.wait(); _ = try await box.client.rebuild() }
                operation.cancel()
                await gate.release()
                do { _ = try await operation.value; XCTFail("Cancelled snapshot returned success") }
                catch is CancellationError {}
                let after = try await client.syncStatus()
                XCTAssertEqual(before.pending.pendingRowCount,after.pending.pendingRowCount)
                try await finishRebuildAfterWorkYields(client)
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
                let gate = CancellationStartGate()
                let box = CancellationClientBox(limited)
                let decision = Task { await gate.wait();try await box.client.resolveConflict(pending,result:.acceptServer) }
                decision.cancel()
                await gate.release()
                do { try await decision.value;XCTFail("Cancelled conflict returned success") } catch is CancellationError {}
                let cancelledPending = try await limited.pendingConflict();XCTAssertEqual(pending,cancelledPending)
                let cancelledVector = try await persistentVector(limitedCore);XCTAssertEqual(before,cancelledVector)
                try await finishConflictResolutionAfterWorkYields(limited, pending: pending)
                let resolved = try await limited.pendingConflict()
                XCTAssertNil(resolved)
            }
        }
    }

    private func finishRebuildAfterWorkYields(_ client: SQLiteNowSyncClient) async throws {
        for _ in 0..<32 {
            do { _ = try await client.rebuild(); return }
            catch let error as SQLiteNowError {
                guard case .syncWorkLimit = error else { throw error }
            }
        }
        XCTFail("rebuild made no completion progress after 32 bounded invocations")
    }

    private func finishConflictResolutionAfterWorkYields(
        _ client: SQLiteNowSyncClient, pending: SQLiteNowPendingConflict
    ) async throws {
        for _ in 0..<32 {
            do { try await client.resolveConflict(pending, result: .acceptServer); return }
            catch let error as SQLiteNowError {
                guard case .syncWorkLimit = error else { throw error }
            }
        }
        XCTFail("conflict resolution made no completion progress after 32 bounded invocations")
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
                    tables: ["_sync_outbox_bundle", "_sync_outbox_rows", "_sync_source_state", "_sync_upload_journal"])
                // The fixture exposes arrival counts, not HTTP bodies. Prove exact
                // retained request/source state across a second cancelled attempt;
                // do not label eventual drain as a wire-byte replay witness.
                try await setBarrier(baseURL,path:"resolve")
                let replayBlocked = try await blockedCount(baseURL)
                let replay = Task { try await box.client.pushPending() }
                defer { replay.cancel() }
                try await waitForBarrier(baseURL,after:replayBlocked)
                replay.cancel()
                do { _ = try await replay.value; XCTFail("Cancelled replay returned success") }
                catch is CancellationError {}
                let retainedRequest = try await persistentVector(finishCore,
                    tables: ["_sync_outbox_bundle", "_sync_outbox_rows", "_sync_source_state", "_sync_upload_journal"])
                XCTAssertEqual(frozenRequest, retainedRequest, "retry retains the complete frozen request and source state")
                try await setBarrier(baseURL,path:"none")
                _ = try await finish.pushPending() // resolve and acknowledge the cancelled original F
                let restored = try await text(finishCore,"SELECT count(*) FROM _sync_dirty_rows")
                XCTAssertEqual(restored,"1","cancelled F restores original intent for its successor")
                _ = try await finish.pushPending()
                let drained = try await text(finishCore,"SELECT count(*) FROM _sync_dirty_rows")
                XCTAssertEqual(drained,"0")
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

    func testFinalStateRecoveryCancellationPreservesOriginalIntent() async throws {
#if A3_TEST_BRIDGE
        try await withLiveFixture { fixture in
            let id = "11111111-1111-4111-8111-111111111111"
            var originalRequest = ""
            var originalHash = ""
            var originalSource = ""
            var terminal = ""
            var retainedCertificate: [String: [String]] = [:]
            try await withConnection(fixture) { core, client in
                try await core.execute(sql: "UPDATE users SET name='frozen recovery' WHERE id='\(id)'", bindValues: [], affectedTables: ["users"])
                // The authority has committed before it holds this response.
                try await cancelPushAtBarrier(fixture.baseURL, client: client, phase: "push-response")
                originalRequest = try await text(core,"SELECT hex(request_body) FROM _sync_upload_journal")
                originalHash = try await text(core,"SELECT canonical_request_hash FROM _sync_outbox_bundle")
                originalSource = try await text(core,"SELECT source_id FROM _sync_source_state")
                XCTAssertFalse(originalRequest.isEmpty)
                XCTAssertEqual(originalHash.count,64)
                try await core.execute(sql: "UPDATE users SET name='later intent' WHERE id='\(id)'", bindValues: [], affectedTables: ["users"])
                let beforeResolve = try await persistentVector(core, includeApplyOwners: true)
                try await cancelPushAtBarrier(fixture.baseURL, client: client, phase: "resolve")
                let afterResolve = try await persistentVector(core, includeApplyOwners: true)
                XCTAssertEqual(beforeResolve,afterResolve,"resolve cancellation retains complete original F, bases and later intent")
                try recordRecoveryVector("resolve", afterResolve, detail: "source=\(originalSource) hash=\(originalHash)")
                try await setBarrier(fixture.baseURL,path:"none")
                retainedCertificate = afterResolve
            }
            // Reopen the file before consuming the immutable terminal result.
            try await withConnection(fixture) { core, client in
                let reopened = try await persistentVector(core, includeApplyOwners: true)
                XCTAssertEqual(retainedCertificate,reopened)
                let oldState = try await persistentVector(core)
                try await cancelPushAtFinalApply(client)
                let captured = try await persistentVector(core)
                XCTAssertEqual(oldState.filter { $0.key != "_sync_upload_journal" },
                               captured.filter { $0.key != "_sync_upload_journal" },
                               "precommit cancellation preserves every old business, base, capture, dirty, source and checkpoint field")
                let request = try await text(core,"SELECT hex(request_body) FROM _sync_upload_journal")
                let hash = try await text(core,"SELECT canonical_request_hash FROM _sync_outbox_bundle")
                let phase = try await text(core,"SELECT phase FROM _sync_upload_journal")
                let outcomeStatus = try await text(core,"SELECT json_extract(CAST(outcome_body AS TEXT),'$.status') FROM _sync_upload_journal")
                terminal = try await text(core,"SELECT hex(outcome_body) FROM _sync_upload_journal")
                XCTAssertEqual(originalRequest,request);XCTAssertEqual(originalHash,hash)
                XCTAssertEqual("frozen",phase);XCTAssertEqual("committed",outcomeStatus);XCTAssertFalse(terminal.isEmpty)
                try await assertReleasedApplyOwners(core)
                // The second cut starts with durable C already present. Nothing,
                // including C or reusable preparation, may change across rollback.
                let beforeFinal = try await persistentVector(core, includeApplyOwners: true)
                try await cancelPushAtFinalApply(client)
                let afterFinal = try await persistentVector(core, includeApplyOwners: true)
                XCTAssertEqual(beforeFinal,afterFinal,"final apply cancellation preserves complete pre-existing F/C state")
                retainedCertificate = afterFinal
                try recordRecoveryVector("final_apply", afterFinal)
            }
            try await withConnection(fixture) { core, client in
                let reopened = try await persistentVector(core, includeApplyOwners: true)
                XCTAssertEqual(retainedCertificate,reopened,"terminal evidence survives physical reopen")
                try await cancelPushAtBarrier(fixture.baseURL, client: client, phase: "ack")
                let phase = try await text(core,"SELECT phase FROM _sync_upload_journal")
                let request = try await text(core,"SELECT hex(request_body) FROM _sync_upload_journal")
                let outcome = try await text(core,"SELECT hex(outcome_body) FROM _sync_upload_journal")
                let name = try await text(core,"SELECT name FROM users WHERE id='\(id)'")
                let dirty = try await text(core,"SELECT count(*) FROM _sync_dirty_rows")
                let outbox = try await text(core,"SELECT count(*) FROM _sync_outbox_rows")
                let next = try await text(core,"SELECT next_source_bundle_id FROM _sync_source_state")
                let checkpoint = try await text(core,"SELECT last_bundle_seq_seen FROM _sync_attachment_state")
                let source = try await text(core,"SELECT source_id FROM _sync_source_state")
                XCTAssertEqual("ack_pending",phase,"after-commit cancellation retains durable completion")
                XCTAssertEqual(originalRequest,request);XCTAssertEqual(terminal,outcome);XCTAssertEqual(originalSource,source)
                XCTAssertEqual("later intent",name);XCTAssertEqual("1",dirty);XCTAssertEqual("0",outbox)
                XCTAssertEqual("3",next);XCTAssertEqual("2",checkpoint)
                try await assertReleasedApplyOwners(core)
                let committed = try await persistentVector(core, includeApplyOwners: true)
                try recordRecoveryVector("ack", committed)
                try await setBarrier(fixture.baseURL,path:"none")
                _ = try await client.pushPending() // retries only the retained ack
                let released = try await text(core,"SELECT count(*) FROM _sync_upload_journal")
                XCTAssertEqual("0",released)
                _ = try await client.pushPending() // publish the later edit as its own successor
                let remaining = try await text(core,"SELECT count(*) FROM _sync_dirty_rows")
                XCTAssertEqual("0",remaining)
                let beforeDownload = try await persistentVector(core, snapshotAdmission: true)
                let box = CancellationClientBox(client)
                let blocked = try await blockedCount(fixture.baseURL)
                try await setBarrier(fixture.baseURL,path:"snapshot")
                let snapshot = Task { try await box.client.rebuild() }
                do {
                    try await waitForBarrier(fixture.baseURL,after:blocked)
                    snapshot.cancel()
                    do { _ = try await snapshot.value;XCTFail("snapshot cancellation returned success") } catch is CancellationError {}
                } catch {
                    snapshot.cancel();_ = try? await snapshot.value;throw error
                }
                let afterDownload = try await persistentVector(core, snapshotAdmission: true)
                XCTAssertEqual(beforeDownload,afterDownload,"download cancellation preserves the full old logical state")
                try await assertReleasedApplyOwners(core)
                let staged = try await persistentVector(core, includeApplyOwners: true)
                try recordRecoveryVector("snapshot_download", staged)
                try await setBarrier(fixture.baseURL,path:"none")
                _ = try await client.rebuild()
                let finalName = try await text(core,"SELECT name FROM users WHERE id='\(id)'")
                let journal = try await text(core,"SELECT count(*) FROM _sync_upload_journal")
                let stage = try await text(core,"SELECT count(*) FROM _sync_snapshot_stage")
                XCTAssertEqual("later intent",finalName);XCTAssertEqual("0",journal);XCTAssertEqual("0",stage)
                try await assertReleasedApplyOwners(core)
            }
        }
        FileHandle.standardOutput.write(Data("RECOVERY_SWIFT_COMPLETE original_intent resolve snapshot_download final_apply ack reopen\n".utf8))
#else
        throw XCTSkip("Final-state apply cancellation requires the isolated test framework")
#endif
    }

    // Persist large vectors separately so XCTest stderr cannot split its native
    // completion line into the middle of a long stdout write.
    private func recordRecoveryVector(_ phase: String, _ vector: [String: [String]], detail: String = "") throws {
        guard let path = ProcessInfo.processInfo.environment["OVERSQLITE_SWIFT_RECOVERY_EVIDENCE_ROOT"] else {
            throw SQLiteNowError.protocol(message: "Owned recovery evidence directory is required")
        }
        let root = URL(fileURLWithPath: path, isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let target = root.appendingPathComponent("\(phase).json")
        guard !FileManager.default.fileExists(atPath: target.path) else {
            throw SQLiteNowError.protocol(message: "Recovery phase evidence already exists")
        }
        try JSONSerialization.data(withJSONObject: vector, options: [.sortedKeys]).write(to: target, options: .atomic)
        FileHandle.standardOutput.write(Data("RECOVERY_SWIFT_BOUNDARY \(phase) vector_file=\(target.lastPathComponent) \(detail)\n".utf8))
    }

    private func cancelPushAtBarrier(_ baseURL: URL, client: SQLiteNowSyncClient, phase: String) async throws {
        let box = CancellationClientBox(client)
        let blocked = try await blockedCount(baseURL)
        try await setBarrier(baseURL,path:phase)
        let operation = Task { try await box.client.pushPending() }
        do {
            try await waitForBarrier(baseURL,after:blocked)
            operation.cancel()
            do { _ = try await operation.value;XCTFail("\(phase) cancellation returned success") } catch is CancellationError {}
        } catch {
            operation.cancel();_ = try? await operation.value;throw error
        }
    }

    private func assertReleasedApplyOwners(_ core: SQLiteNowCoreRuntimeDatabase) async throws {
        let apply = try await text(core,"SELECT apply_mode FROM _sync_apply_state")
        let reservation = try await text(core,"SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state")
        let foreignKeys = try await text(core,"SELECT count(*) FROM pragma_foreign_key_check")
        XCTAssertEqual("0",apply);XCTAssertEqual("0",reservation);XCTAssertEqual("0",foreignKeys)
    }

#if A3_TEST_BRIDGE
    private func cancelPushAtFinalApply(_ client: SQLiteNowSyncClient) async throws {
        XCTAssertEqual(1,SwiftA3ConflictApplyProbeKt.armRecoveryApplyForSwiftTest())
        let box = CancellationClientBox(client)
        let operation = Task { try await box.client.pushPending() }
        do {
            try await waitForConflictApplyMarker()
            operation.cancel()
            do { _ = try await operation.value;XCTFail("final apply cancellation returned success") } catch is CancellationError {}
        } catch {
            operation.cancel();_ = SwiftA3ConflictApplyProbeKt.releaseA3ConflictApplyForSwiftTest()
            _ = try? await operation.value;throw error
        }
        XCTAssertEqual(1,SwiftA3ConflictApplyProbeKt.releaseA3ConflictApplyForSwiftTest())
    }
#endif

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
        let tables = selectedTables ?? (["users","_sync_row_state","_sync_dirty_rows","_sync_outbox_rows","_sync_outbox_bundle","_sync_upload_journal","_sync_source_state","_sync_attachment_state","_sync_relationships","_sync_conflict_state",
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
