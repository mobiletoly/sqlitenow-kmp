import Foundation
@preconcurrency import SQLiteNowSyncRuntime
@testable import SQLiteNowSyncSupport
import XCTest

final class SyncConvergentMappingTests: XCTestCase {
    func testDefaultWorkspaceEntriesReachRuntime() throws {
        let config = SQLiteNowSyncConfig(generationId: 1, databaseId: "fixture", serviceId: "business")
        XCTAssertEqual(config.maxSyncWorkspaceEntries, 131_072)
        XCTAssertEqual(try config.runtimeConfig(syncTables: []).maxSyncWorkspaceEntries, 131_072)
    }

    func testLocalPlannerMappingPreservesExactInt64ProgressAndIdentity() throws {
        let body = #"{"reason":"push_dependency_group_exceeds_admitted_limit","dimension":"rows","limit":1,"actual":2,"componentRows":2,"componentDecodedBytes":9007199254740993,"diagnosticId":"diagnostic","sample":[{"schema":"main","table":"records","key":{"id":"typed-key"}}],"hasMore":true,"progress":{"dirtyRows":9007199254740993,"outboundRows":0,"committedBundles":1}}"#
        let payload = SQLiteNowSyncRuntimeErrorPayload(
            category: "protocol", code: "PushDependencyGroupExceedsAdmittedLimitException", message: "local limit",
            remoteContract: false, httpStatus: 0, operation: "", remediation: nil, retryable: false, generationId: nil,
            incompleteReason: nil, dirtyCount: 0, outboundCount: 0, confirmedBundleCount: 0, pushAttempts: 0,
            localPlanningJson: body
        )
        guard case let .pushDependencyGroupExceedsAdmittedLimit(result) = SQLiteNowError.from(payload) else {
            return XCTFail("planner error lost its typed result")
        }
        XCTAssertEqual(result.componentDecodedBytes, 9007199254740993)
        XCTAssertEqual(result.progress.dirtyRows, 9007199254740993)
        XCTAssertEqual(result.progress.committedBundles, 1)
        XCTAssertEqual(result.sample.first?.key, ["id": "typed-key"])
        XCTAssertTrue(result.hasMore)
    }

    func testAllOtherLocalPlanningResultsRemainTypedAndMalformedFailsClosed() {
        let progress = #""progress":{"dirtyRows":3,"outboundRows":2,"committedBundles":1}"#
        let bodies: [(String, String)] = [
            ("FKReconciliationRequiredException", #"{"reason":"fk_reconciliation_required","detail":"dependency_unavailable","unit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sample":[],"hasMore":false,"# + progress + "}"),
            ("ResolutionBaseUnavailableException", #"{"reason":"resolution_base_unavailable","unit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sample":[],"hasMore":false,"# + progress + "}"),
            ("SyncWorkLimitException", #"{"reason":"sync_work_limit","unit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","limit":100,"used":99,"# + progress + "}"),
            ("SyncWorkspaceLimitException", #"{"reason":"sync_workspace_limit","unit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","dimension":"entries","limit":100,"used":99,"reserved":2,"resumable":false,"# + progress + "}"),
            ("StalePushDependencyDiagnosticException", #"{"reason":"stale_push_dependency_diagnostic","diagnosticId":"diagnostic"}"#)
        ]
        for (code, body) in bodies {
            let payload = SQLiteNowSyncRuntimeErrorPayload(category: "state", code: code, message: "local", remoteContract: false,
                httpStatus: 0, operation: "", remediation: nil, retryable: false, generationId: nil, incompleteReason: nil,
                dirtyCount: 0, outboundCount: 0, confirmedBundleCount: 0, pushAttempts: 0, localPlanningJson: body)
            switch SQLiteNowError.from(payload) {
            case .fkReconciliationRequired, .resolutionBaseUnavailable, .syncWorkLimit, .syncWorkspaceLimit, .stalePushDependencyDiagnostic: break
            default: XCTFail("lost local typed result: \(code)")
            }
        }
        let malformed = SQLiteNowSyncRuntimeErrorPayload(category: "state", code: "SyncWorkLimitException", message: "local", remoteContract: false,
            httpStatus: 0, operation: "", remediation: nil, retryable: false, generationId: nil, incompleteReason: nil,
            dirtyCount: 0, outboundCount: 0, confirmedBundleCount: 0, pushAttempts: 0, localPlanningJson: "{}")
        XCTAssertEqual(SQLiteNowError.from(malformed), .protocol(message: "invalid local planning result"))
    }

    func testA3ClosedPendingShapeAndExactCounters() throws {
        let unit = String(repeating: "a", count: 64)
        let progress: [String: Any] = ["dirtyRows": Int64(9007199254740993), "outboundRows": 2, "committedBundles": 1]
        let valid: [String: Any] = ["reason": "fk_reconciliation_required", "detail": "dependency_unavailable",
            "unit": unit, "sample": [], "hasMore": false, "progress": progress]
        func payload(_ body: [String: Any], code: String = "FKReconciliationRequiredException", category: String = "state") throws -> SQLiteNowSyncRuntimeErrorPayload {
            .init(category: category, code: code, message: "local", remoteContract: false, httpStatus: 0,
                  operation: "", remediation: nil, retryable: false, generationId: nil, incompleteReason: nil,
                  dirtyCount: 0, outboundCount: 0, confirmedBundleCount: 0, pushAttempts: 0,
                  localPlanningJson: String(decoding: try JSONSerialization.data(withJSONObject: body), as: UTF8.self))
        }
        for detail in ["relationship_unknown", "dependency_unavailable", "historical_repair_required"] {
            var body = valid; body["detail"] = detail
            guard case let .fkReconciliationRequired(result) = SQLiteNowError.from(try payload(body)) else { return XCTFail("lost valid detail") }
            XCTAssertEqual(result.progress.dirtyRows, 9007199254740993)
            XCTAssertEqual(result.detail, detail)
        }
        var invalid: [[String: Any]] = []
        for (field, value) in [("detail", "missing_parent"), ("reason", "other"), ("unit", "unit"), ("unit", unit.uppercased())] {
            var body = valid; body[field] = value; invalid.append(body)
        }
        for count: Any in [-1, "3", true, 0.5] {
            var body = valid; var p = progress; p["dirtyRows"] = count; body["progress"] = p; invalid.append(body)
        }
        let ref: [String: Any] = ["schema": "main", "table": "nodes", "key": ["id": "C"]]
        var tooMany = valid; tooMany["sample"] = Array(repeating: ref, count: 17); invalid.append(tooMany)
        var tooWide = valid; tooWide["sample"] = [["schema": "main", "table": "nodes", "key": ["id": String(repeating: "x", count: 4096)]]]; invalid.append(tooWide)
        var extra = valid; extra["extra"] = 1; invalid.append(extra)
        for body in invalid {
            XCTAssertEqual(SQLiteNowError.from(try payload(body)), .protocol(message: "invalid local planning result"))
        }
        XCTAssertEqual(SQLiteNowError.from(try payload(valid, category: "protocol")), .protocol(message: "invalid local planning result"))
        XCTAssertEqual(SQLiteNowError.from(try payload(valid, category: "cancelled")), .cancelled(message: "local"))
        for dimension in ["entries", "bytes", "atomic_apply_work", "other"] {
            let body: [String: Any] = ["reason": "sync_workspace_limit", "unit": unit, "dimension": dimension,
                "limit": Int64.max, "used": Int64(9007199254740993), "reserved": Int64.max, "resumable": false, "progress": progress]
            let mapped = SQLiteNowError.from(try payload(body, code: "SyncWorkspaceLimitException"))
            if dimension == "other" || dimension == "atomic_apply_work" {
                XCTAssertEqual(mapped, .protocol(message: "invalid local planning result"))
            }
            else if case let .syncWorkspaceLimit(result) = mapped {
                XCTAssertEqual(result.used, 9007199254740993); XCTAssertEqual(result.reserved, Int64.max); XCTAssertFalse(result.resumable)
            } else { XCTFail("lost workspace dimension") }
        }
    }

    func testIncompleteErrorAndBlockedDetachKeepTypedProgress() {
        let payload = SQLiteNowSyncRuntimeErrorPayload(
            category: "state", code: "SyncIncompleteException", message: "bounded sync",
            remoteContract: false, httpStatus: 0, operation: "sync", remediation: nil,
            retryable: false, generationId: nil, incompleteReason: "PUSH_ATTEMPT_LIMIT",
            dirtyCount: 1, outboundCount: 2, confirmedBundleCount: 64, pushAttempts: 64, localPlanningJson: nil
        )
        XCTAssertEqual(
            SQLiteNowError.from(payload),
            .syncIncomplete(reason: .pushAttemptLimit, dirtyCount: 1, outboundCount: 2,
                            confirmedBundleCount: 64, pushAttempts: 64)
        )

        let runtime = SQLiteNowSyncRuntimeSyncThenDetachResult(
            lastSync: nil,
            detach: SQLiteNowSyncRuntimeDetachOutcome(outcome: "BLOCKED_UNSYNCED_DATA"),
            syncRounds: 1,
            remainingPendingRowCount: 3,
            blockedReason: "PUSH_ATTEMPT_LIMIT",
            success: false
        )
        let result = SQLiteNowSyncThenDetachResult(runtime)
        XCTAssertNil(result.lastSync)
        XCTAssertEqual(result.blockedReason, .pushAttemptLimit)
        XCTAssertEqual(result.remainingPendingRowCount, 3)
        XCTAssertFalse(result.success)
    }
}
