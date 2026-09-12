import Foundation
import SchemaEvolutionGenerationOneDatabaseSQLiteNow
import SchemaEvolutionGenerationTargetDatabaseSQLiteNow
import SchemaEvolutionInvalidTargetDatabaseSQLiteNow
import SchemaEvolutionLocalTwoDatabaseSQLiteNow
import XCTest

private typealias SchemaEvolutionGenerationOne = SchemaEvolutionGenerationOneDatabaseSQLiteNow.SchemaEvolutionGenerationOneDatabase
private typealias SchemaEvolutionLocalTwo = SchemaEvolutionLocalTwoDatabaseSQLiteNow.SchemaEvolutionLocalTwoDatabase
private typealias SchemaEvolutionTarget = SchemaEvolutionGenerationTargetDatabaseSQLiteNow.SchemaEvolutionGenerationTargetDatabase
private typealias SchemaEvolutionInvalidTarget = SchemaEvolutionInvalidTargetDatabaseSQLiteNow.SchemaEvolutionInvalidTargetDatabase

final class SchemaEvolutionGeneratedLiveAuthorityTests: XCTestCase {
    func testGeneratedSwiftLocalThenAdditiveInvalidRollbackAndRetry() async throws {
        let (baseURL, token) = try await authority()
        let path = temporaryPath("main")
        let user = "schemaevolution-swift-\(UUID().uuidString.lowercased())"
        let authOne = SchemaEvolutionGenerationOneDatabaseSQLiteNow.SQLiteNowSyncAuth.bearer(token: token)
        let authLocal = SchemaEvolutionLocalTwoDatabaseSQLiteNow.SQLiteNowSyncAuth.bearer(token: token)
        let authTarget = SchemaEvolutionGenerationTargetDatabaseSQLiteNow.SQLiteNowSyncAuth.bearer(token: token)
        let authInvalid = SchemaEvolutionInvalidTargetDatabaseSQLiteNow.SQLiteNowSyncAuth.bearer(token: token)
        var oneOpening: SchemaEvolutionGenerationOneDatabaseSQLiteNow.SQLiteNowSyncOpening<SchemaEvolutionGenerationOne>?
        var localOpening: SchemaEvolutionLocalTwoDatabaseSQLiteNow.SQLiteNowSyncOpening<SchemaEvolutionLocalTwo>?
        var invalidOpening: SchemaEvolutionInvalidTargetDatabaseSQLiteNow.SQLiteNowSyncOpening<SchemaEvolutionInvalidTarget>?
        var targetOpening: SchemaEvolutionGenerationTargetDatabaseSQLiteNow.SQLiteNowSyncOpening<SchemaEvolutionTarget>?

        do {
            let one = SchemaEvolutionGenerationOne(path: path)
            let oneConfig = SchemaEvolutionGenerationOneDatabaseSQLiteNow.SQLiteNowSyncConfig(
                generationId: 1, databaseId: Self.databaseId, serviceId: Self.serviceId
            )
            oneOpening = try await one.openForSync(baseURL: baseURL, auth: authOne, config: oneConfig)
            XCTAssertNil(oneOpening?.readyOrNil())
            let oneAdvance = try await XCTUnwrap(oneOpening).advance()
            XCTAssertEqual(oneAdvance.outcome, .ready)
            let oneClient = try one.makeSyncClient(baseURL: baseURL, auth: authOne, config: oneConfig)
            _ = try await oneClient.attach(userId: user)
            try await one.person.insert(.init(id: Self.personId, name: "Ada"))
            try await one.note.insert(.init(id: Self.noteId, body: "generation one"))
            try await one.draft.insert(.init(id: Self.draftId, label: "mixed", payload: Data([0, 1, 2, 255])))
            _ = try await oneClient.sync()
            try await oneOpening?.close()
            oneOpening = nil

            let local = SchemaEvolutionLocalTwo(path: path, onMigrationStep: { step in
                if step.toVersion == 2 {
                    try await step.connection.execute(
                        "CREATE TRIGGER drafts_touch AFTER UPDATE OF label ON drafts " +
                            "BEGIN UPDATE drafts SET normalized=upper(new.label) WHERE id=new.id; END"
                    )
                }
            })
            let localConfig = SchemaEvolutionLocalTwoDatabaseSQLiteNow.SQLiteNowSyncConfig(
                generationId: 1, databaseId: Self.databaseId, serviceId: Self.serviceId
            )
            localOpening = try await local.openForSync(
                baseURL: baseURL,
                auth: authLocal,
                config: localConfig,
                upgradePlan: SchemaEvolutionLocalTwoDatabaseSQLiteNow.SQLiteNowSyncUpgradePlan(
                    migrationId: "schemaevolution-swift-local-v1-v2",
                    fromGenerationId: 1,
                    toGenerationId: 1,
                    fromLocalVersion: 1,
                    toLocalVersion: 2,
                    preservedLocalTables: ["drafts"]
                )
            )
            XCTAssertNil(localOpening?.readyOrNil())
            let localAdvance = try await XCTUnwrap(localOpening).advance()
            XCTAssertEqual(localAdvance.outcome, .ready)
            let transformed = try await local.draft.selectAll().list()
            XCTAssertEqual(transformed.single?.normalized, "MIXED")
            XCTAssertEqual(transformed.single?.payload, Data([0, 1, 2, 255]))
            try await localOpening?.close()
            localOpening = nil

            let targetConfig = SchemaEvolutionGenerationTargetDatabaseSQLiteNow.SQLiteNowSyncConfig(
                generationId: 2, databaseId: Self.databaseId, serviceId: Self.serviceId
            )
            let plan = SchemaEvolutionGenerationTargetDatabaseSQLiteNow.SQLiteNowSyncUpgradePlan(
                migrationId: "schemaevolution-swift-generation-v1-v2",
                fromGenerationId: 1,
                toGenerationId: 2,
                fromLocalVersion: 2,
                toLocalVersion: 3,
                preservedLocalTables: ["drafts"]
            )
            let invalid = SchemaEvolutionInvalidTarget(path: path)
            invalidOpening = try await invalid.openForSync(
                baseURL: baseURL,
                auth: authInvalid,
                config: .init(generationId: 2, databaseId: Self.databaseId, serviceId: Self.serviceId),
                upgradePlan: .init(
                    migrationId: plan.migrationId,
                    fromGenerationId: 1,
                    toGenerationId: 2,
                    fromLocalVersion: 2,
                    toLocalVersion: 3,
                    preservedLocalTables: ["drafts"]
                )
            )
            do {
                _ = try await XCTUnwrap(invalidOpening).advance()
                XCTFail("The target without physical foreign keys must fail U4")
            } catch {
                let invalidStatus = try await XCTUnwrap(invalidOpening).status()
                XCTAssertEqual(invalidStatus.outcome, .localMigrationFailed)
                XCTAssertNil(invalidOpening?.readyOrNil())
            }
            try await invalidOpening?.close()
            invalidOpening = nil

            let target = SchemaEvolutionTarget(path: path)
            targetOpening = try await target.openForSync(
                baseURL: baseURL, auth: authTarget, config: targetConfig, upgradePlan: plan
            )
            XCTAssertNil(targetOpening?.readyOrNil())
            XCTAssertThrowsError(try target.makeSyncClient(baseURL: baseURL, auth: authTarget, config: targetConfig))
            do {
                _ = try await target.person.selectAll().list()
                XCTFail("Generated target queries must remain unavailable before U8")
            } catch {}
            var blockedObserver = target.person.selectAll().stream().makeAsyncIterator()
            do {
                _ = try await blockedObserver.next()
                XCTFail("Generated target observers must remain unavailable before U8")
            } catch {}

            let targetAdvance = try await XCTUnwrap(targetOpening).advance()
            XCTAssertEqual(targetAdvance.outcome, .ready)
            _ = try target.makeSyncClient(baseURL: baseURL, auth: authTarget, config: targetConfig)
            let people = try await target.person.selectAll().list()
            XCTAssertEqual(people.single?.name, "Ada")
            XCTAssertNil(people.single?.details)
            XCTAssertNil(people.single?.projectId)
            let notes = try await target.note.selectAll().list()
            let drafts = try await target.draft.selectAll().list()
            XCTAssertEqual(notes.single?.body, "generation one")
            XCTAssertEqual(drafts.single?.normalized, "MIXED")

            let initialObserver = expectation(description: "generated target observer publishes after U8")
            let changedObserver = expectation(description: "generated target observer receives person mutation")
            let observerTask = Task {
                var emission = 0
                for try await rows in target.person.selectAll().stream() {
                    emission += 1
                    if emission == 1 {
                        initialObserver.fulfill()
                    } else if emission == 2 {
                        changedObserver.fulfill()
                        return rows.count
                    }
                }
                return -1
            }
            await fulfillment(of: [initialObserver], timeout: 5.0)

            try await target.project.insert(.init(id: Self.projectId, title: "Additive"))
            try await target.person.insert(.init(
                id: Self.personTwoId, name: "Grace", details: "target", projectId: Self.projectId
            ))
            await fulfillment(of: [changedObserver], timeout: 5.0)
            let observerEmissionCount = try await observerTask.value
            XCTAssertEqual(observerEmissionCount, 2)
            try await target.attachment.insert(.init(
                id: Self.attachmentId, personId: Self.personTwoId, filename: "proof.bin"
            ))
            let attachments = try await target.attachment.selectAll().list()
            XCTAssertEqual(attachments.count, 1)
            try await targetOpening?.close()
            targetOpening = nil
            try? FileManager.default.removeItem(at: path)
        } catch {
            try? await targetOpening?.close()
            try? await invalidOpening?.close()
            try? await localOpening?.close()
            try? await oneOpening?.close()
            try? FileManager.default.removeItem(at: path)
            throw error
        }
    }

    func testGeneratedSwiftCollisionRejectsBeforeTargetMigration() async throws {
        let (baseURL, token) = try await authority()
        let path = temporaryPath("collision")
        let user = "schemaevolution-swift-collision-\(UUID().uuidString.lowercased())"
        let authOne = SchemaEvolutionGenerationOneDatabaseSQLiteNow.SQLiteNowSyncAuth.bearer(token: token)
        let authLocal = SchemaEvolutionLocalTwoDatabaseSQLiteNow.SQLiteNowSyncAuth.bearer(token: token)
        let authTarget = SchemaEvolutionGenerationTargetDatabaseSQLiteNow.SQLiteNowSyncAuth.bearer(token: token)
        let oneConfig = SchemaEvolutionGenerationOneDatabaseSQLiteNow.SQLiteNowSyncConfig(
            generationId: 1, databaseId: Self.databaseId, serviceId: Self.serviceId
        )
        let one = SchemaEvolutionGenerationOne(path: path)
        let oneOpening = try await one.openForSync(baseURL: baseURL, auth: authOne, config: oneConfig)
        _ = try await oneOpening.advance()
        _ = try await one.makeSyncClient(baseURL: baseURL, auth: authOne, config: oneConfig).attach(userId: user)
        try await oneOpening.close()

        let local = SchemaEvolutionLocalTwo(path: path, onMigrationStep: { step in
            if step.toVersion == 2 {
                try await step.connection.execute(
                    "CREATE TRIGGER drafts_touch AFTER UPDATE OF label ON drafts " +
                        "BEGIN UPDATE drafts SET normalized=upper(new.label) WHERE id=new.id; END"
                )
                try await step.connection.execute(
                    "CREATE TABLE projects(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL)"
                )
            }
        })
        let localOpening = try await local.openForSync(
            baseURL: baseURL,
            auth: authLocal,
            config: SchemaEvolutionLocalTwoDatabaseSQLiteNow.SQLiteNowSyncConfig(
                generationId: 1, databaseId: Self.databaseId, serviceId: Self.serviceId
            ),
            upgradePlan: SchemaEvolutionLocalTwoDatabaseSQLiteNow.SQLiteNowSyncUpgradePlan(
                migrationId: "schemaevolution-swift-collision-local",
                fromGenerationId: 1,
                toGenerationId: 1,
                fromLocalVersion: 1,
                toLocalVersion: 2,
                preservedLocalTables: ["drafts"]
            )
        )
        _ = try await localOpening.advance()
        try await localOpening.close()

        let migrationCalls = SchemaEvolutionCounter()
        let target = SchemaEvolutionTarget(path: path, onMigrationStep: { _ in migrationCalls.increment() })
        let targetOpening = try await target.openForSync(
            baseURL: baseURL,
            auth: authTarget,
            config: SchemaEvolutionGenerationTargetDatabaseSQLiteNow.SQLiteNowSyncConfig(
                generationId: 2, databaseId: Self.databaseId, serviceId: Self.serviceId
            ),
            upgradePlan: SchemaEvolutionGenerationTargetDatabaseSQLiteNow.SQLiteNowSyncUpgradePlan(
                migrationId: "schemaevolution-swift-collision-target",
                fromGenerationId: 1,
                toGenerationId: 2,
                fromLocalVersion: 2,
                toLocalVersion: 3,
                preservedLocalTables: ["drafts", "projects"]
            )
        )
        do {
            _ = try await targetOpening.advance()
            XCTFail("Expected the target namespace collision to reject before U0")
        } catch {
            let status = try await targetOpening.status()
            XCTAssertEqual(status.outcome, .upgradePlanMismatch)
            XCTAssertEqual(status.nextAction, .repairConfiguration)
            XCTAssertEqual(status.diagnosticCode, "upgrade_plan_mismatch")
            XCTAssertEqual(status.diagnosticMessage, "schema_evolution_compatibility_new_table_namespace")
            XCTAssertEqual(migrationCalls.value, 0)
            XCTAssertNil(targetOpening.readyOrNil())
        }
        try await targetOpening.close()
        try? FileManager.default.removeItem(at: path)
    }

    private func authority() async throws -> (URL, String) {
        guard let raw = ProcessInfo.processInfo.environment["SCHEMA_EVOLUTION_ADDITIVE_BASE_URL"],
              let baseURL = URL(string: raw) else {
            throw XCTSkip("Run through tools/qualification/schema_evolution_authority.py --selector swift")
        }
        let user = "schemaevolution-swift-token-\(UUID().uuidString.lowercased())"
        var request = URLRequest(url: baseURL.appendingPathComponent("dummy-signin"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONEncoder().encode(SchemaEvolutionSigninRequest(user: user, password: "anything", device: "swift"))
        let (data, response) = try await URLSession.shared.data(for: request)
        XCTAssertEqual((response as? HTTPURLResponse)?.statusCode, 200)
        return (baseURL, try JSONDecoder().decode(SchemaEvolutionSigninResponse.self, from: data).token)
    }

    private func temporaryPath(_ suffix: String) -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlitenow-schemaevolution-swift-\(suffix)-\(UUID().uuidString).sqlite3")
    }

    private static let databaseId = "schemaevolution-swift-generated"
    private static let serviceId = "schemaevolution-authority"
    private static let personId = "11111111-1111-4111-8111-111111111111"
    private static let noteId = "22222222-2222-4222-8222-222222222222"
    private static let draftId = "draft-1"
    private static let projectId = "33333333-3333-4333-8333-333333333333"
    private static let personTwoId = "44444444-4444-4444-8444-444444444444"
    private static let attachmentId = "55555555-5555-4555-8555-555555555555"
}

private extension Array {
    var single: Element? { count == 1 ? self[0] : nil }
}

private final class SchemaEvolutionCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var storage = 0
    func increment() { lock.withLock { storage += 1 } }
    var value: Int { lock.withLock { storage } }
}

private struct SchemaEvolutionSigninRequest: Encodable {
    let user: String
    let password: String
    let device: String
}

private struct SchemaEvolutionSigninResponse: Decodable {
    let token: String
}
