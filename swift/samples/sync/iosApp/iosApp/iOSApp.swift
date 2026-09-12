import Foundation
import NowSampleSyncDatabaseSQLiteNow
import SwiftUI

@main
struct iOSApp: App {
    private let lifecycle = ProcessInfo.processInfo.environment["SQLITENOW_LIFECYCLE"] == "1"
    private let lifecycleRole = ProcessInfo.processInfo.environment["SQLITENOW_LIFECYCLE_ROLE"] ?? "unknown"
    private let lifecycleToken = ProcessInfo.processInfo.environment["SQLITENOW_LIFECYCLE_TOKEN"] ?? "missing"
    private let lifecycleBaseURL = ProcessInfo.processInfo.environment["SQLITENOW_LIFECYCLE_BASE_URL"] ?? ""

    var body: some Scene {
        WindowGroup {
            if lifecycle {
                LifecycleProbeView(
                    role: lifecycleRole,
                    token: lifecycleToken,
                    baseURL: lifecycleBaseURL
                )
            } else {
                ContentView()
            }
        }
    }
}

private struct LifecycleProbeView: View {
    @StateObject private var model: LifecycleProbeModel

    init(role: String, token: String, baseURL: String) {
        _model = StateObject(
            wrappedValue: LifecycleProbeModel(role: role, token: token, baseURL: baseURL)
        )
    }

    var body: some View {
        Text("lifecycle probe")
            .task { model.start() }
    }
}

@MainActor
private final class LifecycleProbeModel: ObservableObject {
    private let role: String
    private let token: String
    private let baseURL: String
    private var database: NowSampleSyncDatabase?
    private var opening: SQLiteNowSyncOpening<NowSampleSyncDatabase>?
    private var started = false

    init(role: String, token: String, baseURL: String) {
        self.role = role
        self.token = token
        self.baseURL = baseURL
    }

    func start() {
        guard !started else { return }
        started = true
        Task {
            do {
                let fileManager = FileManager.default
                let support = fileManager.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
                let directory = support.appendingPathComponent("SQLiteNowLifecycle", isDirectory: true)
                if role == "owner" {
                    try? fileManager.removeItem(at: directory)
                }
                try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
                let databaseURL = directory.appendingPathComponent("lifecycle-probe.sqlite")
                guard let authorityURL = URL(string: baseURL) else {
                    throw NSError(domain: "SQLiteNowLifecycle", code: 1, userInfo: [
                        NSLocalizedDescriptionKey: "invalid lifecycle authority URL"
                    ])
                }
                let database = NowSampleSyncDatabase(path: databaseURL)
                let opening = try await database.openForSync(
                    baseURL: authorityURL,
                    auth: .bearer(token: ""),
                    config: .init(
                        generationId: 1,
                        databaseId: "lifecycle-probe",
                        serviceId: "lifecycle-probe",
                        verboseLogs: true
                    )
                )
                let status = try await opening.advance()
                guard status.outcome == .ready, opening.readyOrNil() != nil else {
                    throw NSError(domain: "SQLiteNowLifecycle", code: 2, userInfo: [
                        NSLocalizedDescriptionKey: "opening did not publish ready"
                    ])
                }
                if role == "owner" {
                    try await database.person.add(
                        PersonAddParams(
                            email: "lifecycle-probe@example.test",
                            firstName: "SQLITENOW",
                            lastName: "Owner",
                            phone: nil,
                            birthDate: nil,
                            ssn: nil,
                            score: nil,
                            isActive: true,
                            notes: "device-death witness"
                        )
                    )
                }
                let rows = try await database.person.selectAll(
                    PersonSelectAllParams(limit: 100, offset: 0)
                ).list()
                guard let witness = rows.first(where: { $0.email == "lifecycle-probe@example.test" }) else {
                    throw NSError(domain: "SQLiteNowLifecycle", code: 3, userInfo: [
                        NSLocalizedDescriptionKey: "business witness is missing"
                    ])
                }
                let witnessID = witness.id.map { String(format: "%02X", $0) }.joined()
                let record: [String: Any] = [
                    "result": "SQLITENOW_IOS_LIFECYCLE_PASS",
                    "token": token,
                    "role": role,
                    "pid": Int(getpid()),
                    "phase": status.phase ?? "",
                    "revision": status.revision,
                    "outcome": status.outcome?.rawValue ?? "",
                    "writes_available": status.writesAvailable ?? false,
                    "witness_id": witnessID,
                    "witness_first_name": witness.myFirstName,
                    "witness_last_name": witness.myLastName,
                    "witness_email": witness.email
                ]
                let marker = directory.appendingPathComponent("marker.json")
                try JSONSerialization.data(withJSONObject: record, options: [.sortedKeys])
                    .write(to: marker, options: [.atomic])
                self.database = database
                self.opening = opening
                print("SQLITENOW_IOS_DURABLE_CUT \(String(data: try Data(contentsOf: marker), encoding: .utf8) ?? "")")
            } catch {
                print("SQLITENOW_IOS_DURABLE_CUT_FAILURE token=\(token) role=\(role) error=\(error)")
            }
        }
    }
}
