import Foundation
import SwiftOnlyCoreDatabaseSQLiteNow
import SwiftUI

@main
struct SwiftOnlyCoreXcodeApp: App {
    var body: some Scene {
        WindowGroup {
            ContentView()
        }
    }
}

private struct ContentView: View {
    @State private var status = "Opening SQLiteNow"

    var body: some View {
        Text(status)
            .padding()
            .task {
                await runDatabaseSmoke()
            }
    }

    @MainActor
    private func runDatabaseSmoke() async {
        do {
            let db = SwiftOnlyCoreDatabase(path: temporaryDatabaseURL())
            try await db.open()
            defer {
                Task {
                    try? await db.close()
                }
            }

            try await db.person.insert(PersonInsertParams(id: 2, name: "Grace"))
            let rows = try await db.person.selectAll().list()
            status = rows.map(\.name).joined(separator: ", ")
        } catch {
            status = String(describing: error)
        }
    }

    private func temporaryDatabaseURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlitenow-swift-only-core-xcode-\(UUID().uuidString).db")
    }
}
