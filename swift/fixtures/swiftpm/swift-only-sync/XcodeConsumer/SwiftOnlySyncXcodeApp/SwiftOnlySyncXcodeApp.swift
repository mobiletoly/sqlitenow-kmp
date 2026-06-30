import Foundation
import SwiftOnlySyncDatabaseSQLiteNow
import SwiftUI

@main
struct SwiftOnlySyncXcodeApp: App {
    var body: some Scene {
        WindowGroup {
            ContentView()
        }
    }
}

private struct ContentView: View {
    @State private var status = "Opening SQLiteNow Sync"

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
            let db = SwiftOnlySyncDatabase(path: temporaryDatabaseURL())
            try await db.open()
            defer {
                Task {
                    try? await db.close()
                }
            }

            try await db.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "doc-1", title: "First"))
            let rows = try await db.docs.selectAll().list()

            let sync = try db.makeSyncClient(
                baseURL: URL(string: "http://127.0.0.1:1")!,
                auth: .bearer(token: "fixture-token")
            )
            try await sync.open()
            let sourceInfo = try await sync.sourceInfo()
            sync.close()

            status = "\(rows.map(\.title).joined(separator: ", ")) / \(sourceInfo.currentSourceId)"
        } catch {
            status = String(describing: error)
        }
    }

    private func temporaryDatabaseURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlitenow-swift-only-sync-xcode-\(UUID().uuidString).db")
    }
}
