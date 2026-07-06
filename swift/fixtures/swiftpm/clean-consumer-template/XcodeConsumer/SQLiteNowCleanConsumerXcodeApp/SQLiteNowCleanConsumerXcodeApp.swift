import Foundation
import CleanConsumerCoreDatabaseSQLiteNow
import CleanConsumerSyncDatabaseSQLiteNow
import SwiftUI

@main
struct SQLiteNowCleanConsumerXcodeApp: App {
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
            let coreDb = CleanConsumerCoreDatabase(path: temporaryDatabaseURL(prefix: "core"))
            try await coreDb.open()
            let people = try await withAsyncCleanup({
                try await coreDb.close()
            }) {
                try await coreDb.person.insert(PersonInsertParams(id: 1, name: "Ada"))
                return try await coreDb.person.selectAll().list()
            }

            let syncDb = SwiftOnlySyncDatabase(path: temporaryDatabaseURL(prefix: "sync"))
            try await syncDb.open()
            let (docs, sourceInfo) = try await withAsyncCleanup({
                try await syncDb.close()
            }) {
                try await syncDb.docs.insertOrReplace(DocsInsertOrReplaceParams(docId: "doc-1", title: "First"))
                let docs = try await syncDb.docs.selectAll().list()

                let sync = try syncDb.makeSyncClient(
                    baseURL: URL(string: "http://127.0.0.1:1")!,
                    auth: .bearer(token: "fixture-token")
                )
                try await sync.open()
                defer {
                    sync.close()
                }

                let sourceInfo = try await sync.sourceInfo()
                return (docs, sourceInfo)
            }

            status = [
                people.map(\.name).joined(separator: ", "),
                docs.map(\.title).joined(separator: ", "),
                sourceInfo.currentSourceId,
            ].joined(separator: " / ")
        } catch {
            status = String(describing: error)
        }
    }

    private func temporaryDatabaseURL(prefix: String) -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlitenow-clean-consumer-xcode-\(prefix)-\(UUID().uuidString).db")
    }

    private func withAsyncCleanup<T>(
        _ cleanup: () async throws -> Void,
        operation: () async throws -> T
    ) async throws -> T {
        do {
            let result = try await operation()
            try await cleanup()
            return result
        } catch {
            try? await cleanup()
            throw error
        }
    }
}
