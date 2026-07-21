import Foundation
import NowSampleSyncDatabaseSQLiteNow
import SwiftUI

private let expectedSampleSyncServerAppName = "nethttp-server-example"

enum SyncMode: String, CaseIterable, Identifiable {
    case watch = "Watch"
    case polling = "Polling"

    var id: String { rawValue }

    var automaticDownloadConfig: SQLiteNowAutomaticDownloadConfig {
        switch self {
        case .watch:
            return SQLiteNowAutomaticDownloadConfig(
                automaticDownloadIntervalMillis: 60_000,
                bundleChangeWatchMode: .auto,
                bundleChangeWatchReconnectMinMillis: 1_000,
                bundleChangeWatchReconnectMaxMillis: 60_000
            )
        case .polling:
            return SQLiteNowAutomaticDownloadConfig(
                automaticDownloadIntervalMillis: 10_000,
                bundleChangeWatchMode: .off,
                bundleChangeWatchReconnectMinMillis: 1_000,
                bundleChangeWatchReconnectMaxMillis: 60_000
            )
        }
    }
}

private final class SyncTokenStore: @unchecked Sendable {
    private let lock = NSLock()
    private var accessToken = ""

    var auth: SQLiteNowSyncAuth {
        SQLiteNowSyncAuth.bearer(accessToken: { [weak self] in
            self?.currentAccessToken() ?? ""
        })
    }

    func setAccessToken(_ token: String) {
        lock.lock()
        accessToken = token
        lock.unlock()
    }

    func clear() {
        setAccessToken("")
    }

    private func currentAccessToken() -> String {
        lock.lock()
        let token = accessToken
        lock.unlock()
        return token
    }
}

private struct ServerStatusResponse: Decodable {
    let appName: String

    enum CodingKeys: String, CodingKey {
        case appName = "app_name"
    }
}

private struct SignInRequest: Encodable {
    let user: String
    let password: String
    let device: String
}

private struct SignInResponse: Decodable {
    let token: String
    let expiresIn: Int64
    let user: String

    enum CodingKeys: String, CodingKey {
        case token
        case expiresIn = "expires_in"
        case user
    }
}

@MainActor
final class SyncSampleViewModel: ObservableObject {
    @Published var people: [PersonWithAddressRow] = []
    @Published var isOpeningDatabase = true
    @Published var isSyncing = false
    @Published var isSignedIn = false
    @Published var showsSignIn = false
    @Published var baseURLText = "http://127.0.0.1:8080"
    @Published var username = "user-sample"
    @Published var password = "demo"
    @Published var syncMode: SyncMode = .watch
    @Published var signedInUser: String?
    @Published var sourceId: String?
    @Published var statusText = "Opening local database"
    @Published var progressText = "Idle"
    @Published var errorMessage: String?

    private var database: NowSampleSyncDatabase?
    private var syncClient: SQLiteNowSyncClient?
    private var tokenStore: SyncTokenStore?
    private var automaticDownloads: SQLiteNowAutomaticDownloads?
    private var peopleTask: Task<Void, Never>?
    private var progressTask: Task<Void, Never>?
    private let jsonDecoder = JSONDecoder()
    private let jsonEncoder = JSONEncoder()
    private let isoFormatter = ISO8601DateFormatter()

    deinit {
        peopleTask?.cancel()
        progressTask?.cancel()
        automaticDownloads?.cancel()
        syncClient?.close()
    }

    func start() {
        guard database == nil else {
            return
        }

        Task {
            do {
                _ = try await openDatabase()
            } catch {
                isOpeningDatabase = false
                errorMessage = displayMessage(for: error)
                statusText = "Local database failed"
            }
        }
    }

    func signIn() {
        Task {
            await runSyncAction {
                let db = try await openDatabase()
                let baseURL = try normalizedBaseURL()
                let store = SyncTokenStore()
                tokenStore = store

                closeSyncClient()

                let sync = try db.makeSyncClient(
                    baseURL: baseURL,
                    auth: store.auth,
                    config: .init(schema: "business", verboseLogs: false)
                )
                syncClient = sync
                statusText = "Opening sync"
                observeProgress(sync)
                try await sync.open()

                let source = try await sync.sourceInfo()
                sourceId = source.currentSourceId
                try await ensureSampleSyncServer(baseURL: baseURL)

                statusText = "Signing in"
                let signIn = try await requestSignInToken(
                    baseURL: baseURL,
                    user: username.trimmingCharacters(in: .whitespacesAndNewlines),
                    password: password,
                    sourceId: source.currentSourceId
                )
                store.setAccessToken(signIn.token)
                signedInUser = signIn.user

                statusText = "Attaching"
                let attachResult = try await sync.attach(userId: signIn.user)
                switch attachResult {
                case let .connected(_, status, _):
                    isSignedIn = true
                    showsSignIn = false
                    statusText = statusSummary(status)
                case let .retryLater(retryAfterSeconds):
                    statusText = "Attach retry in \(retryAfterSeconds)s"
                    return
                case let .unknown(raw):
                    throw SQLiteNowError.unknown(message: "Unknown attach result: \(raw)")
                }

                let recoveryInfo = try await sync.sourceInfo()
                sourceId = recoveryInfo.currentSourceId
                if recoveryInfo.rebuildRequired || recoveryInfo.sourceRecoveryRequired {
                    statusText = "Rebuilding local data"
                    let rebuild = try await sync.rebuild()
                    statusText = statusSummary(rebuild.status)
                }

                statusText = "Syncing"
                let report = try await sync.sync()
                statusText = statusSummary(report.status)
                startAutomaticDownloads(sync)
            }
        }
    }

    func signOut() {
        Task {
            await runSyncAction {
                guard let sync = syncClient else {
                    clearSession()
                    return
                }

                statusText = "Syncing before sign out"
                let result = try await sync.syncThenDetach()
                if result.success {
                    clearSession()
                    statusText = "Signed out"
                    progressText = "Idle"
                } else {
                    statusText = "Detach blocked"
                    errorMessage = "Detach blocked by \(result.remainingPendingRowCount) pending rows."
                }
            }
        }
    }

    func syncNow() {
        Task {
            await runSyncAction {
                guard let sync = syncClient, isSignedIn else {
                    showsSignIn = true
                    return
                }

                statusText = "Syncing"
                let report = try await sync.sync()
                statusText = statusSummary(report.status)
            }
        }
    }

    func addRandomPerson() {
        Task {
            await runDatabaseAction {
                let db = try await openDatabase()
                let firstName = SampleData.firstNames.randomElement() ?? "Ada"
                let lastName = SampleData.lastNames.randomElement() ?? "Lovelace"
                let email = "\(firstName.lowercased()).\(lastName.lowercased()).\(Int.random(in: 1000...9999))@sample.local"

                try await db.person.add(
                    PersonAddParams(
                        email: email,
                        firstName: firstName,
                        lastName: lastName,
                        phone: Bool.random() ? randomPhone() : nil,
                        birthDate: randomBirthDate(),
                        ssn: nil,
                        score: Double.random(in: 10...99),
                        isActive: true,
                        notes: "Created on iOS"
                    )
                )

                if let person = try await personByEmail(email, db: db) {
                    try await addAddress(for: person.id, db: db)
                }
            }
        }
    }

    func randomize(_ person: PersonWithAddressRow) {
        Task {
            await runDatabaseAction {
                let db = try await openDatabase()
                guard let personId = person.personId,
                      let current = try await personById(personId, db: db) else {
                    return
                }

                try await db.person.updateById(
                    PersonUpdateByIdParams(
                        firstName: SampleData.firstNames.randomElement() ?? current.myFirstName,
                        lastName: SampleData.lastNames.randomElement() ?? current.myLastName,
                        email: current.email,
                        phone: Bool.random() ? randomPhone() : current.phone,
                        birthDate: current.birthDate,
                        ssn: current.ssn,
                        score: Double.random(in: 10...99),
                        isActive: current.isActive,
                        notes: "Updated on iOS",
                        id: current.id
                    )
                )
            }
        }
    }

    func addAddress(_ person: PersonWithAddressRow) {
        Task {
            await runDatabaseAction {
                guard let personId = person.personId else {
                    return
                }
                try await addAddress(for: personId, db: try await openDatabase())
            }
        }
    }

    func addComment(_ person: PersonWithAddressRow) {
        Task {
            await runDatabaseAction {
                let db = try await openDatabase()
                guard let personId = person.personId else {
                    return
                }
                let comment = SampleData.comments.randomElement() ?? "Checked from iOS"
                try await db.comment.add(
                    CommentAddParams(
                        id: UUID().uuidString,
                        personId: personId,
                        comment: comment,
                        createdAt: isoFormatter.string(from: Date()),
                        tags: "ios,sample"
                    )
                )
            }
        }
    }

    func delete(_ person: PersonWithAddressRow) {
        Task {
            await runDatabaseAction {
                guard let personId = person.personId else {
                    return
                }
                try await openDatabase().person.deleteById(PersonDeleteByIdParams(id: personId))
            }
        }
    }

    private func openDatabase() async throws -> NowSampleSyncDatabase {
        if let database {
            return database
        }

        isOpeningDatabase = true
        let db = NowSampleSyncDatabase(path: try databaseURL())
        database = db
        try await db.open()
        observePeople(db)
        isOpeningDatabase = false
        statusText = "Local database ready"
        return db
    }

    private func observePeople(_ db: NowSampleSyncDatabase) {
        peopleTask?.cancel()
        peopleTask = Task {
            do {
                let params = PersonSelectAllWithAddressesParams(limit: 250, offset: 0)
                for try await rows in db.person.selectAllWithAddresses(params).stream() {
                    people = rows
                    isOpeningDatabase = false
                }
            } catch {
                if !Task.isCancelled {
                    errorMessage = displayMessage(for: error)
                }
            }
        }
    }

    private func observeProgress(_ sync: SQLiteNowSyncClient) {
        progressTask?.cancel()
        progressTask = Task {
            do {
                for try await progress in sync.progress() {
                    progressText = progressSummary(progress)
                }
            } catch {
                if !Task.isCancelled {
                    errorMessage = displayMessage(for: error)
                }
            }
        }
    }

    private func startAutomaticDownloads(_ sync: SQLiteNowSyncClient) {
        automaticDownloads?.cancel()
        automaticDownloads = sync.startAutomaticDownloads(syncMode.automaticDownloadConfig) { [weak self] error in
            Task { @MainActor in
                self?.errorMessage = error.description
            }
        }
    }

    private func closeSyncClient() {
        progressTask?.cancel()
        progressTask = nil
        automaticDownloads?.cancel()
        automaticDownloads = nil
        syncClient?.close()
        syncClient = nil
    }

    private func clearSession() {
        closeSyncClient()
        tokenStore?.clear()
        tokenStore = nil
        isSignedIn = false
        signedInUser = nil
    }

    private func runDatabaseAction(_ action: () async throws -> Void) async {
        do {
            try await action()
        } catch {
            errorMessage = displayMessage(for: error)
        }
    }

    private func runSyncAction(_ action: () async throws -> Void) async {
        guard !isSyncing else {
            return
        }

        isSyncing = true
        defer { isSyncing = false }

        do {
            try await action()
        } catch {
            errorMessage = displayMessage(for: error)
            statusText = "Sync failed"
        }
    }

    private func ensureSampleSyncServer(baseURL: URL) async throws {
        let url = endpoint("syncx/status", baseURL: baseURL)
        let (data, response) = try await URLSession.shared.data(from: url)
        try validateHTTP(response, endpoint: url)
        let status = try jsonDecoder.decode(ServerStatusResponse.self, from: data)
        guard status.appName == expectedSampleSyncServerAppName else {
            throw SQLiteNowError.misuse(
                message: "Expected \(expectedSampleSyncServerAppName), got \(status.appName)."
            )
        }
    }

    private func requestSignInToken(
        baseURL: URL,
        user: String,
        password: String,
        sourceId: String
    ) async throws -> SignInResponse {
        let url = endpoint("dummy-signin", baseURL: baseURL)
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try jsonEncoder.encode(SignInRequest(user: user, password: password, device: sourceId))

        let (data, response) = try await URLSession.shared.data(for: request)
        try validateHTTP(response, endpoint: url)
        return try jsonDecoder.decode(SignInResponse.self, from: data)
    }

    private func validateHTTP(_ response: URLResponse, endpoint: URL) throws {
        guard let http = response as? HTTPURLResponse else {
            throw SQLiteNowError.unknown(message: "No HTTP response from \(endpoint.absoluteString).")
        }
        guard (200..<300).contains(http.statusCode) else {
            throw SQLiteNowError.unknown(
                message: "HTTP \(http.statusCode) from \(endpoint.absoluteString)."
            )
        }
    }

    private func endpoint(_ path: String, baseURL: URL) -> URL {
        let baseString = baseURL.absoluteString.hasSuffix("/") ? baseURL.absoluteString : baseURL.absoluteString + "/"
        return URL(string: path, relativeTo: URL(string: baseString))!.absoluteURL
    }

    private func normalizedBaseURL() throws -> URL {
        let text = baseURLText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let url = URL(string: text), url.scheme != nil, url.host != nil else {
            throw SQLiteNowError.misuse(message: "Invalid server URL.")
        }
        return url
    }

    private func databaseURL() throws -> URL {
        let root = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        let directory = root.appendingPathComponent("SQLiteNowSwiftSyncSample", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory.appendingPathComponent("now-sample-sync.sqlite")
    }

    private func personByEmail(_ email: String, db: NowSampleSyncDatabase) async throws -> PersonRow? {
        let rows = try await db.person.selectAll(PersonSelectAllParams(limit: 500, offset: 0)).list()
        return rows.first { $0.email == email }
    }

    private func personById(_ id: Data, db: NowSampleSyncDatabase) async throws -> PersonRow? {
        let rows = try await db.person.selectAll(PersonSelectAllParams(limit: 500, offset: 0)).list()
        return rows.first { $0.id == id }
    }

    private func addAddress(for personId: Data, db: NowSampleSyncDatabase) async throws {
        try await db.personAddress.add(
            PersonAddressAddParams(
                personId: personId,
                addressType: Bool.random() ? "home" : "work",
                street: "\(Int.random(in: 10...999)) \(SampleData.streets.randomElement() ?? "Main St")",
                city: SampleData.cities.randomElement() ?? "Seattle",
                state: SampleData.states.randomElement(),
                postalCode: String(Int.random(in: 10000...99999)),
                country: "US",
                isPrimary: true
            )
        )
    }

    private func statusSummary(_ status: SQLiteNowSyncStatus) -> String {
        "Pending \(status.pending.pendingRowCount), bundle \(status.lastBundleSeqSeen)"
    }

    private func progressSummary(_ progress: SQLiteNowSyncProgress) -> String {
        switch progress {
        case .idle:
            return "Idle"
        case let .active(operation, phase):
            return "\(operation.label) / \(phase.label)"
        }
    }

    private func randomBirthDate() -> String {
        String(format: "%04d-%02d-%02d", Int.random(in: 1950...2005), Int.random(in: 1...12), Int.random(in: 1...28))
    }

    private func randomPhone() -> String {
        "555-\(Int.random(in: 100...999))-\(Int.random(in: 1000...9999))"
    }

    private func displayMessage(for error: Error) -> String {
        if let sqliteNowError = error as? SQLiteNowError {
            return sqliteNowError.description
        }
        return String(describing: error)
    }
}

struct ContentView: View {
    @StateObject private var viewModel = SyncSampleViewModel()

    var body: some View {
        NavigationView {
            List {
                statusSection
                peopleSection
            }
            .listStyle(.insetGrouped)
            .navigationTitle("SQLiteNow Sync")
            .toolbar {
                ToolbarItemGroup(placement: .navigationBarLeading) {
                    if viewModel.isSignedIn {
                        Button(action: viewModel.signOut) {
                            Image(systemName: "rectangle.portrait.and.arrow.right")
                        }
                        .accessibilityLabel("Sign out")
                    } else {
                        Button(action: { viewModel.showsSignIn = true }) {
                            Image(systemName: "person.crop.circle.badge.checkmark")
                        }
                        .accessibilityLabel("Sign in")
                    }
                }
                ToolbarItemGroup(placement: .navigationBarTrailing) {
                    Button(action: viewModel.syncNow) {
                        Image(systemName: "arrow.triangle.2.circlepath")
                    }
                    .disabled(viewModel.isSyncing)
                    .accessibilityLabel("Sync")

                    Button(action: viewModel.addRandomPerson) {
                        Image(systemName: "plus")
                    }
                    .accessibilityLabel("Add person")
                }
            }
        }
        .onAppear {
            viewModel.start()
        }
        .sheet(isPresented: $viewModel.showsSignIn) {
            SignInView(viewModel: viewModel)
        }
        .alert(
            "Sync Error",
            isPresented: Binding(
                get: { viewModel.errorMessage != nil },
                set: { isPresented in
                    if !isPresented {
                        viewModel.errorMessage = nil
                    }
                }
            )
        ) {
            Button("OK", role: .cancel) {
                viewModel.errorMessage = nil
            }
        } message: {
            Text(viewModel.errorMessage ?? "")
        }
    }

    private var statusSection: some View {
        Section {
            HStack {
                Label(viewModel.isSignedIn ? "Signed in" : "Signed out", systemImage: viewModel.isSignedIn ? "checkmark.circle" : "person.crop.circle")
                Spacer()
                if viewModel.isSyncing {
                    ProgressView()
                }
            }

            StatusRow(label: "User", value: viewModel.signedInUser ?? "Local")
            StatusRow(label: "Server", value: viewModel.baseURLText)
            StatusRow(label: "Status", value: viewModel.statusText)
            StatusRow(label: "Progress", value: viewModel.progressText)
            if let sourceId = viewModel.sourceId {
                StatusRow(label: "Source", value: sourceId)
            }
        }
    }

    private var peopleSection: some View {
        Section {
            if viewModel.isOpeningDatabase {
                HStack {
                    Spacer()
                    ProgressView()
                    Spacer()
                }
            } else if viewModel.people.isEmpty {
                EmptyPeopleView(addAction: viewModel.addRandomPerson)
            } else {
                ForEach(viewModel.people, id: \.personId) { person in
                    PersonCard(
                        person: person,
                        randomizeAction: { viewModel.randomize(person) },
                        addAddressAction: { viewModel.addAddress(person) },
                        addCommentAction: { viewModel.addComment(person) },
                        deleteAction: { viewModel.delete(person) }
                    )
                    .listRowSeparator(.hidden)
                    .listRowInsets(EdgeInsets(top: 8, leading: 16, bottom: 8, trailing: 16))
                }
            }
        } header: {
            Text("\(viewModel.people.count) \(viewModel.people.count == 1 ? "person" : "persons")")
        }
    }
}

private struct StatusRow: View {
    let label: String
    let value: String

    var body: some View {
        HStack(alignment: .firstTextBaseline) {
            Text(label)
                .foregroundStyle(.secondary)
            Spacer(minLength: 16)
            Text(value)
                .multilineTextAlignment(.trailing)
                .lineLimit(2)
        }
    }
}

private struct SignInView: View {
    @ObservedObject var viewModel: SyncSampleViewModel
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationView {
            Form {
                Section {
                    TextField("Server", text: $viewModel.baseURLText)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                        .keyboardType(.URL)
                    TextField("User", text: $viewModel.username)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                    SecureField("Password", text: $viewModel.password)
                }

                Section {
                    Picker("Mode", selection: $viewModel.syncMode) {
                        ForEach(SyncMode.allCases) { mode in
                            Text(mode.rawValue).tag(mode)
                        }
                    }
                    .pickerStyle(.segmented)
                }
            }
            .navigationTitle("Sign In")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") {
                        dismiss()
                    }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Sign In") {
                        viewModel.signIn()
                    }
                    .disabled(viewModel.isSyncing)
                }
            }
        }
    }
}

private struct EmptyPeopleView: View {
    let addAction: () -> Void

    var body: some View {
        VStack(spacing: 16) {
            Image(systemName: "person.crop.circle.badge.plus")
                .font(.system(size: 44, weight: .regular))
                .foregroundStyle(.tint)

            VStack(spacing: 4) {
                Text("No persons")
                    .font(.headline)
                Text("Add a generated person.")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            }

            Button(action: addAction) {
                Label("Add Person", systemImage: "plus")
            }
            .buttonStyle(.borderedProminent)
        }
        .frame(maxWidth: .infinity)
        .padding(32)
    }
}

private struct PersonCard: View {
    let person: PersonWithAddressRow
    let randomizeAction: () -> Void
    let addAddressAction: () -> Void
    let addCommentAction: () -> Void
    let deleteAction: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(alignment: .top, spacing: 12) {
                ZStack {
                    Circle()
                        .fill(Color.accentColor.opacity(0.14))
                    Text(initials)
                        .font(.headline)
                        .foregroundStyle(.tint)
                }
                .frame(width: 48, height: 48)

                VStack(alignment: .leading, spacing: 4) {
                    Text("\(person.myFirstName) \(person.myLastName)")
                        .font(.headline)
                    Text(person.email)
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                    if let phone = person.phone, !phone.isEmpty {
                        Text(phone)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }

                Spacer()

                Menu {
                    Button(action: randomizeAction) {
                        Label("Randomize", systemImage: "shuffle")
                    }
                    Button(action: addAddressAction) {
                        Label("Add Address", systemImage: "house.badge.plus")
                    }
                    Button(action: addCommentAction) {
                        Label("Add Comment", systemImage: "text.badge.plus")
                    }
                    Button(role: .destructive, action: deleteAction) {
                        Label("Delete", systemImage: "trash")
                    }
                } label: {
                    Image(systemName: "ellipsis.circle")
                        .font(.title3)
                }
                .accessibilityLabel("Person actions")
            }

            HStack(spacing: 8) {
                Label("\(person.addresses.count)", systemImage: "house")
                Label("\(person.comments.count)", systemImage: "text.bubble")
                if let birthDate = person.birthDate, !birthDate.isEmpty {
                    Label(birthDate, systemImage: "calendar")
                }
            }
            .font(.caption)
            .foregroundStyle(.secondary)
        }
        .padding(16)
        .background(Color(.secondarySystemGroupedBackground))
        .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
        .swipeActions(edge: .trailing, allowsFullSwipe: false) {
            Button(role: .destructive, action: deleteAction) {
                Label("Delete", systemImage: "trash")
            }
            Button(action: randomizeAction) {
                Label("Randomize", systemImage: "shuffle")
            }
            .tint(.indigo)
        }
    }

    private var initials: String {
        let first = person.myFirstName.first.map(String.init) ?? ""
        let last = person.myLastName.first.map(String.init) ?? ""
        return (first + last).uppercased()
    }
}

private extension SQLiteNowOversqliteOperation {
    var label: String {
        switch self {
        case .attach:
            return "Attach"
        case .pushPending:
            return "Push"
        case .pullToStable:
            return "Pull"
        case .sync:
            return "Sync"
        case .rebuildKeepSource:
            return "Rebuild"
        case .rebuildRotateSource:
            return "Rotate"
        case let .unknown(raw):
            return raw
        }
    }
}

private extension SQLiteNowOversqlitePhase {
    var label: String {
        switch self {
        case .attaching:
            return "Attaching"
        case .seeding:
            return "Seeding"
        case .pushing:
            return "Pushing"
        case .pulling:
            return "Pulling"
        case .stagingRemoteState:
            return "Staging"
        case .applyingRemoteState:
            return "Applying"
        case let .unknown(raw):
            return raw
        }
    }
}

private enum SampleData {
    static let firstNames = [
        "Ada", "Grace", "Katherine", "Edsger", "Margaret", "Donald", "Barbara", "Alan",
        "Radia", "Ken", "Dennis", "Frances", "Jean", "Evelyn", "Linus", "Guido",
    ]

    static let lastNames = [
        "Lovelace", "Hopper", "Johnson", "Dijkstra", "Hamilton", "Knuth", "Liskov",
        "Turing", "Perlman", "Thompson", "Ritchie", "Allen", "Sammet", "Granville",
        "Torvalds", "Rossum",
    ]

    static let streets = ["Main St", "Market St", "Pine Ave", "Cedar Rd", "Lake Way"]
    static let cities = ["Seattle", "Portland", "Austin", "Boston", "Denver", "Chicago"]
    static let states = ["WA", "OR", "TX", "MA", "CO", "IL"]
    static let comments = ["Checked from iOS", "Ready to sync", "Updated offline", "Reviewed on simulator"]
}

#Preview {
    ContentView()
}
