import Foundation
import NowSampleDatabaseSQLiteNow
import SwiftUI

@MainActor
final class PeopleViewModel: ObservableObject {
    @Published var people: [PersonWithAddressRow] = []
    @Published var isLoading = true
    @Published var errorMessage: String?

    private var database: NowSampleDatabase?
    private var streamTask: Task<Void, Never>?

    func start() {
        guard streamTask == nil else {
            return
        }

        streamTask = Task {
            await openAndObserve()
        }
    }

    func addRandomPerson() {
        Task {
            await runDatabaseAction {
                let db = try requireDatabase()
                let firstName = SampleData.firstNames.randomElement() ?? "Ada"
                let lastName = SampleData.lastNames.randomElement() ?? "Lovelace"
                let domain = SampleData.domains.randomElement() ?? "example.com"
                let email = "\(firstName.lowercased()).\(lastName.lowercased()).\(Int.random(in: 1000...9999))@\(domain)"
                let created = try await db.person.addReturning(
                    PersonAddReturningParams(
                        email: email,
                        firstName: firstName,
                        lastName: lastName,
                        phone: Bool.random() ? "555-\(Int.random(in: 100...999))-\(Int.random(in: 1000...9999))" : nil,
                        birthDate: Bool.random() ? randomBirthDate() : nil,
                        notes: Bool.random() ? "Random note for \(firstName) \(lastName)" : nil
                    )
                ).one()

                try await db.personAddress.add(
                    PersonAddressAddParams(
                        personId: created.id,
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
        }
    }

    func randomize(_ person: PersonWithAddressRow) {
        Task {
            await runDatabaseAction {
                let db = try requireDatabase()
                guard let id = person.personId else {
                    return
                }
                let firstName = SampleData.firstNames.randomElement() ?? person.myFirstName ?? "Ada"
                let lastName = SampleData.lastNames.randomElement() ?? person.myLastName ?? "Lovelace"
                try await db.transaction { transaction in
                    try transaction.person.updateById(
                        PersonUpdateByIdParams(
                            firstName: firstName,
                            lastName: lastName,
                            email: person.personEmail ?? "\(firstName.lowercased()).\(lastName.lowercased())@example.com",
                            phone: person.personPhone,
                            birthDate: person.personBirthDate,
                            notes: person.personNotes,
                            id: id
                        )
                    )
                }
            }
        }
    }

    func delete(_ person: PersonWithAddressRow) {
        Task {
            await runDatabaseAction {
                let db = try requireDatabase()
                if let id = person.personId {
                    try await db.person.deleteByIds(PersonDeleteByIdsParams(ids: [id]))
                }
            }
        }
    }

    private func openAndObserve() async {
        do {
            let db = NowSampleDatabase(path: try databaseURL())
            database = db
            try await db.open()

            let params = PersonSelectAllWithAddressesParams(limit: 250, offset: 0)
            for try await rows in db.person.selectAllWithAddresses(params).stream() {
                people = rows
                isLoading = false
            }
        } catch {
            isLoading = false
            errorMessage = String(describing: error)
        }
    }

    private func runDatabaseAction(_ action: () async throws -> Void) async {
        do {
            try await action()
        } catch {
            errorMessage = String(describing: error)
        }
    }

    private func requireDatabase() throws -> NowSampleDatabase {
        if let database {
            return database
        }
        throw SQLiteNowError.misuse(message: "Database is not open yet.")
    }

    private func databaseURL() throws -> URL {
        let root = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        let directory = root.appendingPathComponent("SQLiteNowSwiftSample", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory.appendingPathComponent("now-sample.sqlite")
    }

    private func randomBirthDate() -> String {
        let year = Int.random(in: 1950...2005)
        let month = Int.random(in: 1...12)
        let day = Int.random(in: 1...28)
        return String(format: "%04d-%02d-%02d", year, month, day)
    }
}

struct ContentView: View {
    @StateObject private var viewModel = PeopleViewModel()

    var body: some View {
        NavigationView {
            Group {
                if viewModel.isLoading {
                    ProgressView()
                } else if viewModel.people.isEmpty {
                    EmptyPeopleView(addAction: viewModel.addRandomPerson)
                } else {
                    List {
                        Section {
                            ForEach(viewModel.people, id: \.personId) { person in
                                PersonCard(
                                    person: person,
                                    randomizeAction: { viewModel.randomize(person) },
                                    deleteAction: { viewModel.delete(person) }
                                )
                                .listRowSeparator(.hidden)
                                .listRowInsets(EdgeInsets(top: 8, leading: 16, bottom: 8, trailing: 16))
                            }
                        } header: {
                            Text("\(viewModel.people.count) \(viewModel.people.count == 1 ? "person" : "persons")")
                        }
                    }
                    .listStyle(.plain)
                    .refreshable {
                        viewModel.start()
                    }
                }
            }
            .navigationTitle("SQLiteNow Showcase")
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button(action: viewModel.addRandomPerson) {
                        Image(systemName: "plus")
                    }
                    .accessibilityLabel("Add random person")
                }
            }
        }
        .onAppear {
            viewModel.start()
        }
        .alert(
            "Database Error",
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
}

private struct EmptyPeopleView: View {
    let addAction: () -> Void

    var body: some View {
        VStack(spacing: 16) {
            Image(systemName: "person.crop.circle.badge.plus")
                .font(.system(size: 44, weight: .regular))
                .foregroundStyle(.tint)

            VStack(spacing: 4) {
                Text("No persons yet")
                    .font(.headline)
                Text("Add your first generated person.")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            }

            Button(action: addAction) {
                Label("Add Random Person", systemImage: "plus")
            }
            .buttonStyle(.borderedProminent)
        }
        .padding(32)
    }
}

private struct PersonCard: View {
    let person: PersonWithAddressRow
    let randomizeAction: () -> Void
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
                    Text("\(person.myFirstName ?? "Unknown") \((person.myLastName ?? "").uppercased())")
                        .font(.headline)
                    Text(person.personEmail ?? "No email")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                    if let phone = person.personPhone, !phone.isEmpty {
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
                if let birthDate = person.personBirthDate, !birthDate.isEmpty {
                    Label(birthDate, systemImage: "calendar")
                }
                if let notes = person.personNotes, !notes.isEmpty {
                    Label("Note", systemImage: "note.text")
                }
            }
            .font(.caption)
            .foregroundStyle(.secondary)
        }
        .padding(16)
        .background(Color(.secondarySystemBackground))
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
        let first = person.myFirstName?.first.map(String.init) ?? ""
        let last = person.myLastName?.first.map(String.init) ?? ""
        return (first + last).uppercased()
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

    static let domains = ["example.com", "sqlite.dev", "swift.local", "sample.app"]
    static let streets = ["Main St", "Market St", "Pine Ave", "Cedar Rd", "Lake Way"]
    static let cities = ["Seattle", "Portland", "Austin", "Boston", "Denver", "Chicago"]
    static let states = ["WA", "OR", "TX", "MA", "CO", "IL"]
}

#Preview {
    ContentView()
}
