@_exported import SQLiteNowCoreRuntime
import Foundation

public enum SQLiteNowError: Error, CustomStringConvertible, Equatable {
    case sqlite(message: String)
    case migration(message: String)
    case misuse(message: String)
    case cancelled(message: String)
    case observation(message: String)
    case adapter(message: String)
    case sync(message: String)
    case unknown(message: String)

    public var description: String {
        switch self {
        case let .sqlite(message): return message
        case let .migration(message): return message
        case let .misuse(message): return message
        case let .cancelled(message): return message
        case let .observation(message): return message
        case let .adapter(message): return message
        case let .sync(message): return message
        case let .unknown(message): return message
        }
    }

    public static func from(_ error: Error) -> SQLiteNowError {
        if let sqliteNowError = error as? SQLiteNowError {
            return sqliteNowError
        }
        let message = String(describing: error)
        let lowercased = message.lowercased()
        if lowercased.contains("adapter") { return .adapter(message: message) }
        if lowercased.contains("cancel") { return .cancelled(message: message) }
        if lowercased.contains("migration") { return .migration(message: message) }
        if lowercased.contains("sync") || lowercased.contains("oversqlite") || lowercased.contains("network") || lowercased.contains("auth") {
            return .sync(message: message)
        }
        if lowercased.contains("sqlite") || lowercased.contains("constraint") { return .sqlite(message: message) }
        if lowercased.contains("illegalstate") || lowercased.contains("open") { return .misuse(message: message) }
        return .unknown(message: message)
    }

    public static func from(_ payload: SQLiteNowCoreRuntimeErrorPayload) -> SQLiteNowError {
        switch payload.category {
        case "sqlite": return .sqlite(message: payload.message)
        case "migration": return .migration(message: payload.message)
        case "misuse": return .misuse(message: payload.message)
        case "cancelled": return .cancelled(message: payload.message)
        default: return .unknown(message: payload.message)
        }
    }
}

public func mapRuntimeErrors<T>(_ operation: () async throws -> T) async throws -> T {
    do {
        return try await operation()
    } catch let error as CancellationError {
        throw error
    } catch {
        throw SQLiteNowError.from(error)
    }
}

public func sqliteNowRequireOne<Row>(_ rows: [Row], resultName: String) throws -> Row {
    guard rows.count == 1, let row = rows.first else {
        throw SQLiteNowError.misuse(message: "Expected exactly one \(resultName) row, got \(rows.count)")
    }
    return row
}

public func sqliteNowRequireOneOrNull<Row>(_ rows: [Row], resultName: String) throws -> Row? {
    guard rows.count <= 1 else {
        throw SQLiteNowError.misuse(message: "Expected at most one \(resultName) row, got \(rows.count)")
    }
    return rows.first
}

public final class SQLiteNowSelectQuery<Row: Sendable>: Sendable {
    private let listBlock: @Sendable () async throws -> [Row]
    private let oneBlock: @Sendable () async throws -> Row
    private let oneOrNullBlock: @Sendable () async throws -> Row?
    private let streamBlock: @Sendable () -> AsyncThrowingStream<[Row], Error>

    public init(
        list: @escaping @Sendable () async throws -> [Row],
        one: @escaping @Sendable () async throws -> Row,
        oneOrNull: @escaping @Sendable () async throws -> Row?,
        stream: @escaping @Sendable () -> AsyncThrowingStream<[Row], Error>
    ) {
        self.listBlock = list
        self.oneBlock = one
        self.oneOrNullBlock = oneOrNull
        self.streamBlock = stream
    }

    public func list() async throws -> [Row] { try await listBlock() }

    public func one() async throws -> Row { try await oneBlock() }

    public func oneOrNull() async throws -> Row? { try await oneOrNullBlock() }

    public func stream() -> AsyncThrowingStream<[Row], Error> { streamBlock() }
}

public final class SQLiteNowExecuteReturningQuery<Row: Sendable>: Sendable {
    private let listBlock: @Sendable () async throws -> [Row]
    private let oneBlock: @Sendable () async throws -> Row
    private let oneOrNullBlock: @Sendable () async throws -> Row?

    public init(
        list: @escaping @Sendable () async throws -> [Row],
        one: @escaping @Sendable () async throws -> Row,
        oneOrNull: @escaping @Sendable () async throws -> Row?
    ) {
        self.listBlock = list
        self.oneBlock = one
        self.oneOrNullBlock = oneOrNull
    }

    public func list() async throws -> [Row] { try await listBlock() }

    public func one() async throws -> Row { try await oneBlock() }

    public func oneOrNull() async throws -> Row? { try await oneOrNullBlock() }
}

public func sqliteNowBind(_ value: Int64) -> SQLiteNowCoreRuntimeBindValue { SQLiteNowCoreRuntimeBindValue(int64Value: value) }
public func sqliteNowBind(_ value: Int64?) -> SQLiteNowCoreRuntimeBindValue { value.map { SQLiteNowCoreRuntimeBindValue(int64Value: $0) } ?? SQLiteNowCoreRuntimeBindValue() }
public func sqliteNowBind(_ value: Double) -> SQLiteNowCoreRuntimeBindValue { SQLiteNowCoreRuntimeBindValue(doubleValue: value) }
public func sqliteNowBind(_ value: Double?) -> SQLiteNowCoreRuntimeBindValue { value.map { SQLiteNowCoreRuntimeBindValue(doubleValue: $0) } ?? SQLiteNowCoreRuntimeBindValue() }
public func sqliteNowBind(_ value: String) -> SQLiteNowCoreRuntimeBindValue { SQLiteNowCoreRuntimeBindValue(textValue: value) }
public func sqliteNowBind(_ value: String?) -> SQLiteNowCoreRuntimeBindValue { value.map { SQLiteNowCoreRuntimeBindValue(textValue: $0) } ?? SQLiteNowCoreRuntimeBindValue() }
public func sqliteNowBind(_ value: Bool) -> SQLiteNowCoreRuntimeBindValue { SQLiteNowCoreRuntimeBindValue(boolValue: value) }
public func sqliteNowBind(_ value: Bool?) -> SQLiteNowCoreRuntimeBindValue { value.map { SQLiteNowCoreRuntimeBindValue(boolValue: $0) } ?? SQLiteNowCoreRuntimeBindValue() }
public func sqliteNowBind(_ value: Data) -> SQLiteNowCoreRuntimeBindValue { SQLiteNowCoreRuntimeBindValue(dataValue: value) }
public func sqliteNowBind(_ value: Data?) -> SQLiteNowCoreRuntimeBindValue { value.map { SQLiteNowCoreRuntimeBindValue(dataValue: $0) } ?? SQLiteNowCoreRuntimeBindValue() }
public func sqliteNowBindJsonArray<T>(_ values: [T]) -> SQLiteNowCoreRuntimeBindValue { SQLiteNowCoreRuntimeBindValue(textValue: sqliteNowJsonArray(values)) }

public func sqliteNowJsonArray<T>(_ values: [T]) -> String {
    let encoded = values.map { value -> String in
        switch value {
        case let string as String: return sqliteNowJsonString(string)
        case let int64 as Int64: return String(int64)
        case let int as Int: return String(int)
        case let double as Double: return String(double)
        case let bool as Bool: return bool ? "true" : "false"
        default: return sqliteNowJsonString(String(describing: value))
        }
    }
    return "[" + encoded.joined(separator: ",") + "]"
}

public func sqliteNowJsonString(_ value: String) -> String {
    var result = "\""
    for scalar in value.unicodeScalars {
        switch scalar.value {
        case 0x22: result += "\\\""
        case 0x5C: result += "\\\\"
        case 0x08: result += "\\b"
        case 0x0C: result += "\\f"
        case 0x0A: result += "\\n"
        case 0x0D: result += "\\r"
        case 0x09: result += "\\t"
        case 0x00...0x1F:
            result += String(format: "\\u%04X", scalar.value)
        default:
            result.unicodeScalars.append(scalar)
        }
    }
    result += "\""
    return result
}

public func sqliteNowReadInt64(_ cell: SQLiteNowCoreRuntimeCell, column: String) throws -> Int64 {
    guard !cell.isNull else { throw SQLiteNowError.misuse(message: "Expected Int64 for \(column)") }
    return cell.int64Value
}

public func sqliteNowReadOptionalInt64(_ cell: SQLiteNowCoreRuntimeCell) -> Int64? { cell.isNull ? nil : cell.int64Value }

public func sqliteNowReadDouble(_ cell: SQLiteNowCoreRuntimeCell, column: String) throws -> Double {
    guard !cell.isNull else { throw SQLiteNowError.misuse(message: "Expected Double for \(column)") }
    return cell.doubleValue
}

public func sqliteNowReadOptionalDouble(_ cell: SQLiteNowCoreRuntimeCell) -> Double? { cell.isNull ? nil : cell.doubleValue }

public func sqliteNowReadBool(_ cell: SQLiteNowCoreRuntimeCell, column: String) throws -> Bool {
    guard !cell.isNull else { throw SQLiteNowError.misuse(message: "Expected Bool for \(column)") }
    return cell.boolValue
}

public func sqliteNowReadOptionalBool(_ cell: SQLiteNowCoreRuntimeCell) -> Bool? { cell.isNull ? nil : cell.boolValue }

public func sqliteNowReadString(_ cell: SQLiteNowCoreRuntimeCell, column: String) throws -> String {
    guard let value = cell.textValue else { throw SQLiteNowError.misuse(message: "Expected String for \(column)") }
    return value
}

public func sqliteNowReadOptionalString(_ cell: SQLiteNowCoreRuntimeCell) -> String? { cell.textValue }

public func sqliteNowReadData(_ cell: SQLiteNowCoreRuntimeCell, column: String) throws -> Data {
    guard let value = cell.dataValue else { throw SQLiteNowError.misuse(message: "Expected Data for \(column)") }
    return value as Data
}

public func sqliteNowReadOptionalData(_ cell: SQLiteNowCoreRuntimeCell) -> Data? { cell.dataValue.map { $0 as Data } }

public func sqliteNowCellKey(_ cell: SQLiteNowCoreRuntimeCell, columnType: String, column: String) throws -> String {
    if cell.isNull { return "null" }
    switch columnType {
    case "bool": return "bool:\(cell.boolValue)"
    case "blob": return "blob:\(try sqliteNowReadData(cell, column: column).base64EncodedString())"
    case "double": return "double:\(cell.doubleValue)"
    case "int64": return "int64:\(cell.int64Value)"
    default: return "text:\(try sqliteNowReadString(cell, column: column))"
    }
}

public final class SQLiteNowStreamRefreshCoordinator<Row>: @unchecked Sendable {
    private let lock = NSLock()
    private let loadRows: @Sendable () async throws -> [Row]
    private let continuation: AsyncThrowingStream<[Row], Error>.Continuation
    private var task: Task<Void, Never>?
    private var isRunning = false
    private var needsRefresh = false
    private var isCancelled = false

    public init(
        continuation: AsyncThrowingStream<[Row], Error>.Continuation,
        loadRows: @escaping @Sendable () async throws -> [Row]
    ) {
        self.continuation = continuation
        self.loadRows = loadRows
    }

    public func refresh() {
        lock.lock()
        if isCancelled {
            lock.unlock()
            return
        }
        if isRunning {
            needsRefresh = true
            lock.unlock()
            return
        }
        isRunning = true
        task = Task { [weak self] in
            guard let self else { return }
            await self.run()
        }
        lock.unlock()
    }

    public func cancel() {
        lock.lock()
        if isCancelled {
            lock.unlock()
            return
        }
        isCancelled = true
        let task = task
        self.task = nil
        lock.unlock()
        task?.cancel()
    }

    private func run() async {
        while true {
            do {
                let rows = try await loadRows()
                continuation.yield(rows)
            } catch {
                continuation.finish(throwing: error)
                cancel()
                return
            }

            if shouldContinueAfterLoad() { continue }
            return
        }
    }

    private func shouldContinueAfterLoad() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        if isCancelled {
            isRunning = false
            return false
        }
        if needsRefresh {
            needsRefresh = false
            return true
        }
        isRunning = false
        task = nil
        return false
    }
}

public final class RuntimeTableObserver: SQLiteNowCoreRuntimeTableObserver, @unchecked Sendable {
    private let onChangedBlock: @Sendable () -> Void
    private let onErrorBlock: @Sendable (SQLiteNowCoreRuntimeErrorPayload) -> Void
    private var retainedSelf: RuntimeTableObserver?

    public init(
        onChanged: @escaping @Sendable () -> Void,
        onError: @escaping @Sendable (SQLiteNowCoreRuntimeErrorPayload) -> Void
    ) {
        self.onChangedBlock = onChanged
        self.onErrorBlock = onError
        self.retainedSelf = self
    }

    public func onChanged() { onChangedBlock() }

    public func onError(payload: SQLiteNowCoreRuntimeErrorPayload) { onErrorBlock(payload) }

    public func cancel() { retainedSelf = nil }
}
