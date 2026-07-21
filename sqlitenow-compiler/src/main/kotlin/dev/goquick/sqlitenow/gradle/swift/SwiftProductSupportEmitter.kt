/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.gradle.swift

internal class SwiftProductSupportEmitter(
    private val syncEnabled: Boolean,
) {
    fun emit(writer: SwiftWriter) {
        with(writer) {
            emitErrorTypes()
            emitSelectQueryContainer()
            emitExecuteReturningQueryContainer()
            emitRuntimeHelpers()
        }
    }

    private fun SwiftWriter.emitErrorTypes() {
        line("public enum SQLiteNowError: Error, CustomStringConvertible, Equatable {")
        indent {
            line("case sqlite(message: String)")
            line("case migration(message: String)")
            line("case misuse(message: String)")
            line("case cancelled(message: String)")
            line("case observation(message: String)")
            line("case adapter(message: String)")
            if (syncEnabled) line("case sync(message: String)")
            if (syncEnabled) line("case `protocol`(message: String)")
            line("case unknown(message: String)")
            line()
            line("public var description: String {")
            indent {
                line("switch self {")
                line("case let .sqlite(message): return message")
                line("case let .migration(message): return message")
                line("case let .misuse(message): return message")
                line("case let .cancelled(message): return message")
                line("case let .observation(message): return message")
                line("case let .adapter(message): return message")
                if (syncEnabled) line("case let .sync(message): return message")
                if (syncEnabled) line("case let .protocol(message): return message")
                line("case let .unknown(message): return message")
                line("}")
            }
            line("}")
            line()
            line("internal static func from(_ error: Error) -> SQLiteNowError {")
            indent {
                line("if let sqliteNowError = error as? SQLiteNowError {")
                indent {
                    line("return sqliteNowError")
                }
                line("}")
                line("if let runtimeError = error as? SQLiteNowCoreRuntimeException {")
                indent {
                    line("return from(runtimeError.payload)")
                }
                line("}")
                line("if let runtimeError = (error as NSError).userInfo[\"K\" + \"otlinException\"] as? SQLiteNowCoreRuntimeException {")
                indent {
                    line("return from(runtimeError.payload)")
                }
                line("}")
                if (syncEnabled) {
                    line("if let runtimeError = error as? SQLiteNowSyncRuntimeException {")
                    indent {
                        line("return from(runtimeError.payload)")
                    }
                    line("}")
                    line("if let runtimeError = (error as NSError).userInfo[\"K\" + \"otlinException\"] as? SQLiteNowSyncRuntimeException {")
                    indent {
                        line("return from(runtimeError.payload)")
                    }
                    line("}")
                }
                line("if error is CancellationError {")
                indent {
                    line("return .cancelled(message: String(describing: error))")
                }
                line("}")
                line("return .unknown(message: String(describing: error))")
            }
            line("}")
            line()
            line("internal static func from(_ payload: SQLiteNowCoreRuntimeErrorPayload) -> SQLiteNowError {")
            indent {
                line("switch payload.category {")
                line("case \"sqlite\": return .sqlite(message: payload.message)")
                line("case \"migration\": return .migration(message: payload.message)")
                line("case \"misuse\": return .misuse(message: payload.message)")
                line("case \"cancelled\": return .cancelled(message: payload.message)")
                line("default: return .unknown(message: payload.message)")
                line("}")
            }
            line("}")
            if (syncEnabled) {
                line()
                line("internal static func from(_ payload: SQLiteNowSyncRuntimeErrorPayload) -> SQLiteNowError {")
                indent {
                    line("switch payload.category {")
                    line("case \"cancelled\": return .cancelled(message: payload.message)")
                    line("case \"state\": return .misuse(message: payload.message)")
                    line("case \"network\", \"auth\", \"conflict\": return .sync(message: payload.message)")
                    line("case \"protocol\": return .protocol(message: payload.message)")
                    line("default: return .unknown(message: payload.message)")
                    line("}")
                }
                line("}")
            }
        }
        line("}")
        line()
        line("internal func mapRuntimeErrors<T>(_ operation: () async throws -> T) async throws -> T {")
        indent {
            line("do {")
            indent {
                line("return try await operation()")
            }
            line("} catch let error as CancellationError {")
            indent {
                line("throw error")
            }
            line("} catch {")
            indent {
                line("throw SQLiteNowError.from(error)")
            }
            line("}")
        }
        line("}")
        line()
        emitRowCardinalityHelpers()
    }

    private fun SwiftWriter.emitRowCardinalityHelpers() {
        line("internal func sqliteNowRequireOne<Row>(_ rows: [Row], resultName: String) throws -> Row {")
        indent {
            line("guard rows.count == 1, let row = rows.first else {")
            indent {
                line("throw SQLiteNowError.misuse(message: \"Expected exactly one \\(resultName) row, got \\(rows.count)\")")
            }
            line("}")
            line("return row")
        }
        line("}")
        line("internal func sqliteNowRequireOneOrNull<Row>(_ rows: [Row], resultName: String) throws -> Row? {")
        indent {
            line("guard rows.count <= 1 else {")
            indent {
                line("throw SQLiteNowError.misuse(message: \"Expected at most one \\(resultName) row, got \\(rows.count)\")")
            }
            line("}")
            line("return rows.first")
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitSelectQueryContainer() =
        emitQueryContainer(
            typeName = "SQLiteNowSelectQuery",
            includeStream = true,
        )

    private fun SwiftWriter.emitExecuteReturningQueryContainer() =
        emitQueryContainer(
            typeName = "SQLiteNowExecuteReturningQuery",
            includeStream = false,
        )

    private fun SwiftWriter.emitQueryContainer(
        typeName: String,
        includeStream: Boolean,
    ) {
        line("public final class $typeName<Row: Sendable>: Sendable {")
        indent {
            line("private let listBlock: @Sendable () async throws -> [Row]")
            line("private let oneBlock: @Sendable () async throws -> Row")
            line("private let oneOrNullBlock: @Sendable () async throws -> Row?")
            if (includeStream) {
                line("private let streamBlock: @Sendable () -> AsyncThrowingStream<[Row], Error>")
            }
            line()
            line("internal init(")
            indent {
                line("list: @escaping @Sendable () async throws -> [Row],")
                line("one: @escaping @Sendable () async throws -> Row,")
                if (includeStream) {
                    line("oneOrNull: @escaping @Sendable () async throws -> Row?,")
                    line("stream: @escaping @Sendable () -> AsyncThrowingStream<[Row], Error>")
                } else {
                    line("oneOrNull: @escaping @Sendable () async throws -> Row?")
                }
            }
            line(") {")
            indent {
                line("self.listBlock = list")
                line("self.oneBlock = one")
                line("self.oneOrNullBlock = oneOrNull")
                if (includeStream) {
                    line("self.streamBlock = stream")
                }
            }
            line("}")
            line()
            line("public func list() async throws -> [Row] { try await listBlock() }")
            line()
            line("public func one() async throws -> Row { try await oneBlock() }")
            line()
            line("public func oneOrNull() async throws -> Row? { try await oneOrNullBlock() }")
            line()
            if (includeStream) {
                line("public func stream() -> AsyncThrowingStream<[Row], Error> { streamBlock() }")
            }
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitRuntimeHelpers() {
        line("internal func sqliteNowBind(_ value: Int64) -> SQLiteNowCoreRuntimeBindValue { SQLiteNowCoreRuntimeBindValue(int64Value: value) }")
        line("internal func sqliteNowBind(_ value: Int64?) -> SQLiteNowCoreRuntimeBindValue { value.map { SQLiteNowCoreRuntimeBindValue(int64Value: ${'$'}0) } ?? SQLiteNowCoreRuntimeBindValue() }")
        line("internal func sqliteNowBind(_ value: Double) -> SQLiteNowCoreRuntimeBindValue { SQLiteNowCoreRuntimeBindValue(doubleValue: value) }")
        line("internal func sqliteNowBind(_ value: Double?) -> SQLiteNowCoreRuntimeBindValue { value.map { SQLiteNowCoreRuntimeBindValue(doubleValue: ${'$'}0) } ?? SQLiteNowCoreRuntimeBindValue() }")
        line("internal func sqliteNowBind(_ value: String) -> SQLiteNowCoreRuntimeBindValue { SQLiteNowCoreRuntimeBindValue(textValue: value) }")
        line("internal func sqliteNowBind(_ value: String?) -> SQLiteNowCoreRuntimeBindValue { value.map { SQLiteNowCoreRuntimeBindValue(textValue: ${'$'}0) } ?? SQLiteNowCoreRuntimeBindValue() }")
        line("internal func sqliteNowBind(_ value: Bool) -> SQLiteNowCoreRuntimeBindValue { SQLiteNowCoreRuntimeBindValue(boolValue: value) }")
        line("internal func sqliteNowBind(_ value: Bool?) -> SQLiteNowCoreRuntimeBindValue { value.map { SQLiteNowCoreRuntimeBindValue(boolValue: ${'$'}0) } ?? SQLiteNowCoreRuntimeBindValue() }")
        line("internal func sqliteNowBind(_ value: Data) -> SQLiteNowCoreRuntimeBindValue { SQLiteNowCoreRuntimeBindValue(dataValue: value) }")
        line("internal func sqliteNowBind(_ value: Data?) -> SQLiteNowCoreRuntimeBindValue { value.map { SQLiteNowCoreRuntimeBindValue(dataValue: ${'$'}0) } ?? SQLiteNowCoreRuntimeBindValue() }")
        line("internal func sqliteNowBindJsonArray<T>(_ values: [T]) -> SQLiteNowCoreRuntimeBindValue { SQLiteNowCoreRuntimeBindValue(textValue: sqliteNowJsonArray(values)) }")
        line()
        line("internal func sqliteNowJsonArray<T>(_ values: [T]) -> String {")
        indent {
            line("let encoded = values.map { value -> String in")
            indent {
                line("switch value {")
                line("case let string as String: return sqliteNowJsonString(string)")
                line("case let int64 as Int64: return String(int64)")
                line("case let int as Int: return String(int)")
                line("case let double as Double: return String(double)")
                line("case let bool as Bool: return bool ? \"true\" : \"false\"")
                line("default: return sqliteNowJsonString(String(describing: value))")
                line("}")
            }
            line("}")
            line("return \"[\" + encoded.joined(separator: \",\") + \"]\"")
        }
        line("}")
        line()
        line("internal func sqliteNowJsonString(_ value: String) -> String {")
        indent {
            line("var result = \"\\\"\"")
            line("for scalar in value.unicodeScalars {")
            indent {
                line("switch scalar.value {")
                line("case 0x22: result += \"\\\\\\\"\"")
                line("case 0x5C: result += \"\\\\\\\\\"")
                line("case 0x08: result += \"\\\\b\"")
                line("case 0x0C: result += \"\\\\f\"")
                line("case 0x0A: result += \"\\\\n\"")
                line("case 0x0D: result += \"\\\\r\"")
                line("case 0x09: result += \"\\\\t\"")
                line("case 0x00...0x1F:")
                indent {
                    line("result += String(format: \"\\\\u%04X\", scalar.value)")
                }
                line("default:")
                indent {
                    line("result.unicodeScalars.append(scalar)")
                }
                line("}")
            }
            line("}")
            line("result += \"\\\"\"")
            line("return result")
        }
        line("}")
        line()
        emitReadHelpers()
        emitObserver()
    }

    private fun SwiftWriter.emitReadHelpers() {
        line("internal func sqliteNowReadInt64(_ cell: SQLiteNowCoreRuntimeCell, column: String) throws -> Int64 {")
        indent {
            line("guard !cell.isNull else { throw SQLiteNowError.misuse(message: \"Expected Int64 for \\(column)\") }")
            line("return cell.int64Value")
        }
        line("}")
        line("internal func sqliteNowReadOptionalInt64(_ cell: SQLiteNowCoreRuntimeCell) -> Int64? { cell.isNull ? nil : cell.int64Value }")
        line("internal func sqliteNowReadDouble(_ cell: SQLiteNowCoreRuntimeCell, column: String) throws -> Double {")
        indent {
            line("guard !cell.isNull else { throw SQLiteNowError.misuse(message: \"Expected Double for \\(column)\") }")
            line("return cell.doubleValue")
        }
        line("}")
        line("internal func sqliteNowReadOptionalDouble(_ cell: SQLiteNowCoreRuntimeCell) -> Double? { cell.isNull ? nil : cell.doubleValue }")
        line("internal func sqliteNowReadBool(_ cell: SQLiteNowCoreRuntimeCell, column: String) throws -> Bool {")
        indent {
            line("guard !cell.isNull else { throw SQLiteNowError.misuse(message: \"Expected Bool for \\(column)\") }")
            line("return cell.boolValue")
        }
        line("}")
        line("internal func sqliteNowReadOptionalBool(_ cell: SQLiteNowCoreRuntimeCell) -> Bool? { cell.isNull ? nil : cell.boolValue }")
        line("internal func sqliteNowReadString(_ cell: SQLiteNowCoreRuntimeCell, column: String) throws -> String {")
        indent {
            line("guard let value = cell.textValue else { throw SQLiteNowError.misuse(message: \"Expected String for \\(column)\") }")
            line("return value")
        }
        line("}")
        line("internal func sqliteNowReadOptionalString(_ cell: SQLiteNowCoreRuntimeCell) -> String? { cell.textValue }")
        line("internal func sqliteNowReadData(_ cell: SQLiteNowCoreRuntimeCell, column: String) throws -> Data {")
        indent {
            line("guard let value = cell.dataValue else { throw SQLiteNowError.misuse(message: \"Expected Data for \\(column)\") }")
            line("return value as Data")
        }
        line("}")
        line("internal func sqliteNowReadOptionalData(_ cell: SQLiteNowCoreRuntimeCell) -> Data? { cell.dataValue.map { ${'$'}0 as Data } }")
        line()
        line("internal func sqliteNowCellKey(_ cell: SQLiteNowCoreRuntimeCell, columnType: String, column: String) throws -> String {")
        indent {
            line("if cell.isNull { return \"null\" }")
            line("switch columnType {")
            line("case \"bool\": return \"bool:\\(cell.boolValue)\"")
            line("case \"blob\": return \"blob:\\(try sqliteNowReadData(cell, column: column).base64EncodedString())\"")
            line("case \"double\": return \"double:\\(cell.doubleValue)\"")
            line("case \"int64\": return \"int64:\\(cell.int64Value)\"")
            line("default: return \"text:\\(try sqliteNowReadString(cell, column: column))\"")
            line("}")
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitObserver() {
        line("final class SQLiteNowStreamRefreshCoordinator<Row>: @unchecked Sendable {")
        indent {
            line("private let lock = NSLock()")
            line("private let loadRows: @Sendable () async throws -> [Row]")
            line("private let continuation: AsyncThrowingStream<[Row], Error>.Continuation")
            line("private var task: Task<Void, Never>?")
            line("private var isRunning = false")
            line("private var needsRefresh = false")
            line("private var isCancelled = false")
            line()
            line("init(")
            indent {
                line("continuation: AsyncThrowingStream<[Row], Error>.Continuation,")
                line("loadRows: @escaping @Sendable () async throws -> [Row]")
            }
            line(") {")
            indent {
                line("self.continuation = continuation")
                line("self.loadRows = loadRows")
            }
            line("}")
            line()
            line("func refresh() {")
            indent {
                line("lock.lock()")
                line("if isCancelled {")
                indent {
                    line("lock.unlock()")
                    line("return")
                }
                line("}")
                line("if isRunning {")
                indent {
                    line("needsRefresh = true")
                    line("lock.unlock()")
                    line("return")
                }
                line("}")
                line("isRunning = true")
                line("task = Task { [weak self] in")
                indent {
                    line("guard let self else { return }")
                    line("await self.run()")
                }
                line("}")
                line("lock.unlock()")
            }
            line("}")
            line()
            line("func cancel() {")
            indent {
                line("lock.lock()")
                line("if isCancelled {")
                indent {
                    line("lock.unlock()")
                    line("return")
                }
                line("}")
                line("isCancelled = true")
                line("let task = task")
                line("self.task = nil")
                line("lock.unlock()")
                line("task?.cancel()")
            }
            line("}")
            line()
            line("private func run() async {")
            indent {
                line("while true {")
                indent {
                    line("do {")
                    indent {
                        line("let rows = try await loadRows()")
                        line("continuation.yield(rows)")
                    }
                    line("} catch {")
                    indent {
                        line("continuation.finish(throwing: error)")
                        line("cancel()")
                        line("return")
                    }
                    line("}")
                    line()
                    line("if shouldContinueAfterLoad() { continue }")
                    line("return")
                }
                line("}")
            }
            line("}")
            line()
            line("private func shouldContinueAfterLoad() -> Bool {")
            indent {
                line("lock.lock()")
                line("defer { lock.unlock() }")
                line("if isCancelled {")
                indent {
                    line("isRunning = false")
                    line("return false")
                }
                line("}")
                line("if needsRefresh {")
                indent {
                    line("needsRefresh = false")
                    line("return true")
                }
                line("}")
                line("isRunning = false")
                line("task = nil")
                line("return false")
            }
            line("}")
        }
        line("}")
        line()

        line("final class RuntimeTableObserver: SQLiteNowCoreRuntimeTableObserver, @unchecked Sendable {")
        indent {
            line("private let onChangedBlock: @Sendable () -> Void")
            line("private let onErrorBlock: @Sendable (SQLiteNowCoreRuntimeErrorPayload) -> Void")
            line("private var retainedSelf: RuntimeTableObserver?")
            line()
            line("init(")
            indent {
                line("onChanged: @escaping @Sendable () -> Void,")
                line("onError: @escaping @Sendable (SQLiteNowCoreRuntimeErrorPayload) -> Void")
            }
            line(") {")
            indent {
                line("self.onChangedBlock = onChanged")
                line("self.onErrorBlock = onError")
                line("self.retainedSelf = self")
            }
            line("}")
            line()
            line("func onChanged() { onChangedBlock() }")
            line()
            line("func onError(payload: SQLiteNowCoreRuntimeErrorPayload) { onErrorBlock(payload) }")
            line()
            line("func cancel() { retainedSelf = nil }")
        }
        line("}")
        line()
    }

}
