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

import dev.goquick.sqlitenow.gradle.database.MigrationInspector
import dev.goquick.sqlitenow.gradle.sqlinspect.SQLBatchInspector
import dev.goquick.sqlitenow.gradle.sqlinspect.SchemaInspector

internal class SwiftProductDatabaseEmitter(
    private val databaseName: String,
    private val schemaInspector: SchemaInspector,
    private val sqlBatchInspector: SQLBatchInspector,
    private val migrationInspector: MigrationInspector,
    private val model: SwiftProductGenerationModel,
    private val syncEnabled: Boolean,
    private val syncSourceEmitter: SwiftProductSyncSourceEmitter,
) {
    fun emit(writer: SwiftWriter) {
        with(writer) {
            emitDatabase()
            emitAdapterContainer()
            emitTransactionType()
        }
    }

    private fun SwiftWriter.emitAdapterContainer() {
        val adapters = model.adapterDescriptors
        line("public struct ${databaseName}Adapters: Sendable {")
        indent {
            if (adapters.isEmpty()) {
                line("public init() {}")
            } else {
                adapters.forEach { adapter ->
                    line("public let ${adapter.name}: @Sendable (${adapter.inputSwiftType}) throws -> ${adapter.outputSwiftType}")
                }
                line()
                line("public init(")
                indent {
                    adapters.forEachIndexed { index, adapter ->
                        val suffix = if (index == adapters.lastIndex) "" else ","
                        line("${adapter.name}: @escaping @Sendable (${adapter.inputSwiftType}) throws -> ${adapter.outputSwiftType}$suffix")
                    }
                }
                line(") {")
                indent {
                    adapters.forEach { adapter ->
                        line("self.${adapter.name} = ${adapter.name}")
                    }
                }
                line("}")
            }
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitDatabase() {
        val adapterDefault = if (model.adapterDescriptors.isEmpty()) " = .init()" else ""
        line("public final class $databaseName: @unchecked Sendable {")
        indent {
            line("private let runtime: SQLiteNowCoreRuntimeDatabase")
            if (syncEnabled) {
                line("private let syncClientLease = SQLiteNowSyncClientLease()")
            }
            line("fileprivate let adapters: ${databaseName}Adapters")
            model.namespaces.forEach { namespace ->
                line("public let ${namespace.swiftNamespacePropertyName().swiftIdentifier()}: ${namespace.swiftNamespaceTypeName("Queries")}")
            }
            line()
            line("public init(path: URL, adapters: ${databaseName}Adapters$adapterDefault, debug: Bool = false) {")
            indent {
                line("let runtime = SQLiteNowCoreRuntimeDatabase(")
                indent {
                    line("path: path.path,")
                    line("migrationPlan: Self.migrationPlan(),")
                    line("debug: debug")
                }
                line(")")
                line("self.runtime = runtime")
                line("self.adapters = adapters")
                model.namespaces.forEach { namespace ->
                    line("self.${namespace.swiftNamespacePropertyName().swiftIdentifier()} = ${namespace.swiftNamespaceTypeName("Queries")}(runtime: runtime, adapters: adapters)")
                }
            }
            line("}")
            line()
            line("public func open() async throws {")
            indent {
                line("_ = try await mapRuntimeErrors {")
                indent {
                    line("try await runtime.open()")
                }
                line("}")
            }
            line("}")
            line()
            line("public func close() async throws {")
            indent {
                if (syncEnabled) {
                    line("syncClientLease.closeActiveClient()")
                }
                line("_ = try await mapRuntimeErrors {")
                indent {
                    line("try await runtime.close()")
                }
                line("}")
            }
            line("}")
            line()
            line("public func transaction(_ block: (${databaseName}Transaction) throws -> Void) async throws {")
            indent {
                line("let batch = SQLiteNowCoreRuntimeMutationBatch()")
                line("try block(${databaseName}Transaction(batch: batch, adapters: adapters))")
                line("_ = try await mapRuntimeErrors {")
                indent {
                    line("try await runtime.transaction(batch: batch)")
                }
                line("}")
            }
            line("}")
            line()
            if (syncEnabled) {
                syncSourceEmitter.emitMakeSyncClient(this)
            }
            emitMigrationPlan()
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitMigrationPlan() {
        line("private static func migrationPlan() -> SQLiteNowCoreRuntimeMigrationPlan {")
        indent {
            line("SQLiteNowCoreRuntimeMigrationPlan(")
            indent {
                line("latestVersion: ${migrationInspector.latestVersion},")
                line("schemaSql: [")
                indent {
                    schemaInspector.sqlStatements.forEachIndexed { index, statement ->
                        emitSwiftStringArrayElement(statement.sql, index == schemaInspector.sqlStatements.lastIndex)
                    }
                }
                line("],")
                line("initSql: [")
                indent {
                    sqlBatchInspector.sqlStatements.forEachIndexed { index, statement ->
                        emitSwiftStringArrayElement(statement.sql, index == sqlBatchInspector.sqlStatements.lastIndex)
                    }
                }
                line("],")
                line("migrationSteps: [")
                indent {
                    val entries = migrationInspector.sqlStatements.entries.toList()
                    entries.forEachIndexed { entryIndex, (version, statements) ->
                        line("SQLiteNowCoreRuntimeMigrationStep(")
                        indent {
                            line("version: $version,")
                            line("sql: [")
                            indent {
                                statements.forEachIndexed { index, statement ->
                                    emitSwiftStringArrayElement(statement.sql, index == statements.lastIndex)
                                }
                            }
                            line("]")
                        }
                        line(")${if (entryIndex == entries.lastIndex) "" else ","}")
                    }
                }
                line("]")
            }
            line(")")
        }
        line("}")
    }

    private fun SwiftWriter.emitTransactionType() {
        line("public final class ${databaseName}Transaction {")
        indent {
            model.namespaces.forEach { namespace ->
                line("public let ${namespace.swiftNamespacePropertyName().swiftIdentifier()}: ${namespace.swiftNamespaceTypeName("TransactionQueries")}")
            }
            line()
            line("internal init(batch: SQLiteNowCoreRuntimeMutationBatch, adapters: ${databaseName}Adapters) {")
            indent {
                model.namespaces.forEach { namespace ->
                    line("self.${namespace.swiftNamespacePropertyName().swiftIdentifier()} = ${namespace.swiftNamespaceTypeName("TransactionQueries")}(batch: batch, adapters: adapters)")
                }
            }
            line("}")
        }
        line("}")
        line()
    }

}
