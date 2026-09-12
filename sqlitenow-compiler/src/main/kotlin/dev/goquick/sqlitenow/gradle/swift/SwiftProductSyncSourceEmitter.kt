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

import dev.goquick.sqlitenow.gradle.oversqlite.ResolvedOversqliteSyncTable

internal class SwiftProductSyncSourceEmitter(
    private val databaseName: String,
    private val syncTables: List<ResolvedOversqliteSyncTable>,
) {
    fun emitSyncTypes(writer: SwiftWriter) {
        SYNC_SUPPORT_SOURCE.lines().forEach { writer.line(it) }
        writer.line()
    }

    fun emitMakeSyncClient(writer: SwiftWriter) {
        writer.line("public func openForSync(")
        writer.indent {
            line("baseURL: URL,")
            line("auth: SQLiteNowSyncAuth,")
            line("config: SQLiteNowSyncConfig,")
            line("resolver: SQLiteNowSyncResolver? = nil,")
            line("upgradePlan: SQLiteNowSyncUpgradePlan? = nil")
        }
        writer.line(") async throws -> SQLiteNowSyncOpening<${databaseName}> {")
        writer.indent {
            line("let runtimeConfig = try config.runtimeConfig(syncTables: [")
            indent {
                syncTables.forEachIndexed { index, syncTable ->
                    val suffix = if (index == syncTables.lastIndex) "" else ","
                    line(
                        "SQLiteNowSyncRuntimeTableSpec(tableName: " +
                            "${syncTable.table.name.toSwiftStringLiteral()}, " +
                            "syncKeyColumnName: ${syncTable.syncKeyColumnName.toSwiftStringLiteral()})$suffix"
                    )
                }
            }
            line("])")
            line("let binding = SQLiteNowSyncClientBinding(baseURL: baseURL, auth: auth, config: config, resolver: resolver)")
            line("let runtimeOpening = try await mapRuntimeErrors {")
            indent {
                line("let result = try await awaitSyncOpeningOperation { completion in")
                indent {
                    line("SQLiteNowSyncRuntimeFactory().startOpenForSync(")
                    indent {
                        line("coreDatabase: self.runtime,")
                        line("baseUrl: baseURL.absoluteString,")
                        line("auth: auth.runtimeAuth,")
                        line("config: runtimeConfig,")
                        line("resolver: resolver?.runtimeResolver,")
                        line("upgradePlan: upgradePlan?.runtimePlan,")
                        line("completion: completion")
                    }
                    line(")")
                }
                line("}")
                line("guard let opening = result.opening else {")
                indent { line("throw SQLiteNowError.sync(message: \"KMP omitted the coordinated-opening result.\")") }
                line("}")
                line("return opening")
            }
            line("}")
            line("return SQLiteNowSyncOpening(runtime: runtimeOpening, database: self, lease: syncClientLease, binding: binding)")
        }
        writer.line("}")
        writer.line()
        writer.line("public func makeSyncClient(")
        writer.indent {
            line("baseURL: URL,")
            line("auth: SQLiteNowSyncAuth,")
            line("config: SQLiteNowSyncConfig,")
            line("resolver: SQLiteNowSyncResolver? = nil")
        }
        writer.line(") throws -> SQLiteNowSyncClient {")
        writer.indent {
            line("let binding = SQLiteNowSyncClientBinding(baseURL: baseURL, auth: auth, config: config, resolver: resolver)")
            line("if let readyClient = try syncClientLease.readyClient(matching: binding) { return readyClient }")
            line("throw SQLiteNowError.misuse(message: \"openForSync().advance() must reach ready before makeSyncClient(...).\")")
        }
        writer.line("}")
        writer.line()
    }

}

private const val SYNC_SUPPORT_RESOURCE = "/swift/product/SQLiteNowSyncSupport.swift"

private val SYNC_SUPPORT_SOURCE: String by lazy {
    SwiftProductSyncSourceEmitter::class.java.getResourceAsStream(SYNC_SUPPORT_RESOURCE)
        ?.bufferedReader(Charsets.UTF_8)
        ?.use { it.readText().trimEnd() }
        ?: error("Missing Swift product sync support resource: $SYNC_SUPPORT_RESOURCE")
}
