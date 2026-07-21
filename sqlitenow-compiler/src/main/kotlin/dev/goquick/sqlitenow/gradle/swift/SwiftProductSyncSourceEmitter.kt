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
    private val syncTables: List<ResolvedOversqliteSyncTable>,
) {
    fun emitSyncTypes(writer: SwiftWriter) {
        SYNC_SUPPORT_SOURCE.lines().forEach { writer.line(it) }
        writer.line()
    }

    fun emitMakeSyncClient(writer: SwiftWriter) {
        writer.line("public func makeSyncClient(")
        writer.indent {
            line("baseURL: URL,")
            line("auth: SQLiteNowSyncAuth,")
            line("config: SQLiteNowSyncConfig = .init(),")
            line("resolver: SQLiteNowSyncResolver? = nil")
        }
        writer.line(") throws -> SQLiteNowSyncClient {")
        writer.indent {
            line("guard runtime.isOpen() else {")
            indent {
                line("throw SQLiteNowError.misuse(message: \"Call open() before makeSyncClient(...).\")")
            }
            line("}")
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
            line("let runtimeClient = SQLiteNowSyncRuntimeClient(")
            indent {
                line("coreDatabase: runtime,")
                line("baseUrl: baseURL.absoluteString,")
                line("auth: auth.runtimeAuth,")
                line("config: runtimeConfig,")
                line("resolver: resolver?.runtimeResolver")
            }
            line(")")
            line("return try syncClientLease.bind(runtime: runtimeClient)")
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
