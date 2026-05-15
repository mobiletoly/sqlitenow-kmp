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
package dev.goquick.sqlitenow.gradle.dart

import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.oversqlite.resolveOversqliteSyncTables

internal class DartOversqliteEmitter(
    private val databaseName: String,
    private val createTableStatements: List<AnnotatedCreateTableStatement>,
    private val oversqlite: Boolean,
) {
    private val resolvedSyncTables by lazy {
        resolveOversqliteSyncTables(
            databaseClassName = databaseName,
            createTableStatements = createTableStatements,
            oversqlite = oversqlite,
        )
    }

    fun validateFlag() {
        if (!oversqlite && resolvedSyncTables.isNotEmpty()) {
            error(
                "Dart database '$databaseName' has tables annotated with enableSync=true, but oversqlite=false. " +
                    "Set oversqlite=true for Dart Oversqlite metadata generation or remove enableSync=true."
            )
        }
    }

    fun emit(writer: DartWriter) {
        if (!oversqlite) return

        writer.line("static const List<SyncTable> syncTables = <SyncTable>[")
        writer.indent {
            resolvedSyncTables.forEach { syncTable ->
                line(
                    "SyncTable(tableName: '${syncTable.table.name}', " +
                        "syncKeyColumnName: '${syncTable.syncKeyColumnName}'),"
                )
            }
        }
        writer.line("];")
        writer.line()
        writer.line("OversqliteConfig buildOversqliteConfig({")
        writer.indent {
            line("required String schema,")
            line("int uploadLimit = 200,")
            line("int downloadLimit = 1000,")
            line("bool verboseLogs = false,")
        }
        writer.line("}) {")
        writer.indent {
            line("return OversqliteConfig(")
            indent {
                line("schema: schema,")
                line("syncTables: syncTables,")
                line("uploadLimit: uploadLimit,")
                line("downloadLimit: downloadLimit,")
                line("verboseLogs: verboseLogs,")
            }
            line(");")
        }
        writer.line("}")
        writer.line()
        writer.line("DefaultOversqliteClient newOversqliteClient({")
        writer.indent {
            line("required String schema,")
            line("required OversqliteHttpClient httpClient,")
            line("int uploadLimit = 200,")
            line("int downloadLimit = 1000,")
            line("bool verboseLogs = false,")
        }
        writer.line("}) {")
        writer.indent {
            line("final config = buildOversqliteConfig(")
            indent {
                line("schema: schema,")
                line("uploadLimit: uploadLimit,")
                line("downloadLimit: downloadLimit,")
                line("verboseLogs: verboseLogs,")
            }
            line(");")
            line("return DefaultOversqliteClient(")
            indent {
                line("database: _database,")
                line("config: config,")
                line("httpClient: httpClient,")
            }
            line(");")
        }
        writer.line("}")
        writer.line()
    }
}
