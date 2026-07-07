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
import dev.goquick.sqlitenow.gradle.generator.data.DataStructCodeGenerator
import dev.goquick.sqlitenow.gradle.oversqlite.ResolvedOversqliteSyncTable
import dev.goquick.sqlitenow.gradle.processing.AffectedTablesResolver
import dev.goquick.sqlitenow.gradle.sqlinspect.SQLBatchInspector
import dev.goquick.sqlitenow.gradle.sqlinspect.SchemaInspector
import java.io.File

const val DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME = "SQLiteNowCoreRuntime"
const val DEFAULT_SWIFT_SYNC_RUNTIME_MODULE_NAME = "SQLiteNowSyncRuntime"

data class SqliteNowSwiftProductExportConfig(
    val swiftOutputDirectory: File,
    val swiftModuleName: String,
    val runtimeModuleName: String = DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME,
    val runtimeMode: SwiftProductRuntimeMode = SwiftProductRuntimeMode.CORE,
    val emitSupportSources: Boolean = true,
)

enum class SwiftProductRuntimeMode(val id: String) {
    CORE("core"),
    SYNC("sync"),
    ;

    fun defaultRuntimeModuleName(): String =
        when (this) {
            CORE -> DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME
            SYNC -> DEFAULT_SWIFT_SYNC_RUNTIME_MODULE_NAME
        }

    companion object {
        fun fromId(value: String): SwiftProductRuntimeMode =
            entries.firstOrNull { it.id == value.lowercase() }
                ?: error("Unsupported Swift product runtime '$value'. Expected '${CORE.id}' or '${SYNC.id}'.")
    }
}

internal class SwiftCoreRuntimeCodeGenerator(
    private val databaseName: String,
    private val schemaInspector: SchemaInspector,
    private val sqlBatchInspector: SQLBatchInspector,
    private val migrationInspector: MigrationInspector,
    dataStructCodeGenerator: DataStructCodeGenerator,
    private val config: SqliteNowSwiftProductExportConfig,
    private val syncTables: List<ResolvedOversqliteSyncTable> = emptyList(),
) {
    private val context = dataStructCodeGenerator.generatorContext
    private val plan = SwiftGenerationPlan(context, dataStructCodeGenerator)
    private val syncEnabled: Boolean
        get() = config.runtimeMode == SwiftProductRuntimeMode.SYNC
    private val syncSourceEmitter = SwiftProductSyncSourceEmitter(syncTables)
    private val model = SwiftProductGenerationModel(
        context = context,
        plan = plan,
        affectedTablesResolver = AffectedTablesResolver.fromStatements(
            createTableStatements = context.createTableStatements,
            createViewStatements = context.createViewStatements,
            includeSchemaStatements = false,
        ),
    )

    fun generateCode(): List<File> {
        model.validateSupportedSurface(syncEnabled = syncEnabled, syncTables = syncTables)
        return SwiftProductSourceAssembler(
            databaseName = databaseName,
            config = config,
            model = model,
            databaseEmitter = SwiftProductDatabaseEmitter(
                databaseName = databaseName,
                schemaInspector = schemaInspector,
                sqlBatchInspector = sqlBatchInspector,
                migrationInspector = migrationInspector,
                model = model,
                syncEnabled = syncEnabled,
                syncSourceEmitter = syncSourceEmitter,
            ),
            supportEmitter = SwiftProductSupportEmitter(
                syncEnabled = syncEnabled,
            ),
            syncSourceEmitter = syncSourceEmitter,
            namespaceEmitter = SwiftProductNamespaceEmitter(
                databaseName = databaseName,
                context = context,
                model = model,
            ),
        ).generateCode()
    }
}
