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
package dev.goquick.sqlitenow.gradle.compiler

import dev.goquick.sqlitenow.gradle.MigratorTempStorage
import dev.goquick.sqlitenow.gradle.StandardErrorHandler
import dev.goquick.sqlitenow.gradle.TempDatabaseConnector
import dev.goquick.sqlitenow.gradle.database.DatabaseCodeGenerator
import dev.goquick.sqlitenow.gradle.database.MigrationInspector
import dev.goquick.sqlitenow.gradle.database.MigratorCodeGenerator
import dev.goquick.sqlitenow.gradle.dart.DartCodeGenerator
import dev.goquick.sqlitenow.gradle.generator.data.DataStructCodeGenerator
import dev.goquick.sqlitenow.gradle.generator.query.QueryCodeGenerator
import dev.goquick.sqlitenow.gradle.oversqlite.resolveOversqliteSyncTables
import dev.goquick.sqlitenow.gradle.sqlinspect.SQLBatchInspector
import dev.goquick.sqlitenow.gradle.sqlinspect.SchemaInspector
import dev.goquick.sqlitenow.gradle.swift.DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME
import dev.goquick.sqlitenow.gradle.swift.DEFAULT_SWIFT_SYNC_RUNTIME_MODULE_NAME
import dev.goquick.sqlitenow.gradle.swift.SqliteNowSwiftExportConfig
import dev.goquick.sqlitenow.gradle.swift.SqliteNowSwiftProductExportConfig
import dev.goquick.sqlitenow.gradle.swift.SwiftCodeGenerator
import dev.goquick.sqlitenow.gradle.swift.SwiftCoreRuntimeCodeGenerator
import dev.goquick.sqlitenow.gradle.swift.SwiftProductRuntimeMode
import dev.goquick.sqlitenow.gradle.swift.SwiftSyncTable
import java.io.File
import java.io.FileNotFoundException
import java.sql.Connection

data class SqliteNowCompilerInput(
    val databaseName: String,
    val sqlDirectory: File,
    val packageName: String,
    val outputDirectory: File,
    val schemaDatabaseFile: File? = null,
    val debug: Boolean = false,
    val oversqlite: Boolean = false,
    val backend: SqliteNowCompilerBackend = SqliteNowCompilerBackend.KOTLIN,
    val swiftExport: SqliteNowSwiftExportConfig? = null,
    val swiftProductExport: SqliteNowSwiftProductExportConfig? = null,
)

enum class SqliteNowCompilerBackend {
    KOTLIN,
    DART,
}

enum class SqliteNowCompilerDiagnosticSeverity {
    WARNING,
}

data class SqliteNowCompilerDiagnostic(
    val severity: SqliteNowCompilerDiagnosticSeverity,
    val message: String,
)

data class SqliteNowCompilerResult(
    val generatedFiles: List<File>,
    val warnings: List<String>,
    val diagnostics: List<SqliteNowCompilerDiagnostic>,
    val swiftPackageSyncTables: List<SwiftSyncTable> = emptyList(),
)

fun compileSqliteNowDatabase(
    input: SqliteNowCompilerInput,
    lifecycleReporter: (String) -> Unit = {},
): SqliteNowCompilerResult {
    if (!input.sqlDirectory.exists()) {
        throw FileNotFoundException("SQL database directory '${input.sqlDirectory.path}' not found")
    }

    input.outputDirectory.mkdirs()

    val warnings = mutableListOf<String>()
    val schemaDir = input.sqlDirectory.resolve("schema")
    val initSqlDir = input.sqlDirectory.resolve("init")
    val migrationDir = input.sqlDirectory.resolve("migration")
    val queriesDirs = input.sqlDirectory.resolve("queries")

    val conn: Connection = if (input.schemaDatabaseFile != null) {
        input.schemaDatabaseFile.delete()
        TempDatabaseConnector(
            storage = MigratorTempStorage.File(input.schemaDatabaseFile),
            lifecycleReporter = lifecycleReporter,
        ).connection
    } else {
        TempDatabaseConnector(MigratorTempStorage.Memory).connection
    }

    try {
        val schemaInspector = SchemaInspector(schemaDirectory = schemaDir)
        val sqlBatchInspector = SQLBatchInspector(sqlDirectory = initSqlDir, mandatory = false)
        val migrationInspector = MigrationInspector(sqlDirectory = migrationDir)

        if (input.outputDirectory.path.contains("/generated/")) {
            input.outputDirectory.deleteRecursively()
        }

        input.swiftProductExport?.let { swiftProductExport ->
            require(input.backend == SqliteNowCompilerBackend.KOTLIN) {
                "Product Swift source export is only available from the Kotlin metadata path."
            }

            val dataStructCodeGenerator = DataStructCodeGenerator(
                conn = conn,
                queriesDir = queriesDirs,
                statementExecutors = schemaInspector.statementExecutors,
                packageName = input.packageName,
                outputDir = input.outputDirectory
            )
            val runtimeMode = swiftProductExport.runtimeMode
            val effectiveSwiftProductExport = if (
                runtimeMode == SwiftProductRuntimeMode.SYNC &&
                swiftProductExport.runtimeModuleName == DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME
            ) {
                swiftProductExport.copy(runtimeModuleName = DEFAULT_SWIFT_SYNC_RUNTIME_MODULE_NAME)
            } else {
                swiftProductExport
            }
            val syncTables = if (runtimeMode == SwiftProductRuntimeMode.SYNC) {
                resolveOversqliteSyncTables(
                    databaseClassName = input.databaseName,
                    createTableStatements = dataStructCodeGenerator.createTableStatements,
                    oversqlite = false,
                )
            } else {
                emptyList()
            }
            val generatedSwiftFiles = SwiftCoreRuntimeCodeGenerator(
                databaseName = input.databaseName,
                schemaInspector = schemaInspector,
                sqlBatchInspector = sqlBatchInspector,
                migrationInspector = migrationInspector,
                dataStructCodeGenerator = dataStructCodeGenerator,
                config = effectiveSwiftProductExport,
                syncTables = syncTables,
            ).generateCode()
            val swiftPackageSyncTables = syncTables.map { syncTable ->
                SwiftSyncTable(
                    tableName = syncTable.table.name,
                    syncKeyColumnName = syncTable.syncKeyColumnName,
                )
            }
            conn.commit()

            return SqliteNowCompilerResult(
                generatedFiles = generatedSwiftFiles
                    .sortedBy { it.relativeTo(swiftProductExport.swiftOutputDirectory).invariantSeparatorsPath }
                    .toList(),
                warnings = warnings,
                diagnostics = warnings.map {
                    SqliteNowCompilerDiagnostic(
                        severity = SqliteNowCompilerDiagnosticSeverity.WARNING,
                        message = it,
                    )
                },
                swiftPackageSyncTables = swiftPackageSyncTables,
            )
        }

        if (input.backend == SqliteNowCompilerBackend.DART) {
            val dataStructCodeGenerator = DataStructCodeGenerator(
                conn = conn,
                queriesDir = queriesDirs,
                statementExecutors = schemaInspector.statementExecutors,
                packageName = input.packageName,
                outputDir = input.outputDirectory
            )
            DartCodeGenerator(
                databaseName = input.databaseName,
                outputDir = input.outputDirectory,
                schemaInspector = schemaInspector,
                sqlBatchInspector = sqlBatchInspector,
                migrationInspector = migrationInspector,
                dataStructCodeGenerator = dataStructCodeGenerator,
                oversqlite = input.oversqlite,
            ).generateCode()
            conn.commit()

            return SqliteNowCompilerResult(
                generatedFiles = input.outputDirectory
                    .walkTopDown()
                    .filter { it.isFile }
                    .sortedBy { it.relativeTo(input.outputDirectory).invariantSeparatorsPath }
                    .toList(),
                warnings = warnings,
                diagnostics = warnings.map {
                    SqliteNowCompilerDiagnostic(
                        severity = SqliteNowCompilerDiagnosticSeverity.WARNING,
                        message = it,
                    )
                },
            )
        }

        val migratorCodeGenerator = MigratorCodeGenerator(
            schemaInspector = schemaInspector,
            sqlBatchInspector = sqlBatchInspector,
            migrationInspector = migrationInspector,
            packageName = input.packageName,
            outputDir = input.outputDirectory,
            debug = input.debug
        )
        migratorCodeGenerator.generateCode()

        val dataStructCodeGenerator = DataStructCodeGenerator(
            conn = conn,
            queriesDir = queriesDirs,
            statementExecutors = schemaInspector.statementExecutors,
            packageName = input.packageName,
            outputDir = input.outputDirectory
        )
        dataStructCodeGenerator.generateCode()

        val queryCodeGenerator = QueryCodeGenerator(
            generatorContext = dataStructCodeGenerator.generatorContext,
            dataStructCodeGenerator = dataStructCodeGenerator,
            debug = input.debug
        )
        queryCodeGenerator.generateCode()

        val allStatements = sqlBatchInspector.sqlStatements
        conn.autoCommit = true
        allStatements.forEach {
            conn.createStatement().use { stmt ->
                try {
                    stmt.executeUpdate(it.sql)
                } catch (e: Throwable) {
                    StandardErrorHandler.handleSqlExecutionError(it.sql, e, "SqliteNowCompiler")
                }
            }
        }

        val dbCodeGen = DatabaseCodeGenerator(
            nsWithStatements = dataStructCodeGenerator.nsWithStatements,
            createTableStatements = dataStructCodeGenerator.createTableStatements,
            createViewStatements = dataStructCodeGenerator.createViewStatements,
            packageName = input.packageName,
            outputDir = input.outputDirectory,
            databaseClassName = input.databaseName,
            debug = input.debug,
            oversqlite = input.oversqlite,
        )
        val syncTables = dataStructCodeGenerator.createTableStatements
            .filter { it.annotations.enableSync }
            .distinct()
        if (!input.oversqlite && syncTables.isNotEmpty()) {
            warnings +=
                "Database '${input.databaseName}' has tables annotated with enableSync=true, but oversqlite=false. " +
                    "Oversqlite bridge helpers will not be generated."
        }
        dbCodeGen.generateDatabaseClass()

        input.swiftExport?.let { swiftExport ->
            SwiftCodeGenerator(
                databaseName = input.databaseName,
                databasePackageName = input.packageName,
                kotlinOutputDir = input.outputDirectory,
                dataStructCodeGenerator = dataStructCodeGenerator,
                config = swiftExport,
                oversqlite = input.oversqlite,
            ).generateCode()
        }

        return SqliteNowCompilerResult(
            generatedFiles = buildList {
                addAll(
                    input.outputDirectory
                        .walkTopDown()
                        .filter { it.isFile }
                        .sortedBy { it.relativeTo(input.outputDirectory).invariantSeparatorsPath }
                        .toList()
                )
                input.swiftExport?.swiftOutputDirectory?.takeIf { it.exists() }?.let { swiftOutput ->
                    addAll(
                        swiftOutput
                            .walkTopDown()
                            .filter { it.isFile }
                            .sortedBy { it.relativeTo(swiftOutput).invariantSeparatorsPath }
                            .toList()
                    )
                }
            },
            warnings = warnings,
            diagnostics = warnings.map {
                SqliteNowCompilerDiagnostic(
                    severity = SqliteNowCompilerDiagnosticSeverity.WARNING,
                    message = it,
                )
            },
        )
    } finally {
        conn.close()
    }
}
