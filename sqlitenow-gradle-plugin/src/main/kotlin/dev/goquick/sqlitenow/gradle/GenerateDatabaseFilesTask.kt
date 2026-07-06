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
package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.compiler.SqliteNowCompilerInput
import dev.goquick.sqlitenow.gradle.compiler.SqliteNowCompilerResult
import dev.goquick.sqlitenow.gradle.compiler.compileSqliteNowDatabase
import dev.goquick.sqlitenow.gradle.swift.SqliteNowSwiftExportConfig
import dev.goquick.sqlitenow.gradle.swift.SqliteNowSwiftProductExportConfig
import dev.goquick.sqlitenow.gradle.swift.SwiftProductRuntimeMode
import dev.goquick.sqlitenow.gradle.swift.requireValidSwiftIdentifier
import groovy.json.JsonOutput
import java.io.File
import javax.inject.Inject
import org.gradle.api.DefaultTask
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.ProjectLayout
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction

fun main() {
//    val rootSqlDir = "/Users/pochkin/Projects/my/sqlitenow-kmp/sample-kmp/composeApp/src/commonMain/sql"
//    val schemaDatabaseDir = "/Users/pochkin/Projects/my/sqlitenow-kmp/sample-kmp/composeApp/tmp"
//
//    generateDatabaseFiles(
//        dbName = "NowSampleDatabase",
//        sqlDir = File("$rootSqlDir/NowSampleDatabase"),
//        packageName = "dev.goquick.sqlitenow.samplekmp.db",
//        outDir = File("/Users/pochkin/Projects/my/sqlitenow-kmp/sample-kmp/composeApp/build/generated/sqlitenow/code"),
//        schemaDatabaseFile = File("$schemaDatabaseDir/schema.db"),
//        debug = true,
//    )
}

abstract class GenerateDatabaseFilesTask @Inject constructor(
    objects: ObjectFactory,
    layout: ProjectLayout,
) : DefaultTask() {

    /** Name of the DB, provided by the user */
    @get:Input
    abstract val dbName: Property<String>

    /** Where to write generated Kotlin code */
    @get:OutputDirectory
    val outputDir: DirectoryProperty =
        objects.directoryProperty().convention(layout.buildDirectory.dir("generated/sqlitenow/code"))

    @get:InputDirectory
    @get:Optional
    val sqlDir: DirectoryProperty =
        objects.directoryProperty().convention(layout.projectDirectory.dir("src/commonMain/sql"))

    @get:Input
    abstract val packageName: Property<String>

    @get:OutputFile
    @get:Optional
    val schemaDatabaseFile: RegularFileProperty = objects.fileProperty()

    @get:Input
    abstract val debug: Property<Boolean>

    @get:Input
    abstract val oversqlite: Property<Boolean>

    @get:Input
    abstract val oversqliteRuntimePresent: Property<Boolean>

    @get:OutputDirectory
    @get:Optional
    val swiftOverlayOutputDir: DirectoryProperty = objects.directoryProperty()

    @get:Input
    @get:Optional
    abstract val swiftOverlayModuleName: Property<String>

    @get:Input
    @get:Optional
    abstract val swiftFrameworkModuleName: Property<String>

    @get:Input
    @get:Optional
    abstract val swiftBridgePackageName: Property<String>

    @get:OutputDirectory
    @get:Optional
    val swiftProductOutputDir: DirectoryProperty = objects.directoryProperty()

    @get:Input
    @get:Optional
    abstract val swiftProductModuleName: Property<String>

    @get:Input
    @get:Optional
    abstract val swiftProductRuntimeModuleName: Property<String>

    @get:Input
    @get:Optional
    abstract val swiftProductRuntimeMode: Property<String>

    @get:OutputFile
    @get:Optional
    val swiftProductMetadataFile: RegularFileProperty = objects.fileProperty()

    @TaskAction
    fun generate() {
        // 1. Ensure the output directory exists
        val outDir: File = outputDir.asFile.get().apply { mkdirs() }

        val dbDir = sqlDir.dir(dbName.get())
        val dbFile = if (schemaDatabaseFile.isPresent) schemaDatabaseFile.asFile.get() else null

        val sqlDir = dbDir.get().asFile
        val packageName = packageName.get()
        val databaseName = if (swiftProductOutputDir.isPresent) {
            requireValidSwiftIdentifier(dbName.get(), "Swift product databaseName")
        } else {
            dbName.get()
        }
        val oversqliteEnabled = oversqlite.get()
        val resolvedSwiftProductRuntimeMode = if (swiftProductOutputDir.isPresent && swiftProductRuntimeMode.isPresent) {
            SwiftProductRuntimeMode.fromId(swiftProductRuntimeMode.get())
        } else if (oversqliteEnabled) {
            SwiftProductRuntimeMode.SYNC
        } else {
            SwiftProductRuntimeMode.CORE
        }
        val swiftExport = if (swiftOverlayOutputDir.isPresent) {
            SqliteNowSwiftExportConfig(
                swiftOutputDirectory = swiftOverlayOutputDir.get().asFile,
                swiftModuleName = swiftOverlayModuleName.get(),
                frameworkModuleName = swiftFrameworkModuleName.get(),
                bridgePackageName = swiftBridgePackageName.get(),
            )
        } else {
            null
        }
        val swiftProductExport = if (swiftProductOutputDir.isPresent && swiftProductRuntimeModuleName.isPresent) {
            SqliteNowSwiftProductExportConfig(
                swiftOutputDirectory = swiftProductOutputDir.get().asFile,
                swiftModuleName = requireValidSwiftIdentifier(
                    swiftProductModuleName.get(),
                    "Swift product swiftModuleName",
                ),
                runtimeModuleName = requireValidSwiftIdentifier(
                    swiftProductRuntimeModuleName.get(),
                    "Swift product runtimeModuleName",
                ),
                runtimeMode = resolvedSwiftProductRuntimeMode,
            )
        } else if (swiftProductOutputDir.isPresent) {
            SqliteNowSwiftProductExportConfig(
                swiftOutputDirectory = swiftProductOutputDir.get().asFile,
                swiftModuleName = requireValidSwiftIdentifier(
                    swiftProductModuleName.get(),
                    "Swift product swiftModuleName",
                ),
                runtimeMode = resolvedSwiftProductRuntimeMode,
            )
        } else {
            null
        }
        require(swiftExport == null || swiftProductExport == null) {
            "Configure either Swift overlay export or product Swift source export, not both, for '${dbName.get()}'."
        }

        if (oversqliteEnabled && !oversqliteRuntimePresent.get()) {
            error(
                "Database '${dbName.get()}' sets oversqlite=true, but no oversqlite runtime dependency was found. " +
                    "Add implementation(\"dev.goquick.sqlitenow:oversqlite:<version>\") or depend on project(\":library-oversqlite\")."
            )
        }

        val result = compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = databaseName,
                sqlDirectory = sqlDir,
                packageName = packageName,
                outputDirectory = outDir,
                schemaDatabaseFile = dbFile,
                debug = debug.get(),
                oversqlite = oversqliteEnabled,
                swiftExport = swiftExport,
                swiftProductExport = swiftProductExport,
            )
        ) { logger.lifecycle(it) }
        if (swiftProductExport != null && swiftProductMetadataFile.isPresent) {
            writeSwiftProductMetadata(
                metadataFile = swiftProductMetadataFile.get().asFile,
                swiftOutputDirectory = swiftProductExport.swiftOutputDirectory,
                result = result,
            )
        }
        result.warnings.forEach { logger.warn(it) }
    }

    private fun writeSwiftProductMetadata(
        metadataFile: File,
        swiftOutputDirectory: File,
        result: SqliteNowCompilerResult,
    ) {
        metadataFile.parentFile.mkdirs()
        val generatedSwiftFiles = result.generatedFiles
            .filter { it.isFile && it.extension == "swift" }
            .map { it.relativeTo(swiftOutputDirectory).path.replace(File.separatorChar, '/') }
            .sorted()
        val syncTables = result.swiftPackageSyncTables.map { table ->
            mapOf(
                "tableName" to table.tableName,
                "syncKeyColumnName" to table.syncKeyColumnName,
            )
        }
        metadataFile.writeText(
            JsonOutput.prettyPrint(
                JsonOutput.toJson(
                    mapOf(
                        "generatedSwiftFiles" to generatedSwiftFiles,
                        "syncTables" to syncTables,
                    )
                )
            ) + "\n"
        )
    }
}

fun generateDatabaseFiles(
    dbName: String, sqlDir: File, packageName: String, outDir: File, schemaDatabaseFile: File?,
    debug: Boolean,
    oversqlite: Boolean = false,
    warningReporter: (String) -> Unit = {},
) {
    val result = compileSqliteNowDatabase(
        SqliteNowCompilerInput(
            databaseName = dbName,
            sqlDirectory = sqlDir,
            packageName = packageName,
            outputDirectory = outDir,
            schemaDatabaseFile = schemaDatabaseFile,
            debug = debug,
            oversqlite = oversqlite,
        )
    )
    result.warnings.forEach(warningReporter)
}
