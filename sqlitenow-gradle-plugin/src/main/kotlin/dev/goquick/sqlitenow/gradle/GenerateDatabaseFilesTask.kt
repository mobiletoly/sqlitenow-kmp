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
import dev.goquick.sqlitenow.gradle.compiler.compileSqliteNowDatabase
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

    @TaskAction
    fun generate() {
        // 1. Ensure the output directory exists
        val outDir: File = outputDir.asFile.get().apply { mkdirs() }

        val dbDir = sqlDir.dir(dbName.get())
        val dbFile = if (schemaDatabaseFile.isPresent) schemaDatabaseFile.asFile.get() else null

        val sqlDir = dbDir.get().asFile
        val packageName = packageName.get()
        val oversqliteEnabled = oversqlite.get()

        if (oversqliteEnabled && !oversqliteRuntimePresent.get()) {
            error(
                "Database '${dbName.get()}' sets oversqlite=true, but no oversqlite runtime dependency was found. " +
                    "Add implementation(\"dev.goquick.sqlitenow:oversqlite:<version>\") or depend on project(\":library-oversqlite\")."
            )
        }

        val result = compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = dbName.get(),
                sqlDirectory = sqlDir,
                packageName = packageName,
                outputDirectory = outDir,
                schemaDatabaseFile = dbFile,
                debug = debug.get(),
                oversqlite = oversqliteEnabled,
            )
        ) { logger.lifecycle(it) }
        result.warnings.forEach { logger.warn(it) }
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
