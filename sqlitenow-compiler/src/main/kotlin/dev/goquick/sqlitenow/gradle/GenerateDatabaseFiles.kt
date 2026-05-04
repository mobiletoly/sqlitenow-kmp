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

fun generateDatabaseFiles(
    dbName: String,
    sqlDir: File,
    packageName: String,
    outDir: File,
    schemaDatabaseFile: File?,
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
