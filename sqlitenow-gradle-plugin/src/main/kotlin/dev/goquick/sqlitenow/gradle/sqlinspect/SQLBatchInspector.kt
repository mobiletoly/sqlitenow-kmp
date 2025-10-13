/*
 * Copyright 2025 Anatoliy Pochkin
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
package dev.goquick.sqlitenow.gradle.sqlinspect

import dev.goquick.sqlitenow.gradle.SqlFileProcessor
import dev.goquick.sqlitenow.gradle.sqlite.SqlSingleStatement
import java.io.File

/**
 * Inspects and collects SQL statements from all .sql files in a directory.
 * This class is similar to SchemaInspector but with simplified functionality
 * that only collects SQL statements without any inspection or analysis.
 */
internal class SQLBatchInspector(
    sqlDirectory: File,
    mandatory: Boolean,
) {
    val sqlStatements: List<SqlSingleStatement>
    val sqlFiles: List<File>

    init {
        SqlFileProcessor.validateDirectory(sqlDirectory, "SQL", mandatory = mandatory)
        sqlFiles = SqlFileProcessor.findSqlFiles(sqlDirectory)
        sqlStatements = SqlFileProcessor.parseAllSqlFiles(sqlFiles)
    }
}