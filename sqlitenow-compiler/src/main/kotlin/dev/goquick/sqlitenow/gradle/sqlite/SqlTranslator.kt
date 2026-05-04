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
package dev.goquick.sqlitenow.gradle.sqlite

/**
 * Translates a SQL statement into a Kotlin string with margin prefix.
 * This formats the SQL statement to be used in a Kotlin multi-line string with trimMargin().
 *
 * @param sql The SQL statement to translate
 * @return The SQL statement formatted as a Kotlin string with margin prefix
 */
fun translateSqliteStatementToKotlin(sql: String): String {
    // Normalize line endings to \n
    val normalizedSql = sql.replace("\r\n", "\n").replace("\r", "\n")

    // Split the SQL into lines
    val lines = normalizedSql.split("\n")

    // Process each line: trim whitespace and add margin prefix
    val formattedSql = lines
        .map { it.trim() }
        .filter { it.isNotEmpty() }
        .joinToString("\n|", "|")

    return formattedSql
}
