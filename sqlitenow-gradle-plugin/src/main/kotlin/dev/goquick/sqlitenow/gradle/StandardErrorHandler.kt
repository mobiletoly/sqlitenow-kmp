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
package dev.goquick.sqlitenow.gradle

/**
 * Standardized error handling utilities.
 * Provides consistent error handling patterns across the codebase.
 */
import java.util.Locale

object StandardErrorHandler {

    /**
     * Handles SQL parsing errors with consistent logging and re-throwing.
     */
    fun handleSqlParsingError(sql: String, error: Exception, context: String = ""): Nothing {
        val contextInfo = if (context.isNotEmpty()) " in $context" else ""
        logger.error("Failed to parse SQL statement$contextInfo: ${error.message}")
        logger.error("SQL statement:\n${formatSqlWithLineNumbers(sql)}")
        throw RuntimeException("SQL parsing failed$contextInfo", error)
    }

    /**
     * Handles SQL execution errors with consistent logging and re-throwing.
     */
    fun handleSqlExecutionError(sql: String, error: Throwable, context: String = ""): Nothing {
        val contextInfo = if (context.isNotEmpty()) " in $context" else ""
        logger.error("Failed to execute SQL statement$contextInfo: ${error.message}")
        logger.error("SQL statement:\n${formatSqlWithLineNumbers(sql)}")
        throw RuntimeException("SQL execution failed$contextInfo", error)
    }

    private fun formatSqlWithLineNumbers(sql: String): String {
        val lines = sql.lines()
        if (lines.isEmpty()) return "<empty SQL>"
        val width = lines.size.toString().length
        return lines.mapIndexed { index, line ->
            val lineNumber = String.format(Locale.US, "%${width}d", index + 1)
            "$lineNumber | $line"
        }.joinToString(separator = "\n")
    }
}
