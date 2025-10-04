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
