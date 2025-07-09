package dev.goquick.sqlitenow.gradle

/**
 * Standardized error handling utilities.
 * Provides consistent error handling patterns across the codebase.
 */
object StandardErrorHandler {

    /**
     * Handles SQL parsing errors with consistent logging and re-throwing.
     */
    fun handleSqlParsingError(sql: String, error: Exception, context: String = ""): Nothing {
        val contextInfo = if (context.isNotEmpty()) " in $context" else ""
        System.err.println("Failed to parse SQL statement$contextInfo: ${error.message}")
        System.err.println("SQL statement: $sql")
        throw RuntimeException("SQL parsing failed$contextInfo", error)
    }

    /**
     * Handles SQL execution errors with consistent logging and re-throwing.
     */
    fun handleSqlExecutionError(sql: String, error: Throwable, context: String = ""): Nothing {
        val contextInfo = if (context.isNotEmpty()) " in $context" else ""
        System.err.println("Failed to execute SQL statement$contextInfo: ${error.message}")
        System.err.println("SQL statement: $sql")
        throw RuntimeException("SQL execution failed$contextInfo", error)
    }

    /**
     * Handles code generation errors with consistent logging.
     */
    fun handleCodeGenerationError(namespace: String, error: Exception, generationType: String = "code"): Nothing {
        println("Failed to generate $generationType for namespace '$namespace': ${error.message}")
        throw RuntimeException("$generationType generation failed for namespace '$namespace'", error)
    }
}
