package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.*

/**
 * Utility class for common statement operations.
 */
object StatementUtils {

    /**
     * Extracts named parameters from any type of statement.
     */
    fun getNamedParameters(statement: AnnotatedStatement): List<String> {
        return when (statement) {
            is AnnotatedSelectStatement -> statement.src.namedParameters
            is AnnotatedExecuteStatement -> statement.src.namedParameters
            is AnnotatedCreateTableStatement -> statement.src.namedParameters
            is AnnotatedCreateViewStatement -> statement.src.namedParameters
        }
    }

    /**
     * Extracts all named parameters including those from WITH clauses.
     * This is used for comprehensive parameter collection in data class generation.
     */
    fun getAllNamedParameters(statement: AnnotatedStatement): Set<String> {
        val allNamedParameters = mutableSetOf<String>()

        // Add basic named parameters
        allNamedParameters.addAll(getNamedParameters(statement))

        // Add parameters from WITH clauses for execute statements
        if (statement is AnnotatedExecuteStatement) {
            when (val src = statement.src) {
                is InsertStatement -> {
                    src.withSelectStatements.forEach { withSelectStatement ->
                        allNamedParameters.addAll(withSelectStatement.namedParameters)
                    }
                }
                is DeleteStatement -> {
                    src.withSelectStatements.forEach { withSelectStatement ->
                        allNamedParameters.addAll(withSelectStatement.namedParameters)
                    }
                }
                is UpdateStatement -> {
                    src.withSelectStatements.forEach { withSelectStatement ->
                        allNamedParameters.addAll(withSelectStatement.namedParameters)
                    }
                }
            }
        }

        return allNamedParameters
    }
}
