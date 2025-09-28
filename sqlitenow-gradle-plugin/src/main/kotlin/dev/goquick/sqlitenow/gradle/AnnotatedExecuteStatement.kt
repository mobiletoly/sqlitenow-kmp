package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.ExecuteStatement
import dev.goquick.sqlitenow.gradle.inspect.InsertStatement
import dev.goquick.sqlitenow.gradle.inspect.UpdateStatement
import dev.goquick.sqlitenow.gradle.inspect.DeleteStatement

data class AnnotatedExecuteStatement(
    override val name: String,
    val src: ExecuteStatement,
    override val annotations: StatementAnnotationOverrides,
) : AnnotatedStatement {

    /**
     * Returns true if this is an INSERT, UPDATE, or DELETE statement with a RETURNING clause
     */
    fun hasReturningClause(): Boolean {
        return when (src) {
            is InsertStatement -> src.hasReturningClause
            is UpdateStatement -> src.hasReturningClause
            is DeleteStatement -> src.hasReturningClause
            else -> false
        }
    }

    /**
     * Returns the list of columns in the RETURNING clause, or empty list if none
     */
    fun getReturningColumns(): List<String> {
        return when (src) {
            is InsertStatement -> src.returningColumns
            is UpdateStatement -> src.returningColumns
            is DeleteStatement -> src.returningColumns
            else -> emptyList()
        }
    }

    companion object {
        fun parse(
            name: String,
            execStatement: ExecuteStatement,
            topComments: List<String>,
        ): AnnotatedExecuteStatement {
            val statementAnnotations = StatementAnnotationOverrides.parse(
                extractAnnotations(topComments)
            )
            return AnnotatedExecuteStatement(
                name = name,
                src = execStatement,
                annotations = statementAnnotations,
            )
        }
    }
}
