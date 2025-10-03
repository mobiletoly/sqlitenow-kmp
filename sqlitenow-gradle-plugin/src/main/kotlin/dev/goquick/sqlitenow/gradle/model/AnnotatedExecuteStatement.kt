package dev.goquick.sqlitenow.gradle.model

import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.extractAnnotations
import dev.goquick.sqlitenow.gradle.sqlinspect.ExecuteStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.InsertStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.DeleteStatement

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
            val statementAnnotations = StatementAnnotationOverrides.Companion.parse(
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
