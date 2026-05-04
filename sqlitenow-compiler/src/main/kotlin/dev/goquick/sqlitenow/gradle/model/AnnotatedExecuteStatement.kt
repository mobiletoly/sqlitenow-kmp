package dev.goquick.sqlitenow.gradle.model

import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationContext
import dev.goquick.sqlitenow.gradle.processing.extractAnnotations
import dev.goquick.sqlitenow.gradle.sqlinspect.ExecuteStatement

data class AnnotatedExecuteStatement(
    override val name: String,
    val src: ExecuteStatement,
    override val annotations: StatementAnnotationOverrides,
) : AnnotatedStatement {

    /**
     * Returns true if this is an INSERT, UPDATE, or DELETE statement with a RETURNING clause
     */
    fun hasReturningClause(): Boolean = src.hasReturningClause

    /**
     * Returns the list of columns in the RETURNING clause, or empty list if none
     */
    fun getReturningColumns(): List<String> = src.returningColumns

    companion object {
        fun parse(
            name: String,
            execStatement: ExecuteStatement,
            topComments: List<String>,
        ): AnnotatedExecuteStatement {
            val statementAnnotations = StatementAnnotationOverrides.Companion.parse(
                extractAnnotations(topComments),
                context = StatementAnnotationContext.EXECUTE
            )
            return AnnotatedExecuteStatement(
                name = name,
                src = execStatement,
                annotations = statementAnnotations,
            )
        }
    }
}
