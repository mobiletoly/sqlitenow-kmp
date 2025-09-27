package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.ExecuteStatement
import dev.goquick.sqlitenow.gradle.inspect.InsertStatement

data class AnnotatedExecuteStatement(
    override val name: String,
    val src: ExecuteStatement,
    override val annotations: StatementAnnotationOverrides,
) : AnnotatedStatement {

    /**
     * Returns true if this is an INSERT statement with a RETURNING clause
     */
    fun hasReturningClause(): Boolean {
        return src is InsertStatement && src.hasReturningClause
    }

    /**
     * Returns the list of columns in the RETURNING clause, or empty list if none
     */
    fun getReturningColumns(): List<String> {
        return if (src is InsertStatement) src.returningColumns else emptyList()
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
