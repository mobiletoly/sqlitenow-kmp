package dev.goquick.sqlitenow.gradle.inspect

import dev.goquick.sqlitenow.gradle.inspect.ExecuteStatement.Companion.buildReturningColumns
import dev.goquick.sqlitenow.gradle.inspect.ExecuteStatement.Companion.buildSelectStatementFromWithItemsList
import java.sql.Connection
import net.sf.jsqlparser.statement.delete.Delete

class DeleteStatement(
    override val sql: String,
    override val table: String,
    override val namedParameters: List<String>,
    val namedParametersToColumns: Map<String, AssociatedColumn>,
    override val withSelectStatements: List<SelectStatement>,
    override val parameterCastTypes: Map<String, String> = emptyMap(),
    val hasReturningClause: Boolean = false,
    val returningColumns: List<String> = emptyList()
) : ExecuteStatement {

    companion object {
        fun parse(delete: Delete, conn: Connection): DeleteStatement {
            val namedParamsWithColumns = delete.where?.collectNamedParametersAssociatedWithColumns()
                ?: emptyMap()

            val table = delete.table.let {
                it.nameParts[0]
            }
            val withItemsList = delete.withItemsList
            val withSelectStatements = buildSelectStatementFromWithItemsList(conn, withItemsList)


            // Check for RETURNING clause
            val returningClause = delete.returningClause
            val hasReturningClause = returningClause != null
            val returningColumns = buildReturningColumns(returningClause)

            val processor = DeleteParametersProcessor(stmt = delete)
            return DeleteStatement(
                sql = processor.processedSql,
                table = table,
                namedParameters = processor.parameters,
                namedParametersToColumns = namedParamsWithColumns,
                withSelectStatements = withSelectStatements,
                parameterCastTypes = processor.parameterCastTypes,
                hasReturningClause = hasReturningClause,
                returningColumns = returningColumns,
            )
        }
    }
}
