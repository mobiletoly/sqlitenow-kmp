package dev.goquick.sqlitenow.gradle.sqlinspect

import dev.goquick.sqlitenow.gradle.sqlinspect.ExecuteStatement.Companion.buildReturningColumns
import dev.goquick.sqlitenow.gradle.sqlinspect.ExecuteStatement.Companion.buildSelectStatementFromWithItemsList
import java.sql.Connection
import net.sf.jsqlparser.expression.JdbcNamedParameter
import net.sf.jsqlparser.statement.update.Update

class UpdateStatement(
    override val sql: String,
    override val table: String,
    override val namedParameters: List<String>,
    val namedParametersToColumns: Map<String, AssociatedColumn>,
    val namedParametersToColumnNames: Map<String, String>,
    override val withSelectStatements: List<SelectStatement>,
    override val parameterCastTypes: Map<String, String> = emptyMap(),
    override val hasReturningClause: Boolean,
    override val returningColumns: List<String>
) : ExecuteStatement {

    companion object {
        fun parse(update: Update, conn: Connection): UpdateStatement {
            // Extract named parameters from WHERE clause (similar to DELETE)
            val namedParamsWithColumns = update.where?.collectNamedParametersAssociatedWithColumns()
                ?: emptyMap()

            val table = update.table.let {
                it.nameParts[0]
            }

            // Handle WITH clauses (similar to INSERT and DELETE)
            val withItemsList = update.withItemsList
            val withSelectStatements = buildSelectStatementFromWithItemsList(conn, withItemsList)
            val columnNamesAssociatedWithNamedParameters = update.updateSets
                ?.mapNotNull {
                    val column = it.columns.firstOrNull()
                    if (column == null) return@mapNotNull null
                    val expr = it.values.first() as JdbcNamedParameter
                    expr.name to column.columnName
                }
                ?.toMap() ?: emptyMap()

            val returningClause = update.returningClause
            val hasReturningClause = returningClause != null
            val returningColumns = buildReturningColumns(returningClause)

            val processor = UpdateParametersProcessor(stmt = update)
            return UpdateStatement(
                sql = processor.processedSql,
                table = table,
                namedParameters = processor.parameters,
                namedParametersToColumns = namedParamsWithColumns,
                withSelectStatements = withSelectStatements,
                namedParametersToColumnNames = columnNamesAssociatedWithNamedParameters,
                parameterCastTypes = processor.parameterCastTypes,
                hasReturningClause = hasReturningClause,
                returningColumns = returningColumns,
            )
        }
    }
}
