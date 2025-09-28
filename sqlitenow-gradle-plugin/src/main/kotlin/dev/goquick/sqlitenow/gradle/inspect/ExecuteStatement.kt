package dev.goquick.sqlitenow.gradle.inspect

import net.sf.jsqlparser.statement.ReturningClause
import net.sf.jsqlparser.statement.select.SelectItem
import java.sql.Connection
import net.sf.jsqlparser.statement.select.WithItem

/**
 * Base interface for EXECUTE statements (INSERT/UPDATE/DELETE).
 */
interface ExecuteStatement : SqlStatement {
    override val sql: String
    val table: String
    override val namedParameters: List<String>
    val withSelectStatements: List<SelectStatement>
    val parameterCastTypes: Map<String, String>

    companion object {
        fun buildSelectStatementFromWithItemsList(
            conn: Connection,
            withItemsList: List<WithItem>?,
        ): List<SelectStatement> {
            val withSelectStatements = withItemsList
                ?.map { withItem ->
                    val select = withItem.select.plainSelect
                    val stmt = SelectStatement.parse(
                        conn = conn,
                        select = select,
                    )
                    stmt
                }
                ?: emptyList()
            return withSelectStatements
        }

        fun buildReturningColumns(returningClause: ReturningClause?): List<String> {
            val hasReturningClause = returningClause != null
            val returningColumns = if (hasReturningClause) {
                returningClause.map { selectItem ->
                    if (selectItem.alias != null) {
                        throw IllegalArgumentException("RETURNING clause with aliases is currently not supported: $selectItem")
                    }
                    val node = selectItem.astNode
                    if (node.jjtGetFirstToken().toString() != node.jjtGetLastToken().toString()) {
                        throw IllegalArgumentException("RETURNING clause with expressions is currently not supported: $selectItem")
                    }
                    selectItem.toString()
                }
            } else {
                emptyList()
            }
            return returningColumns
        }
    }
}
