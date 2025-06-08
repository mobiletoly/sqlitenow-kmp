package dev.goquick.sqlitenow.gradle.inspect

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
    }
}
