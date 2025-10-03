package dev.goquick.sqlitenow.gradle.sqlinspect

import net.sf.jsqlparser.statement.create.view.CreateView
import java.sql.Connection

/**
 * Represents a CREATE VIEW statement.
 * Views are treated similarly to tables but represent stored queries rather than physical tables.
 */
data class CreateViewStatement(
    override val sql: String,
    val viewName: String,
    val selectStatement: SelectStatement,
    val columnNames: List<String>? = null,
) : SqlStatement {
    override val namedParameters: List<String>
        get() = selectStatement.namedParameters

    companion object {
        fun parse(sql: String, createView: CreateView, conn: Connection): CreateViewStatement {
            val selectStatement = SelectStatement.parse(
                conn = conn,
                select = createView.select.plainSelect
            )

            val columnNames = createView.columnNames?.map { it.columnName }

            return CreateViewStatement(
                sql = sql,
                viewName = createView.view.name,
                selectStatement = selectStatement,
                columnNames = columnNames
            )
        }
    }
}
