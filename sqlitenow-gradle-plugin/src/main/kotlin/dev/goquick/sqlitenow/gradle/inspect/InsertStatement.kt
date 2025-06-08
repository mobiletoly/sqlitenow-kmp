package dev.goquick.sqlitenow.gradle.inspect

import dev.goquick.sqlitenow.gradle.inspect.ExecuteStatement.Companion.buildSelectStatementFromWithItemsList
import java.sql.Connection
import net.sf.jsqlparser.expression.JdbcNamedParameter
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.select.PlainSelect

class InsertStatement(
    override val sql: String,
    override val table: String,
    override val namedParameters: List<String>,
    val columnNamesAssociatedWithNamedParameters: Map<String, String>,
    override val withSelectStatements: List<SelectStatement>
) : ExecuteStatement {

    companion object {
        fun parse(insert: Insert, conn: Connection): InsertStatement {
            val table = insert.table.let {
                it.nameParts[0]
            }
            val withItemsList = insert.withItemsList
            val withSelectStatements = buildSelectStatementFromWithItemsList(conn, withItemsList)
            val allColumnNames = insert.columns?.map { it.columnName } ?: emptyList()
            val values = insert.select?.values?.expressions
            val requiredColumnNames = mutableListOf<String>()
            val columnNamesAssociatedWithNamedParameters = linkedMapOf<String, String>()
            values?.forEachIndexed { index, expr ->
                if (expr is JdbcNamedParameter) {
                    val columnName = allColumnNames[index]
                    requiredColumnNames.add(columnName)
                    columnNamesAssociatedWithNamedParameters[columnName] = expr.name.removePrefix(":")
                }
            }

            val processor = NamedParametersProcessor(stmt = insert)
            return InsertStatement(
                sql = processor.processedSql,
                table = table,
                namedParameters = processor.parameters,
                columnNamesAssociatedWithNamedParameters = columnNamesAssociatedWithNamedParameters,
                withSelectStatements = withSelectStatements,
            )
        }
    }
}
