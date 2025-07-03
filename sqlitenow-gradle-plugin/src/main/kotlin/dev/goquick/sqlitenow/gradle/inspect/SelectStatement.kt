package dev.goquick.sqlitenow.gradle.inspect

import java.sql.Connection
import net.sf.jsqlparser.expression.JdbcNamedParameter
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.schema.Table
import net.sf.jsqlparser.statement.select.PlainSelect

class SelectStatement(
    override val sql: String,
    val fromTable: String?,
    val joinTables: List<String>,
    val fields: List<FieldSource>,
    override val namedParameters: List<String>,
    val namedParametersToColumns: Map<String, AssociatedColumn>,
    val offsetNamedParam: String?,
    val limitNamedParam: String?,
    val parameterCastTypes: Map<String, String> = emptyMap(),
) : SqlStatement {
    data class FieldSource(
        val fieldName: String,
        val tableName: String,
        val originalColumnName: String,
        val dataType: String
    )

    companion object {
        fun parse(conn: Connection, select: PlainSelect): SelectStatement {
            val processor = NamedParametersProcessor(stmt = select)
            val fromTable = select.fromItem?.let {
                if (it is Table) {
                    it.nameParts[0]
                } else {
                    throw IllegalArgumentException("Unsupported item type (FROM): ${it.javaClass.name}")
                }
            }
            val joinTables = select.joins?.map { join ->
                join.fromItem.let {
                    if (it is Table) {
                        it.nameParts[0]
                    } else {
                        throw IllegalArgumentException("Unsupported item type (in JOIN): ${it.javaClass.name}")
                    }
                }
            } ?: emptyList()
            val fields = extractSelectFieldInfo(
                conn,
                select,
                processor.processedSql
            )

            // Prepare a mutable list to collect our results
            val namedParamsWithColumns = select.where?.collectNamedParametersAssociatedWithColumns()
                ?: emptyMap()

            val offsetNamedParam: String? = (select.offset?.offset as? JdbcNamedParameter)?.name
            val limitNamedParam: String? = (select.limit?.rowCount as? JdbcNamedParameter)?.name

            return SelectStatement(
                sql = processor.processedSql,
                fromTable = fromTable,
                joinTables = joinTables,
                fields = fields,
                namedParameters = processor.parameters,
                namedParametersToColumns = namedParamsWithColumns,
                offsetNamedParam = offsetNamedParam,
                limitNamedParam = limitNamedParam,
                parameterCastTypes = processor.parameterCastTypes,
            )
        }

        /**
         * Extracts field sources from a SELECT statement by executing the query with LIMIT 0
         * and examining the result set metadata.
         *
         * @param sql The SELECT statement
         * @return A list of field sources
         */
        private fun extractSelectFieldInfo(conn: Connection, select: PlainSelect, sql: String): List<FieldSource> {
            val fieldSources = mutableListOf<FieldSource>()

            try {
                val limitedSql = rewriteLimitOffset(sql)

                conn.prepareStatement(limitedSql)?.use { stmt ->
                    stmt.executeQuery().use { rs ->
                        val metaData = rs.metaData
                        val columnCount = metaData.columnCount

                        for (i in 1..columnCount) {
                            val fieldName = metaData.getColumnLabel(i)
                            val sqlType = metaData.getColumnTypeName(i)
                            val tableName = metaData.getTableName(i)
                            val columnName = metaData.getColumnName(i)

                            val originalColumnName = findFieldOriginalColumn(select, fieldName) ?: columnName

                            fieldSources.add(
                                FieldSource(
                                    fieldName = fieldName,
                                    tableName = tableName,
                                    originalColumnName = originalColumnName,
                                    dataType = sqlType
                                )
                            )
                        }
                    }
                }
            } catch (e: Exception) {
//                throw e
                throw RuntimeException("Failed to extract field sources from SELECT statement: $sql", e)
            }

            return fieldSources
        }

        /**
         * Find the field source for a given field name/alias.
         *
         * @param fieldName The name of the field
         * @return The field source, or null if not found
         */
        private fun findFieldOriginalColumn(select: PlainSelect, fieldName: String): String? {
            // Inspect select statement
            select.selectItems.forEach { stmt ->
                val aliasName = stmt.alias?.name
                val name = stmt.expression
                if (aliasName == fieldName && name is Column) {
                    return name.columnName
                }
            }
            return null
        }

        private fun rewriteLimitOffset(sql: String): String =
            // Add LIMIT 0 to avoid retrieving actual data for SQL metadata extraction
            listOf(
                " LIMIT ?"  to " LIMIT 0",
                " OFFSET ?" to " OFFSET 0"
            ).fold(sql) { acc, (find, replace) ->
                if (acc.contains(find, ignoreCase = true)) {
                    acc.replace(find, replace, ignoreCase = true)
                }  else {
                    acc
                }
            }
    }
}
