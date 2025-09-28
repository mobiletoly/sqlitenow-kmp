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
    val tableAliases: Map<String, String> = emptyMap(), // alias -> tableName mapping
    val joinConditions: List<JoinCondition> = emptyList()
) : SqlStatement {

    data class JoinCondition(
        val leftTable: String,
        val leftColumn: String,
        val rightTable: String,
        val rightColumn: String
    )

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

            // Extract table aliases (alias -> tableName mapping)
            val tableAliases = mutableMapOf<String, String>()

            // Extract FROM table alias
            select.fromItem?.let { fromItem ->
                if (fromItem is Table) {
                    val tableName = fromItem.nameParts[0]
                    val alias = fromItem.alias?.name ?: tableName
                    tableAliases[alias] = tableName
                }
            }

            // Extract JOIN table aliases and conditions
            val joinConditions = mutableListOf<JoinCondition>()
            select.joins?.forEach { join ->
                join.fromItem?.let { joinItem ->
                    if (joinItem is Table) {
                        val tableName = joinItem.nameParts[0]
                        val alias = joinItem.alias?.name ?: tableName
                        tableAliases[alias] = tableName
                    }
                }

                // Extract JOIN conditions (e.g., p.id = a.person_id)
                join.onExpressions?.forEach { onExpression ->
                    if (onExpression is net.sf.jsqlparser.expression.operators.relational.EqualsTo) {
                        val leftExpr = onExpression.leftExpression
                        val rightExpr = onExpression.rightExpression

                        if (leftExpr is net.sf.jsqlparser.schema.Column && rightExpr is net.sf.jsqlparser.schema.Column) {
                            val leftTable = leftExpr.table?.name ?: ""
                            val leftColumn = leftExpr.columnName
                            val rightTable = rightExpr.table?.name ?: ""
                            val rightColumn = rightExpr.columnName

                            joinConditions.add(JoinCondition(leftTable, leftColumn, rightTable, rightColumn))
                        }
                    }
                }
            }
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
                tableAliases = tableAliases,
                joinConditions = joinConditions
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

                        // First pass: collect all field information
                        val allFieldInfo = mutableListOf<FieldInfo>()
                        for (i in 1..columnCount) {
                            val fieldName = metaData.getColumnLabel(i)
                            val sqlType = metaData.getColumnTypeName(i)
                            val tableName = metaData.getTableName(i)
                            val columnName = metaData.getColumnName(i)
                            val nullable = metaData.isNullable(i)

                            allFieldInfo.add(FieldInfo(fieldName, tableName, columnName, sqlType, nullable))
                        }

                        // Second pass: deduplicate fields that represent the same underlying table.column
                        val deduplicatedFields = deduplicateFields(allFieldInfo)

                        // Third pass: create FieldSource objects
                        deduplicatedFields.forEach { fieldInfo ->
                            val originalColumnName = findFieldOriginalColumn(select, cleanSqliteColumnDisambiguation(fieldInfo.fieldName)) ?: fieldInfo.columnName

                            fieldSources.add(
                                FieldSource(
                                    fieldName = fieldInfo.fieldName,
                                    tableName = fieldInfo.tableName,
                                    originalColumnName = originalColumnName,
                                    dataType = fieldInfo.sqlType
                                )
                            )
                        }
                    }
                }
            } catch (e: Exception) {
                e.printStackTrace()
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

        private data class FieldInfo(
            val fieldName: String,
            val tableName: String,
            val columnName: String,
            val sqlType: String,
            val nullable: Int
        )

        private fun deduplicateFields(allFields: List<FieldInfo>): List<FieldInfo> {
            // Group fields by their underlying table.column (after cleaning SQLite disambiguation)
            val fieldGroups = allFields.groupBy { field ->
                val cleanFieldName = cleanSqliteColumnDisambiguation(field.fieldName)
                val cleanColumnName = cleanSqliteColumnDisambiguation(field.columnName)
                val tableColumnKey = "${field.tableName}.$cleanColumnName"
                tableColumnKey
            }

            val result = mutableListOf<FieldInfo>()
            fieldGroups.forEach { (tableColumn, fieldsInGroup) ->
                if (fieldsInGroup.size == 1) {
                    // Only one field for this table.column, keep as-is
                    result.add(fieldsInGroup.first())
                } else {
                    // Multiple fields for same table.column, deduplicate to the first one without SQLite disambiguation
                    val fieldWithoutDisambiguation = fieldsInGroup.find { field ->
                        !field.fieldName.contains(':') && !field.columnName.contains(':')
                    }

                    if (fieldWithoutDisambiguation != null) {
                        // Use the field without SQLite disambiguation
                        result.add(fieldWithoutDisambiguation)
                    } else {
                        // All fields have disambiguation, keep the first one but clean it
                        val firstField = fieldsInGroup.first()
                        val cleanedField = FieldInfo(
                            fieldName = cleanSqliteColumnDisambiguation(firstField.fieldName),
                            tableName = firstField.tableName,
                            columnName = cleanSqliteColumnDisambiguation(firstField.columnName),
                            sqlType = firstField.sqlType,
                            nullable = firstField.nullable
                        )
                        result.add(cleanedField)
                        val removedFields = fieldsInGroup.drop(1)
                    }
                }
            }
            return result
        }

        private fun cleanSqliteColumnDisambiguation(columnName: String): String {
            // SQLite adds suffixes in the format ":number" for duplicate columns
            val colonIndex = columnName.lastIndexOf(':')
            if (colonIndex > 0) {
                val suffix = columnName.substring(colonIndex + 1)
                // Check if the suffix is a number (SQLite's disambiguation pattern)
                if (suffix.all { it.isDigit() }) {
                    return columnName.substring(0, colonIndex)
                }
            }
            return columnName
        }

        private fun rewriteLimitOffset(sql: String): String =
            // Add LIMIT 0 to avoid retrieving actual data for SQL metadata extraction
            listOf(
                " LIMIT ?" to " LIMIT 0",
                " OFFSET ?" to " OFFSET 0"
            ).fold(sql) { acc, (find, replace) ->
                if (acc.contains(find, ignoreCase = true)) {
                    acc.replace(find, replace, ignoreCase = true)
                } else {
                    acc
                }
            }
    }
}
