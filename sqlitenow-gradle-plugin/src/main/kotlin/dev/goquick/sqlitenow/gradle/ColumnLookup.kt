package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.*

/**
 * Centralized logic for finding columns associated with parameters
 * across different statement types (SELECT, INSERT, DELETE).
 */
class ColumnLookup(
    private val createTableStatements: List<AnnotatedCreateTableStatement>,
    private val createViewStatements: List<AnnotatedCreateViewStatement>,
) {

    fun findTableByName(tableName: String): AnnotatedCreateTableStatement? {
        return createTableStatements.find {
            it.src.tableName.equals(tableName, ignoreCase = true)
        }
    }

    fun findViewByName(viewName: String): AnnotatedCreateViewStatement? {
        return createViewStatements.find {
            it.src.viewName.equals(viewName, ignoreCase = true)
        }
    }

    fun findColumnForParameter(
        statement: AnnotatedStatement,
        paramName: String
    ): AnnotatedCreateTableStatement.Column? {
        return when (statement) {
            is AnnotatedSelectStatement -> findColumnForSelectParameter(statement, paramName)
            is AnnotatedExecuteStatement -> findColumnForExecuteParameter(statement, paramName)
            else -> null
        }
    }

    /**
     * Helper function to find the column for a SELECT statement parameter.
     */
    private fun findColumnForSelectParameter(
        statement: AnnotatedSelectStatement,
        paramName: String
    ): AnnotatedCreateTableStatement.Column? {
        val tableName = statement.src.fromTable ?: return null

        // First try to find as a table
        val table = findTableByName(tableName)
        if (table != null) {
            val associatedColumn = statement.src.namedParametersToColumns[paramName]
            if (associatedColumn != null) {
                return table.findColumnByName(associatedColumn.columnName)
            }

            return table.findColumnByName(paramName) ?: table.columns.find { col ->
                statement.annotations.propertyNameGenerator.convertToPropertyName(col.src.name)
                    .equals(paramName, ignoreCase = true)
            }
        }

        // If not found as a table, try to find as a view
        val view = findViewByName(tableName)
        if (view != null) {
            return findColumnForViewParameter(view, statement, paramName)
        }

        return null
    }

    /**
     * Find the column for an EXECUTE statement (INSERT/DELETE/UPDATE) parameter.
     * Returns the column if found, null otherwise.
     */
    private fun findColumnForExecuteParameter(
        statement: AnnotatedExecuteStatement,
        paramName: String
    ): AnnotatedCreateTableStatement.Column? {
        val tableName = when (val src = statement.src) {
            is InsertStatement -> src.table
            is DeleteStatement -> src.table
            is UpdateStatement -> src.table
            else -> return null
        }

        val table = findTableByName(tableName) ?: return null

        fun findColumnInWithClause(stmts: List<SelectStatement>): AnnotatedCreateTableStatement.Column? {
            for (withSelectStatement in stmts) {
                if (paramName in withSelectStatement.namedParameters) {
                    val associatedColumn = withSelectStatement.namedParametersToColumns[paramName]
                    if (associatedColumn != null) {
                        val fromTable = withSelectStatement.fromTable
                        if (fromTable != null) {
                            val withTable = findTableByName(fromTable) ?: continue
                            return withTable.findColumnByName(associatedColumn.columnName)
                        }
                    }
                }
            }
            return null
        }

        return when (val src = statement.src) {
            is InsertStatement -> {
                val columnName = src.columnNamesAssociatedWithNamedParameters[paramName]

                if (columnName != null) {
                    return table.findColumnByName(columnName)
                }

                return findColumnInWithClause(src.withSelectStatements)
            }

            is DeleteStatement -> {
                val associatedColumn = src.namedParametersToColumns[paramName]
                if (associatedColumn != null) {
                    return table.findColumnByName(associatedColumn.columnName)
                }

                return findColumnInWithClause(src.withSelectStatements)
            }

            is UpdateStatement -> {
                // First check if this parameter is from a SET clause (has direct column mapping)
                val directColumnName = src.namedParametersToColumnNames[paramName]
                if (directColumnName != null) {
                    return table.findColumnByName(directColumnName)
                }

                // If not from SET clause, check WHERE clause parameters
                val associatedColumn = src.namedParametersToColumns[paramName]
                if (associatedColumn != null) {
                    return table.findColumnByName(associatedColumn.columnName)
                }

                return findColumnInWithClause(src.withSelectStatements)
            }

            else -> null
        }
    }

    /** Find a column in a table using both direct name match and property name conversion. */
    private fun findColumnInTable(
        table: AnnotatedCreateTableStatement,
        columnName: String,
        paramName: String,
        statement: AnnotatedSelectStatement
    ): AnnotatedCreateTableStatement.Column? {
        // First try direct column name match
        val column = table.findColumnByName(columnName)
        if (column != null) {
            return column
        }

        // Then try property name conversion
        return table.columns.find { col ->
            statement.annotations.propertyNameGenerator.convertToPropertyName(col.src.name)
                .equals(paramName, ignoreCase = true)
        }
    }

    /**
     * Find the column for a VIEW-based SELECT statement parameter.
     * Looks up the column in the underlying tables that the VIEW references and merges
     * any field annotations from the VIEW definition.
     */
    private fun findColumnForViewParameter(
        view: AnnotatedCreateViewStatement,
        statement: AnnotatedSelectStatement,
        paramName: String
    ): AnnotatedCreateTableStatement.Column? {
        val associatedColumn = statement.src.namedParametersToColumns[paramName]
        val columnName = associatedColumn?.columnName ?: paramName

        val viewSelectStatement = view.src.selectStatement

        // Look for the column in the tables referenced by the VIEW
        val fromTable = viewSelectStatement.fromTable
        if (fromTable != null) {
            val table = findTableByName(fromTable)
            if (table != null) {
                val column = findColumnInTable(table, columnName, paramName, statement)
                if (column != null) {
                    return column
                }
            }
        }

        // Also check joined tables in the VIEW
        for (joinTableName in viewSelectStatement.joinTables) {
            val joinTable = findTableByName(joinTableName)
            if (joinTable != null) {
                val column = findColumnInTable(joinTable, columnName, paramName, statement)
                if (column != null) {
                    return column
                }
            }
        }

        return null
    }


    /** Checks if a parameter's corresponding column is nullable. */
    fun isParameterNullable(statement: AnnotatedStatement, paramName: String): Boolean {
        val column = findColumnForParameter(statement, paramName) ?: return false
        return column.isNullable()
    }
}
