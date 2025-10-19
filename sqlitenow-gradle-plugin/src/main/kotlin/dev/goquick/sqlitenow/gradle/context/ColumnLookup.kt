/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.gradle.context

import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.sqlinspect.DeleteStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.InsertStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateStatement
import java.util.LinkedHashSet

/**
 * Centralized logic for finding columns associated with parameters
 * across different statement types (SELECT, INSERT, DELETE).
 */
class ColumnLookup(
    createTableStatements: List<AnnotatedCreateTableStatement>,
    createViewStatements: List<AnnotatedCreateViewStatement>,
) {

    private val tableLookup: Map<String, AnnotatedCreateTableStatement> =
        createTableStatements.associateBy { it.src.tableName.lowercase() }
    private val viewLookup: Map<String, AnnotatedCreateViewStatement> =
        createViewStatements.associateBy { it.src.viewName.lowercase() }

    fun findTableByName(tableName: String): AnnotatedCreateTableStatement? {
        return tableLookup[tableName.lowercase()]
    }

    fun findViewByName(viewName: String): AnnotatedCreateViewStatement? {
        return viewLookup[viewName.lowercase()]
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
     * Resolve the originating CREATE TABLE column for a SELECT field, traversing view expansions
     * and alias prefixes when necessary.
     */
    fun findColumnForSelectField(
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        aliasPrefixes: List<String> = emptyList(),
    ): AnnotatedCreateTableStatement.Column? {
        val candidates = LinkedHashSet<String>()
        fun addCandidate(name: String?) {
            if (name.isNullOrBlank()) return
            addNameVariants(name, candidates)
        }

        addCandidate(field.src.originalColumnName)
        addCandidate(field.src.fieldName)
        addCandidate(field.annotations.propertyName)

        val propertyNameGenerator: PropertyNameGeneratorType = statement.annotations.propertyNameGenerator
        val inferredPropertyName = propertyNameGenerator.convertToPropertyName(field.src.fieldName)
        addCandidate(inferredPropertyName)

        field.annotations.aliasPrefix?.takeIf { it.isNotBlank() }?.let { prefix ->
            addCandidate(field.src.fieldName.removePrefixIfMatches(prefix))
            addCandidate(field.src.originalColumnName.removePrefixIfMatches(prefix))
        }

        aliasPrefixes.forEach { prefix ->
            addCandidate(field.src.fieldName.removePrefixIfMatches(prefix))
            addCandidate(field.src.originalColumnName.removePrefixIfMatches(prefix))
        }

        val propertyNameHint = field.annotations.propertyName?.takeIf { it.isNotBlank() } ?: inferredPropertyName

        fun lookupInTable(tableName: String?): AnnotatedCreateTableStatement.Column? {
            if (tableName.isNullOrBlank()) return null
            val table = findTableByName(tableName) ?: return null
            candidates.forEach { candidate ->
                findColumnInTable(table, candidate, propertyNameHint, statement)?.let { return it }
            }
            return null
        }

        fun resolveIn(name: String?): AnnotatedCreateTableStatement.Column? {
            if (name.isNullOrBlank()) return null
            lookupInTable(name)?.let { return it }

            val view = findViewByName(name) ?: return null
            candidates.forEach { candidate ->
                resolveColumnFromView(
                    view = view,
                    fieldAlias = candidate,
                    paramName = propertyNameHint,
                    statement = statement,
                    visitedViews = mutableSetOf()
                )?.let { return it }
            }
            return null
        }

        field.aliasPath.reversed().forEach { alias ->
            resolveIn(statement.src.tableAliases[alias] ?: alias)?.let { return it }
        }

        field.src.tableName.takeIf { it.isNotBlank() }?.let { alias ->
            resolveIn(statement.src.tableAliases[alias] ?: alias)?.let { return it }
        }

        resolveIn(statement.src.fromTable)?.let { return it }

        statement.src.tableAliases.values.forEach { tableOrView ->
            resolveIn(tableOrView)?.let { return it }
        }

        tableLookup.values.forEach { table ->
            candidates.forEach { candidate ->
                findColumnInTable(table, candidate, propertyNameHint, statement)?.let { return it }
            }
        }

        viewLookup.values.forEach { view ->
            candidates.forEach { candidate ->
                resolveColumnFromView(
                    view = view,
                    fieldAlias = candidate,
                    paramName = propertyNameHint,
                    statement = statement,
                    visitedViews = mutableSetOf()
                )?.let { return it }
            }
        }

        return null
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

            val paramLower = paramName.lowercase()
            return table.findColumnByName(paramName) ?: table.columns.find { col ->
                statement.annotations.propertyNameGenerator.convertToPropertyName(col.src.name)
                    .lowercase() == paramLower
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
        val table = findTableByName(statement.src.table) ?: return null

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
                columnName?.let(table::findColumnByName)
                    ?: findColumnInWithClause(src.withSelectStatements)
            }

            is DeleteStatement -> {
                val associatedColumn = src.namedParametersToColumns[paramName]
                associatedColumn?.let { table.findColumnByName(it.columnName) }
                    ?: findColumnInWithClause(src.withSelectStatements)
            }

            is UpdateStatement -> {
                // First check if this parameter is from a SET clause (has direct column mapping)
                val directColumnName = src.namedParametersToColumnNames[paramName]
                directColumnName?.let(table::findColumnByName)
                    ?: run {
                        // If not from SET clause, check WHERE clause parameters
                        val associatedColumn = src.namedParametersToColumns[paramName]
                        associatedColumn?.let { table.findColumnByName(it.columnName) }
                            ?: findColumnInWithClause(src.withSelectStatements)
                    }
            }
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
        val paramLower = paramName.lowercase()
        return table.columns.find { col ->
            statement.annotations.propertyNameGenerator.convertToPropertyName(col.src.name)
                .lowercase() == paramLower
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

        resolveColumnFromView(
            view = view,
            fieldAlias = columnName,
            paramName = paramName,
            statement = statement,
            visitedViews = mutableSetOf()
        )?.let { return it }

        return null
    }

    private fun resolveColumnFromView(
        view: AnnotatedCreateViewStatement,
        fieldAlias: String,
        paramName: String,
        statement: AnnotatedSelectStatement,
        visitedViews: MutableSet<String>,
    ): AnnotatedCreateTableStatement.Column? {
        val viewKey = view.src.viewName.lowercase()
        if (!visitedViews.add(viewKey)) {
            return null
        }

        val candidates = view.fields.filter { field ->
            field.src.fieldName.equals(fieldAlias, ignoreCase = true) ||
                field.src.originalColumnName.equals(fieldAlias, ignoreCase = true)
        }

        candidates.forEach { field ->
            val sourceTableName = field.src.tableName
            val originalColumnName = field.src.originalColumnName.ifBlank { field.src.fieldName }

            if (sourceTableName.isNotBlank()) {
                findTableByName(sourceTableName)?.let { table ->
                    findColumnInTable(table, originalColumnName, paramName, statement)?.let { return it }
                }

                findViewByName(sourceTableName)?.let { nestedView ->
                    resolveColumnFromView(
                        view = nestedView,
                        fieldAlias = originalColumnName,
                        paramName = paramName,
                        statement = statement,
                        visitedViews = visitedViews
                    )?.let { return it }
                }
            }
        }

        val selectStatement = view.src.selectStatement

        selectStatement.fromTable?.let { fromTableName ->
            findTableByName(fromTableName)?.let { table ->
                findColumnInTable(table, fieldAlias, paramName, statement)?.let { return it }
            }

            findViewByName(fromTableName)?.let { nestedView ->
                resolveColumnFromView(
                    view = nestedView,
                    fieldAlias = fieldAlias,
                    paramName = paramName,
                    statement = statement,
                    visitedViews = visitedViews
                )?.let { return it }
            }
        }

        for (joinTableName in selectStatement.joinTables) {
            findTableByName(joinTableName)?.let { table ->
                findColumnInTable(table, fieldAlias, paramName, statement)?.let { return it }
            }

            findViewByName(joinTableName)?.let { nestedView ->
                resolveColumnFromView(
                    view = nestedView,
                    fieldAlias = fieldAlias,
                    paramName = paramName,
                    statement = statement,
                    visitedViews = visitedViews
                )?.let { return it }
            }
        }

        return null
    }

    private fun addNameVariants(name: String, sink: MutableSet<String>) {
        val trimmed = name.trim()
        if (trimmed.isEmpty()) return
        sink += trimmed

        val withoutSuffix = trimmed.substringBefore(':')
        if (withoutSuffix.isNotEmpty()) sink += withoutSuffix

        val afterDot = withoutSuffix.substringAfterLast('.', withoutSuffix)
        if (afterDot.isNotEmpty()) sink += afterDot

        val segments = afterDot.split('_').filter { it.isNotEmpty() }
        if (segments.size > 1) {
            segments.indices.forEach { index ->
                sink += segments.drop(index).joinToString("_")
            }
        }
    }

    private fun String?.removePrefixIfMatches(prefix: String): String? {
        if (this.isNullOrBlank()) return null
        return if (this.startsWith(prefix)) {
            this.removePrefix(prefix).takeIf { it.isNotBlank() }
        } else {
            null
        }
    }


    /** Checks if a parameter's corresponding column is nullable. */
    fun isParameterNullable(statement: AnnotatedStatement, paramName: String): Boolean {
        val column = findColumnForParameter(statement, paramName) ?: return false
        return column.isSqlNullable()
    }
}
