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
package dev.goquick.sqlitenow.gradle.processing

import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.util.CaseInsensitiveSet

/**
 * Shared helper for interpreting RETURNING clauses on execute statements.
 */
internal object ReturningColumnsResolver {
    fun resolveColumns(
        generatorContext: GeneratorContext,
        statement: AnnotatedExecuteStatement,
    ): List<AnnotatedCreateTableStatement.Column> {
        return resolveColumns(generatorContext.createTableStatements, statement)
    }

    fun resolveColumns(
        createTableStatements: List<AnnotatedCreateTableStatement>,
        statement: AnnotatedExecuteStatement,
    ): List<AnnotatedCreateTableStatement.Column> {
        val (tableStatement, returningColumns) = resolveMetadata(createTableStatements, statement)
        return selectColumns(tableStatement, returningColumns)
    }

    fun resolveColumnsOrEmpty(
        generatorContext: GeneratorContext,
        statement: AnnotatedExecuteStatement,
    ): List<AnnotatedCreateTableStatement.Column> {
        return resolveColumnsOrEmpty(generatorContext.createTableStatements, statement)
    }

    fun resolveColumnsOrEmpty(
        createTableStatements: List<AnnotatedCreateTableStatement>,
        statement: AnnotatedExecuteStatement,
    ): List<AnnotatedCreateTableStatement.Column> {
        val (tableStatement, returningColumns) = resolveMetadataOrNull(createTableStatements, statement)
            ?: return emptyList()
        return selectColumns(tableStatement, returningColumns)
    }

    fun createSelectLikeFields(
        generatorContext: GeneratorContext,
        statement: AnnotatedExecuteStatement,
    ): List<AnnotatedSelectStatement.Field> {
        val (tableStatement, returningColumns) = resolveMetadata(
            generatorContext.createTableStatements,
            statement,
        )
        val columnsToInclude = selectColumns(tableStatement, returningColumns)

        return columnsToInclude.map { column ->
            val fieldSrc = SelectStatement.FieldSource(
                fieldName = column.src.name,
                tableName = tableStatement.src.tableName,
                originalColumnName = column.src.name,
                dataType = column.src.dataType,
                expression = null,
            )

            AnnotatedSelectStatement.Field(
                src = fieldSrc,
                annotations = FieldAnnotationOverrides.parse(column.annotations),
            )
        }
    }

    private fun resolveMetadata(
        createTableStatements: List<AnnotatedCreateTableStatement>,
        statement: AnnotatedExecuteStatement,
    ): Pair<AnnotatedCreateTableStatement, List<String>> {
        return resolveMetadataOrNull(createTableStatements, statement)
            ?: error("Table '${statement.src.table}' not found for RETURNING clause")
    }

    private fun resolveMetadataOrNull(
        createTableStatements: List<AnnotatedCreateTableStatement>,
        statement: AnnotatedExecuteStatement,
    ): Pair<AnnotatedCreateTableStatement, List<String>>? {
        val tableName = statement.src.table

        val tableLookup =
            createTableStatements.associateBy { it.src.tableName.lowercase() }
        val tableStatement = tableLookup[tableName.lowercase()]
            ?: return null

        val returningColumns = statement.getReturningColumns()
        return tableStatement to returningColumns
    }

    private fun selectColumns(
        tableStatement: AnnotatedCreateTableStatement,
        returningColumns: List<String>,
    ): List<AnnotatedCreateTableStatement.Column> {
        val allColumns = tableStatement.columns
        if (returningColumns.isEmpty() || returningColumns.contains("*")) {
            return allColumns
        }
        val returningSet = CaseInsensitiveSet().apply { addAll(returningColumns) }
        return allColumns.filter { column -> returningSet.containsIgnoreCase(column.src.name) }
    }
}
