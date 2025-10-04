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
        val (tableStatement, returningColumns) = resolveMetadata(generatorContext, statement)
        val allColumns = tableStatement.columns
        if (returningColumns.isEmpty() || returningColumns.contains("*")) {
            return allColumns
        }
        val returningSet = CaseInsensitiveSet().apply { addAll(returningColumns) }
        return allColumns.filter { column -> returningSet.containsIgnoreCase(column.src.name) }
    }

    fun createSelectLikeFields(
        generatorContext: GeneratorContext,
        statement: AnnotatedExecuteStatement,
    ): List<AnnotatedSelectStatement.Field> {
        val (tableStatement, returningColumns) = resolveMetadata(generatorContext, statement)
        val columnsToInclude = if (returningColumns.isEmpty() || returningColumns.contains("*")) {
            tableStatement.columns
        } else {
            val returningSet = CaseInsensitiveSet().apply { addAll(returningColumns) }
            tableStatement.columns.filter { column -> returningSet.containsIgnoreCase(column.src.name) }
        }

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
        generatorContext: GeneratorContext,
        statement: AnnotatedExecuteStatement,
    ): Pair<AnnotatedCreateTableStatement, List<String>> {
        val tableName = statement.src.table

        val tableLookup =
            generatorContext.createTableStatements.associateBy { it.src.tableName.lowercase() }
        val tableStatement = tableLookup[tableName.lowercase()]
            ?: error("Table '$tableName' not found for RETURNING clause")

        val returningColumns = statement.getReturningColumns()
        return tableStatement to returningColumns
    }
}
