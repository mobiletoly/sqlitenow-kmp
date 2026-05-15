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
package dev.goquick.sqlitenow.gradle.dart

import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement

internal class DartFieldMapper(
    private val context: GeneratorContext,
    private val adapterRegistry: DartAdapterRegistry,
) {
    fun usesUint8List(
        selectStatements: List<Pair<String, AnnotatedSelectStatement>>,
        executeStatements: List<Pair<String, AnnotatedExecuteStatement>>,
    ): Boolean {
        return context.createTableStatements.any { table ->
            table.columns.any { it.toDartType().removeSuffix("?") == "Uint8List" }
        } || selectStatements.any { (_, statement) ->
            fields(statement).any { it.dartType.removeSuffix("?") == "Uint8List" }
        } || executeStatements.any { (_, statement) ->
            statement.hasReturningClause() &&
                returningFields(statement).any { it.dartType.removeSuffix("?") == "Uint8List" }
        }
    }

    fun fields(statement: AnnotatedSelectStatement): List<DartField> =
        statement.mappingPlan.regularFields.map { field ->
            toDartField(statement, field)
        } + statement.mappingPlan.includedDynamicFields.map { field ->
            val propertyType = field.annotations.propertyType
                ?: error("Dynamic field '${field.src.fieldName}' is missing propertyType.")
            val mappingType = field.annotations.mappingType?.let { AnnotationConstants.MappingType.fromString(it) }
            DartField(
                index = -1,
                propertyName = dynamicFieldPropertyName(field),
                dartType = propertyType.toDartTypeName().let { type ->
                    if (mappingType == AnnotationConstants.MappingType.COLLECTION || field.annotations.notNull == true) {
                        type.removeSuffix("?")
                    } else {
                        type.withNullable(true)
                    }
                },
                sqliteReadCall = "",
                nullable = field.annotations.notNull != true,
                adapter = null,
                source = field,
                dynamicField = field,
            )
        }

    fun physicalFields(statement: AnnotatedSelectStatement): List<DartField> =
        statement.fields.filterNot { it.annotations.isDynamicField }.map { field ->
            toDartField(statement, field)
        }

    fun mappingColumn(
        statement: AnnotatedSelectStatement,
        dynamicField: AnnotatedSelectStatement.Field,
        column: SelectStatement.FieldSource,
    ): DartMappingColumn {
        val field = statement.fields.firstOrNull { !it.annotations.isDynamicField && it.src.fieldName == column.fieldName }
            ?: error("Mapped column '${column.fieldName}' not found in Dart SELECT '${statement.name}'.")
        val physical = toDartField(statement, field)
        val aliasPrefix = dynamicField.annotations.aliasPrefix
        val rawName = if (!aliasPrefix.isNullOrBlank() && column.fieldName.startsWith(aliasPrefix)) {
            column.fieldName.removePrefix(aliasPrefix)
        } else {
            column.fieldName
        }.substringBefore(":")
        val tableColumn = createTableForSelectField(statement, column)
            ?.findColumnByName(column.originalColumnName)
        val tablePropertyName = tableColumn
            ?.parsedFieldAnnotations()
            ?.propertyName
        val constructorField = tableColumn?.let { resolvedColumn ->
            val adapter = adapterRegistry.adapterFor(
                tableName = createTableForSelectField(statement, column)!!.src.tableName,
                columnName = resolvedColumn.src.name,
                column = resolvedColumn,
            )
            physical.copy(
                dartType = resolvedColumn.toDartType(),
                sqliteReadCall = resolvedColumn.src.dataType.sqliteReadCall(nullable = resolvedColumn.isNullable()),
                nullable = resolvedColumn.isNullable(),
                adapter = adapter,
            )
        } ?: if (dynamicField.annotations.notNull == true) {
            physical.copy(
                dartType = physical.dartType.removeSuffix("?"),
                sqliteReadCall = column.dataType.sqliteReadCall(nullable = false),
                nullable = false,
            )
        } else {
            physical
        }
        return DartMappingColumn(
            sourcePropertyName = physical.propertyName,
            constructorPropertyName = tablePropertyName ?: statement.annotations.propertyNameGenerator.convertToPropertyName(rawName),
            field = constructorField,
        )
    }

    fun dynamicFieldPropertyName(field: AnnotatedSelectStatement.Field): String =
        field.annotations.propertyName ?: field.src.fieldName

    fun returningFields(statement: AnnotatedExecuteStatement): List<DartField> {
        val table = context.createTableStatements.first {
            it.src.tableName.equals(statement.src.table, ignoreCase = true)
        }
        return statement.getReturningColumns().mapIndexed { index, columnName ->
            val column = table.findColumnByName(columnName)
                ?: error("RETURNING column '$columnName' not found on table '${table.src.tableName}'.")
            val propertyName = statement.annotations.propertyNameGenerator.convertToPropertyName(column.src.name)
            val adapter = adapterRegistry.adapterFor(table.src.tableName, column.src.name, column)
            DartField(
                index = index,
                propertyName = propertyName,
                dartType = column.toDartType(),
                sqliteReadCall = column.src.dataType.sqliteReadCall(nullable = column.isNullable()),
                nullable = column.isNullable(),
                adapter = adapter,
            )
        }
    }

    private fun toDartField(
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
    ): DartField {
        val propertyName = field.annotations.propertyName
            ?: statement.annotations.propertyNameGenerator.convertToPropertyName(field.src.fieldName)
        val table = createTableForSelectField(statement, field.src)
        val column = table?.findColumnByName(field.src.originalColumnName)
        val adapter = column?.let { adapterRegistry.adapterFor(table.src.tableName, it.src.name, it) }
        return DartField(
            index = physicalFieldIndex(statement, field.src),
            propertyName = propertyName,
            dartType = field.toDartType(),
            sqliteReadCall = field.src.dataType.sqliteReadCall(nullable = field.isNullable()),
            nullable = field.isNullable(),
            adapter = adapter,
            source = field,
            dynamicField = null,
        )
    }

    private fun physicalFieldIndex(
        statement: AnnotatedSelectStatement,
        column: SelectStatement.FieldSource,
    ): Int =
        statement.src.fields.indexOfFirst { it.fieldName == column.fieldName }.takeIf { it >= 0 }
            ?: error("Column '${column.fieldName}' not found in SELECT '${statement.name}'.")

    private fun createTableForSelectField(
        statement: AnnotatedSelectStatement,
        field: SelectStatement.FieldSource,
    ): AnnotatedCreateTableStatement? {
        val tableName = statement.src.tableAliases[field.tableName] ?: field.tableName
        return context.createTableStatements.firstOrNull {
            it.src.tableName.equals(tableName, ignoreCase = true)
        }
    }

    private fun AnnotatedSelectStatement.Field.toDartType(): String {
        val explicit = annotations.propertyType?.toDartTypeName()
        if (explicit != null) return explicit.withNullable(isNullable())
        return src.dataType.sqliteDartType().withNullable(isNullable())
    }

    private fun AnnotatedCreateTableStatement.Column.toDartType(): String {
        val explicit = parsedFieldAnnotations().propertyType?.toDartTypeName()
        if (explicit != null) return explicit.withNullable(isNullable())
        return src.dataType.sqliteDartType().withNullable(isNullable())
    }

    private fun AnnotatedSelectStatement.Field.isNullable(): Boolean {
        annotations.notNull?.let { return !it }
        return src.isNullable
    }
}
