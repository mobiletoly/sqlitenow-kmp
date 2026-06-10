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
package dev.goquick.sqlitenow.gradle.generator.query

import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.JoinedPropertyNameResolver
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.util.AliasPathUtils
import dev.goquick.sqlitenow.gradle.util.GenericTypeParser
import dev.goquick.sqlitenow.gradle.util.IndentedCodeBuilder

internal class ResultMappingCollectionRenderer(
    private val generatorContext: GeneratorContext,
    private val fieldResolver: ResultMappingFieldResolver,
) {
    /**
     * Prepare comment + assignment blocks used when grouping rows by collection key before mapping to DTOs.
     */
    fun buildCollectionConstructorBlocks(
        statement: AnnotatedSelectStatement,
        firstRowVar: String,
        rowsVar: String,
        collectionMappingCodeProvider: (DynamicFieldInvocation, String) -> String,
        dynamicFieldMappingProvider: DynamicFieldExpression,
        collectionIndentLevel: Int = 3,
        dynamicIndentLevel: Int = 4,
    ): List<List<String>> {
        val propertyNameGenerator = statement.annotations.propertyNameGenerator
        val joinedNameMap = fieldResolver.computeJoinedNameMap(statement)
        val collectionFields = statement.mappingPlan.includedCollectionFields
        val collectionAliasPaths = collectionFields.mapNotNull { field ->
            field.aliasPath.takeIf { it.isNotEmpty() }?.let { AliasPathUtils.lowercase(it) }
        }

        val regularBlocks = statement.mappingPlan.regularFields.map { field ->
            val propertyName = fieldResolver.getPropertyName(field, propertyNameGenerator)
            val key = JoinedPropertyNameResolver.JoinedFieldKey(
                field.src.tableName,
                field.src.fieldName
            )
            val joinedProp = joinedNameMap[key] ?: propertyName
            buildAssignmentBlock(
                field = field,
                statement = statement,
                propertyName = propertyName,
                assignmentExpression = "$firstRowVar.$joinedProp",
                includeTypeInComment = true,
            )
        }

        val perRowBlocks = buildDynamicAssignmentBlocks(
            fields = statement.mappingPlan.includedPerRowEntries.map { it.field },
            statement = statement,
            firstRowVar = firstRowVar,
            rowsVar = rowsVar,
            dynamicIndentLevel = dynamicIndentLevel,
            propertyNameGenerator = propertyNameGenerator,
            collectionAliasPaths = collectionAliasPaths,
            dynamicFieldMappingProvider = dynamicFieldMappingProvider,
        )

        val entityBlocks = buildDynamicAssignmentBlocks(
            fields = statement.mappingPlan.includedEntityEntries.map { it.field },
            statement = statement,
            firstRowVar = firstRowVar,
            rowsVar = rowsVar,
            dynamicIndentLevel = dynamicIndentLevel,
            propertyNameGenerator = propertyNameGenerator,
            collectionAliasPaths = collectionAliasPaths,
            dynamicFieldMappingProvider = dynamicFieldMappingProvider,
        )

        val collectionBlocks = collectionFields.map { collectionField ->
            val propertyName = fieldResolver.getPropertyName(collectionField, propertyNameGenerator)
            val mapping =
                statement.mappingPlan.dynamicMappingsByField[collectionField.src.fieldName]

            if (mapping != null && mapping.columns.isNotEmpty()) {
                val collectionExpr = collectionMappingCodeProvider(
                    DynamicFieldInvocation(
                        field = collectionField,
                        statement = statement,
                        mapping = mapping,
                        sourceVar = firstRowVar,
                        baseIndentLevel = collectionIndentLevel,
                    ),
                    rowsVar,
                )
                val exprLines = collectionExpr.split("\n")
                val firstLine = exprLines.firstOrNull() ?: "emptyList()"
                val trailingLines = if (exprLines.size > 1) exprLines.drop(1) else emptyList()
                buildAssignmentBlock(
                    field = collectionField,
                    statement = statement,
                    propertyName = propertyName,
                    assignmentExpression = firstLine,
                    includeTypeInComment = false,
                    additionalLines = trailingLines,
                )
            } else {
                buildAssignmentBlock(
                    field = collectionField,
                    statement = statement,
                    propertyName = propertyName,
                    assignmentExpression = "emptyList()",
                    includeTypeInComment = false,
                )
            }
        }

        return regularBlocks + perRowBlocks + entityBlocks + collectionBlocks
    }

    private fun buildDynamicAssignmentBlocks(
        fields: List<AnnotatedSelectStatement.Field>,
        statement: AnnotatedSelectStatement,
        firstRowVar: String,
        rowsVar: String,
        dynamicIndentLevel: Int,
        propertyNameGenerator: PropertyNameGeneratorType,
        collectionAliasPaths: List<List<String>>,
        dynamicFieldMappingProvider: DynamicFieldExpression,
    ): List<List<String>> {
        if (fields.isEmpty()) return emptyList()

        return fields
            .filter { field ->
                AliasResolutionHelper.shouldIncludeDynamicForCollection(
                    aliasPath = AliasPathUtils.lowercase(field.aliasPath),
                    collectionAliasPaths = collectionAliasPaths,
                )
            }
            .map { field ->
                val propertyName = fieldResolver.getPropertyName(field, propertyNameGenerator)
                val mappingPlanMapping =
                    statement.mappingPlan.dynamicMappingsByField[field.src.fieldName]
                val mappingCode = if (mappingPlanMapping != null) {
                    dynamicFieldMappingProvider(
                        DynamicFieldInvocation(
                            field = field,
                            statement = statement,
                            mapping = mappingPlanMapping,
                            sourceVar = firstRowVar,
                            baseIndentLevel = dynamicIndentLevel,
                        ),
                        rowsVar,
                    )
                } else {
                    "null"
                }
                buildAssignmentBlock(
                    field = field,
                    statement = statement,
                    propertyName = propertyName,
                    assignmentExpression = mappingCode,
                    includeTypeInComment = false,
                )
            }
    }

    /** Produce a reusable "comment + assignment" block for the generated constructor code. */
    private fun buildAssignmentBlock(
        field: AnnotatedSelectStatement.Field,
        statement: AnnotatedSelectStatement,
        propertyName: String,
        assignmentExpression: String,
        includeTypeInComment: Boolean,
        additionalLines: List<String> = emptyList(),
    ): List<String> {
        val comment = fieldResolver.buildFieldDebugComment(
            field = field,
            selectStatement = statement.src,
            propertyNameGenerator = statement.annotations.propertyNameGenerator,
            includeType = includeTypeInComment,
        )
        return buildStringList { lines ->
            if (comment.isNotEmpty()) {
                lines += "// $comment"
            }
            lines += "$propertyName = $assignmentExpression"
            if (additionalLines.isNotEmpty()) {
                lines += additionalLines
            }
        }
    }

    fun emitCollectionConstructorBlocks(
        builder: IndentedCodeBuilder,
        statement: AnnotatedSelectStatement,
        firstRowVar: String,
        rowsVar: String,
        collectionMappingCodeProvider: (DynamicFieldInvocation, String) -> String,
        dynamicFieldMappingProvider: DynamicFieldExpression,
    ) {
        val blocks = buildCollectionConstructorBlocks(
            statement = statement,
            firstRowVar = firstRowVar,
            rowsVar = rowsVar,
            collectionMappingCodeProvider = collectionMappingCodeProvider,
            dynamicFieldMappingProvider = dynamicFieldMappingProvider,
        )

        blocks.forEachIndexed { blockIndex, lines ->
            val isLastBlock = blockIndex == blocks.lastIndex
            val indentPrefix = " ".repeat(builder.currentIndent())
            lines.forEachIndexed { lineIndex, line ->
                val suffix = if (lineIndex == lines.lastIndex && !isLastBlock) "," else ""
                val content = if (line.isNotEmpty() && line.first().isWhitespace()) {
                    line
                } else {
                    indentPrefix + line
                }
                builder.lineRaw(content + suffix)
            }
        }
    }

    fun findFieldByCollectionKey(
        statement: AnnotatedSelectStatement,
        collectionKey: String,
    ): AnnotatedSelectStatement.Field? {
        val preferredFields = statement.mappingPlan.regularFields
        val candidateSets = listOf(
            preferredFields,
            statement.mappingPlan.includedDynamicFields,
            statement.fields,
        )

        val tableLookup = mutableMapOf<Pair<String, String>, AnnotatedSelectStatement.Field>()
        preferredFields.forEach { field ->
            val table = field.src.tableName
            val column = field.src.originalColumnName.ifBlank { field.src.fieldName }
            tableLookup[table.lowercase() to column.lowercase()] = field
        }

        if (collectionKey.contains(".")) {
            val (tableAliasRaw, columnNameRaw) = collectionKey.split(".", limit = 2)
            val key = tableAliasRaw.lowercase() to columnNameRaw.lowercase()
            tableLookup[key]?.let { return it }

            candidateSets.forEach { fields ->
                fields.firstOrNull { field ->
                    field.src.tableName.equals(tableAliasRaw, ignoreCase = true) &&
                            (field.src.originalColumnName.equals(
                                columnNameRaw,
                                ignoreCase = true
                            ) ||
                                    field.src.fieldName.equals(columnNameRaw, ignoreCase = true))
                }?.let { return it }
            }
        } else {
            preferredFields.firstOrNull { field ->
                field.src.fieldName.equals(collectionKey, ignoreCase = true)
            }?.let { return it }

            candidateSets.forEach { fields ->
                fields.firstOrNull { field ->
                    field.src.fieldName.equals(collectionKey, ignoreCase = true) ||
                            field.src.originalColumnName.equals(collectionKey, ignoreCase = true)
                }?.let { return it }
            }
        }

        return null
    }

    fun findUniqueFieldForCollection(
        collectionField: AnnotatedSelectStatement.Field,
        annotatedStatement: AnnotatedSelectStatement,
    ): String? {
        val collectionKey = collectionField.annotations.collectionKey
        if (collectionKey.isNullOrBlank()) return null

        val matchingField = findFieldByCollectionKey(annotatedStatement, collectionKey)
        if (matchingField == null) {
            val availableFields = annotatedStatement.fields.map { field ->
                val tableInfo = if (field.src.tableName.isNotBlank()) {
                    "${field.src.tableName}.${field.src.originalColumnName}"
                } else {
                    field.src.originalColumnName
                }
                "$tableInfo AS ${field.src.fieldName}"
            }
            throw IllegalArgumentException(
                "collectionKey '$collectionKey' not found in SELECT statement. Available fields: $availableFields"
            )
        }

        val mapping =
            annotatedStatement.mappingPlan.dynamicMappingsByField[collectionField.src.fieldName]
        if (mapping != null) {
            val mappingColumn = mapping.columns.find { it.fieldName == matchingField.src.fieldName }
            if (mappingColumn != null) {
                val basePropertyName = if (mapping.aliasPrefix != null &&
                    mappingColumn.fieldName.startsWith(mapping.aliasPrefix)
                ) {
                    mappingColumn.fieldName.removePrefix(mapping.aliasPrefix)
                } else {
                    mappingColumn.originalColumnName
                }
                return annotatedStatement.annotations.propertyNameGenerator.convertToPropertyName(
                    basePropertyName
                )
            }
        }

        return annotatedStatement.annotations.propertyNameGenerator.convertToPropertyName(
            matchingField.src.fieldName
        )
    }

    fun findDistinctByPathForNestedConstruction(
        targetPropertyType: String,
        uniqueField: String,
    ): String {
        val actualType = GenericTypeParser.Companion.extractFirstTypeArgument(targetPropertyType)
        val resolved = resolveDistinctPathForType(actualType, uniqueField, mutableSetOf())
        return resolved ?: uniqueField
    }

    private fun resolveDistinctPathForType(
        rawType: String,
        uniqueField: String,
        visited: MutableSet<String>,
    ): String? {
        val normalizedType = normalizeType(rawType)
        if (!visited.add(normalizedType)) return null

        val nestedStatement = resolveStatementForType(normalizedType) ?: return null
        val propertyNameGenerator = nestedStatement.annotations.propertyNameGenerator

        nestedStatement.mappingPlan.regularFields.firstOrNull { field ->
            fieldResolver.getPropertyName(field, propertyNameGenerator) == uniqueField
        }?.let {
            return uniqueField
        }

        nestedStatement.mappingPlan.includedDynamicEntries.forEach { entry ->
            val field = entry.field
            val mappingType = field.annotations.mappingType
            if (mappingType != null &&
                mappingType.equals(
                    AnnotationConstants.MappingType.COLLECTION.value,
                    ignoreCase = true
                )
            ) {
                return@forEach
            }
            val propertyName = fieldResolver.getPropertyName(field, propertyNameGenerator)
            if (propertyName == uniqueField) {
                return propertyName
            }

            val propertyType = field.annotations.propertyType
            val nestedType = propertyType?.let { type ->
                val innerType = GenericTypeParser.Companion.extractFirstTypeArgument(type)
                normalizeType(innerType)
            }

            if (!nestedType.isNullOrBlank()) {
                resolveDistinctPathForType(nestedType, uniqueField, visited)?.let { nestedPath ->
                    return if (nestedPath.isEmpty()) propertyName else "$propertyName.$nestedPath"
                }
            }

            val mapping = nestedStatement.mappingPlan.dynamicMappingsByField[field.src.fieldName]
            if (mapping != null) {
                val aliasPrefix = mapping.aliasPrefix
                val match = mapping.columns.firstOrNull { column ->
                    val basePropertyName = when {
                        aliasPrefix != null && column.fieldName.startsWith(aliasPrefix) ->
                            column.fieldName.removePrefix(aliasPrefix)

                        column.originalColumnName.isNotBlank() -> column.originalColumnName
                        else -> column.fieldName
                    }
                    val candidate = propertyNameGenerator.convertToPropertyName(basePropertyName)
                    candidate == uniqueField
                }
                if (match != null) {
                    return "$propertyName.$uniqueField"
                }
            }
        }

        return null
    }

    private fun normalizeType(typeName: String): String = typeName.trim().removeSuffix("?")

    private fun resolveStatementForType(typeName: String): AnnotatedSelectStatement? {
        val simpleName = typeName.substringAfterLast('.')
        return generatorContext.findSelectStatementByResultName(typeName)
            ?: generatorContext.findSelectStatementByResultName(simpleName)
    }
}

private inline fun buildStringList(builderAction: (MutableList<String>) -> Unit): List<String> {
    val lines = mutableListOf<String>()
    builderAction(lines)
    return lines
}
