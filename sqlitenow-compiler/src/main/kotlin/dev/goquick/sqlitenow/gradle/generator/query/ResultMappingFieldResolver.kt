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

import dev.goquick.sqlitenow.gradle.context.AdapterConfig
import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldMapper
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.JoinedPropertyNameResolver
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.util.CaseInsensitiveMap
import java.util.IdentityHashMap

internal data class ResolvedJoinedName(
    val property: String,
    val suffixed: Boolean,
)

internal class ResultMappingFieldResolver(
    private val generatorContext: GeneratorContext,
    private val selectFieldGenerator: SelectFieldCodeGenerator,
    private val adapterConfig: AdapterConfig,
) {
    private val joinedNameMapCache =
        IdentityHashMap<AnnotatedSelectStatement, Map<JoinedPropertyNameResolver.JoinedFieldKey, String>>()
    private val tableLookupCache =
        IdentityHashMap<AnnotatedSelectStatement, CaseInsensitiveMap<AnnotatedCreateTableStatement>>()
    private val tableColumnLookupCache =
        IdentityHashMap<AnnotatedCreateTableStatement, CaseInsensitiveMap<AnnotatedCreateTableStatement.Column>>()

    /** Cache-friendly wrapper around JoinedPropertyNameResolver to avoid recomputing per mapping site. */
    fun computeJoinedNameMap(statement: AnnotatedSelectStatement): Map<JoinedPropertyNameResolver.JoinedFieldKey, String> {
        joinedNameMapCache[statement]?.let { return it }
        val map = JoinedPropertyNameResolver.computeNameMap(
            fields = statement.fields,
            propertyNameGenerator = statement.annotations.propertyNameGenerator,
            selectFieldGenerator = selectFieldGenerator,
        )
        joinedNameMapCache[statement] = map
        return map
    }

    fun resolveJoinedPropertyName(
        column: SelectStatement.FieldSource,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement,
        aliasPath: List<String>?,
        joinedNameMap: Map<JoinedPropertyNameResolver.JoinedFieldKey, String>,
    ): ResolvedJoinedName {
        val aliasPrefix = mapping.aliasPrefix?.takeIf { it.isNotBlank() }
        val tableCandidates = AliasResolutionHelper.buildTableCandidates(column, mapping, statement)
        val candidateNames = AliasResolutionHelper.candidateFieldNames(column, aliasPrefix)
        val preferSuffixed =
            AliasResolutionHelper.shouldPreferSuffixed(aliasPath, mapping, statement)

        tableCandidates.forEach { table ->
            val expandedCandidates =
                AliasResolutionHelper.expandCandidateNamesForTable(
                    table,
                    candidateNames,
                    joinedNameMap,
                    preferSuffixed
                )
            expandedCandidates.forEach { candidate ->
                val key = JoinedPropertyNameResolver.JoinedFieldKey(table, candidate)
                joinedNameMap[key]?.let { return ResolvedJoinedName(it, candidate.contains(':')) }
            }
        }

        val fallbackName = candidateNames.firstOrNull().orEmpty().ifBlank { column.fieldName }
        val resolved =
            statement.annotations.propertyNameGenerator.convertToPropertyName(fallbackName)
        return ResolvedJoinedName(resolved, fallbackName.contains(':'))
    }

    fun getPropertyName(
        field: AnnotatedSelectStatement.Field,
        propertyNameGenerator: PropertyNameGeneratorType,
    ): String {
        return adapterConfig.getPropertyName(field, propertyNameGenerator, selectFieldGenerator)
    }

    fun buildFieldDebugComment(
        field: AnnotatedSelectStatement.Field,
        selectStatement: SelectStatement,
        propertyNameGenerator: PropertyNameGeneratorType,
        includeType: Boolean,
    ): String {
        val parts = mutableListOf<String>()
        if (includeType) {
            val sqlType = field.src.dataType
            val kotlinType = selectFieldGenerator
                .generateProperty(field, propertyNameGenerator)
                .type
                .toString()
            parts += "type=$sqlType -> $kotlinType"
        }
        field.src.fieldName.takeIf { it.isNotBlank() }?.let { parts += "select=$it" }
        val sourceAlias = when {
            !field.annotations.sourceTable.isNullOrBlank() -> field.annotations.sourceTable
            field.src.tableName.isNotBlank() -> field.src.tableName
            else -> null
        }
        sourceAlias?.let { alias ->
            val target = selectStatement.tableAliases[alias] ?: alias
            val descriptor =
                if (!alias.equals(target, ignoreCase = true)) "$alias->$target" else alias
            parts += "source=$descriptor"
        }
        field.src.originalColumnName.takeIf { it.isNotBlank() && it != field.src.fieldName }
            ?.let { parts += "column=$it" }
        field.annotations.aliasPrefix?.takeIf { it.isNotBlank() }?.let { parts += "prefix=$it" }
        if (field.aliasPath.isNotEmpty()) {
            parts += "aliasPath=${field.aliasPath.joinToString("->")}"
        }
        field.annotations.mappingType?.let { mappingType ->
            parts += "mapping=${mappingType.lowercase()}"
            field.annotations.collectionKey?.takeIf { it.isNotBlank() }
                ?.let { key -> parts += "collectionKey=$key" }
        }
        field.annotations.notNull?.let { parts += "notNull=$it" }
        return parts.joinToString(", ")
    }

    fun isTargetPropertyNullable(
        statement: AnnotatedSelectStatement,
        column: SelectStatement.FieldSource,
    ): Boolean {
        val annotatedField = findAnnotatedField(statement, column)
        if (annotatedField != null) {
            return selectFieldGenerator.determineNullability(annotatedField)
        }

        val mockFieldAnnotations = FieldAnnotationOverrides(
            propertyName = null,
            propertyType = null,
            notNull = null,
            adapter = false,
        )
        val mockField = AnnotatedSelectStatement.Field(
            src = column,
            annotations = mockFieldAnnotations,
        )
        return selectFieldGenerator.determineNullability(mockField)
    }

    private fun findAnnotatedField(
        statement: AnnotatedSelectStatement,
        column: SelectStatement.FieldSource,
    ): AnnotatedSelectStatement.Field? {
        val normalizedFieldName = column.fieldName.substringBefore(':')
        return statement.fields.firstOrNull { field ->
            val candidateFieldName = field.src.fieldName.substringBefore(':')
            val candidateOriginal = field.src.originalColumnName.substringBefore(':')
            val matchesField = candidateFieldName.equals(column.fieldName, ignoreCase = true) ||
                    candidateFieldName.equals(normalizedFieldName, ignoreCase = true)
            val matchesOriginal = column.originalColumnName.isNotBlank() &&
                    candidateOriginal.equals(column.originalColumnName, ignoreCase = true)
            val tableMatches = field.src.tableName.equals(column.tableName, ignoreCase = true) ||
                    field.src.tableName.isBlank() || column.tableName.isBlank()
            (matchesField || matchesOriginal) && tableMatches
        }
    }

    fun findOriginalColumnPropertyName(
        baseColumnName: String,
        sourceTableAlias: String,
        statement: AnnotatedSelectStatement,
    ): String? {
        val tableName = statement.src.tableAliases[sourceTableAlias] ?: sourceTableAlias
        val tableLookup = tableLookupCache.getOrPut(statement) {
            CaseInsensitiveMap(generatorContext.createTableStatements.map { it.src.tableName to it })
        }
        val table = tableLookup[tableName] ?: return null

        val columnLookup = tableColumnLookupCache.getOrPut(table) {
            CaseInsensitiveMap(table.columns.map { it.src.name to it })
        }
        val column = columnLookup[baseColumnName] ?: return null

        val propertyName = column.annotations[AnnotationConstants.PROPERTY_NAME] as? String
        return propertyName ?: statement.annotations.propertyNameGenerator.convertToPropertyName(
            baseColumnName
        )
    }
}
