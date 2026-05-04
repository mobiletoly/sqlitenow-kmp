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

import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter

/**
 * Centralized resolver for field annotations across tables and views.
 * Pre-processes all annotation sources and provides a clean lookup interface.
 */
class FieldAnnotationResolver(
    private val createTableStatements: List<AnnotatedCreateTableStatement>,
    private val createViewStatements: List<AnnotatedCreateViewStatement>
) {
    private val viewLookup: Map<String, AnnotatedCreateViewStatement> =
        createViewStatements.associateBy { it.src.viewName.lowercase() }

    /**
     * Map of "tableName.fieldName" to resolved field annotations.
     * This includes annotations from both tables and views.
     */
    private val resolvedAnnotations: Map<String, FieldAnnotationOverrides>
    private val annotationCache = mutableMapOf<String, FieldAnnotationOverrides?>()

    init {
        resolvedAnnotations = buildResolvedAnnotations()
    }

    /**
     * Gets resolved field annotations for a specific table/view and field.
     * Returns null if no annotations are found.
     */
    fun getFieldAnnotations(tableName: String, fieldName: String): FieldAnnotationOverrides? {
        val key = canonicalKey(tableName, fieldName)
        annotationCache[key]?.let { return it }
        val resolved = resolveAnnotation(tableName, fieldName, mutableSetOf())
        annotationCache[key] = resolved
        return resolved
    }

    /**
     * Finds a view by name (case-insensitive).
     */
    fun findView(viewName: String): AnnotatedCreateViewStatement? {
        return viewLookup[viewName.lowercase()]
    }

    /**
     * Pre-processes all tables and views to create a flattened annotation lookup map.
     */
    private fun buildResolvedAnnotations(): Map<String, FieldAnnotationOverrides> {
        val annotations = mutableMapOf<String, FieldAnnotationOverrides>()

        // Process table field annotations
        createTableStatements.forEach { table ->
            table.columns.forEach { column ->
                val fieldAnnotations = FieldAnnotationOverrides.parse(column.annotations)
                if (hasAnyAnnotations(fieldAnnotations)) {
                    val key = canonicalKey(table.name, column.src.name)
                    annotations[key] = fieldAnnotations
                }
            }
        }

        // Process view field annotations
        createViewStatements.forEach { view ->
            view.fields.forEach { field ->
                if (hasAnyAnnotations(field.annotations)) {
                    val key = canonicalKey(view.src.viewName, field.src.fieldName)
                    annotations[key] = field.annotations
                }
            }
        }

        return annotations
    }

    private fun resolveAnnotation(
        tableName: String,
        fieldName: String,
        visited: MutableSet<String>
    ): FieldAnnotationOverrides? {
        val key = canonicalKey(tableName, fieldName)
        if (!visited.add(key)) {
            return resolvedAnnotations[key]
        }

        val direct = resolvedAnnotations[key]

        val view = viewLookup[tableName.lowercase()] ?: return direct
        val viewField = findViewField(view, fieldName)
        var resolved: FieldAnnotationOverrides? =
            if (viewField != null && hasAnyAnnotations(viewField.annotations)) viewField.annotations else null

        val downstreamFieldName = viewField?.src?.originalColumnName?.takeIf { it.isNotBlank() } ?: fieldName

        val tableAliases = view.src.selectStatement.tableAliases
        if (viewField != null) {
            val sourceAlias = viewField.src.tableName
            if (sourceAlias.isNotBlank()) {
                val target = tableAliases[sourceAlias] ?: sourceAlias
                val aliasResolved = resolveAnnotation(target, downstreamFieldName, visited)
                resolved = mergeAnnotations(resolved, aliasResolved)
            }
        }

        // Fallback: attempt to match by scanning referenced views
        if (resolved == null || !hasPropertyType(resolved)) {
            tableAliases.values.forEach { target ->
                val nestedResolved = resolveAnnotation(target, downstreamFieldName, visited)
                resolved = mergeAnnotations(resolved, nestedResolved)
            }
        }

        val merged = mergeAnnotations(resolved, direct) ?: direct ?: viewField?.annotations
        return if (viewField != null) {
            augmentWithFieldSource(merged, viewField.src)
        } else {
            merged
        }
    }

    private fun findViewField(
        view: AnnotatedCreateViewStatement,
        fieldName: String
    ): AnnotatedCreateViewStatement.Field? {
        val normalized = fieldName.lowercase()
        return view.fields.firstOrNull { field ->
            field.src.fieldName.equals(fieldName, ignoreCase = true) ||
                    field.src.originalColumnName.equals(fieldName, ignoreCase = true) ||
                    field.src.fieldName.lowercase() == normalized ||
                    field.src.originalColumnName.lowercase() == normalized
        }
    }

    /**
     * Checks if a FieldAnnotationOverrides has any non-null annotations.
     */
    private fun hasAnyAnnotations(annotations: FieldAnnotationOverrides): Boolean {
        return annotations.propertyName != null ||
                annotations.propertyType != null ||
                annotations.adapter == true ||
                annotations.notNull != null ||
                annotations.sqlTypeHint != null ||
                annotations.isDynamicField ||
                annotations.defaultValue != null ||
                annotations.aliasPrefix != null ||
                annotations.mappingType != null ||
                annotations.sourceTable != null ||
                annotations.suppressProperty
    }

    private fun hasPropertyType(annotations: FieldAnnotationOverrides): Boolean {
        return annotations.propertyType != null
    }

    private fun mergeAnnotations(
        primary: FieldAnnotationOverrides?,
        secondary: FieldAnnotationOverrides?,
    ): FieldAnnotationOverrides? {
        if (primary == null) return secondary
        if (secondary == null) return primary
        return FieldAnnotationOverrides(
            propertyName = primary.propertyName ?: secondary.propertyName,
            propertyType = primary.propertyType ?: secondary.propertyType,
            notNull = primary.notNull ?: secondary.notNull,
            adapter = primary.adapter ?: secondary.adapter,
            isDynamicField = primary.isDynamicField || secondary.isDynamicField,
            defaultValue = primary.defaultValue ?: secondary.defaultValue,
            aliasPrefix = primary.aliasPrefix ?: secondary.aliasPrefix,
            mappingType = primary.mappingType ?: secondary.mappingType,
            sourceTable = primary.sourceTable ?: secondary.sourceTable,
            collectionKey = primary.collectionKey ?: secondary.collectionKey,
            sqlTypeHint = primary.sqlTypeHint ?: secondary.sqlTypeHint,
            suppressProperty = primary.suppressProperty || secondary.suppressProperty,
        )
    }

    private fun augmentWithFieldSource(
        annotations: FieldAnnotationOverrides?,
        fieldSource: SelectStatement.FieldSource,
    ): FieldAnnotationOverrides? {
        if (annotations == null || annotations.propertyType != null) {
            return annotations
        }
        if (!hasAnyAnnotations(annotations)) {
            return annotations
        }
        val computedType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(fieldSource.dataType).toString()
        return annotations.copy(propertyType = computedType)
    }

    private fun canonicalKey(tableName: String, fieldName: String): String {
        return "${tableName.trim().lowercase()}.${fieldName.trim().lowercase()}"
    }
}
