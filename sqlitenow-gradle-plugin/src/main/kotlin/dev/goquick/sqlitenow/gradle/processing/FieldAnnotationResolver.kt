package dev.goquick.sqlitenow.gradle.processing

import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement

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
        if (direct != null && hasAnyAnnotations(direct)) {
            return direct
        }

        val view = viewLookup[tableName.lowercase()] ?: return direct
        val viewField = findViewField(view, fieldName)
        if (viewField != null && hasAnyAnnotations(viewField.annotations)) {
            return viewField.annotations
        }

        val downstreamFieldName = viewField?.src?.originalColumnName?.takeIf { it.isNotBlank() } ?: fieldName

        val tableAliases = view.src.selectStatement.tableAliases
        if (viewField != null) {
            val sourceAlias = viewField.src.tableName
            if (sourceAlias.isNotBlank()) {
                val target = tableAliases[sourceAlias] ?: sourceAlias
                val resolved = resolveAnnotation(target, downstreamFieldName, visited)
                if (resolved != null && hasAnyAnnotations(resolved)) {
                    return resolved
                }
            }
        }

        // Fallback: attempt to match by scanning referenced views
        tableAliases.values.forEach { target ->
            val resolved = resolveAnnotation(target, downstreamFieldName, visited)
            if (resolved != null && hasAnyAnnotations(resolved)) {
                return resolved
            }
        }

        return direct ?: viewField?.annotations
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
                annotations.isDynamicField ||
                annotations.defaultValue != null ||
                annotations.aliasPrefix != null ||
                annotations.mappingType != null ||
                annotations.sourceTable != null
    }

    private fun canonicalKey(tableName: String, fieldName: String): String {
        return "${tableName.trim().lowercase()}.${fieldName.trim().lowercase()}"
    }
}
