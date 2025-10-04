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

    init {
        resolvedAnnotations = buildResolvedAnnotations()
    }

    /**
     * Gets resolved field annotations for a specific table/view and field.
     * Returns null if no annotations are found.
     */
    fun getFieldAnnotations(tableName: String, fieldName: String): FieldAnnotationOverrides? {
        val key = "$tableName.$fieldName"
        return resolvedAnnotations[key]
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
                    val key = "${table.name}.${column.src.name}"
                    annotations[key] = fieldAnnotations
                }
            }
        }

        // Process view field annotations
        createViewStatements.forEach { view ->
            view.fields.forEach { field ->
                if (hasAnyAnnotations(field.annotations)) {
                    val key = "${view.src.viewName}.${field.src.fieldName}"
                    annotations[key] = field.annotations
                }
            }
        }

        return annotations
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
}
