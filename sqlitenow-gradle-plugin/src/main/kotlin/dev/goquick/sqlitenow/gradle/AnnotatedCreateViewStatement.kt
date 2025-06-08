package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.CreateViewStatement
import dev.goquick.sqlitenow.gradle.inspect.SelectStatement

/**
 * Represents a CREATE VIEW statement with annotations extracted from SQL comments.
 * Views are treated similarly to tables but represent stored queries rather than physical tables.
 */
data class AnnotatedCreateViewStatement(
    override val name: String,
    val src: CreateViewStatement,
    override val annotations: StatementAnnotationOverrides,
    val fields: List<Field>
) : AnnotatedStatement {
    data class Field(
        val src: SelectStatement.FieldSource,
        val annotations: FieldAnnotationOverrides
    )
    companion object {
        fun parse(
            name: String,
            createViewStatement: CreateViewStatement,
            topComments: List<String>,
            innerComments: List<String>,
        ): AnnotatedCreateViewStatement {
            val viewAnnotations = StatementAnnotationOverrides.parse(
                extractAnnotations(topComments)
            )
            val fieldAnnotations = extractFieldAssociatedAnnotations(innerComments)

            return AnnotatedCreateViewStatement(
                name = name,
                src = createViewStatement,
                annotations = viewAnnotations,
                fields = createViewStatement.selectStatement.fields.map { field ->
                    val annotations = FieldAnnotationOverrides.parse(
                        fieldAnnotations[field.fieldName] ?: emptyMap()
                    )
                    Field(
                        src = field,
                        annotations = annotations
                    )
                }
            )
        }

    }

    /**
     * Finds a field that corresponds to the given column/field name.
     * Matches by both fieldName and originalColumnName.
     */
    fun findField(columnName: String): Field? {
        return fields.find { field ->
            field.src.fieldName == columnName || field.src.originalColumnName == columnName
        }
    }
}
