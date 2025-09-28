package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.SelectStatement

data class AnnotatedSelectStatement(
    override val name: String,
    val src: SelectStatement,
    override val annotations: StatementAnnotationOverrides,
    val fields: List<Field>
) : AnnotatedStatement {
    data class Field(
        val src: SelectStatement.FieldSource,
        val annotations: FieldAnnotationOverrides,
        val aliasPath: List<String> = emptyList()
    )

    fun hasDynamicFieldMapping() = fields.any {
        it.annotations.isDynamicField && it.annotations.mappingType != null
    }

    fun hasCollectionMapping() = fields.any {
        it.annotations.isDynamicField && when (AnnotationConstants.MappingType.fromString(it.annotations.mappingType)) {
            AnnotationConstants.MappingType.COLLECTION -> true
            AnnotationConstants.MappingType.PER_ROW,
            AnnotationConstants.MappingType.ENTITY -> false
        }
    }
}
