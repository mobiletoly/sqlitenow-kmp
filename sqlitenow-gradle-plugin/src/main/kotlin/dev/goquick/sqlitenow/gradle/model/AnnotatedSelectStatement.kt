package dev.goquick.sqlitenow.gradle.model

import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement

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

    val mappingPlan: ResultMappingPlan by lazy(LazyThreadSafetyMode.NONE) {
        ResultMappingPlanner.create(src, fields)
    }

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
