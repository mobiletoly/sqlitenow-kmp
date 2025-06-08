package dev.goquick.sqlitenow.gradle

/**
 * Utility class for merging field annotations.
 */
object FieldAnnotationMerger {

    /**
     * Merges field annotations into a mutable map of annotations.
     * The field annotations will override existing annotations in the map.
     */
    fun mergeFieldAnnotations(
        targetAnnotations: MutableMap<String, String?>,
        fieldAnnotations: FieldAnnotationOverrides
    ) {
        if (fieldAnnotations.propertyName != null) {
            targetAnnotations[AnnotationConstants.PROPERTY_NAME] = fieldAnnotations.propertyName
        }
        if (fieldAnnotations.propertyType != null) {
            targetAnnotations[AnnotationConstants.PROPERTY_TYPE] = fieldAnnotations.propertyType
        }
        if (fieldAnnotations.adapter == true) {
            targetAnnotations[AnnotationConstants.ADAPTER] = null // adapter annotation has no value
        }
        if (fieldAnnotations.nullable == true) {
            targetAnnotations[AnnotationConstants.NULLABLE] = null
        }
        if (fieldAnnotations.nonNull == true) {
            targetAnnotations[AnnotationConstants.NON_NULL] = null
        }
    }
}
