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
        targetAnnotations: MutableMap<String, Any?>,
        fieldAnnotations: FieldAnnotationOverrides
    ) {
        if (fieldAnnotations.propertyName != null) {
            targetAnnotations[AnnotationConstants.PROPERTY_NAME] = fieldAnnotations.propertyName
        }
        if (fieldAnnotations.propertyType != null) {
            targetAnnotations[AnnotationConstants.PROPERTY_TYPE] = fieldAnnotations.propertyType
        }
        if (fieldAnnotations.adapter == true) {
            targetAnnotations[AnnotationConstants.ADAPTER] = "custom" // custom adapter
        }
        if (fieldAnnotations.notNull != null) {
            targetAnnotations[AnnotationConstants.NOT_NULL] = fieldAnnotations.notNull
        }
        if (fieldAnnotations.isDynamicField) {
            targetAnnotations[AnnotationConstants.IS_DYNAMIC_FIELD] = true
        }
        if (fieldAnnotations.defaultValue != null) {
            targetAnnotations[AnnotationConstants.DEFAULT_VALUE] = fieldAnnotations.defaultValue
        }
    }
}
