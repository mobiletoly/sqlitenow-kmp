package dev.goquick.sqlitenow.gradle

object AnnotationConstants {
    
    // Field-level annotations
    const val ADAPTER = "adapter"
    const val NON_NULL = "nonNull"
    const val NULLABLE = "nullable"
    const val PROPERTY_NAME = "propertyName"
    const val PROPERTY_TYPE = "propertyType"
    const val FIELD = "field"
    
    // Statement-level annotations
    const val NAME = "name"
    const val PROPERTY_NAME_GENERATOR = "propertyNameGenerator"
    const val SHARED_RESULT = "sharedResult"
    const val IMPLEMENTS = "implements"
    const val EXCLUDE_OVERRIDE_FIELDS = "excludeOverrideFields"
}
