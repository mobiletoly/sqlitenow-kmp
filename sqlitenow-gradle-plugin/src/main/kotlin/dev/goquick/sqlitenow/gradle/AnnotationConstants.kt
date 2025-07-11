package dev.goquick.sqlitenow.gradle

object AnnotationConstants {

    // Field-level annotations
    const val ADAPTER = "adapter"
    const val NOT_NULL = "notNull"
    const val PROPERTY_NAME = "propertyName"
    const val PROPERTY_TYPE = "propertyType"
    const val FIELD = "field"
    const val DYNAMIC_FIELD = "dynamicField"
    const val DEFAULT_VALUE = "defaultValue"

    // Internal annotations (used for processing)
    const val IS_DYNAMIC_FIELD = "_isDynamicField"

    // Statement-level annotations
    const val NAME = "name"
    const val PROPERTY_NAME_GENERATOR = "propertyNameGenerator"
    const val SHARED_RESULT = "sharedResult"
    const val IMPLEMENTS = "implements"
    const val EXCLUDE_OVERRIDE_FIELDS = "excludeOverrideFields"
}
