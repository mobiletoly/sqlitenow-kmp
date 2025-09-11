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
    const val REMOVE_ALIAS_PREFIX = "removeAliasPrefix"
    const val MAPPING_TYPE = "mappingType"
    const val SOURCE_TABLE = "sourceTable"

    // Statement-level annotations
    const val NAME = "name"
    const val PROPERTY_NAME_GENERATOR = "propertyNameGenerator"
    const val SHARED_RESULT = "sharedResult"
    const val IMPLEMENTS = "implements"
    const val EXCLUDE_OVERRIDE_FIELDS = "excludeOverrideFields"
    const val ENABLE_SYNC = "enableSync"

    // Common field- and statement-level annotations
    const val COLLECTION_KEY = "collectionKey"

    // Internal annotations (used for processing)
    const val IS_DYNAMIC_FIELD = "_isDynamicField"

    // Values
    const val ADAPTER_DEFAULT = "default"
    const val ADAPTER_CUSTOM = "custom"

    const val MAPPING_TYPE_PER_ROW = "perRow"
    const val MAPPING_TYPE_COLLECTION = "collection"
}
