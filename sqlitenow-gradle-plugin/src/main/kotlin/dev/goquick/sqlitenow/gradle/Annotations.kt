package dev.goquick.sqlitenow.gradle

import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigParseOptions
import dev.goquick.sqlitenow.gradle.SqliteTypeToKotlinCodeConverter.Companion.KOTLIN_STDLIB_TYPES

fun extractAnnotations(comments: List<String>): Map<String, Any?> {
    val combinedComment = cleanedUpComments(comments)
    val allAnnotations = mutableMapOf<String, Any?>()

    val annotationBlocks = iterateBlockAnnotations(combinedComment)
    annotationBlocks.forEach { blockAnnotations ->
        allAnnotations.putAll(blockAnnotations)
    }
    return allAnnotations
}

private fun cleanedUpComments(comments: List<String>): String {
    val cleanedComments = comments.joinToString(" ") { comment ->
        // Remove comment markers (-- or /* */) and trim
        comment.replace(Regex("^\\s*--\\s*"), "").replace(Regex("/\\*|\\*/"), "").trim()
    }
    return cleanedComments
}

private fun iterateBlockAnnotations(content: String): List<Map<String, Any?>> {
    var searchStart = 0
    val allAnnotations = mutableListOf<Map<String, Any?>>()
    while (true) {
        val startIndex = content.indexOf("@@{", searchStart)
        if (startIndex == -1) break

        val endIndex = content.indexOf("}", startIndex)
        if (endIndex == -1) break

        val hoconContent = content.substring(startIndex + 3, endIndex).trim()
        val blockAnnotations = parseHoconAnnotations(hoconContent)

        // Merge annotations from this block
        allAnnotations.add(blockAnnotations)

        // Continue searching after this block
        searchStart = endIndex + 1
    }
    return allAnnotations
}

/**
 * Parses HOCON-style annotation content.
 * Example: field=birth_date, adapter=default, propertyType=kotlinx.datetime.LocalDate
 */
private fun parseHoconAnnotations(content: String): Map<String, Any?> {
    return try {
        // Wrap the content in braces to make it a valid HOCON object
        val hoconText = "{ $content }"

        val config = ConfigFactory.parseString(
            hoconText,
            ConfigParseOptions.defaults().setAllowMissing(false)
        )

        // Convert to our annotation format
        val annotations = mutableMapOf<String, Any?>()
        for (entry in config.entrySet()) {
            val key = entry.key
            val value = entry.value.unwrapped()

            annotations[key] = when (value) {
                is String, is Boolean, is List<*> -> value
                null -> null
                else -> value.toString()  // Convert other types to string
            }
        }

        annotations
    } catch (e: Exception) {
        logger.error(
            """|
            |Failed to parse HOCON annotations
            |---------------------------------
            |$content
            |---------------------------------
            """.trimMargin()
        )
        throw e
    }
}

/**
 * Extracts field-associated annotations from SQL comments using HOCON syntax.
 *
 * This function parses SQL comments to find HOCON-style annotations @@{...}
 * and associates them with specific fields when they have a field=column_name format.
 *
 * Example:
 * -- @@{field=user_id, nullable=true, adapter=default}
 *
 * @param comments List of SQL comment strings
 * @return Map where keys are field names and values are maps of annotations for that field
 */
fun extractFieldAssociatedAnnotations(comments: List<String>): Map<String, Map<String, Any?>> {
    val combinedComment = cleanedUpComments(comments)
    val annotationBlocks = iterateBlockAnnotations(combinedComment)

    val result = mutableMapOf<String, MutableMap<String, Any?>>()
    annotationBlocks.forEach { blockAnnotations ->
        // Every annotation block must have either 'field' or 'dynamicField' annotation with a non-empty value
        val fieldName = blockAnnotations[AnnotationConstants.FIELD] as? String
        val dynamicFieldName = blockAnnotations[AnnotationConstants.DYNAMIC_FIELD] as? String

        val targetFieldName = when {
            fieldName != null && fieldName.isNotBlank() && dynamicFieldName != null && dynamicFieldName.isNotBlank() -> {
                throw IllegalArgumentException("Annotation block cannot contain both 'field' and 'dynamicField' annotations")
            }

            fieldName != null && fieldName.isNotBlank() -> fieldName
            dynamicFieldName != null && dynamicFieldName.isNotBlank() -> dynamicFieldName
            else -> throw IllegalArgumentException("Annotation block must contain either a 'field' or 'dynamicField' annotation with a non-empty value")
        }

        val fieldAnnotations = blockAnnotations.toMutableMap()
        fieldAnnotations.remove(AnnotationConstants.FIELD)
        fieldAnnotations.remove(AnnotationConstants.DYNAMIC_FIELD)

        // For dynamicField annotations, propertyType is required
        if (dynamicFieldName != null) {
            val propertyType = fieldAnnotations[AnnotationConstants.PROPERTY_TYPE] as? String
            if (propertyType == null || propertyType.isBlank()) {
                throw IllegalArgumentException("dynamicField annotation requires a 'propertyType' annotation with a non-empty value")
            }
            // Mark this as a dynamic field for later processing
            fieldAnnotations[AnnotationConstants.IS_DYNAMIC_FIELD] = true
        }

        // Apply adapter logic: validate and normalize adapter values
        val propertyType = fieldAnnotations[AnnotationConstants.PROPERTY_TYPE] as? String
        val adapterValue = fieldAnnotations[AnnotationConstants.ADAPTER] as? String
        if (fieldAnnotations.containsKey(AnnotationConstants.ADAPTER)) {
            validateAdapterValue(adapterValue)
        }

        // Determine if custom adapter should be generated and update annotations accordingly
        if (shouldGenerateCustomAdapter(adapterValue, propertyType)) {
            fieldAnnotations[AnnotationConstants.ADAPTER] = AnnotationConstants.ADAPTER_CUSTOM
        } else {
            fieldAnnotations.remove(AnnotationConstants.ADAPTER)
        }

        result[targetFieldName] = fieldAnnotations
    }
    return result
}

/**
 * Validates an adapter annotation value and throws an exception if invalid.
 * Only "default" and "custom" are allowed values.
 */
private fun validateAdapterValue(adapterValue: String?) {
    if (adapterValue != AnnotationConstants.ADAPTER_CUSTOM && adapterValue != AnnotationConstants.ADAPTER_DEFAULT) {
        throw IllegalArgumentException("Adapter annotation can be adapter=\"default\" or adapter=\"custom\" only")
    }
}

/**
 * Determines the effective adapter behavior based on adapter value and property type.
 * Returns true if custom adapter should be generated, false otherwise.
 */
private fun shouldGenerateCustomAdapter(adapterValue: String?, propertyType: String?): Boolean {
    return when (adapterValue) {
        AnnotationConstants.ADAPTER_CUSTOM -> true
        AnnotationConstants.ADAPTER_DEFAULT -> {
            // adapter=default: use custom adapter only if propertyType is custom
            propertyType != null && isCustomType(propertyType)
        }

        null -> {
            // No adapter specified: same as adapter=default
            propertyType != null && isCustomType(propertyType)
        }

        else -> {
            // This should never happen due to validation, but just in case
            throw IllegalArgumentException("Unexpected adapter value: \"$adapterValue\"")
        }
    }
}

/**
 * Converts a value (Boolean or String) to a Boolean, with validation.
 * Used for parsing notNull annotation values from HOCON.
 */
internal fun parseNotNullValue(value: Any?): Boolean {
    return when (value) {
        is Boolean -> value
        is String -> when (value.lowercase()) {
            "true" -> true
            "false" -> false
            else -> throw IllegalArgumentException("Invalid notNull value: \"$value\". Must be true or false")
        }

        else -> throw IllegalArgumentException("Invalid notNull value type: ${value?.javaClass?.simpleName}. Must be boolean")
    }
}

/**
 * Determines if a property type is a custom type that needs an adapter.
 * Returns true for custom types, false for built-in Kotlin types.
 */
private fun isCustomType(propertyType: String): Boolean {
    // Extract the base type name (remove package, nullability, and generics)
    val baseType = extractBaseTypeName(propertyType)
    return baseType !in KOTLIN_STDLIB_TYPES
}

/**
 * Extracts the base type name from a property type string.
 * Examples:
 * - "kotlinx.datetime.LocalDate" -> "LocalDate"
 * - "kotlin.String?" -> "String"
 * - "List<String>" -> "List"
 * - "dev.example.CustomType" -> "CustomType"
 */
private fun extractBaseTypeName(propertyType: String): String {
    // Remove nullability marker
    val withoutNullability = propertyType.removeSuffix("?").trim()

    // Remove generics (everything after <)
    val withoutGenerics = withoutNullability.substringBefore('<').trim()

    // Get the simple class name (everything after the last dot)
    return withoutGenerics.substringAfterLast('.')
}

data class FieldAnnotationOverrides(
    val propertyName: String?,
    val propertyType: String?,
    val notNull: Boolean?,
    val adapter: Boolean?,
    val isDynamicField: Boolean = false,
    val defaultValue: String? = null,
    val removeAliasPrefix: String? = null,
    val mappingType: String? = null,
    val sourceTable: String? = null,
    val collectionKey: String? = null,
) {
    companion object {
        fun parse(annotations: Map<String, Any?>): FieldAnnotationOverrides {
            val propertyType = annotations[AnnotationConstants.PROPERTY_TYPE] as? String
            val adapterValue = annotations[AnnotationConstants.ADAPTER] as? String

            // Validate adapter value if present
            if (annotations.containsKey(AnnotationConstants.ADAPTER)) {
                validateAdapterValue(adapterValue)
            }

            // Validate mapping type annotations
            val mappingType = annotations[AnnotationConstants.MAPPING_TYPE] as? String
            val sourceTable = annotations[AnnotationConstants.SOURCE_TABLE] as? String
            val removeAliasPrefix = annotations[AnnotationConstants.REMOVE_ALIAS_PREFIX] as? String
            val collectionKey = annotations[AnnotationConstants.COLLECTION_KEY] as? String

            if (mappingType != null) {
                // When mappingType is specified, sourceTable is required
                if (sourceTable == null) {
                    throw IllegalArgumentException("When annotation '${AnnotationConstants.MAPPING_TYPE}' is specified, '${AnnotationConstants.SOURCE_TABLE}' is required")
                }

                // Validate mappingType value
                if (mappingType !in setOf(
                        AnnotationConstants.MAPPING_TYPE_PER_ROW,
                        AnnotationConstants.MAPPING_TYPE_COLLECTION
                    )
                ) {
                    throw IllegalArgumentException(
                        "Invalid annotation '${AnnotationConstants.MAPPING_TYPE}' value: '$mappingType'. " +
                                "Currently supported: '${AnnotationConstants.MAPPING_TYPE_PER_ROW}', '${AnnotationConstants.MAPPING_TYPE_COLLECTION}'"
                    )
                }

                // When mappingType=collection, collectionKey is required
                if (mappingType == AnnotationConstants.MAPPING_TYPE_COLLECTION) {
                    if (collectionKey == null || collectionKey.isBlank()) {
                        throw IllegalArgumentException("When annotation '${AnnotationConstants.MAPPING_TYPE}=collection' is specified, '${AnnotationConstants.COLLECTION_KEY}' is required")
                    }
                }

                // When mappingType is used, this should be a dynamic field
                if (annotations[AnnotationConstants.IS_DYNAMIC_FIELD] != true) {
                    throw IllegalArgumentException("Annotation '${AnnotationConstants.MAPPING_TYPE}' can only be used with dynamic fields")
                }
            }

            // Determine if custom adapter should be used based on new adapter system
            val adapter = shouldGenerateCustomAdapter(adapterValue, propertyType)

            // Parse notNull boolean annotation using HOCON's hasPath() for proper null detection
            val notNull = if (annotations.containsKey(AnnotationConstants.NOT_NULL)) {
                parseNotNullValue(annotations[AnnotationConstants.NOT_NULL])
            } else {
                null // Not specified - inherit from table structure
            }

            return FieldAnnotationOverrides(
                propertyName = annotations[AnnotationConstants.PROPERTY_NAME] as? String,
                propertyType = propertyType,
                notNull = notNull,
                adapter = adapter,
                isDynamicField = annotations[AnnotationConstants.IS_DYNAMIC_FIELD] as? Boolean ?: false,
                defaultValue = annotations[AnnotationConstants.DEFAULT_VALUE] as? String,
                removeAliasPrefix = removeAliasPrefix,
                mappingType = mappingType,
                sourceTable = sourceTable,
                collectionKey = collectionKey,
            )
        }
    }
}

enum class PropertyNameGeneratorType {
    PLAIN,
    LOWER_CAMEL_CASE;

    /**
     * Converts a parameter/field name to a property name based on this generator type.
     * This centralizes the property name conversion logic to avoid duplication.
     */
    fun convertToPropertyName(name: String): String {
        return when (this) {
            PLAIN -> name
            LOWER_CAMEL_CASE -> {
                name.split('_').mapIndexed { index, part ->
                    if (index == 0) part else part.capitalized()
                }.joinToString("")
            }
        }
    }
}

data class StatementAnnotationOverrides(
    val name: String?,
    val propertyNameGenerator: PropertyNameGeneratorType,
    val sharedResult: String?,
    val implements: String?,
    val excludeOverrideFields: Set<String>?,
    val collectionKey: String?,
    val enableSync: Boolean = false,
    val syncKeyColumnName: String? = null
) {
    companion object {
        fun parse(annotations: Map<String, Any?>): StatementAnnotationOverrides {
            val name = annotations[AnnotationConstants.NAME] as? String
            val propertyNameGenerator = annotations[AnnotationConstants.PROPERTY_NAME_GENERATOR] as? String
            val sharedResult = annotations[AnnotationConstants.SHARED_RESULT] as? String
            val implements = annotations[AnnotationConstants.IMPLEMENTS] as? String
            val excludeOverrideFields = annotations[AnnotationConstants.EXCLUDE_OVERRIDE_FIELDS]
            val collectionKey = annotations[AnnotationConstants.COLLECTION_KEY] as? String
            val enableSync = annotations[AnnotationConstants.ENABLE_SYNC] as? Boolean ?: false
            val syncKeyColumnName = annotations[AnnotationConstants.SYNC_KEY_COLUMN_NAME] as? String

            if (annotations.containsKey(AnnotationConstants.NAME) && name?.isBlank() == true) {
                throw IllegalArgumentException("Annotation '${AnnotationConstants.NAME}' cannot be blank")
            }
            if (annotations.containsKey(AnnotationConstants.PROPERTY_NAME_GENERATOR) && propertyNameGenerator?.isBlank() == true) {
                throw IllegalArgumentException("Annotation '${AnnotationConstants.PROPERTY_NAME_GENERATOR}' cannot be blank")
            }
            if (annotations.containsKey(AnnotationConstants.SHARED_RESULT) && sharedResult?.isBlank() == true) {
                throw IllegalArgumentException("Annotation '${AnnotationConstants.SHARED_RESULT}' cannot be blank")
            }
            if (annotations.containsKey(AnnotationConstants.IMPLEMENTS) && implements?.isBlank() == true) {
                throw IllegalArgumentException("Annotation '${AnnotationConstants.IMPLEMENTS}' cannot be blank")
            }

            return StatementAnnotationOverrides(
                name = name,
                propertyNameGenerator = propertyNameGenerator.parsePropertyNameGeneratorType(),
                sharedResult = sharedResult,
                implements = implements,
                excludeOverrideFields = parseExcludeOverrideFieldsFromHocon(excludeOverrideFields),
                collectionKey = collectionKey,
                enableSync = enableSync,
                syncKeyColumnName = syncKeyColumnName
            )
        }

        private fun parseExcludeOverrideFieldsFromHocon(excludeFields: Any?): Set<String>? {
            return when (excludeFields) {
                is List<*> -> {
                    // HOCON parsed it as a native list - use it directly
                    excludeFields.filterIsInstance<String>().toSet()
                }

                null -> null
                else -> throw IllegalArgumentException("excludeOverrideFields must be a list, got: ${excludeFields::class.simpleName}")
            }
        }

        private fun String?.parsePropertyNameGeneratorType(): PropertyNameGeneratorType {
            if (this == null || this == "lowerCamelCase") return PropertyNameGeneratorType.LOWER_CAMEL_CASE
            if (this == "plain") return PropertyNameGeneratorType.PLAIN
            throw IllegalArgumentException(
                "Unsupported propertyNameGenerator value: '$this' (must be 'plain' and 'lowerCamelCase')"
            )
        }
    }
}
