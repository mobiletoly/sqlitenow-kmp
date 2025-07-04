package dev.goquick.sqlitenow.gradle

import org.gradle.internal.extensions.stdlib.capitalized

fun extractAnnotations(comments: List<String>): Map<String, String?> {
    return comments.flatMap { comment ->
        extractAnnotationsFromComment(comment)
    }.toMap()
}

private fun extractAnnotationsFromComment(comment: String): List<Pair<String, String?>> {
    val annotations = mutableListOf<Pair<String, String?>>()

    // Split on spaces first, then handle bracket values specially
    val tokens = comment.split(" ")
    var i = 0

    while (i < tokens.size) {
        val token = tokens[i]
        if (!token.startsWith("@@")) {
            i++
            continue
        }

        val annotationPart = token.substring(2) // Remove @@

        // Check if this annotation has a bracket value that might span multiple tokens
        val equalIndex = annotationPart.indexOf('=')
        if (equalIndex != -1) {
            val key = annotationPart.substring(0, equalIndex)
            var value = annotationPart.substring(equalIndex + 1)

            // If value starts with '[' but doesn't end with ']', collect more tokens
            if (value.startsWith('[') && !value.endsWith(']')) {
                val valueTokens = mutableListOf(value)
                i++
                while (i < tokens.size && !valueTokens.last().endsWith(']')) {
                    valueTokens.add(tokens[i])
                    i++
                }
                value = valueTokens.joinToString(" ")
            }

            annotations.add(key to value)
        } else {
            // Annotation without value
            annotations.add(annotationPart to "")
        }

        i++
    }

    return annotations
}

/**
 * Extracts field-associated annotations from SQL comments.
 *
 * This function parses SQL comments to find annotations that start with @@
 * and associates them with specific fields when they have a @@field=column_name format.
 *
 * Example:
 * -- @@field=user_id @@nullable
 * -- @@field=username @@unique
 *
 * @param comments List of SQL comment strings
 * @return Map where keys are field names and values are maps of annotations for that field
 */
fun extractFieldAssociatedAnnotations(comments: List<String>): Map<String, Map<String, String?>> {
    val result = mutableMapOf<String, MutableMap<String, String?>>()
    var currentField: String? = null

    // Process each comment line
    for (comment in comments) {
        // Extract annotations (words starting with @@)
        val annotations = comment.split("\\s+".toRegex())
            .filter { it.startsWith("@@") }
            .map { it.substring(2) } // Remove @@ prefix

        if (annotations.isEmpty()) continue

        // Check if this comment has a field annotation
        val fieldAnnotation = annotations.find { it.startsWith("${AnnotationConstants.FIELD}=") }

        if (fieldAnnotation != null) {
            // Extract field name from the field annotation
            currentField = fieldAnnotation.substring(6)

            // Create an entry for this field if it doesn't exist
            if (!result.containsKey(currentField)) {
                result[currentField] = mutableMapOf()
            }

            // Process other annotations for this field
            annotations.filter { it != fieldAnnotation }
                .forEach { annotation ->
                    val parts = annotation.split("=", limit = 2)
                    val annotationName = parts[0]
                    val annotationValue = if (parts.size > 1) parts[1] else null

                    result[currentField]!![annotationName] = annotationValue
                }
        } else if (currentField != null) {
            // If no field annotation in this comment, use the current field
            annotations.forEach { annotation ->
                val parts = annotation.split("=", limit = 2)
                val annotationName = parts[0]
                val annotationValue = if (parts.size > 1) parts[1] else null

                result[currentField]!![annotationName] = annotationValue
            }
        }
    }

    return result
}

data class FieldAnnotationOverrides(
    val propertyName: String?,
    val propertyType: String?,
    val nonNull: Boolean?,
    val nullable: Boolean?,
    val adapter: Boolean?,
) {
    companion object {
        fun parse(annotations: Map<String, String?>): FieldAnnotationOverrides {
            return FieldAnnotationOverrides(
                propertyName = annotations[AnnotationConstants.PROPERTY_NAME],
                propertyType = annotations[AnnotationConstants.PROPERTY_TYPE],
                nonNull = annotations.containsKey(AnnotationConstants.NON_NULL),
                nullable = annotations.containsKey(AnnotationConstants.NULLABLE),
                adapter = annotations.containsKey(AnnotationConstants.ADAPTER),
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
    val excludeOverrideFields: Set<String>?
) {
    companion object {
        fun parse(annotations: Map<String, String?>): StatementAnnotationOverrides {
            if (annotations.containsKey(AnnotationConstants.NAME) && annotations[AnnotationConstants.NAME]!!.isBlank()) {
                throw IllegalArgumentException("Annotation @@${AnnotationConstants.NAME} cannot be blank")
            }
            if (annotations.containsKey(AnnotationConstants.PROPERTY_NAME_GENERATOR) && annotations[AnnotationConstants.PROPERTY_NAME_GENERATOR]!!.isBlank()) {
                throw IllegalArgumentException("Annotation @@${AnnotationConstants.PROPERTY_NAME_GENERATOR} cannot be blank")
            }
            if (annotations.containsKey(AnnotationConstants.SHARED_RESULT) && annotations[AnnotationConstants.SHARED_RESULT]!!.isBlank()) {
                throw IllegalArgumentException("Annotation @@${AnnotationConstants.SHARED_RESULT} cannot be blank")
            }
            if (annotations.containsKey(AnnotationConstants.IMPLEMENTS) && annotations[AnnotationConstants.IMPLEMENTS]!!.isBlank()) {
                throw IllegalArgumentException("Annotation @@${AnnotationConstants.IMPLEMENTS} cannot be blank")
            }
            return StatementAnnotationOverrides(
                name = annotations[AnnotationConstants.NAME],
                propertyNameGenerator = annotations[AnnotationConstants.PROPERTY_NAME_GENERATOR].parsePropertyNameGeneratorType(),
                sharedResult = annotations[AnnotationConstants.SHARED_RESULT],
                implements = annotations[AnnotationConstants.IMPLEMENTS],
                excludeOverrideFields = annotations[AnnotationConstants.EXCLUDE_OVERRIDE_FIELDS]?.let { parseExcludeOverrideFields(it) }
            )
        }

        private fun parseExcludeOverrideFields(excludeFieldsStr: String): Set<String> {
            // Remove square brackets if present: [phone, birthDate] -> phone, birthDate
            val cleanedStr = excludeFieldsStr.trim()
                .removePrefix("[")
                .removeSuffix("]")
                .trim()

            val strs = cleanedStr.split(",")
                .map { it.trim() }  // This already trims each field name
                .filter { it.isNotEmpty() }
                .toSet()
            return strs
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
