package dev.goquick.sqlitenow.gradle

/**
 * Utility helpers for reasoning about dynamic field annotations.
 */
object DynamicFieldUtils {

    /**
     * Computes the set of dynamic field names that should be skipped when generating top-level
     * properties because their data is materialised via a collection mapping.
     */
    fun computeSkipSet(fields: List<AnnotatedSelectStatement.Field>): Set<String> {
        if (fields.isEmpty()) return emptySet()

        val collectionAliasPaths = fields.asSequence()
            .filter { it.annotations.isDynamicField && it.annotations.mappingType != null }
            .mapNotNull { field ->
                if (AnnotationConstants.MappingType.fromString(field.annotations.mappingType) == AnnotationConstants.MappingType.COLLECTION) {
                    field.aliasPath.takeIf { it.isNotEmpty() }?.map { it.lowercase() }
                } else {
                    null
                }
            }
            .toList()

        if (collectionAliasPaths.isEmpty()) return emptySet()

        return fields.asSequence()
            .filter { it.annotations.isDynamicField && it.annotations.mappingType != null }
            .filter { field -> field.aliasPath.isNotEmpty() }
            .mapNotNull { field ->
                val aliasPathLower = field.aliasPath.map { it.lowercase() }
                val shouldSkip = collectionAliasPaths.any { collectionPath ->
                    aliasPathLower.startsWithPath(collectionPath) && aliasPathLower.size > collectionPath.size
                }
                if (shouldSkip) field.src.fieldName else null
            }
            .toSet()
    }

    private fun List<String>.startsWithPath(prefix: List<String>): Boolean {
        if (prefix.isEmpty() || prefix.size > size) return false
        prefix.indices.forEach { idx ->
            if (this[idx] != prefix[idx]) return false
        }
        return true
    }

    fun isNestedAlias(fieldName: String, aliasPrefix: String?): Boolean {
        if (aliasPrefix.isNullOrBlank()) return false
        if (fieldName.startsWith(aliasPrefix)) return false
        val idx = fieldName.indexOf(aliasPrefix)
        if (idx <= 0) return false
        val preceding = fieldName.getOrNull(idx - 1)
        return preceding == '_'
    }
}
