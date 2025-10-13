/*
 * Copyright 2025 Anatoliy Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.gradle.processing

import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.util.AliasPathUtils

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
                    field.aliasPath.takeIf { it.isNotEmpty() }?.let { AliasPathUtils.lowercase(it) }
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
                val aliasPathLower = AliasPathUtils.lowercase(field.aliasPath)
                val shouldSkip = collectionAliasPaths.any { collectionPath ->
                    AliasPathUtils.startsWith(aliasPathLower, collectionPath) && aliasPathLower.size > collectionPath.size
                }
                if (shouldSkip) field.src.fieldName else null
            }
            .toSet()
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
