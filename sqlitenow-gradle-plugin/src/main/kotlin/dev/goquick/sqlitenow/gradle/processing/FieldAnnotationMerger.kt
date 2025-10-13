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
            targetAnnotations[AnnotationConstants.ADAPTER] = AnnotationConstants.ADAPTER_CUSTOM // custom adapter
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
        if (fieldAnnotations.aliasPrefix != null) {
            targetAnnotations[AnnotationConstants.ALIAS_PREFIX] = fieldAnnotations.aliasPrefix
        }
        if (fieldAnnotations.mappingType != null) {
            targetAnnotations[AnnotationConstants.MAPPING_TYPE] = fieldAnnotations.mappingType
        }
        if (fieldAnnotations.sourceTable != null) {
            targetAnnotations[AnnotationConstants.SOURCE_TABLE] = fieldAnnotations.sourceTable
        }
        if (fieldAnnotations.sqlTypeHint != null) {
            targetAnnotations[AnnotationConstants.SQL_TYPE_HINT] = fieldAnnotations.sqlTypeHint
        }
    }
}
