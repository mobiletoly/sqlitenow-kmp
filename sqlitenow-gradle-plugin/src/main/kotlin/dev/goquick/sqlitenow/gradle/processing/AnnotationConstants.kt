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

object AnnotationConstants {

    // Field-level annotations
    const val ADAPTER = "adapter"
    const val NOT_NULL = "notNull"
    const val PROPERTY_NAME = "propertyName"
    const val PROPERTY_TYPE = "propertyType"
    const val FIELD = "field"
    const val DYNAMIC_FIELD = "dynamicField"
    const val DEFAULT_VALUE = "defaultValue"
    const val ALIAS_PREFIX = "aliasPrefix"
    const val MAPPING_TYPE = "mappingType"
    const val SOURCE_TABLE = "sourceTable"
    const val SQL_TYPE_HINT = "sqlTypeHint"

    // Statement-level annotations
    const val NAME = "name"
    const val PROPERTY_NAME_GENERATOR = "propertyNameGenerator"
    const val QUERY_RESULT = "queryResult"
    const val ENABLE_SYNC = "enableSync"
    const val SYNC_KEY_COLUMN_NAME = "syncKeyColumnName"
    const val MAP_TO = "mapTo"

    // Common field- and statement-level annotations
    const val COLLECTION_KEY = "collectionKey"

    // Internal annotations (used for processing)
    const val IS_DYNAMIC_FIELD = "_isDynamicField"

    // Values
    const val ADAPTER_DEFAULT = "default"
    const val ADAPTER_CUSTOM = "custom"

    const val MAPPING_TYPE_PER_ROW = "perRow"
    const val MAPPING_TYPE_COLLECTION = "collection"
    const val MAPPING_TYPE_ENTITY = "entity"

    enum class MappingType(val value: String) {
        PER_ROW("perRow"),
        COLLECTION("collection"),
        ENTITY("entity");

        companion object {
            fun fromString(value: String?): MappingType {
                return entries.find { it.value == value }
                    ?: throw IllegalArgumentException("Unknown mapping type: '$value'. Valid types are: ${entries.map { it.value }}")
            }
        }
    }
}
