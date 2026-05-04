/*
 * Copyright 2025 Toly Pochkin
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
package dev.goquick.sqlitenow.gradle.model

import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement

data class AnnotatedSelectStatement(
    override val name: String,
    val src: SelectStatement,
    override val annotations: StatementAnnotationOverrides,
    val fields: List<Field>,
    val sourceFile: String? = null,
) : AnnotatedStatement {
    data class Field(
        val src: SelectStatement.FieldSource,
        val annotations: FieldAnnotationOverrides,
        val aliasPath: List<String> = emptyList()
    )

    val mappingPlan: ResultMappingPlan by lazy(LazyThreadSafetyMode.NONE) {
        ResultMappingPlanner.create(src, fields)
    }

    fun hasDynamicFieldMapping() = fields.any {
        it.annotations.isDynamicField && it.annotations.mappingType != null
    }

    fun hasCollectionMapping() = fields.any {
        it.annotations.isDynamicField && when (AnnotationConstants.MappingType.fromString(it.annotations.mappingType)) {
            AnnotationConstants.MappingType.COLLECTION -> true
            AnnotationConstants.MappingType.PER_ROW,
            AnnotationConstants.MappingType.ENTITY -> false
        }
    }
}
