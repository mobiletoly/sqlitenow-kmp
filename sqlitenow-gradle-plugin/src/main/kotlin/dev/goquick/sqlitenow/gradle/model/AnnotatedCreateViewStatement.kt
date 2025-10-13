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
package dev.goquick.sqlitenow.gradle.model

import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationContext
import dev.goquick.sqlitenow.gradle.processing.extractAnnotations
import dev.goquick.sqlitenow.gradle.processing.extractFieldAssociatedAnnotations
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateViewStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement

/**
 * Represents a CREATE VIEW statement with annotations extracted from SQL comments.
 * Views are treated similarly to tables but represent stored queries rather than physical tables.
 */
data class AnnotatedCreateViewStatement(
    override val name: String,
    val src: CreateViewStatement,
    override val annotations: StatementAnnotationOverrides,
    val fields: List<Field>,
    val dynamicFields: List<DynamicField>
) : AnnotatedStatement {
    data class Field(
        val src: SelectStatement.FieldSource,
        val annotations: FieldAnnotationOverrides
    )
    data class DynamicField(
        val name: String,
        val annotations: FieldAnnotationOverrides
    )
    companion object {
        fun parse(
            name: String,
            createViewStatement: CreateViewStatement,
            topComments: List<String>,
            innerComments: List<String>,
        ): AnnotatedCreateViewStatement {
            val viewAnnotations = StatementAnnotationOverrides.Companion.parse(
                extractAnnotations(topComments),
                context = StatementAnnotationContext.CREATE_VIEW
            )
            val fieldAnnotations = extractFieldAssociatedAnnotations(innerComments)
            // Collect dynamic field annotations declared at VIEW level
            val dynamicFields = mutableListOf<DynamicField>()
            fieldAnnotations.forEach { (fieldName, ann) ->
                if (ann[AnnotationConstants.IS_DYNAMIC_FIELD] == true) {
                    val overrides = FieldAnnotationOverrides.Companion.parse(ann)
                    dynamicFields += DynamicField(name = fieldName, annotations = overrides)
                }
            }
            return AnnotatedCreateViewStatement(
                name = name,
                src = createViewStatement,
                annotations = viewAnnotations,
                fields = createViewStatement.selectStatement.fields.map { field ->
                    val annotations = FieldAnnotationOverrides.Companion.parse(
                        fieldAnnotations[field.fieldName] ?: emptyMap()
                    )
                    Field(
                        src = field,
                        annotations = annotations
                    )
                },
                dynamicFields = dynamicFields
            )
        }
    }
}
