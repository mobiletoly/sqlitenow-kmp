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
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationContext
import dev.goquick.sqlitenow.gradle.processing.extractAnnotations
import dev.goquick.sqlitenow.gradle.processing.extractFieldAssociatedAnnotations
import dev.goquick.sqlitenow.gradle.processing.parseNotNullValue
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateTableStatement
import dev.goquick.sqlitenow.gradle.util.CaseInsensitiveMap

data class AnnotatedCreateTableStatement(
    override val name: String,
    val src: CreateTableStatement,
    override val annotations: StatementAnnotationOverrides,
    val columns: List<Column>
) : AnnotatedStatement {

    private val columnLookup = CaseInsensitiveMap(columns.map { it.src.name to it })

    data class Column(
        val src: CreateTableStatement.Column,
        val annotations: Map<String, Any?>
    ) {
        private val annotationNotNull: Boolean? = annotations[AnnotationConstants.NOT_NULL]?.let {
            parseNotNullValue(it)
        }

        /**
         * Determines Kotlin-side nullability for generated properties/params.
         * Respects explicit annotation overrides; otherwise falls back to table schema.
         */
        fun isNullable(): Boolean {
            return when (annotationNotNull) {
                true -> false
                false -> true
                null -> !src.notNull
            }
        }

        /**
         * Determines the SQL nullability used for adapter return types and parameter binding.
         * Explicit notNull=true enforces non-null, but notNull=false does not relax a NOT NULL column.
         */
        fun isSqlNullable(): Boolean {
            return when (annotationNotNull) {
                true -> false
                else -> !src.notNull
            }
        }
    }

    /** Finds a column by name (case-insensitive); null if not found */
    fun findColumnByName(columnName: String): Column? {
        return columnLookup[columnName]
    }

    companion object {
        fun parse(
            name: String,
            createTableStatement: CreateTableStatement,
            topComments: List<String>,
            innerComments: List<String>,
        ): AnnotatedCreateTableStatement {
            val tableAnnotations = StatementAnnotationOverrides.Companion.parse(
                extractAnnotations(topComments),
                context = StatementAnnotationContext.CREATE_TABLE
            )
            val fieldAnnotations = extractFieldAssociatedAnnotations(innerComments)

            return AnnotatedCreateTableStatement(
                name = name,
                src = createTableStatement,
                annotations = tableAnnotations,
                columns = createTableStatement.columns.map { column ->
                    Column(
                        src = column,
                        annotations = fieldAnnotations[column.name] ?: emptyMap()
                    )
                }
            )
        }
    }
}
