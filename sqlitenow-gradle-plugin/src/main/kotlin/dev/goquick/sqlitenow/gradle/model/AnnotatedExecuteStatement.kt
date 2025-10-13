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

import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationContext
import dev.goquick.sqlitenow.gradle.processing.extractAnnotations
import dev.goquick.sqlitenow.gradle.sqlinspect.ExecuteStatement

data class AnnotatedExecuteStatement(
    override val name: String,
    val src: ExecuteStatement,
    override val annotations: StatementAnnotationOverrides,
) : AnnotatedStatement {

    /**
     * Returns true if this is an INSERT, UPDATE, or DELETE statement with a RETURNING clause
     */
    fun hasReturningClause(): Boolean = src.hasReturningClause

    /**
     * Returns the list of columns in the RETURNING clause, or empty list if none
     */
    fun getReturningColumns(): List<String> = src.returningColumns

    companion object {
        fun parse(
            name: String,
            execStatement: ExecuteStatement,
            topComments: List<String>,
        ): AnnotatedExecuteStatement {
            val statementAnnotations = StatementAnnotationOverrides.Companion.parse(
                extractAnnotations(topComments),
                context = StatementAnnotationContext.EXECUTE
            )
            return AnnotatedExecuteStatement(
                name = name,
                src = execStatement,
                annotations = statementAnnotations,
            )
        }
    }
}
