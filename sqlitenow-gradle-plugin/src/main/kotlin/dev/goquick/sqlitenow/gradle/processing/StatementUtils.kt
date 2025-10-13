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

import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement

/**
 * Utility class for common statement operations.
 */
object StatementUtils {

    /**
     * Extracts named parameters from any type of statement.
     */
    fun getNamedParameters(statement: AnnotatedStatement): List<String> {
        return when (statement) {
            is AnnotatedSelectStatement -> statement.src.namedParameters
            is AnnotatedExecuteStatement -> statement.src.namedParameters
            is AnnotatedCreateTableStatement -> statement.src.namedParameters
            is AnnotatedCreateViewStatement -> statement.src.namedParameters
        }
    }

    /**
     * Extracts all named parameters including those from WITH clauses.
     * This is used for comprehensive parameter collection in data class generation.
     */
    fun getAllNamedParameters(statement: AnnotatedStatement): Set<String> {
        val allNamedParameters = mutableSetOf<String>()

        // Add basic named parameters
        allNamedParameters.addAll(getNamedParameters(statement))

        // Add parameters from WITH clauses for execute statements
        if (statement is AnnotatedExecuteStatement) {
            statement.src.withSelectStatements.forEach { withSelectStatement ->
                allNamedParameters.addAll(withSelectStatement.namedParameters)
            }
        }

        return allNamedParameters
    }
}
