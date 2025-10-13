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

import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement

/**
 * Helper class for processing and filtering SQL statements.
 * Eliminates duplicate statement filtering logic across generator classes.
 */
class StatementProcessor(private val allStatements: List<AnnotatedStatement>) {

    /**
     * Lazily filtered SELECT statements.
     * Cached to avoid repeated filtering operations.
     */
    val selectStatements: List<AnnotatedSelectStatement> by lazy {
        allStatements.filterIsInstance<AnnotatedSelectStatement>()
    }

    /**
     * Lazily filtered EXECUTE statements (INSERT/DELETE).
     * Cached to avoid repeated filtering operations.
     */
    val executeStatements: List<AnnotatedExecuteStatement> by lazy {
        allStatements.filterIsInstance<AnnotatedExecuteStatement>()
    }

    /**
     * Processes both SELECT and EXECUTE statements with a unified callback approach.
     * Eliminates the need for separate forEach loops in generator classes.
     *
     * @param onSelectStatement Callback for processing SELECT statements
     * @param onExecuteStatement Callback for processing EXECUTE statements
     */
    fun processStatements(
        onSelectStatement: (AnnotatedSelectStatement) -> Unit,
        onExecuteStatement: (AnnotatedExecuteStatement) -> Unit
    ) {
        selectStatements.forEach(onSelectStatement)
        executeStatements.forEach(onExecuteStatement)
    }
}
