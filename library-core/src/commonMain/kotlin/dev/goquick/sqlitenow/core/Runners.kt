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
package dev.goquick.sqlitenow.core

import kotlinx.coroutines.flow.Flow

/**
 * Reactive facade returned for generated `SELECT` statements.
 *
 * Each runner operates on a specific statement instance and ensures result mapping stays in sync
 * with the generated data classes.
 *
 * @param T projected result type produced by the statement.
 */

interface SelectRunners<T : Any> {
    /**
     * Executes the statement and returns every row.
     */
    suspend fun asList(): List<T>

    /**
     * Executes the statement and asserts exactly one row is returned.
     *
     * @throws IllegalStateException when the query returns zero or more than one row.
     */
    suspend fun asOne(): T

    /**
     * Executes the statement and returns the single row or `null` when no rows match.
     *
     * @throws IllegalStateException when more than one row is returned.
     */
    suspend fun asOneOrNull(): T?

    /**
     * Observes the statement as a cold [Flow] that re-emits whenever affected tables change.
     */
    fun asFlow(): Flow<List<T>>
}

/** Wrapper emitted for non-returning execute statements (INSERT/UPDATE/DELETE). */
class ExecuteStatement<Params>(
    private val executeBlock: suspend (Params) -> Unit,
) {
    /**
     * Executes the statement with an existing [Params] instance.
     */
    suspend operator fun invoke(params: Params) = executeBlock(params)

    /**
     * Alias for [invoke] to emphasize imperative semantics when needed.
     */
    suspend fun run(params: Params) = executeBlock(params)
}

/**
 * Wrapper emitted for execute statements that include a `RETURNING` clause.
 *
 * @param Params generated parameter data class for the statement.
 * @param Result projection generated for returned rows.
 * @property listBlock executes the statement and returns all rows.
 * @property oneBlock executes the statement and asserts exactly one row.
 * @property oneOrNullBlock executes the statement and returns one row or `null`.
 */
class ExecuteReturningStatement<Params, Result>(
    private val listBlock: suspend (Params) -> List<Result>,
    private val oneBlock: suspend (Params) -> Result,
    private val oneOrNullBlock: suspend (Params) -> Result?,
) {
    /**
     * Executes the statement and returns every row.
     */
    suspend operator fun invoke(params: Params): List<Result> = listBlock(params)

    /**
     * Executes the statement and returns every row.
     */
    suspend fun list(params: Params): List<Result> = listBlock(params)

    /**
     * Executes the statement and returns exactly one row.
     *
     * @throws IllegalStateException when zero or multiple rows are returned.
     */
    suspend fun one(params: Params): Result = oneBlock(params)

    /**
     * Executes the statement and returns a single row or `null`.
     *
     * @throws IllegalStateException when more than one row is returned.
     */
    suspend fun oneOrNull(params: Params): Result? = oneOrNullBlock(params)

    /**
     * Executes the statement and returns every row. Synonym for [list].
     */
    suspend fun run(params: Params): List<Result> = list(params)
}
