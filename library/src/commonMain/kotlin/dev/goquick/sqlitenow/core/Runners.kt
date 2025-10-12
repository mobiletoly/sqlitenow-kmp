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

/**
 * Wrapper emitted for non-returning execute statements (INSERT/UPDATE/DELETE).
 *
 * @param Params generated parameter data class for the statement.
 * @param Builder builder DSL that constructs [Params]; for statements without parameters this is `Unit`.
 * @property executeBlock underlying suspend function that performs the statement.
 * @property buildFromBuilder optional factory that turns the DSL into [Params].
 */
class ExecuteStatement<Params, Builder>(
    private val executeBlock: suspend (Params) -> Unit,
    private val buildFromBuilder: ((Builder.() -> Unit) -> Params)? = null,
) {
    /**
     * Executes the statement with an existing [Params] instance.
     */
    suspend operator fun invoke(params: Params) = executeBlock(params)

    /**
     * Executes the statement using the builder DSL to construct [Params].
     *
     * @throws IllegalStateException when mandatory fields are missing from the builder.
     */
    suspend operator fun invoke(configure: Builder.() -> Unit) = executeBlock(build(configure))

    /**
     * Alias for [invoke] to emphasize imperative semantics when needed.
     */
    suspend fun run(params: Params) = executeBlock(params)

    /**
     * Executes the statement using the builder DSL. Equivalent to `invoke(configure)`.
     */
    suspend fun run(configure: Builder.() -> Unit) = invoke(configure)

    /**
     * Builds a [Params] instance from the builder DSL without executing the statement.
     *
     * @throws IllegalStateException when the statement does not support builder-style configuration.
     */
    fun build(configure: Builder.() -> Unit): Params = buildFromBuilder?.invoke(configure)
        ?: error("This statement does not support builder-based configuration.")
}

/**
 * Wrapper emitted for execute statements that include a `RETURNING` clause.
 *
 * @param Params generated parameter data class for the statement.
 * @param Result projection generated for returned rows.
 * @param Builder builder DSL that constructs [Params]; for statements without parameters this is `Unit`.
 * @property listBlock executes the statement and returns all rows.
 * @property oneBlock executes the statement and asserts exactly one row.
 * @property oneOrNullBlock executes the statement and returns one row or `null`.
 * @property buildFromBuilder optional factory that turns the DSL into [Params].
 */
class ExecuteReturningStatement<Params, Result, Builder>(
    private val listBlock: suspend (Params) -> List<Result>,
    private val oneBlock: suspend (Params) -> Result,
    private val oneOrNullBlock: suspend (Params) -> Result?,
    private val buildFromBuilder: ((Builder.() -> Unit) -> Params)? = null,
) {
    /**
     * Executes the statement and returns every row.
     */
    suspend operator fun invoke(params: Params): List<Result> = listBlock(params)

    /**
     * Executes the statement using the builder DSL and returns every row.
     *
     * @throws IllegalStateException when mandatory fields are missing from the builder.
     */
    suspend operator fun invoke(configure: Builder.() -> Unit): List<Result> =
        listBlock(build(configure))

    /**
     * Executes the statement and returns every row.
     */
    suspend fun list(params: Params): List<Result> = listBlock(params)

    /**
     * Executes the statement using the builder DSL and returns every row.
     *
     * @throws IllegalStateException when mandatory fields are missing from the builder.
     */
    suspend fun list(configure: Builder.() -> Unit): List<Result> = listBlock(build(configure))

    /**
     * Executes the statement and returns exactly one row.
     *
     * @throws IllegalStateException when zero or multiple rows are returned.
     */
    suspend fun one(params: Params): Result = oneBlock(params)

    /**
     * Executes the statement using the builder DSL and returns exactly one row.
     *
     * @throws IllegalStateException when zero or multiple rows are returned or required fields are missing.
     */
    suspend fun one(configure: Builder.() -> Unit): Result = oneBlock(build(configure))

    /**
     * Executes the statement and returns a single row or `null`.
     *
     * @throws IllegalStateException when more than one row is returned.
     */
    suspend fun oneOrNull(params: Params): Result? = oneOrNullBlock(params)

    /**
     * Executes the statement using the builder DSL and returns a single row or `null`.
     *
     * @throws IllegalStateException when more than one row is returned or required fields are missing.
     */
    suspend fun oneOrNull(configure: Builder.() -> Unit): Result? = oneOrNullBlock(build(configure))

    /**
     * Builds a [Params] instance from the builder DSL without executing the statement.
     *
     * @throws IllegalStateException when the statement does not support builder-style configuration.
     */
    fun build(configure: Builder.() -> Unit): Params = buildFromBuilder?.invoke(configure)
        ?: error("This statement does not support builder-based configuration.")

    /**
     * Executes the statement and returns every row. Synonym for [list].
     */
    suspend fun run(params: Params): List<Result> = list(params)

    /**
     * Executes the statement using the builder DSL and returns every row. Synonym for [list].
     */
    suspend fun run(configure: Builder.() -> Unit): List<Result> = list(configure)
}
