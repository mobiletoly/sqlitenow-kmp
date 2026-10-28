@file:OptIn(dev.goquick.sqlitenow.core.SqliteNowInternalApi::class)

package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SqliteNowDiagnosticContext
import dev.goquick.sqlitenow.core.sqliteNowDiagnosticPhase
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext

/** Captures existing operation outcomes without allowing logging to change Result semantics. */
internal suspend fun <T> diagnosticSyncResult(
    config: OversqliteConfig,
    name: String,
    connection: dev.goquick.sqlitenow.core.SafeSQLiteConnection,
    block: suspend () -> Result<T>,
): Result<T> {
    val diagnostics = config.diagnostics ?: return block()
    return try {
        Result.success(diagnostics.operation(name, connection.diagnosticConnectionIdentity()) { block().getOrThrow() })
    } catch (failure: CancellationException) { throw failure }
    catch (failure: Throwable) { Result.failure(failure) }
}

internal suspend fun syncDiagnosticEvent(
    name: String,
    outcome: String = "observed",
    counts: (() -> Map<String, Long>)? = null,
    countBasis: String = "observed",
    schema: String? = null,
    table: String? = null,
) {
    SqliteNowDiagnosticContext.current(currentCoroutineContext())?.span?.event(name, outcome, counts, countBasis, schema, table)
}

internal suspend fun <T> syncDiagnosticWork(name: String, block: suspend () -> T): T =
    sqliteNowDiagnosticPhase(name, block = block)

/** Hot per-row work is aggregated, avoiding per-row log records. Durations are inclusive. */
internal suspend fun <T> syncDiagnosticAggregate(name: String, block: suspend () -> T): T {
    val span = SqliteNowDiagnosticContext.current(currentCoroutineContext())?.span ?: return block()
    val began = span.nowNanos()
    var failed = false
    try { return dev.goquick.sqlitenow.core.withSqliteNowDiagnosticContext(span, name) { block() } }
    catch (failure: Throwable) { failed = true; throw failure }
    finally { span.aggregateWork(name, span.nowNanos() - began, failed) }
}

/** Fixed SQL family attribution without an extra clock or work metric. */
internal suspend fun <T> syncDiagnosticSql(name: String, block: suspend () -> T): T {
    val span = SqliteNowDiagnosticContext.current(currentCoroutineContext())?.span ?: return block()
    return dev.goquick.sqlitenow.core.withSqliteNowDiagnosticContext(span, name, block)
}
