/*
 * Copyright 2025 Toly Pochkin
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlin.random.Random
import kotlin.time.TimeSource

internal suspend fun <T> withSnapshotCapacityRetry(
    policy: OversqliteSnapshotCapacityRetryPolicy,
    operation: String,
    onCapacityResponse: () -> Unit = {},
    onRetry: () -> Unit = {},
    onWait: (Long) -> Unit = {},
    block: suspend () -> T,
): T {
    val started = TimeSource.Monotonic.markNow()
    while (true) {
        try {
            return block()
        } catch (error: CancellationException) {
            throw error
        } catch (error: SnapshotCapacityException) {
            onCapacityResponse()
            val waitedMillis = started.elapsedNow().inWholeMilliseconds.coerceAtLeast(0L)
            if (!policy.enabled) {
                throw SnapshotCapacityRetryExhaustedException(operation, error.errorCode, waitedMillis)
            }
            val retryDelay = capacityRetryDelayMillis(
                policy.maxWaitMillis, policy.fallbackDelayMillis, policy.jitterRatio, error.retryAfterMillis, waitedMillis,
            ) ?: throw SnapshotCapacityRetryExhaustedException(operation, error.errorCode, waitedMillis)
            onRetry()
            val waitStarted = TimeSource.Monotonic.markNow()
            delay(retryDelay)
            onWait(waitStarted.elapsedNow().inWholeMilliseconds.coerceAtLeast(0L))
        }
    }
}

/**
 * Wait before the next dispatch after a capacity refusal: a positive `Retry-After`, otherwise the
 * fallback, plus up to [jitterRatio] of it as positive jitter. Null when the elapsed window cannot
 * hold that wait.
 */
internal fun capacityRetryDelayMillis(
    maxWaitMillis: Long,
    fallbackDelayMillis: Long,
    jitterRatio: Double,
    retryAfterMillis: Long?,
    waitedMillis: Long,
): Long? {
    val baseDelay = retryAfterMillis ?: fallbackDelayMillis
    val jitterSpan = (baseDelay.toDouble() * jitterRatio)
        .coerceAtMost((Long.MAX_VALUE - baseDelay).toDouble())
        .toLong()
        .coerceAtLeast(0L)
    val jitter = if (jitterSpan == 0L) 0L else Random.nextLong(jitterSpan + 1L)
    val retryDelay = baseDelay + jitter
    if (waitedMillis >= maxWaitMillis || retryDelay <= 0L || retryDelay > maxWaitMillis - waitedMillis) return null
    return retryDelay
}

/**
 * Elapsed-time window for HTTP 503 `history_projection_capacity_exceeded` refusals of push,
 * push-outcome and pull requests (p0001 history-projection retry timing, amended 2026-11-14).
 * The defaults match the snapshot capacity window and the Go client's push and pull windows.
 */
internal data class HistoryCapacityRetryPolicy(
    val enabled: Boolean = true,
    val maxWaitMillis: Long = 30_000,
    val fallbackDelayMillis: Long = 1_000,
    val jitterRatio: Double = 1.0,
)

/**
 * One window per request, opened by its first capacity refusal. Elapsed time is the
 * larger of the monotonic clock and the waits already taken, so a virtual-time scheduler cannot keep
 * the window open forever.
 */
internal class HistoryCapacityRetryWindow(private val policy: HistoryCapacityRetryPolicy) {
    private var started: TimeSource.Monotonic.ValueTimeMark? = null
    private var waitedMillis = 0L

    fun governs(error: Throwable): Boolean =
        policy.enabled && error is OversqliteRemoteContractException &&
            error.httpStatus == 503 && error.code == "history_projection_capacity_exceeded"

    /** Null when the window is exhausted. */
    fun nextDelayMillis(error: OversqliteRemoteContractException): Long? {
        val mark = started ?: TimeSource.Monotonic.markNow().also { started = it }
        val elapsed = maxOf(mark.elapsedNow().inWholeMilliseconds.coerceAtLeast(0L), waitedMillis)
        return capacityRetryDelayMillis(
            policy.maxWaitMillis, policy.fallbackDelayMillis, policy.jitterRatio, error.retryAfterMillis, elapsed,
        )
    }

    fun recordWait(millis: Long) {
        waitedMillis = if (Long.MAX_VALUE - waitedMillis < millis) Long.MAX_VALUE else waitedMillis + millis
    }
}
