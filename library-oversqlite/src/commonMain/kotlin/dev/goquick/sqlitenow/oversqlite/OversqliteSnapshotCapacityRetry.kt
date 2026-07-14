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
            val baseDelay = error.retryAfterMillis ?: policy.fallbackDelayMillis
            val jitterSpan = (baseDelay.toDouble() * policy.jitterRatio)
                .coerceAtMost((Long.MAX_VALUE - baseDelay).toDouble())
                .toLong()
                .coerceAtLeast(0L)
            val jitter = if (jitterSpan == 0L) 0L else Random.nextLong(jitterSpan + 1L)
            val retryDelay = baseDelay + jitter
            if (
                waitedMillis >= policy.maxWaitMillis ||
                retryDelay <= 0L ||
                retryDelay > policy.maxWaitMillis - waitedMillis
            ) {
                throw SnapshotCapacityRetryExhaustedException(operation, error.errorCode, waitedMillis)
            }
            onRetry()
            val waitStarted = TimeSource.Monotonic.markNow()
            delay(retryDelay)
            onWait(waitStarted.elapsedNow().inWholeMilliseconds.coerceAtLeast(0L))
        }
    }
}
