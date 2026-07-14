/*
 * Copyright 2025 Toly Pochkin
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.goquick.sqlitenow.oversqlite

import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class SnapshotCapacityRetryPolicyTest {
    @Test
    fun retriesCapacityUntilSuccessWithoutAttemptLimit() = runTest {
        var attempts = 0
        var responses = 0
        var retries = 0
        val result = withSnapshotCapacityRetry(
            policy = OversqliteSnapshotCapacityRetryPolicy(
                maxWaitMillis = 100,
                fallbackDelayMillis = 1,
                jitterRatio = 0.0,
            ),
            operation = "snapshot create",
            onCapacityResponse = { responses++ },
            onRetry = { retries++ },
        ) {
            attempts++
            if (attempts < 4) {
                throw SnapshotCapacityException(
                    HttpStatusCode.TooManyRequests,
                    "snapshot_build_capacity",
                    retryAfterMillis = null,
                )
            }
            "ok"
        }

        assertEquals("ok", result)
        assertEquals(4, attempts)
        assertEquals(3, responses)
        assertEquals(3, retries)
    }

    @Test
    fun rejectsRetryAfterThatCannotFitInBudget() = runTest {
        val error = assertFailsWith<SnapshotCapacityRetryExhaustedException> {
            withSnapshotCapacityRetry(
                policy = OversqliteSnapshotCapacityRetryPolicy(
                    maxWaitMillis = 100,
                    fallbackDelayMillis = 1,
                    jitterRatio = 0.0,
                ),
                operation = "snapshot chunk",
            ) {
                throw SnapshotCapacityException(
                    HttpStatusCode.TooManyRequests,
                    "snapshot_chunk_capacity",
                    retryAfterMillis = 1_000,
                )
            }
        }
        assertEquals("snapshot_chunk_capacity", error.errorCode)
    }

    @Test
    fun parsesOnlyPositiveRetryAfterDeltaSeconds() {
        assertEquals(2_000L, parseSnapshotRetryAfterMillis("2"))
        listOf(null, "", "0", "-1", "1.5", "Wed, 21 Oct 2015 07:28:00 GMT").forEach {
            assertEquals(null, parseSnapshotRetryAfterMillis(it))
        }
    }
}
