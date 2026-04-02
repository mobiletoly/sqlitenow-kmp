package dev.goquick.sqlitenow.core.test

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull

fun runPlatformTest(block: suspend () -> Unit) = runTest {
    block()
}

suspend fun <T> withRealTimeout(
    timeoutMs: Long,
    block: suspend () -> T,
): T = withContext(Dispatchers.Default) {
    withTimeout(timeoutMs) {
        block()
    }
}

suspend fun awaitConditionWithRealTimeout(
    timeoutMs: Long,
    pollIntervalMs: Long = 25,
    condition: () -> Boolean,
): Boolean = withContext(Dispatchers.Default) {
    withTimeoutOrNull(timeoutMs) {
        while (!condition()) {
            delay(pollIntervalMs)
        }
        true
    } == true
}
