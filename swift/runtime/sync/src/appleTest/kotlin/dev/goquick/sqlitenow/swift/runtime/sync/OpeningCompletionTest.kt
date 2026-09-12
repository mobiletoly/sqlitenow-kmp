package dev.goquick.sqlitenow.swift.runtime.sync

import kotlinx.coroutines.*
import kotlinx.coroutines.test.*
import kotlin.test.*

@OptIn(ExperimentalCoroutinesApi::class)
class OpeningCompletionTest {
    @Test
    fun completionOwnsPredispatchCancellationAndStartedCleanup() = runTest {
        for (scenario in listOf("before dispatch", "started cleanup", "success before cancel")) {
            val parent = Job()
            val dispatcher = StandardTestDispatcher(testScheduler)
            val cleanup = CompletableDeferred<Unit>()
            val entered = CompletableDeferred<Unit>()
            var results = 0
            var failures = 0
            var bodyCalls = 0
            val handle = startOpeningOperation(
                object : SQLiteNowSyncRuntimeOpeningOperationCompletion {
                    override fun onSuccess(result: SQLiteNowSyncRuntimeOpeningOperationResult) { results++ }
                    override fun onFailure(payload: SQLiteNowSyncRuntimeErrorPayload) { failures++ }
                },
                dispatcher + parent,
            ) {
                bodyCalls++
                if (scenario == "started cleanup") {
                    try { awaitCancellation() }
                    finally { withContext(NonCancellable) { entered.complete(Unit); cleanup.await() } }
                }
                SQLiteNowSyncRuntimeOpeningOperationResult()
            }
            if (scenario != "before dispatch") runCurrent()
            handle.cancel()
            runCurrent()
            if (scenario == "started cleanup") {
                assertTrue(entered.isCompleted, scenario)
                assertEquals(0, results + failures, scenario)
                assertTrue(parent.children.any(), scenario)
                cleanup.complete(Unit)
                runCurrent()
            }
            assertEquals(if (scenario == "success before cancel") 1 else 0, results, scenario)
            assertEquals(if (scenario == "success before cancel") 0 else 1, failures, scenario)
            assertEquals(if (scenario == "before dispatch") 0 else 1, bodyCalls, scenario)
            assertFalse(parent.children.any(), scenario)
            handle.cancel()
            runCurrent()
            assertEquals(1, results + failures, scenario)
            parent.complete()
        }
    }
}
