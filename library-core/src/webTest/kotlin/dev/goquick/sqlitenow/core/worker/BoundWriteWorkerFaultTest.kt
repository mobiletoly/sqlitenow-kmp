package dev.goquick.sqlitenow.core.worker

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/** Establishes actual fault injection on existing raw worker owners before product edits. */
class BoundWriteWorkerFaultTest {
    @Test fun actualFinalizeCompletesNormally() = prove("normal")
    @Test fun thrownFinalizeBeforeNativeCallRequiresTermination() = prove("finalize-before")
    @Test fun thrownFinalizeAfterNativeCallRequiresTermination() = prove("finalize-after")
    @Test fun actualSQLiteDeniedRollbackHasNoSuccessfulRollback() = prove("rollback-denied")
    @Test fun cancellationInsideSQLiteProgressCallbackInterruptsExecution() = prove("active")

    private fun prove(scenario: String) = runTest(timeout = 180.seconds) {
        val receipt = runBoundWriteFaultFixture(scenario)
        val result = Json.parseToJsonElement(receipt).jsonObject
        assertEquals(scenario, result.getValue("scenario").jsonPrimitive.content)
        assertEquals("true", result.getValue("cleanup").jsonObject
            .getValue("terminationConfirmed").jsonPrimitive.content)
        val runtime = result.getValue("runtime").jsonPrimitive.content
        assertTrue(runtime == "js-node-worker" || runtime == "browser-worker")
        if (runtime == "browser-worker") {
            assertEquals("0", result.getValue("cleanup").jsonObject
                .getValue("durableReopenRows").jsonPrimitive.content)
        }
        println("BOUND_WRITE_FAULT_PROOF $receipt")
    }
}
