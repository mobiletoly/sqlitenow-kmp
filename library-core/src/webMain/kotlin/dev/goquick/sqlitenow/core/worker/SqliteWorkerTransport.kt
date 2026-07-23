/*
 * Copyright 2026 Toly Pochkin
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
package dev.goquick.sqlitenow.core.worker

internal expect class SqliteWorkerTransport {
    suspend fun request(requestId: Int, requestJson: String): String

    suspend fun requestWithLegacyBytes(
        requestId: Int,
        requestJson: String,
        legacyBytes: ByteArray,
    ): String

    suspend fun cancel(requestId: Int): String

    suspend fun acknowledge(requestId: Int)

    fun release(requestId: Int)

    fun sendOneWay(requestJson: String): Int

    suspend fun flush(): String

    suspend fun shutdown(requestId: Int, requestJson: String): String

    suspend fun forceTerminate()

    suspend fun awaitCleanupDeadline()

    fun runtimeKind(): String

    fun diagnosticsForTest(): String

    fun injectResponseForTest(responseJson: String)

    fun setCancellationCleanupFailuresForTest(failuresJson: String)

    fun setNegativeReconciliationForTest()

    fun setShutdownFailuresForTest(failuresJson: String)

    fun holdActivePageForTest()

    suspend fun awaitActivePageForTest()

    fun setAcknowledgementModeForTest(mode: String)

    fun setTerminationModeForTest(mode: String)

    fun setResponseModeForTest(modeJson: String)

    fun failWorkerForTest(message: String)

    fun setNextOneWayIdForTest(requestId: Int)

    suspend fun cleanupMigrationStateForTest(databaseName: String)

    fun setMigrationInterruptionForTest(databaseName: String, stage: String)

    fun holdMigrationCancellationForTest(databaseName: String, stage: String): Int

    fun holdNextCompleteOpenCancellationForTest(): Int

    suspend fun awaitCancellationHoldForTest(controlId: Int): Int

    suspend fun pendingOpenCountForTest(): Int

    fun setMigrationHeapSamplesForTest(samplesJson: String)

    fun seedMigrationMarkerForTest(databaseName: String, mode: String)

    companion object {
        suspend fun create(
            configJson: String,
            workerModuleUrl: String? = null,
            startupModeForTest: String = "normal",
            cleanupTimeoutMillis: Int = SQLITE_WORKER_CLEANUP_TIMEOUT_MILLIS,
        ): SqliteWorkerTransport

        fun globalDiagnosticsForTest(): String
    }
}
