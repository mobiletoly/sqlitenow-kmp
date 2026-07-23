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

import dev.goquick.sqlitenow.core.sqlite.toUint8Array
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.await
import kotlinx.coroutines.withContext

internal actual class SqliteWorkerTransport private constructor(
    private val client: dynamic,
) {
    actual suspend fun request(requestId: Int, requestJson: String): String =
        sqliteWorkerRequest(client, requestId, requestJson).await()

    actual suspend fun requestWithLegacyBytes(
        requestId: Int,
        requestJson: String,
        legacyBytes: ByteArray,
    ): String =
        sqliteWorkerRequestWithLegacyBytes(
            client,
            requestId,
            requestJson,
            legacyBytes.toUint8Array(),
        ).await()

    actual suspend fun cancel(requestId: Int): String =
        cancelSqliteWorkerRequest(client, requestId).await()

    actual suspend fun acknowledge(requestId: Int) {
        acknowledgeSqliteWorkerRequest(client, requestId).await()
    }

    actual fun release(requestId: Int) {
        releaseSqliteWorkerRequest(client, requestId)
    }

    actual fun sendOneWay(requestJson: String): Int =
        sendSqliteWorkerOneWay(client, requestJson)

    actual suspend fun flush(): String = flushSqliteWorkerOneWays(client).await()

    actual suspend fun shutdown(requestId: Int, requestJson: String): String =
        shutdownSqliteWorker(client, requestId, requestJson).await()

    actual suspend fun forceTerminate() {
        forceTerminateSqliteWorker(client).await()
    }

    actual suspend fun awaitCleanupDeadline() {
        waitForSqliteWorkerCleanupDeadline(client).await()
    }

    actual fun runtimeKind(): String = sqliteWorkerRuntimeKind(client)

    actual fun diagnosticsForTest(): String = sqliteWorkerClientDiagnostics(client)

    actual fun injectResponseForTest(responseJson: String) {
        injectSqliteWorkerResponseForTest(client, responseJson)
    }

    actual fun setCancellationCleanupFailuresForTest(failuresJson: String) {
        setSqliteWorkerCleanupFailuresForTest(client, failuresJson)
    }

    actual fun setNegativeReconciliationForTest() {
        setSqliteWorkerNegativeReconciliationForTest(client)
    }

    actual fun setShutdownFailuresForTest(failuresJson: String) {
        setSqliteWorkerShutdownFailuresForTest(client, failuresJson)
    }

    actual fun holdActivePageForTest() {
        holdSqliteWorkerActivePageForTest(client)
    }

    actual suspend fun awaitActivePageForTest() {
        awaitSqliteWorkerActivePageForTest(client).await()
    }

    actual fun setAcknowledgementModeForTest(mode: String) {
        setSqliteWorkerAcknowledgementModeForTest(client, mode)
    }

    actual fun setTerminationModeForTest(mode: String) {
        setSqliteWorkerTerminationModeForTest(client, mode)
    }

    actual fun setResponseModeForTest(modeJson: String) {
        setSqliteWorkerResponseModeForTest(client, modeJson)
    }

    actual fun failWorkerForTest(message: String) {
        failSqliteWorkerForTest(client, message)
    }

    actual fun setNextOneWayIdForTest(requestId: Int) {
        setSqliteWorkerNextOneWayIdForTest(client, requestId)
    }

    actual suspend fun cleanupMigrationStateForTest(databaseName: String) {
        cleanupSqliteWorkerMigrationForTest(client, databaseName).await()
    }

    actual fun setMigrationInterruptionForTest(databaseName: String, stage: String) {
        setSqliteWorkerMigrationInterruptionForTest(client, databaseName, stage)
    }

    actual fun holdMigrationCancellationForTest(databaseName: String, stage: String): Int =
        holdSqliteWorkerMigrationCancellationForTest(client, databaseName, stage)

    actual fun holdNextCompleteOpenCancellationForTest(): Int =
        holdSqliteWorkerNextCompleteOpenCancellationForTest(client)

    actual suspend fun awaitCancellationHoldForTest(controlId: Int): Int =
        awaitSqliteWorkerCancellationHoldForTest(client, controlId).await().toInt()

    actual suspend fun pendingOpenCountForTest(): Int =
        sqliteWorkerPendingOpenCountForTest(client).await().toInt()

    actual fun setMigrationHeapSamplesForTest(samplesJson: String) {
        setSqliteWorkerMigrationHeapSamplesForTest(client, samplesJson)
    }

    actual fun seedMigrationMarkerForTest(databaseName: String, mode: String) {
        seedSqliteWorkerMigrationMarkerForTest(client, databaseName, mode)
    }

    actual companion object {
        actual suspend fun create(
            configJson: String,
            workerModuleUrl: String?,
            startupModeForTest: String,
            cleanupTimeoutMillis: Int,
        ): SqliteWorkerTransport {
            val startup = createSqliteWorkerClient(
                configJson,
                workerModuleUrl,
                startupModeForTest,
                cleanupTimeoutMillis,
            )
            return try {
                SqliteWorkerTransport(startup.await())
            } catch (cancelled: CancellationException) {
                try {
                    withContext(NonCancellable) {
                        cancelSqliteWorkerStartup(startup).await()
                    }
                } catch (cleanupFailure: Throwable) {
                    if (
                        cleanupFailure !== cancelled &&
                        cancelled.suppressedExceptions.none { it === cleanupFailure }
                    ) {
                        cancelled.addSuppressed(cleanupFailure)
                    }
                }
                throw cancelled
            }
        }

        actual fun globalDiagnosticsForTest(): String =
            sqliteWorkerGlobalDiagnosticsForTest()
    }
}
