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
@file:JsModule("./sqlitenow-worker-v1/client.mjs")
@file:JsNonModule

package dev.goquick.sqlitenow.core.worker

import kotlin.js.Promise

internal external fun createSqliteWorkerClient(
    configJson: String,
    workerModuleUrl: String?,
    startupModeForTest: String,
    cleanupTimeoutMillis: Int,
): Promise<dynamic>

internal external fun sqliteWorkerRequest(
    client: dynamic,
    requestId: Int,
    requestJson: String,
): Promise<String>

internal external fun sqliteWorkerRequestWithLegacyBytes(
    client: dynamic,
    requestId: Int,
    requestJson: String,
    legacyBytes: dynamic,
): Promise<String>

internal external fun cancelSqliteWorkerRequest(client: dynamic, requestId: Int): Promise<String>

internal external fun acknowledgeSqliteWorkerRequest(
    client: dynamic,
    requestId: Int,
): Promise<dynamic>

internal external fun releaseSqliteWorkerRequest(client: dynamic, requestId: Int)

internal external fun cancelSqliteWorkerStartup(startup: Promise<dynamic>): Promise<dynamic>

internal external fun sendSqliteWorkerOneWay(client: dynamic, requestJson: String): Int

internal external fun flushSqliteWorkerOneWays(client: dynamic): Promise<String>

internal external fun shutdownSqliteWorker(
    client: dynamic,
    requestId: Int,
    requestJson: String,
): Promise<String>

internal external fun forceTerminateSqliteWorker(client: dynamic): Promise<dynamic>

internal external fun waitForSqliteWorkerCleanupDeadline(client: dynamic): Promise<dynamic>

internal external fun sqliteWorkerRuntimeKind(client: dynamic): String

internal external fun sqliteWorkerClientDiagnostics(client: dynamic): String

internal external fun injectSqliteWorkerResponseForTest(client: dynamic, responseJson: String)

internal external fun setSqliteWorkerCleanupFailuresForTest(client: dynamic, failuresJson: String)

internal external fun setSqliteWorkerNegativeReconciliationForTest(client: dynamic)

internal external fun setSqliteWorkerShutdownFailuresForTest(client: dynamic, failuresJson: String)

internal external fun holdSqliteWorkerActivePageForTest(client: dynamic)

internal external fun awaitSqliteWorkerActivePageForTest(client: dynamic): Promise<dynamic>

internal external fun setSqliteWorkerAcknowledgementModeForTest(client: dynamic, mode: String)

internal external fun setSqliteWorkerTerminationModeForTest(client: dynamic, mode: String)

internal external fun setSqliteWorkerResponseModeForTest(client: dynamic, modeJson: String)

internal external fun failSqliteWorkerForTest(client: dynamic, message: String)

internal external fun setSqliteWorkerNextOneWayIdForTest(client: dynamic, requestId: Int)

internal external fun cleanupSqliteWorkerMigrationForTest(
    client: dynamic,
    databaseName: String,
): Promise<dynamic>

internal external fun setSqliteWorkerMigrationInterruptionForTest(
    client: dynamic,
    databaseName: String,
    stage: String,
)

internal external fun holdSqliteWorkerMigrationCancellationForTest(
    client: dynamic,
    databaseName: String,
    stage: String,
): Int

internal external fun holdSqliteWorkerNextCompleteOpenCancellationForTest(client: dynamic): Int

internal external fun awaitSqliteWorkerCancellationHoldForTest(
    client: dynamic,
    controlId: Int,
): Promise<String>

internal external fun sqliteWorkerPendingOpenCountForTest(client: dynamic): Promise<String>

internal external fun setSqliteWorkerMigrationHeapSamplesForTest(
    client: dynamic,
    samplesJson: String,
)

internal external fun seedSqliteWorkerMigrationMarkerForTest(
    client: dynamic,
    databaseName: String,
    mode: String,
)

internal external fun sqliteWorkerGlobalDiagnosticsForTest(): String
