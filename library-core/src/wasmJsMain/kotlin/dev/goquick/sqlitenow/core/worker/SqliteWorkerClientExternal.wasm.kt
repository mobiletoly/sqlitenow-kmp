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
@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)
@file:JsModule("./sqlitenow-worker-v1/client.mjs")

package dev.goquick.sqlitenow.core.worker

import kotlin.js.JsAny
import kotlin.js.Promise

internal external fun createSqliteWorkerClient(
    configJson: String,
    workerModuleUrl: String?,
    startupModeForTest: String,
    cleanupTimeoutMillis: Int,
): Promise<JsAny>

internal external fun sqliteWorkerRequest(
    client: JsAny,
    requestId: Int,
    requestJson: String,
): Promise<JsAny>

internal external fun sqliteWorkerRequestWithLegacyBytes(
    client: JsAny,
    requestId: Int,
    requestJson: String,
    legacyBytes: JsAny,
): Promise<JsAny>

internal external fun cancelSqliteWorkerRequest(client: JsAny, requestId: Int): Promise<JsAny>

internal external fun acknowledgeSqliteWorkerRequest(
    client: JsAny,
    requestId: Int,
): Promise<JsAny>

internal external fun releaseSqliteWorkerRequest(client: JsAny, requestId: Int)

internal external fun cancelSqliteWorkerStartup(startup: Promise<JsAny>): Promise<JsAny>

internal external fun sendSqliteWorkerOneWay(client: JsAny, requestJson: String): Int

internal external fun flushSqliteWorkerOneWays(client: JsAny): Promise<JsAny>

internal external fun shutdownSqliteWorker(
    client: JsAny,
    requestId: Int,
    requestJson: String,
): Promise<JsAny>

internal external fun forceTerminateSqliteWorker(client: JsAny): Promise<JsAny>

internal external fun waitForSqliteWorkerCleanupDeadline(client: JsAny): Promise<JsAny>

internal external fun sqliteWorkerRuntimeKind(client: JsAny): String

internal external fun sqliteWorkerClientDiagnostics(client: JsAny): String

internal external fun injectSqliteWorkerResponseForTest(client: JsAny, responseJson: String)

internal external fun setSqliteWorkerCleanupFailuresForTest(client: JsAny, failuresJson: String)

internal external fun setSqliteWorkerNegativeReconciliationForTest(client: JsAny)

internal external fun setSqliteWorkerShutdownFailuresForTest(client: JsAny, failuresJson: String)

internal external fun holdSqliteWorkerActivePageForTest(client: JsAny)

internal external fun awaitSqliteWorkerActivePageForTest(client: JsAny): Promise<JsAny>

internal external fun setSqliteWorkerAcknowledgementModeForTest(client: JsAny, mode: String)

internal external fun setSqliteWorkerTerminationModeForTest(client: JsAny, mode: String)

internal external fun setSqliteWorkerResponseModeForTest(client: JsAny, modeJson: String)

internal external fun failSqliteWorkerForTest(client: JsAny, message: String)

internal external fun setSqliteWorkerNextOneWayIdForTest(client: JsAny, requestId: Int)

internal external fun cleanupSqliteWorkerMigrationForTest(
    client: JsAny,
    databaseName: String,
): Promise<JsAny>

internal external fun setSqliteWorkerMigrationInterruptionForTest(
    client: JsAny,
    databaseName: String,
    stage: String,
)

internal external fun holdSqliteWorkerMigrationCancellationForTest(
    client: JsAny,
    databaseName: String,
    stage: String,
): Int

internal external fun holdSqliteWorkerNextCompleteOpenCancellationForTest(client: JsAny): Int

internal external fun awaitSqliteWorkerCancellationHoldForTest(
    client: JsAny,
    controlId: Int,
): Promise<JsAny>

internal external fun sqliteWorkerPendingOpenCountForTest(client: JsAny): Promise<JsAny>

internal external fun setSqliteWorkerMigrationHeapSamplesForTest(
    client: JsAny,
    samplesJson: String,
)

internal external fun seedSqliteWorkerMigrationMarkerForTest(
    client: JsAny,
    databaseName: String,
    mode: String,
)

internal external fun sqliteWorkerGlobalDiagnosticsForTest(): String
