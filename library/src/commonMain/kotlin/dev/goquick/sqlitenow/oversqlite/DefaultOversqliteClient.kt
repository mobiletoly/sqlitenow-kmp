/*
 * Copyright 2025 Toly Pochkin
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
package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import kotlinx.coroutines.sync.Mutex
import kotlinx.serialization.json.Json
import kotlin.concurrent.Volatile

class DefaultOversqliteClient(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val http: HttpClient,
    private val resolver: Resolver = ServerWinsResolver,
    private val tablesUpdateListener: (table: Set<String>) -> Unit,
) : OversqliteClient {
    private val syncGate = Mutex()
    private val tableInfoCache = TableInfoCache()
    private val bootstrapper = SyncBootstrapper(config, tableInfoCache)
    private val json = Json { ignoreUnknownKeys = true }
    private val clientStateStore = OversqliteClientStateStore(db)
    private val syncStateStore = OversqliteSyncStateStore(db)
    private val remoteApi = OversqliteRemoteApi(http, json) { message -> verboseLog(message) }
    private val localStore = OversqliteLocalStore(db, tableInfoCache, json, ::requireBootstrapped)
    private val bundleApplier = OversqliteBundleApplier(localStore, syncStateStore)
    private val stageStore = OversqliteStageStore(db, localStore, syncStateStore, json)
    private val applyExecutor = OversqliteApplyExecutor(db, clientStateStore)
    private val pushWorkflow = OversqlitePushWorkflow(
        config = config,
        resolver = resolver,
        db = db,
        tableInfoCache = tableInfoCache,
        remoteApi = remoteApi,
        clientStateStore = clientStateStore,
        syncStateStore = syncStateStore,
        localStore = localStore,
        bundleApplier = bundleApplier,
        stageStore = stageStore,
        applyExecutor = applyExecutor,
        json = json,
    ) { message -> verboseLog(message) }
    private val downloadWorkflow = OversqliteDownloadWorkflow(
        db = db,
        config = config,
        remoteApi = remoteApi,
        clientStateStore = clientStateStore,
        syncStateStore = syncStateStore,
        localStore = localStore,
        bundleApplier = bundleApplier,
        stageStore = stageStore,
        applyExecutor = applyExecutor,
        json = json,
    ) { message -> verboseLog(message) }

    @Volatile
    private var uploadsPaused = false

    @Volatile
    private var downloadsPaused = false

    @Volatile
    private var validatedConfig: ValidatedConfig? = null

    @Volatile
    private var currentUserId: String? = null

    @Volatile
    private var currentSourceId: String? = null

    private fun verboseLog(message: () -> String) {
        if (config.verboseLogs) {
            sqliteNowLogger.i(message = message)
        }
    }

    override suspend fun pauseUploads() {
        uploadsPaused = true
    }

    override suspend fun resumeUploads() {
        uploadsPaused = false
    }

    override suspend fun pauseDownloads() {
        downloadsPaused = true
    }

    override suspend fun resumeDownloads() {
        downloadsPaused = false
    }

    override suspend fun bootstrap(userId: String, sourceId: String): Result<Unit> {
        val result = bootstrapper.bootstrap(db, userId, sourceId)
        return result.map { validated ->
            validatedConfig = validated
            currentUserId = userId.trim()
            currentSourceId = sourceId.trim()
        }
    }

    override suspend fun pushPending(): Result<Unit> = runSyncOperation {
        if (uploadsPaused) return@runSyncOperation

        val state = requireBootstrapped()
        val updatedTables = pushWorkflow.pushPending(state)
        if (updatedTables.isNotEmpty()) {
            tablesUpdateListener(updatedTables)
        }
    }

    override suspend fun pullToStable(): Result<Unit> = runSyncOperation {
        if (downloadsPaused) return@runSyncOperation

        val state = requireBootstrapped()
        val result = downloadWorkflow.pullToStable(state)
        applyDownloadResult(result)
    }

    override suspend fun sync(): Result<Unit> = runSyncOperation {
        val state = requireBootstrapped()
        val updatedTables = linkedSetOf<String>()
        if (!uploadsPaused) {
            updatedTables += pushWorkflow.pushPending(state)
        }
        if (!downloadsPaused) {
            val result = downloadWorkflow.pullToStable(state)
            result.rotatedSourceId?.let { currentSourceId = it }
            updatedTables += result.updatedTables
        }
        if (updatedTables.isNotEmpty()) {
            tablesUpdateListener(updatedTables)
        }
    }

    override suspend fun hydrate(): Result<Unit> = runSyncOperation {
        if (downloadsPaused) return@runSyncOperation
        val state = requireBootstrapped()
        applyDownloadResult(downloadWorkflow.rebuildFromSnapshot(state, rotateSource = false))
    }

    override suspend fun recover(): Result<Unit> = runSyncOperation {
        if (downloadsPaused) return@runSyncOperation
        val state = requireBootstrapped()
        applyDownloadResult(downloadWorkflow.rebuildFromSnapshot(state, rotateSource = true))
    }

    override suspend fun lastBundleSeqSeen(): Result<Long> = runCatching {
        val state = requireBootstrapped()
        clientStateStore.loadLastBundleSeqSeen(state.userId)
    }

    override fun close() {
    }

    private fun requireBootstrapped(): RuntimeState {
        val validated = validatedConfig ?: error("oversqlite bootstrap(userId, sourceId) must complete successfully before sync operations run")
        val userId = currentUserId ?: error("oversqlite bootstrap(userId, sourceId) must complete successfully before sync operations run")
        val sourceId = currentSourceId ?: error("oversqlite bootstrap(userId, sourceId) must complete successfully before sync operations run")
        return RuntimeState(validated = validated, userId = userId, sourceId = sourceId)
    }

    private fun applyDownloadResult(result: DownloadWorkflowResult) {
        result.rotatedSourceId?.let { currentSourceId = it }
        if (result.updatedTables.isNotEmpty()) {
            tablesUpdateListener(result.updatedTables)
        }
    }

    private suspend fun <T> runSyncOperation(block: suspend () -> T): Result<T> = runCatching {
        if (!syncGate.tryLock()) {
            throw SyncOperationInProgressException()
        }
        try {
            block()
        } finally {
            syncGate.unlock()
        }
    }
}
