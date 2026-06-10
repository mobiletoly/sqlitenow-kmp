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

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay

internal class OversqliteAutomaticDownloads(
    private val automaticDownloadConfig: OversqliteAutomaticDownloadConfig,
    private val remoteApi: OversqliteRemoteApi,
    private val attachmentStateStore: OversqliteAttachmentStateStore,
    private val downloadsPaused: () -> Boolean,
    private val connectedRuntimeState: suspend (String) -> RuntimeState,
    private val fetchCapabilities: suspend (String) -> CapabilitiesResponse,
    private val pullToStable: suspend () -> Result<RemoteSyncReport>,
    private val log: ((() -> String)) -> Unit,
) {
    suspend fun run(): Nothing {
        val backoff = AutomaticDownloadBackoff(
            minMillis = automaticDownloadConfig.bundleChangeWatchReconnectMinMillis,
            maxMillis = automaticDownloadConfig.bundleChangeWatchReconnectMaxMillis,
        )
        while (true) {
            if (downloadsPaused()) {
                delay(automaticDownloadConfig.automaticDownloadIntervalMillis)
                continue
            }
            try {
                if (shouldUseBundleChangeWatch()) {
                    runBundleChangeWatchIteration()
                    backoff.delayNext()
                } else {
                    runAutomaticPollingIteration()
                    backoff.reset()
                }
            } catch (error: Throwable) {
                if (error is CancellationException) {
                    throw error
                }
                log {
                    "oversqlite automatic downloads iteration failed " +
                        "error=${error::class.simpleName ?: "Throwable"}: ${error.message.orEmpty()}"
                }
                runAutomaticPullToStable("automatic downloads fallback after failure")
                backoff.delayNext()
            }
        }
    }

    private suspend fun shouldUseBundleChangeWatch(): Boolean {
        if (automaticDownloadConfig.bundleChangeWatchMode != BundleChangeWatchMode.AUTO) {
            return false
        }
        return try {
            val state = automaticDownloadState("automatic downloads capability check")
            val capabilities = fetchCapabilities(state.sourceId)
            capabilities.bundleChangeWatchSupported
        } catch (error: Throwable) {
            if (error is CancellationException) {
                throw error
            }
            log {
                "oversqlite automatic downloads capability check failed; polling fallback will run " +
                    "error=${error::class.simpleName ?: "Throwable"}: ${error.message.orEmpty()}"
            }
            false
        }
    }

    private suspend fun runAutomaticPollingIteration() {
        if (!downloadsPaused()) {
            runAutomaticPullToStable("automatic downloads polling")
        }
        delay(automaticDownloadConfig.automaticDownloadIntervalMillis)
    }

    private suspend fun runBundleChangeWatchIteration() {
        var pullFailed = false
        val state = automaticDownloadState("bundle change watch")
        remoteApi.watchBundleChanges(
            sourceId = state.sourceId,
            afterBundleSeq = state.lastBundleSeqSeen,
        ) { event ->
            if (event.bundleSeq > 0 && !downloadsPaused()) {
                if (!runAutomaticPullToStable("bundle change watch event")) {
                    pullFailed = true
                }
            }
        }
        if (!pullFailed && !downloadsPaused()) {
            runAutomaticPullToStable("bundle change watch reconnect")
        }
    }

    private suspend fun automaticDownloadState(operation: String): AutomaticDownloadState {
        val runtimeState = connectedRuntimeState(operation)
        val attachment = attachmentStateStore.loadState()
        return AutomaticDownloadState(
            sourceId = attachment.currentSourceId.ifBlank { runtimeState.sourceId },
            lastBundleSeqSeen = attachment.lastBundleSeqSeen,
        )
    }

    private suspend fun runAutomaticPullToStable(operation: String): Boolean {
        val result = pullToStable()
        val error = result.exceptionOrNull() ?: return true
        if (error is CancellationException) {
            throw error
        }
        val expectedContention = error is SyncOperationInProgressException
        log {
            "oversqlite automatic downloads pull skipped op=$operation " +
                "expectedContention=$expectedContention " +
                "error=${error::class.simpleName ?: "Throwable"}: ${error.message.orEmpty()}"
        }
        return false
    }
}

private data class AutomaticDownloadState(
    val sourceId: String,
    val lastBundleSeqSeen: Long,
)

private class AutomaticDownloadBackoff(
    private val minMillis: Long,
    private val maxMillis: Long,
) {
    private var nextDelayMillis = minMillis

    suspend fun delayNext() {
        val delayMillis = nextDelayMillis
        nextDelayMillis = if (nextDelayMillis > maxMillis / 2) {
            maxMillis
        } else {
            minOf(maxMillis, nextDelayMillis * 2)
        }
        delay(delayMillis)
    }

    fun reset() {
        nextDelayMillis = minMillis
    }
}
