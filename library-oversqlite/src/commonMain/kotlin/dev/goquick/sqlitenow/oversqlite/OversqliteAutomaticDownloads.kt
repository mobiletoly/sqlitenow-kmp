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
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlin.time.Duration.Companion.milliseconds

private const val operationContentionRetryMillis = 250L

suspend fun OversqliteClient.runAutomaticDownloads(
    automaticDownloadConfig: OversqliteAutomaticDownloadConfig = OversqliteAutomaticDownloadConfig(),
): Nothing {
    val defaultClient = this as? DefaultOversqliteClient
        ?: throw UnsupportedOperationException(
            "automatic downloads require DefaultOversqliteClient or generated clients backed by DefaultOversqliteClient",
        )
    return defaultClient.runAutomaticDownloads(automaticDownloadConfig)
}

internal class OversqliteAutomaticDownloads(
    private val automaticDownloadConfig: OversqliteAutomaticDownloadConfig,
    private val remoteApi: OversqliteRemoteApi,
    private val attachmentStateStore: OversqliteAttachmentStateStore,
    private val downloadsPaused: () -> Boolean,
    private val connectedRuntimeState: suspend (String) -> RuntimeState,
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
                    if (!downloadsPaused()) {
                        runAutomaticPullToStable("automatic downloads fallback after watch ended")
                    }
                    backoff.delayNext()
                } else {
                    runAutomaticPollingIteration()
                    backoff.reset()
                }
            } catch (error: Throwable) {
                if (error is CancellationException) {
                    throw error
                }
                if (error.isTerminalSyncCompatibilityFailure()) {
                    throw error
                }
                log {
                    "oversqlite automatic downloads iteration failed " +
                        "error=${error::class.simpleName ?: "Throwable"}"
                }
                if (!downloadsPaused()) {
                    runAutomaticPullToStable("automatic downloads fallback after failure")
                }
                backoff.delayNext()
            }
        }
    }

    private suspend fun shouldUseBundleChangeWatch(): Boolean {
        if (automaticDownloadConfig.bundleChangeWatchMode != BundleChangeWatchMode.AUTO) {
            return false
        }
        return try {
            automaticDownloadState("automatic downloads watch check").watchEnabled
        } catch (error: Throwable) {
            if (error is CancellationException) {
                throw error
            }
            if (error.isTerminalSyncCompatibilityFailure()) {
                throw error
            }
            log {
                "oversqlite automatic downloads admitted-contract check failed; polling fallback will run " +
                    "error=${error::class.simpleName ?: "Throwable"}"
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
        val state = automaticDownloadState("bundle change watch")
        coroutineScope {
            val signals = Channel<Long>(Channel.CONFLATED)
            var announcedHighWater = state.lastBundleSeqSeen
            val worker = launch {
                var targetHighWater = state.lastBundleSeqSeen
                for (signal in signals) {
                    targetHighWater = maxOf(targetHighWater, signal)
                    while (true) {
                        val checkpoint = attachmentStateStore.loadState().lastBundleSeqSeen
                        if (checkpoint >= targetHighWater) break
                        if (!downloadsPaused()) {
                            val outcome = runAutomaticPullToStable("bundle change watch high-water")
                            val refreshed = attachmentStateStore.loadState().lastBundleSeqSeen
                            if (refreshed >= targetHighWater) break
                            if (outcome == AutomaticPullOutcome.SUCCEEDED && refreshed > checkpoint) continue
                            if (outcome == AutomaticPullOutcome.CONTENDED) {
                                delay(operationContentionRetryMillis.milliseconds)
                                continue
                            }
                        }
                        // Keep the high-water target when a pull is blocked, fails, or is paused.
                        // The stream may remain open without another bundle event.
                        delay(automaticDownloadConfig.automaticDownloadIntervalMillis.milliseconds)
                    }
                }
            }
            try {
                remoteApi.watchBundleChanges(
                    sourceId = state.sourceId,
                    afterBundleSeq = state.lastBundleSeqSeen,
                ) { event ->
                    if (event.bundleSeq > announcedHighWater) {
                        announcedHighWater = event.bundleSeq
                        signals.trySend(announcedHighWater)
                    }
                }
                val checkpoint = attachmentStateStore.loadState().lastBundleSeqSeen
                if (announcedHighWater > checkpoint) signals.trySend(announcedHighWater)
            } finally {
                signals.close()
                worker.cancelAndJoin()
            }
        }
    }

    private suspend fun automaticDownloadState(operation: String): AutomaticDownloadState {
        val runtimeState = connectedRuntimeState(operation)
        val attachment = attachmentStateStore.loadState()
        return AutomaticDownloadState(
            sourceId = attachment.currentSourceId
                .takeIf { it.isNotEmpty() }
                ?.let(::requireValidOversqliteSourceId)
                ?: requireValidOversqliteSourceId(runtimeState.sourceId),
            lastBundleSeqSeen = attachment.lastBundleSeqSeen,
            watchEnabled = runtimeState.admittedContract.watchEnabled,
        )
    }

    private suspend fun runAutomaticPullToStable(operation: String): AutomaticPullOutcome {
        val result = pullToStable()
        val error = result.exceptionOrNull() ?: return AutomaticPullOutcome.SUCCEEDED
        if (error is CancellationException) {
            throw error
        }
        if (error.isTerminalSyncCompatibilityFailure()) {
            throw error
        }
        val expectedContention = error is SyncOperationInProgressException
        log {
            "oversqlite automatic downloads pull skipped op=$operation " +
                "expectedContention=$expectedContention " +
                "error=${error::class.simpleName ?: "Throwable"}"
        }
        return if (expectedContention) AutomaticPullOutcome.CONTENDED else AutomaticPullOutcome.FAILED
    }
}

private enum class AutomaticPullOutcome {
    SUCCEEDED,
    CONTENDED,
    FAILED,
}

private data class AutomaticDownloadState(
    val sourceId: String,
    val lastBundleSeqSeen: Long,
    val watchEnabled: Boolean,
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
        delay(delayMillis.milliseconds)
    }

    fun reset() {
        nextDelayMillis = minMillis
    }
}
