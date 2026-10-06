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

import io.ktor.client.network.sockets.ConnectTimeoutException
import io.ktor.client.network.sockets.SocketTimeoutException
import dev.goquick.sqlitenow.core.TransactionMode
import io.ktor.client.plugins.HttpRequestTimeoutException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.io.IOException
import kotlin.concurrent.Volatile
import kotlin.random.Random

internal class OversqliteClientOperationController(
    private val config: OversqliteConfig,
    private val resources: OversqliteClientResourceOwner,
    private val snapshotDiagnosticsState: SnapshotDiagnosticsState,
    private val log: (message: () -> String) -> Unit,
) {
    private val progressState = MutableStateFlow<OversqliteProgress>(OversqliteProgress.Idle)
    private val operationGate = Mutex()
    private var syncWorkUsed = 0L
    private var readyAtomicUnitDispatched = false
    var a3Stats=A3AccountingStats()
        private set
    private val confirmedBundles = linkedSetOf<Pair<String,Long>>()

    fun recordConfirmedBundle(source: String, id: Long) { confirmedBundles += source to id }
    fun confirmedBundleCount(): Long = confirmedBundles.size.toLong()

    fun usedSyncWork(): Long = syncWorkUsed

    suspend fun ensureSyncWorkAvailable(unit: String) {
        currentCoroutineContext().ensureActive()
        if (syncWorkUsed >= config.maxSyncWorkUnitsPerInvocation) {
            throw SyncWorkLimitException(unit, config.maxSyncWorkUnitsPerInvocation, syncWorkUsed)
        }
    }

    suspend fun reserveSyncWork(unit: String, work: Long, readyAtomic: Boolean = false) {
        currentCoroutineContext().ensureActive()
        val limit = config.maxSyncWorkUnitsPerInvocation
        require(work >= 0 && syncWorkUsed >= 0) { "invalid sync work accounting" }
        if (work == 0L) return
        if (syncWorkUsed >= limit && (!readyAtomic || readyAtomicUnitDispatched)) {
            throw SyncWorkLimitException(unit, limit, syncWorkUsed)
        }
        if (readyAtomic) readyAtomicUnitDispatched = true
        syncWorkUsed = if (work > Long.MAX_VALUE - syncWorkUsed) Long.MAX_VALUE else syncWorkUsed + work
    }

    @Volatile
    private var uploadsPaused = false

    @Volatile
    private var downloadsPaused = false

    val progress: StateFlow<OversqliteProgress> = progressState.asStateFlow()

    fun pauseUploads() {
        uploadsPaused = true
    }

    fun resumeUploads() {
        uploadsPaused = false
    }

    fun pauseDownloads() {
        downloadsPaused = true
    }

    fun resumeDownloads() {
        downloadsPaused = false
    }

    fun uploadsPaused(): Boolean = uploadsPaused

    fun downloadsPaused(): Boolean = downloadsPaused

    fun resetSnapshotRestoreDiagnosticsForTest() {
        if (!operationGate.tryLock()) {
            throw SyncOperationInProgressException()
        }
        try {
            snapshotDiagnosticsState.reset()
        } finally {
            operationGate.unlock()
        }
    }

    fun setProgress(
        operation: OversqliteOperation,
        phase: OversqlitePhase,
    ) {
        progressState.value = OversqliteProgress.Active(operation, phase)
    }

    suspend fun <T> withOperationProgress(
        operation: OversqliteOperation,
        block: suspend () -> T,
    ): T {
        try {
            return block()
        } finally {
            progressState.value = OversqliteProgress.Idle
        }
    }

    suspend fun <T> runExclusiveOperation(block: suspend () -> T): Result<T> {
        return try {
            resources.ensureOpen()
            if (!operationGate.tryLock()) {
                throw SyncOperationInProgressException()
            }
            try {
                syncWorkUsed = 0
                readyAtomicUnitDispatched = false
                a3Stats=A3AccountingStats()
                confirmedBundles.clear()
                try {
                    Result.success(resources.trackOwnedOperation(block))
                } catch (error: Throwable) {
                    currentCoroutineContext().ensureActive()
                    if (error is CancellationException) throw error
                    try {
                        attachErrorProgress(error)
                    } catch (progressError: Throwable) {
                        currentCoroutineContext().ensureActive()
                        if (progressError is CancellationException) throw progressError
                        error.addSuppressed(progressError)
                    }
                    Result.failure(error)
                }
            } finally {
                operationGate.unlock()
            }
        } catch (error: CancellationException) {
            throw error
        } catch (error: Throwable) {
            Result.failure(error)
        }
    }

    private suspend fun attachErrorProgress(error: Throwable) {
        if (error !is PushDependencyGroupExceedsAdmittedLimitException && error !is FKReconciliationRequiredException &&
            error !is ResolutionBaseUnavailableException && error !is SyncWorkLimitException && error !is SyncWorkspaceLimitException) return
        val db=resources.connection
        val progress=db.transaction(TransactionMode.IMMEDIATE) {
            SyncProgress(db.scalarLong("SELECT count(*) FROM _sync_dirty_rows"),db.scalarLong("SELECT count(*) FROM _sync_outbox_rows"),confirmedBundleCount())
        }
        when(error) {
            is PushDependencyGroupExceedsAdmittedLimitException -> error.progress=progress
            is FKReconciliationRequiredException -> error.progress=progress
            is ResolutionBaseUnavailableException -> error.progress=progress
            is SyncWorkLimitException -> error.progress=progress
            is SyncWorkspaceLimitException -> error.progress=progress
        }
    }

    suspend fun <T> runSyncOperation(
        operation: String,
        block: suspend () -> T,
    ): Result<T> = runExclusiveOperation {
        withSnapshotDiagnosticsScope {
            withTransientRetry(operation, block)
        }
    }

    suspend fun <T> withSnapshotDiagnosticsScope(block: suspend () -> T): T {
        val scope = snapshotDiagnosticsState.begin()
        return try {
            block()
        } finally {
            snapshotDiagnosticsState.finish(scope)
        }
    }

    suspend fun <T> withTransientRetry(
        operation: String,
        block: suspend () -> T,
    ): T = withSnapshotCapacityRetry(
        policy = config.snapshotCapacityRetryPolicy,
        operation = operation,
        onCapacityResponse = { snapshotDiagnosticsState.current()?.recordCapacityResponse() },
        onRetry = { snapshotDiagnosticsState.current()?.recordCapacityRetry() },
        onWait = { snapshotDiagnosticsState.current()?.recordCapacityWait(it) },
    ) {
        withGenericTransientRetry(operation, block)
    }

    private suspend fun <T> withGenericTransientRetry(
        operation: String,
        block: suspend () -> T,
    ): T {
        val maxAttempts = config.transientRetryPolicy.maxAttempts.coerceAtLeast(1)
        repeat(maxAttempts) { attemptIndex ->
            try {
                return block()
            } catch (error: Throwable) {
                if (error is CancellationException) {
                    throw error
                }
                val retryable = shouldRetryTransportFailure(error)
                log {
                    "oversqlite operation failure op=$operation attempt=${attemptIndex + 1}/$maxAttempts " +
                        "retryable=$retryable error=${error::class.simpleName ?: "Throwable"}"
                }
                if (!retryable) {
                    throw error
                }
                if (attemptIndex == maxAttempts - 1) {
                    log {
                        "oversqlite transient retry exhausted op=$operation attempts=$maxAttempts " +
                            "error=${error::class.simpleName ?: "Throwable"}"
                    }
                    throw TransientRetryExhaustedException(operation, maxAttempts)
                }
                log {
                    "oversqlite transient retry op=$operation attempt=${attemptIndex + 1}/$maxAttempts " +
                        "error=${error::class.simpleName ?: "Throwable"}"
                }
                maybeRetryTransportFailure(attemptIndex + 1)
            }
        }
        throw IllegalStateException("oversqlite retry state is invalid")
    }

    private suspend fun maybeRetryTransportFailure(attempt: Int) {
        if (attempt <= 0) {
            return
        }
        val policy = config.transientRetryPolicy
        var backoff = policy.initialBackoffMillis
        repeat(attempt - 1) {
            backoff = minOf(backoff * 2, policy.maxBackoffMillis)
        }
        val jitterSpan = (backoff * policy.jitterRatio).toLong().coerceAtLeast(0L)
        val jitter = if (jitterSpan == 0L) 0L else Random.nextLong(0L, jitterSpan + 1)
        delay(backoff + jitter)
    }

    fun shouldRetryTransportFailure(error: Throwable): Boolean {
        return when (error) {
            is OversqliteRemoteContractException -> error.retryable
            is UploadHttpException -> error.status.value in 500..599
            is DownloadHttpException -> error.status.value in 500..599
            is HttpRequestTimeoutException,
            is ConnectTimeoutException,
            is SocketTimeoutException,
            is IOException,
            -> true
            else -> false
        }
    }
}
