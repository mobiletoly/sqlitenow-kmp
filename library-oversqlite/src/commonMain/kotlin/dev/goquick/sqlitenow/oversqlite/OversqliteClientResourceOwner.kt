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

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowInternalApi
import io.ktor.client.HttpClient
import io.ktor.client.plugins.HttpClientPlugin
import io.ktor.client.plugins.HttpRedirect
import io.ktor.client.plugins.pluginOrNull
import io.ktor.client.statement.HttpReceivePipeline
import io.ktor.util.AttributeKey
import io.ktor.util.pipeline.PipelinePhase
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.concurrent.Volatile

private object OversqliteRedirectRefusal : HttpClientPlugin<Unit, OversqliteRedirectRefusal> {
    override val key: AttributeKey<OversqliteRedirectRefusal> = AttributeKey("OversqliteRedirectRefusal")

    override fun prepare(block: Unit.() -> Unit): OversqliteRedirectRefusal = this

    override fun install(plugin: OversqliteRedirectRefusal, scope: HttpClient) {
        val redirectPhase = PipelinePhase("OversqliteRedirectRefusal")
        scope.receivePipeline.insertPhaseBefore(HttpReceivePipeline.State, redirectPhase)
        scope.receivePipeline.intercept(redirectPhase) { response ->
            if (response.status.value !in 300..399) return@intercept
            val path = response.call.request.url.encodedPath
            val syncNamespace = path.lastIndexOf("/sync/")
            val endpointPath = if (syncNamespace >= 0) path.substring(syncNamespace) else path
            response.cancel(CancellationException("oversqlite redirect response abandoned"))
            throw when {
                endpointPath.startsWith("/sync/snapshot") -> SnapshotHttpException(response.status, "redirect_refused")
                endpointPath == "/sync/push" -> UploadHttpException(response.status, "")
                else -> DownloadHttpException(response.status, "")
            }
        }
    }
}

internal fun oversqliteRedirectSafeHttpClient(http: HttpClient): HttpClient = http.config {
    expectSuccess = false
    followRedirects = false
    install(OversqliteRedirectRefusal)
}.also { client ->
    require(client.pluginOrNull(HttpRedirect) == null) {
        "Oversqlite requires an HTTP client without Ktor's redirect plugin"
    }
}

internal class OversqliteClientResourceOwner(
    private val db: SafeSQLiteConnection,
    sourceHttp: HttpClient,
) {
    internal val connection: SafeSQLiteConnection get() = db
    val http: HttpClient = oversqliteRedirectSafeHttpClient(sourceHttp)

    private val httpJob = checkNotNull(http.coroutineContext[Job])
    private val ownedOperationJobsMutex = Mutex()
    private val ownedOperationJobs = mutableMapOf<Job, Int>()
    private val cleanupMutex = Mutex()
    private val cleanupScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    @Volatile
    private var activeClientLeaseOwned = false

    internal var activeClientLeaseGroup: Any? = null

    private val cleanupComplete: Boolean
        get() = transportCloseInvoked && httpJob.isCompleted && !activeClientLeaseOwned
    private var cleanupAttempt: CompletableDeferred<Throwable?>? = null
    private var transportCloseFailure: Throwable? = null
    internal var cleanupObservationTimeoutMillis: Long = 5_000

    @Volatile
    private var coordinatedCloseHandler: (() -> Unit)? = null

    @Volatile
    private var closed = false

    @Volatile
    private var transportCloseInvoked = false

    internal fun ensureOpen() {
        check(!closed) { "Oversqlite client is closed" }
    }

    internal fun close() {
        coordinatedCloseHandler?.let { handler ->
            closed = true
            handler()
            return
        }
        closed = true
        cleanupScope.launch { closeFromCoordinatedOwner() }
    }

    internal fun bindCoordinatedClose(handler: () -> Unit) {
        check(coordinatedCloseHandler == null) { "Coordinated close handler is already installed" }
        check(!closed) { "Cannot bind a closed Oversqlite client" }
        coordinatedCloseHandler = handler
    }

    internal val cleanupPending: Boolean
        get() = closed && !cleanupComplete

    internal suspend fun closeFromCoordinatedOwner() {
        closed = true
        val (attempt, owner) = cleanupMutex.withLock {
            cleanupAttempt?.let { return@withLock it to false }
            if (cleanupComplete) return
            CompletableDeferred<Throwable?>().also { cleanupAttempt = it } to true
        }
        val callerJob = currentCoroutineContext()[Job]
        val failure = if (!owner) {
            withContext(NonCancellable) { attempt.await() }
        } else {
            val result = withContext(NonCancellable) {
                try {
                    observeOwnedCleanup(callerJob)
                } catch (failure: Throwable) {
                    failure
                }
            }
            attempt.complete(result)
            cleanupMutex.withLock { if (cleanupAttempt === attempt) cleanupAttempt = null }
            result
        }
        failure?.let { throw it }
    }

    private suspend fun observeOwnedCleanup(owner: Job?): Throwable? {
        var failure: Throwable?
        val operations = ownedOperationJobsMutex.withLock { ownedOperationJobs.keys.toList() }
            .filter { it !== owner }
        // These jobs already belong to the client. The timeout only observes them; it does not
        // parent the work, so a non-cooperative child cannot hold the timeout scope open.
        val complete = withTimeoutOrNull(cleanupObservationTimeoutMillis) {
            operations.forEach { it.cancel() }
            operations.forEach { it.join() }
            closeTransport()
            true
        } == true
        failure = transportCloseFailure
        if (!complete) {
            val incomplete = IllegalStateException(
                "Oversqlite owned HTTP cleanup remains incomplete after ${cleanupObservationTimeoutMillis}ms",
            )
            failure = if (failure == null) {
                incomplete
            } else {
                IllegalStateException("Oversqlite HTTP cleanup failed", failure).also { it.addSuppressed(incomplete) }
            }
        } else {
            try {
                releaseActiveClientLease()
            } catch (releaseFailure: Throwable) {
                failure = if (failure == null) {
                    releaseFailure
                } else {
                    IllegalStateException("Oversqlite lease cleanup failed", failure).also {
                        it.addSuppressed(releaseFailure)
                    }
                }
            }
        }
        return failure
    }

    internal suspend fun acquireActiveClientLease() {
        if (activeClientLeaseOwned) return
        OversqliteActiveClientRegistry.acquire(db, activeClientLeaseGroup ?: this)
        activeClientLeaseOwned = true
    }

    internal suspend fun releaseActiveClientLease() {
        if (!activeClientLeaseOwned) return
        OversqliteActiveClientRegistry.release(db, activeClientLeaseGroup ?: this)
        activeClientLeaseOwned = false
    }

    private suspend fun closeTransport() {
        if (!transportCloseInvoked) {
            transportCloseInvoked = true
            try {
                http.close()
            } catch (closeFailure: Throwable) {
                transportCloseFailure = closeFailure
            }
        }
        if (!httpJob.isCompleted) {
            httpJob.cancel(CancellationException("Oversqlite derived HTTP client is closing"))
        }
        httpJob.join()
    }

    internal suspend fun <T> trackOwnedOperation(block: suspend () -> T): T {
        val job = checkNotNull(currentCoroutineContext()[Job]) {
            "Oversqlite operations require a coroutine job"
        }
        ownedOperationJobsMutex.withLock {
            ensureOpen()
            ownedOperationJobs[job] = (ownedOperationJobs[job] ?: 0) + 1
        }
        try {
            return block()
        } finally {
            ownedOperationJobsMutex.withLock {
                val remaining = checkNotNull(ownedOperationJobs[job]) - 1
                if (remaining == 0) ownedOperationJobs.remove(job) else ownedOperationJobs[job] = remaining
            }
        }
    }
}

@OptIn(SqliteNowInternalApi::class)
private object OversqliteActiveClientRegistry {
    private data class Owner(
        val connectionIdentity: Any,
        val leaseGroup: Any,
        var references: Int,
    )

    private val mutex = Mutex()
    private val owners = mutableListOf<Owner>()

    suspend fun acquire(connection: SafeSQLiteConnection, leaseGroup: Any) = mutex.withLock {
        val identity = connection.activeClientIdentity
        val existing = owners.firstOrNull { it.connectionIdentity == identity }
        if (existing != null && existing.leaseGroup !== leaseGroup) {
            throw OversqliteOpeningBusyException()
        }
        if (existing == null) {
            owners += Owner(identity, leaseGroup, 1)
        } else {
            existing.references += 1
        }
    }

    suspend fun release(connection: SafeSQLiteConnection, leaseGroup: Any) = mutex.withLock {
        val identity = connection.activeClientIdentity
        val existing = owners.firstOrNull {
            it.connectionIdentity == identity && it.leaseGroup === leaseGroup
        } ?: return@withLock
        existing.references -= 1
        if (existing.references == 0) owners.remove(existing)
    }
}
