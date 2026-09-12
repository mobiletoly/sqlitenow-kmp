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
package dev.goquick.sqlitenow.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

/** Common ownership state for providers whose inspection and writable opens are separate. */
@OptIn(SqliteNowInternalApi::class)
internal class DefaultSqliteNowCoordinatedProviderSession(
    override val inspectionConnection: SafeSQLiteConnection?,
    override val preparationRequired: Boolean,
    private val prepare: suspend () -> SafeSQLiteConnection,
) : SqliteNowCoordinatedProviderSession {
    private val mutex = Mutex()
    private var preparedConnection: SafeSQLiteConnection? = null
    private var inspectionConnectionClosed = false
    private var published = false
    private var closed = false
    private var cleanupOnly = false
    private var closeAttempt: CoordinatedProviderCloseAttempt? = null
    private var preparationJob: Job? = null

    override suspend fun prepareConnection(): SafeSQLiteConnection = mutex.withLock {
        check(!closed && !published && !cleanupOnly && closeAttempt == null) {
            "Coordinated provider session is not active"
        }
        preparedConnection?.let { return@withLock it }

        preparationJob = currentCoroutineContext()[Job]
        try {
            if (!inspectionConnectionClosed) {
                try {
                    inspectionConnection?.close()
                } catch (failure: Throwable) {
                    cleanupOnly = true
                    inspectionConnectionClosed = inspectionConnection?.ownedCloseComplete == true
                    throw failure
                }
                inspectionConnectionClosed = true
            }
            prepare().also { preparedConnection = it }
        } finally {
            preparationJob = null
        }
    }

    override fun publish(connection: SafeSQLiteConnection) {
        check(!closed) { "Coordinated provider session is closed" }
        check(!published) { "Coordinated provider session is already published" }
        check(preparedConnection === connection) {
            "Only the prepared coordinated provider connection can be published"
        }
        published = true
        preparedConnection = null
    }

    override suspend fun close() {
        check(preparationJob !== currentCoroutineContext()[Job]) {
            "Cannot close a coordinated provider from its own preparation"
        }
        val (attempt, owner) = mutex.withLock {
            if (closed || published) return
            closeAttempt?.let { return@withLock it to false }
            cleanupOnly = true
            CoordinatedProviderCloseAttempt().also { closeAttempt = it } to true
        }
        if (!owner) {
            attempt.completed.await()?.let { throw it }
            return
        }
        val failure = withContext(NonCancellable) { closeOwnedResources() }
        attempt.completed.complete(failure)
        mutex.withLock {
            if (closeAttempt === attempt) closeAttempt = null
        }
        failure?.let { throw it }
    }

    private suspend fun closeOwnedResources(): Throwable? {
        var failure: Throwable? = null
        preparedConnection?.let { prepared ->
            try {
                prepared.close()
            } catch (t: Throwable) {
                failure = t
            }
            if (prepared.ownedCloseComplete) preparedConnection = null
        }
        if (!inspectionConnectionClosed) {
            try {
                inspectionConnection?.close()
            } catch (t: Throwable) {
                failure = if (failure == null) t else failure.addSuppressedAndReturn(t)
            }
            inspectionConnectionClosed = inspectionConnection?.ownedCloseComplete != false
        }
        if (preparedConnection == null && inspectionConnectionClosed) closed = true
        return failure
    }
}

private class CoordinatedProviderCloseAttempt {
    val completed = CompletableDeferred<Throwable?>()
}

private fun Throwable.addSuppressedAndReturn(later: Throwable): Throwable {
    addSuppressed(later)
    return this
}
