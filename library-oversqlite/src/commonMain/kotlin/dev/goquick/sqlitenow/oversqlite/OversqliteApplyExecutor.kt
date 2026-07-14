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
import dev.goquick.sqlitenow.core.TransactionMode
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout

private const val SNAPSHOT_CLEANUP_TIMEOUT_MILLIS = 5_000L

internal class SnapshotApplyFaultInjector(
    val afterAppliedRow: (suspend () -> Unit)? = null,
    val afterApplyPageLoaded: (suspend () -> Unit)? = null,
    val afterStatementCacheClosed: (suspend () -> Unit)? = null,
    val beforeCommit: (suspend () -> Unit)? = null,
    val statementCacheOperations: StatementCacheOperations? = null,
    val cleanupTimeoutMillis: Long = SNAPSHOT_CLEANUP_TIMEOUT_MILLIS,
)

internal class OversqliteApplyExecutor(
    private val db: SafeSQLiteConnection,
    private val applyStateStore: OversqliteApplyStateStore,
) {
    internal var faultInjector: SnapshotApplyFaultInjector? = null

    internal suspend fun afterAppliedRowForTest() {
        faultInjector?.afterAppliedRow?.invoke()
    }

    internal suspend fun afterApplyPageLoadedForTest() {
        faultInjector?.afterApplyPageLoaded?.invoke()
    }

    suspend fun <T> inApplyModeTransaction(
        state: RuntimeState,
        block: suspend (StatementCache) -> T,
    ): T {
        return db.transaction(TransactionMode.IMMEDIATE) {
            val injector = faultInjector
            val cleanupTimeoutMillis = injector?.cleanupTimeoutMillis ?: SNAPSHOT_CLEANUP_TIMEOUT_MILLIS
            require(cleanupTimeoutMillis > 0L) { "snapshot cleanup timeout must be positive" }
            db.execSQL("PRAGMA defer_foreign_keys = ON")
            val statementCache = StatementCache(
                db = db,
                operations = injector?.statementCacheOperations ?: StatementCacheOperations(),
            )
            var primaryFailure: Throwable? = null
            var result: T? = null
            try {
                applyStateStore.setApplyMode(true, statementCache)
                result = block(statementCache)
            } catch (error: Throwable) {
                primaryFailure = error
            }

            val cleanupFailures = mutableListOf<Throwable>()
            try {
                withContext(NonCancellable) {
                    withTimeout(cleanupTimeoutMillis) {
                        try {
                            applyStateStore.setApplyMode(false, statementCache)
                        } catch (cleanupFailure: Throwable) {
                            cleanupFailures += cleanupFailure
                            if (cleanupFailure is TimeoutCancellationException) throw cleanupFailure
                        }
                        var cacheClosed = false
                        try {
                            statementCache.close()
                            cacheClosed = true
                        } catch (cleanupFailure: Throwable) {
                            cleanupFailures += cleanupFailure
                            if (cleanupFailure is TimeoutCancellationException) throw cleanupFailure
                        }
                        if (cacheClosed) {
                            try {
                                injector?.afterStatementCacheClosed?.invoke()
                            } catch (cleanupFailure: Throwable) {
                                cleanupFailures += cleanupFailure
                                if (cleanupFailure is TimeoutCancellationException) throw cleanupFailure
                            }
                        }
                    }
                }
            } catch (cleanupTimeout: TimeoutCancellationException) {
                if (cleanupFailures.none { it === cleanupTimeout }) cleanupFailures += cleanupTimeout
            }

            val failure = primaryFailure ?: cleanupFailures.firstOrNull()
            if (failure != null) {
                addSnapshotSuppressedInOrder(failure, cleanupFailures)
                throw failure
            }

            injector?.beforeCommit?.invoke()
            @Suppress("UNCHECKED_CAST")
            result as T
        }
    }
}

private fun addSnapshotSuppressedInOrder(
    primary: Throwable,
    additional: Iterable<Throwable>,
) {
    val added = mutableListOf<Throwable>()
    for (failure in additional) {
        if (failure !== primary && added.none { it === failure }) {
            attachSuppressedOnce(primary, failure)
            added += failure
        }
    }
}
