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
package dev.goquick.sqlitenow.core

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

private val sqliteConnectionThreadIds = AtomicInteger(0)

internal actual fun createSqliteConnectionExecutionContext(nameHint: String): SqliteConnectionExecutionContext {
    val dispatcher = Executors.newSingleThreadExecutor { runnable ->
        Thread(
            runnable,
            "SqliteNow-${sanitizeSqliteConnectionName(nameHint)}-${sqliteConnectionThreadIds.incrementAndGet()}",
        ).apply {
            isDaemon = true
        }
    }.asCoroutineDispatcher()
    return CloseableSqliteConnectionExecutionContext(dispatcher)
}

internal actual fun sqliteNetworkDispatcher(): CoroutineDispatcher = Dispatchers.IO

private class CloseableSqliteConnectionExecutionContext(
    override val dispatcher: ExecutorCoroutineDispatcher,
) : SqliteConnectionExecutionContext {
    override fun close() {
        dispatcher.close()
    }
}

private fun sanitizeSqliteConnectionName(nameHint: String): String {
    val sanitized = buildString(nameHint.length) {
        for (ch in nameHint) {
            append(
                when {
                    ch.isLetterOrDigit() -> ch
                    ch == '-' || ch == '_' || ch == '.' -> ch
                    else -> '_'
                },
            )
        }
    }.trim('_')
    if (sanitized.isEmpty()) return "db"
    return sanitized.takeLast(24)
}
