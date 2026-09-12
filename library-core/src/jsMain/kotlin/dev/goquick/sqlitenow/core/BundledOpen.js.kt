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

import androidx.sqlite.SQLiteConnection
import dev.goquick.sqlitenow.core.worker.SQLITE_WORKER_CLEANUP_TIMEOUT_MILLIS
import dev.goquick.sqlitenow.core.worker.SQLITE_WORKER_DEFAULT_PAGE_BYTES
import dev.goquick.sqlitenow.core.worker.SQLITE_WORKER_DEFAULT_PAGE_ROWS
import dev.goquick.sqlitenow.core.worker.bundledSqliteWorkerStartupModeForTest
import dev.goquick.sqlitenow.core.worker.openSqliteWorkerConnection
import dev.goquick.sqlitenow.core.worker.openSqliteWorkerCoordinatedSession

@Suppress("UNUSED_PARAMETER")
internal actual suspend fun openBundledSqliteConnection(
    dbName: String,
    debug: Boolean,
    initialBytes: ByteArray?,
    config: SqliteConnectionConfig,
): SQLiteConnection {
    check(initialBytes == null) {
        "Bundled web worker opens must receive migration sources through worker discovery."
    }
    return openSqliteWorkerConnection(dbName = dbName, config = config)
}

@OptIn(SqliteNowInternalApi::class)
internal actual suspend fun createBundledCoordinatedOpeningProviderSession(
    dbName: String,
    debug: Boolean,
    config: SqliteConnectionConfig,
): SqliteNowCoordinatedProviderSession = openSqliteWorkerCoordinatedSession(
    dbName = dbName,
    debug = debug,
    config = config,
    workerModuleUrl = null,
    pageRows = SQLITE_WORKER_DEFAULT_PAGE_ROWS,
    pageBytes = SQLITE_WORKER_DEFAULT_PAGE_BYTES,
    startupModeForTest = bundledSqliteWorkerStartupModeForTest,
    cleanupTimeoutMillis = SQLITE_WORKER_CLEANUP_TIMEOUT_MILLIS,
)
