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
import dev.goquick.sqlitenow.core.worker.SqliteWorkerSQLiteConnection

internal actual fun sqliteNowActiveClientIdentity(dbName: String, connection: SQLiteConnection): Any {
    val workerConnection = connection as? SqliteWorkerSQLiteConnection ?: return Any()
    return if (workerConnection.runtimeKind == "js-node-worker") Any() else "web-worker:$dbName"
}

internal actual suspend fun acquireSqliteNowLifecycleLease(dbName: String): SqliteNowLifecycleLease {
    val platformOwner = acquirePlatformWebLifecycleOwner(dbName)
    return object : SqliteNowLifecycleLease {
        override val identity: String = platformOwner.identity
        private var closed = false

        override suspend fun close() {
            if (closed) return
            closed = true
            releasePlatformWebLifecycleOwner(platformOwner)
        }
    }
}

internal expect class PlatformWebLifecycleOwner {
    val identity: String
}

internal expect suspend fun acquirePlatformWebLifecycleOwner(dbName: String): PlatformWebLifecycleOwner

internal expect suspend fun releasePlatformWebLifecycleOwner(owner: PlatformWebLifecycleOwner)
