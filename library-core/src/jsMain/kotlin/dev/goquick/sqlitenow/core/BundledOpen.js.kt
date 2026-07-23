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
import dev.goquick.sqlitenow.core.worker.openSqliteWorkerConnection

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
