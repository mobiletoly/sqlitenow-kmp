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
package dev.goquick.sqlitenow.core.persistence

import dev.goquick.sqlitenow.core.SqlitePersistence
import dev.goquick.sqlitenow.core.sqlite.asByteArray
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.sqlite.asJsArray
import dev.goquick.sqlitenow.core.sqlite.toJsArray
import kotlin.js.JsAny
import kotlinx.coroutines.await

private const val DEFAULT_STORAGE_NAME = "SqliteNow"
private const val DEFAULT_STORE_NAME = "sqlite-databases"

/**
 * Source-compatible IndexedDB implementation for importing retained legacy database bytes.
 *
 * The direct Wasm browser worker may call [load] when explicitly configured as a custom migration
 * source. It never calls [persist] or [clear], and ordinary/default browser storage is direct OPFS.
 */
class IndexedDbSqlitePersistence(
    private val storageName: String = DEFAULT_STORAGE_NAME,
    private val storeName: String = DEFAULT_STORE_NAME,
) : SqlitePersistence {

    override suspend fun load(dbName: String): ByteArray? {
        val result = indexedDbLoad(storageName, storeName, dbName).await<JsAny?>()
        if (result == null) {
            sqliteNowLogger.d { "[SqliteNow][IndexedDB] Kotlin load -> no snapshot for $dbName" }
            return null
        }
        val jsArray = asJsArray(result)
        val bytes = jsArray.asByteArray()
        sqliteNowLogger.d { "[SqliteNow][IndexedDB] Kotlin load -> ${bytes.size} bytes for $dbName" }
        return bytes
    }

    override suspend fun persist(dbName: String, bytes: ByteArray) {
        sqliteNowLogger.d { "[SqliteNow][IndexedDB] Kotlin persist request -> ${bytes.size} bytes for $dbName" }
        indexedDbPersist(storageName, storeName, dbName, bytes.toJsArray()).await<JsAny?>()
    }

    override suspend fun clear(dbName: String) {
        indexedDbClear(storageName, storeName, dbName).await<JsAny?>()
    }
}
