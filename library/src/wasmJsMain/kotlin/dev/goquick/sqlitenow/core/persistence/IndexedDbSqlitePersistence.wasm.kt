package dev.goquick.sqlitenow.core.persistence

import dev.goquick.sqlitenow.core.SqlitePersistence
import dev.goquick.sqlitenow.core.sqlite.asByteArray
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.sqlite.asJsArray
import dev.goquick.sqlitenow.core.sqlite.toSqlJsArray
import kotlin.js.JsAny
import kotlinx.coroutines.await

private const val DEFAULT_STORAGE_NAME = "SqliteNow"
private const val DEFAULT_STORE_NAME = "sqlite-databases"

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
        indexedDbPersist(storageName, storeName, dbName, bytes.toSqlJsArray()).await<JsAny?>()
    }

    override suspend fun clear(dbName: String) {
        indexedDbClear(storageName, storeName, dbName).await<JsAny?>()
    }
}
