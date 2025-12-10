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
package dev.goquick.sqlitenow.core.persistence

import dev.goquick.sqlitenow.core.SqlitePersistence
import dev.goquick.sqlitenow.core.sqlite.toByteArray
import dev.goquick.sqlitenow.core.sqlite.toUint8Array
import kotlinx.browser.window
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.khronos.webgl.ArrayBuffer
import org.khronos.webgl.Uint8Array
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.js.Console
import kotlin.js.console
import kotlin.js.jsTypeOf
import kotlin.js.unsafeCast

private const val DEFAULT_STORAGE_NAME = "SqliteNow"
private const val DEFAULT_STORE_NAME = "sqlite-databases"

/**
 * Simple [SqlitePersistence] implementation backed by IndexedDB.
 *
 * Uses dynamic interop instead of typed wrappers so it works in both browser and Node test environments.
 */
class IndexedDbSqlitePersistence(
    private val storageName: String = DEFAULT_STORAGE_NAME,
    private val storeName: String = DEFAULT_STORE_NAME,
    private val logger: Console = console,
) : SqlitePersistence {

    private val databaseMutex = Mutex()
    private var databaseDeferred: CompletableDeferred<dynamic>? = null

    override suspend fun load(dbName: String): ByteArray? {
        val factory = indexedDbFactory() ?: return null
        val database = database(factory)
        val transaction = database.transaction(arrayOf(storeName), "readonly")
        val store = transaction.objectStore(storeName)
        val request = store.get(dbName)
        val result = runCatching { awaitRequest(request) }
            .onFailure { logger.warn("[SqliteNow][IndexedDB] Failed to load $dbName", it) }
            .getOrNull()
        awaitTransaction(transaction)
        return convertResultToBytes(result)
    }

    override suspend fun persist(dbName: String, bytes: ByteArray) {
        val factory = indexedDbFactory() ?: return
        val database = database(factory)
        val transaction = database.transaction(arrayOf(storeName), "readwrite")
        val store = transaction.objectStore(storeName)
        val request = store.put(bytes.toUint8Array(), dbName)
        runCatching { awaitRequest(request) }
            .onFailure { logger.error("[SqliteNow][IndexedDB] Failed to persist $dbName", it) }
        awaitTransaction(transaction)
    }

    override suspend fun clear(dbName: String) {
        val factory = indexedDbFactory() ?: return
        val database = database(factory)
        val transaction = database.transaction(arrayOf(storeName), "readwrite")
        val store = transaction.objectStore(storeName)
        runCatching { awaitRequest(store.delete(dbName)) }
            .onFailure { logger.warn("[SqliteNow][IndexedDB] Failed to clear $dbName", it) }
        awaitTransaction(transaction)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun database(factory: dynamic): dynamic {
        val completed = databaseDeferred?.takeIf { it.isCompleted }?.getCompleted()
        if (completed != null) return completed

        val deferred = databaseMutex.withLock {
            val current = databaseDeferred
            if (current != null) return@withLock current

            val fresh = CompletableDeferred<dynamic>()
            databaseDeferred = fresh

            val request = factory.open(storageName, 1)
            request.onupgradeneeded = {
                val db = request.result
                if (!hasObjectStore(db, storeName)) {
                    db.createObjectStore(storeName)
                }
            }
            request.onsuccess = {
                val db = request.result
                db.onversionchange = {
                    logger.warn("[SqliteNow][IndexedDB] Version change detected; closing database")
                    db.close()
                    databaseDeferred = null
                }
                fresh.complete(db)
            }
            request.onerror = {
                val throwable = dynamicError(request.error, "IndexedDB open failure")
                databaseDeferred = null
                fresh.completeExceptionally(throwable)
            }
            request.onblocked = {
                logger.warn("[SqliteNow][IndexedDB] Open request for $storageName blocked")
            }

            fresh
        }

        return deferred.await()
    }

    private fun indexedDbFactory(): dynamic {
        val factory = window.asDynamic().indexedDB
        if (factory == null || jsTypeOf(factory) == "undefined") {
            logger.warn("[SqliteNow][IndexedDB] indexedDB unavailable; persistence disabled")
            return null
        }
        return factory
    }

    private fun convertResultToBytes(value: dynamic): ByteArray? {
        if (value == null || jsTypeOf(value) == "undefined") return null

        if (value is ByteArray) {
            return value
        }

        val buffer = asArrayBuffer(value)
        if (buffer != null) {
            return buffer.toByteArray()
        }

        val dyn = value.asDynamic()
        val constructorName = if (isDefined(dyn)) {
            val ctor = dyn.constructor
            if (isDefined(ctor)) {
                val ctorName = ctor.name
                if (isDefined(ctorName)) {
                    ctorName.unsafeCast<String>()
                } else {
                    jsTypeOf(value)
                }
            } else {
                jsTypeOf(value)
            }
        } else {
            jsTypeOf(value)
        }
        logger.warn("[SqliteNow][IndexedDB] Unsupported value type: $constructorName, jsType: ${jsTypeOf(value)}")
        return null
    }

    private fun hasObjectStore(db: dynamic, name: String): Boolean {
        val stores = db.objectStoreNames
        if (stores == null || jsTypeOf(stores) == "undefined") return false
        return when {
            stores.contains != undefined -> (stores.contains(name) as Boolean)
            stores.indexOf != undefined -> (stores.indexOf(name) as Int) >= 0
            stores.length != undefined -> {
                val length = (stores.length as Number).toInt()
                (0 until length).any { stores.item(it) == name }
            }
            else -> false
        }
    }
}

@OptIn(ExperimentalCoroutinesApi::class)
private suspend fun awaitRequest(request: dynamic): dynamic = suspendCancellableCoroutine { cont ->
    request.onsuccess = {
        cont.resume(request.result)
    }
    request.onerror = {
        cont.resumeWithException(dynamicError(request.error, "IndexedDB request error"))
    }
}

@OptIn(ExperimentalCoroutinesApi::class)
private suspend fun awaitTransaction(transaction: dynamic): Unit = suspendCancellableCoroutine { cont ->
    val resumeError: (dynamic) -> Unit = { error ->
        val throwable = dynamicError(error, "IndexedDB transaction error")
        cont.resumeWithException(throwable)
    }
    transaction.oncomplete = { cont.resume(Unit) }
    transaction.onerror = resumeError
    transaction.onabort = resumeError
}

private fun dynamicError(error: dynamic, prefix: String): Throwable {
    val message = when {
        error == null -> prefix
        error.message != undefined -> "$prefix: ${error.message}"
        error.name != undefined -> "$prefix: ${error.name}"
        else -> "$prefix: ${error.toString()}"
    }
    return IllegalStateException(message)
}

private fun asArrayBuffer(value: dynamic): ArrayBuffer? {
    if (value == null || jsTypeOf(value) == "undefined") return null

    if (value is ArrayBuffer) return value
    if (value is Uint8Array) return value.buffer

    val dyn = value.asDynamic()
    return when {
        dyn.buffer != undefined -> dyn.buffer.unsafeCast<ArrayBuffer>()
        else -> null
    }
}

private fun isDefined(value: dynamic): Boolean {
    return value != null && jsTypeOf(value) != "undefined"
}
