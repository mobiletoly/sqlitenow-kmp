@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)

package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.persistence.IndexedDbSqlitePersistence
import kotlin.JsFun
import kotlin.js.JsAny
import kotlin.js.Promise
import kotlinx.coroutines.await

private const val DEFAULT_STORAGE_NAME = "SqliteNow"
private const val DEFAULT_STORE_NAME = "sqlite-databases"

private var opfsOverride: Boolean? = null

internal actual fun chooseDefaultWebPersistence(dbName: String): SqlitePersistence {
    val storage = navigatorStorage()
    opfsOverride?.let { forced ->
        return if (forced && storage != null) {
            sqliteNowLogger.i { "[SqliteNow][OPFS] Using Origin Private File System persistence for $dbName (forced)" }
            OpfsSqlitePersistence(storage, DEFAULT_STORAGE_NAME)
        } else {
            sqliteNowLogger.i { "[SqliteNow][IndexedDB] Using IndexedDB persistence for $dbName (forced)" }
            IndexedDbSqlitePersistence(storageName = DEFAULT_STORAGE_NAME, storeName = DEFAULT_STORE_NAME)
        }
    }

    if (!supportsOpfs() || storage == null) {
        sqliteNowLogger.i { "[SqliteNow][IndexedDB] Using IndexedDB persistence for $dbName" }
        return IndexedDbSqlitePersistence(storageName = DEFAULT_STORAGE_NAME, storeName = DEFAULT_STORE_NAME)
    }
    sqliteNowLogger.i { "[SqliteNow][OPFS] Using Origin Private File System persistence for $dbName" }
    return OpfsSqlitePersistence(storage, DEFAULT_STORAGE_NAME)
}

internal actual fun forceWebPersistenceOverride(override: Boolean?) {
    opfsOverride = override
}

internal class OpfsSqlitePersistence(
    private val storage: JsAny,
    private val storageName: String,
) : SqlitePersistence {

    private val directoryPromise: Promise<JsAny> = storageGetOrCreateDirectory(storage, storageName)

    override suspend fun load(dbName: String): ByteArray? {
        val directory = directoryPromise.await<JsAny>()
        val handle = runCatching {
            directoryGetFileHandle(directory, fileName(dbName), create = false).await<JsAny>()
        }.getOrNull() ?: return null
        val file = fileHandleGetFile(handle).await<JsAny>()
        val buffer = fileArrayBuffer(file).await<JsAny>()
        return bufferToByteArray(buffer)
    }

    override suspend fun persist(dbName: String, bytes: ByteArray) {
        val directory = directoryPromise.await<JsAny>()
        val handle = directoryGetFileHandle(directory, fileName(dbName), create = true).await<JsAny>()
        val writable = fileHandleCreateWritable(handle).await<JsAny>()
        val payload = byteArrayToUint8Array(bytes)
        try {
            writableWrite(writable, payload).await<JsAny?>()
            writableClose(writable).await<JsAny?>()
        } catch (t: Throwable) {
            runCatching { writableAbort(writable).await<JsAny?>() }
            throw t
        }
    }

    override suspend fun clear(dbName: String) {
        val directory = directoryPromise.await<JsAny>()
        runCatching { directoryRemoveEntry(directory, fileName(dbName)).await<JsAny?>() }
    }

    private fun fileName(dbName: String): String = "$dbName.sqlite3"

    companion object {
        fun supportsOpfs(): Boolean = hasOpfs() && navigatorStorage() != null

        private fun bufferToByteArray(buffer: JsAny): ByteArray {
            val view = arrayBufferToUint8Array(buffer)
            val length = uint8ArrayLength(view)
            val result = ByteArray(length)
            for (i in 0 until length) {
                result[i] = uint8ArrayGet(view, i).toByte()
            }
            return result
        }

        private fun byteArrayToUint8Array(bytes: ByteArray): JsAny {
            val view = createUint8Array(bytes.size)
            for (i in bytes.indices) {
                setUint8ArrayValue(view, i, (bytes[i].toInt() and 0xFF))
            }
            return view
        }
    }
}

private fun supportsOpfs(): Boolean {
    return hasOpfs() && hasOpfsSyncAccessHandle()
}

private fun navigatorStorage(): JsAny? = getNavigatorStorage()

private fun storageGetOrCreateDirectory(storage: JsAny, name: String): Promise<JsAny> {
    return storageGetDirectory(storage).then { dir: JsAny ->
        directoryGetDirectoryHandle(dir, name).unsafeCast<Promise<JsAny>>()
    }
}

private fun directoryGetFileHandle(directory: JsAny, name: String, create: Boolean): Promise<JsAny> {
    return directoryGetFileHandleJs(directory, name, if (create) createOptionsTrue() else createOptionsFalse())
}

private fun fileHandleCreateWritable(handle: JsAny): Promise<JsAny> = fileHandleCreateWritableJs(handle, createWritableOptions())

// JS bridges

@JsFun(
    """
    () => !!(
      typeof self !== 'undefined' &&
      self.isSecureContext &&
      typeof navigator !== 'undefined' &&
      navigator.storage &&
      typeof navigator.storage.getDirectory === 'function'
    )
    """
)
private external fun hasOpfs(): Boolean

@JsFun(
    """
    () => !!(
      typeof self !== 'undefined' &&
      self.isSecureContext &&
      typeof FileSystemFileHandle !== 'undefined' &&
      typeof FileSystemFileHandle.prototype.createSyncAccessHandle === 'function' &&
      typeof DedicatedWorkerGlobalScope !== 'undefined' &&
      self instanceof DedicatedWorkerGlobalScope
    )
    """
)
private external fun hasOpfsSyncAccessHandle(): Boolean

@JsFun("() => navigator.storage ?? null")
private external fun getNavigatorStorage(): JsAny?

@JsFun("(storage) => storage.getDirectory()")
private external fun storageGetDirectory(storage: JsAny): Promise<JsAny>

@JsFun("(dir, name) => dir.getDirectoryHandle(name, { create: true })")
private external fun directoryGetDirectoryHandle(dir: JsAny, name: String): Promise<JsAny>

@JsFun("(dir, name, options) => dir.getFileHandle(name, options)")
private external fun directoryGetFileHandleJs(dir: JsAny, name: String, options: JsAny): Promise<JsAny>

@JsFun("() => ({ create: true })")
private external fun createOptionsTrue(): JsAny

@JsFun("() => ({})")
private external fun createOptionsFalse(): JsAny

@JsFun("(handle) => handle.getFile()")
private external fun fileHandleGetFile(handle: JsAny): Promise<JsAny>

@JsFun("(file) => file.arrayBuffer()")
private external fun fileArrayBuffer(file: JsAny): Promise<JsAny>

@JsFun("(buffer) => new Uint8Array(buffer)")
private external fun arrayBufferToUint8Array(buffer: JsAny): JsAny

@JsFun("(array) => array.length")
private external fun uint8ArrayLength(array: JsAny): Int

@JsFun("(array, index) => array[index]")
private external fun uint8ArrayGet(array: JsAny, index: Int): Int

@JsFun("(length) => new Uint8Array(length)")
private external fun createUint8Array(length: Int): JsAny

@JsFun("(array, index, value) => { array[index] = value; }")
private external fun setUint8ArrayValue(array: JsAny, index: Int, value: Int)

@JsFun("() => ({ keepExistingData: false })")
private external fun createWritableOptions(): JsAny

@JsFun("(handle, options) => handle.createWritable(options)")
private external fun fileHandleCreateWritableJs(handle: JsAny, options: JsAny): Promise<JsAny>

@JsFun("(writable, data) => writable.write(data)")
private external fun writableWrite(writable: JsAny, data: JsAny): Promise<JsAny?>

@JsFun("(writable) => writable.close()")
private external fun writableClose(writable: JsAny): Promise<JsAny?>

@JsFun("(writable) => writable.abort()")
private external fun writableAbort(writable: JsAny): Promise<JsAny?>

@JsFun("(dir, name) => dir.removeEntry(name)")
private external fun directoryRemoveEntry(dir: JsAny, name: String): Promise<JsAny?>
