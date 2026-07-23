@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)

package dev.goquick.sqlitenow.core.sqlite

import dev.goquick.sqlitenow.common.sqliteNowLogger
import kotlin.JsFun
import kotlin.js.JsAny
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.await
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

internal data class SqlJsDatabaseHandle(val value: Int)

internal data class SqlJsStatementHandle(val value: Int)

private val moduleMutex = Mutex()
private val moduleDeferred = CompletableDeferred<Unit>()

private fun buildSqlJsConfig(): JsAny {
    val locateFile: (String) -> String = { fileName ->
        if (fileName == "sql-wasm.wasm") "/sql-wasm.wasm" else "/$fileName"
    }
    return createSqlJsConfig(locateFile)
}

@JsFun("(locateFile) => ({ locateFile })")
private external fun createSqlJsConfig(locateFile: (String) -> String): JsAny

internal suspend fun ensureSqlJsLoaded() {
    if (moduleDeferred.isCompleted) {
        moduleDeferred.await()
        return
    }

    moduleMutex.withLock {
        if (!moduleDeferred.isCompleted) {
            try {
                sqliteNowLogger.i { "[SqlJs][Wasm] Loading sql.js module…" }
                loadSqlJs(buildSqlJsConfig()).await<JsAny>()
                sqliteNowLogger.i { "[SqlJs][Wasm] sql.js module ready" }
                moduleDeferred.complete(Unit)
            } catch (t: Throwable) {
                sqliteNowLogger.e(t) { "[SqlJs][Wasm] Failed to load sql.js" }
                moduleDeferred.completeExceptionally(t)
            }
        }
    }

    moduleDeferred.await()
}
