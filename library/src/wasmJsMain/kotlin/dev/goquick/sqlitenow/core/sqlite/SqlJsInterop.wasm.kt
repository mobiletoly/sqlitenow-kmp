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
package dev.goquick.sqlitenow.core.sqlite

import dev.goquick.sqlitenow.common.sqliteNowLogger
import kotlin.JsFun
import kotlin.js.JsArray
import kotlin.js.JsAny
import kotlin.js.toJsNumber
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

internal fun jsArrayOfSize(size: Int): JsArray<JsAny?> {
    val array = JsArray<JsAny?>()
    for (index in 0 until size) {
        jsArraySetNull(array, index)
    }
    return array
}

internal fun ByteArray.toSqlJsArray(): JsArray<JsAny?> {
    val array = JsArray<JsAny?>()
    for (i in indices) {
        jsArraySetValue(array, i, (this[i].toInt() and 0xFF).toJsNumber())
    }
    return array
}

internal fun JsArray<JsAny?>.asByteArray(): ByteArray {
    val result = ByteArray(length)
    for (i in 0 until length) {
        result[i] = (toNumber(this[i]).toInt() and 0xFF).toByte()
    }
    return result
}

@JsFun("(value) => Array.isArray(value)")
internal external fun isJsArray(value: JsAny?): Boolean

@JsFun("(value) => value == null")
internal external fun isNull(value: JsAny?): Boolean

@JsFun("(value) => Number(value)")
internal external fun toNumber(value: JsAny?): Double

@JsFun("(value) => String(value)")
internal external fun toStringValue(value: JsAny?): String

@JsFun("(value) => value")
internal external fun asJsArray(value: JsAny?): JsArray<JsAny?>

@JsFun("(array, index) => { array[index] = null; }")
internal external fun jsArraySetNull(array: JsArray<JsAny?>, index: Int)

@JsFun("(array, index, value) => { array[index] = value; }")
internal external fun jsArraySetValue(array: JsArray<JsAny?>, index: Int, value: JsAny?)

@JsFun("(value) => typeof value")
internal external fun jsTypeOf(value: JsAny?): String
