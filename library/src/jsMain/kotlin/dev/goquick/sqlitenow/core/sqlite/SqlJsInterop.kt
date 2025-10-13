/*
 * Copyright 2025 Anatoliy Pochkin
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

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.await
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.js.Promise
import kotlin.js.jsTypeOf

@JsModule("sql.js/dist/sql-wasm.js")
@JsNonModule
external fun initSqlJs(config: SqlJsConfig = definedExternally): Promise<SqlJsModule>

external interface SqlJsConfig {
    var locateFile: (String) -> String
}

external interface SqlJsModule {
    @JsName("Database")
    val Database: dynamic
}

external interface SqlJsDatabase {
    fun run(sql: String)
    fun exec(sql: String)
    fun prepare(sql: String): SqlJsStatement
    fun close()
}

external interface SqlJsStatement {
    fun bind(values: Array<dynamic>): Boolean
    fun step(): Boolean
    fun get(params: dynamic = definedExternally, config: dynamic = definedExternally): Array<dynamic>
    fun reset()
    fun free()
}

private val moduleMutex = Mutex()
private val moduleDeferred = CompletableDeferred<SqlJsModule>()

internal suspend fun loadSqlJsModule(): SqlJsModule {
    if (!moduleDeferred.isCompleted) {
        moduleMutex.withLock {
            if (!moduleDeferred.isCompleted) {
                val config = js("({})").unsafeCast<SqlJsConfig>()
                config.locateFile = { fileName: String ->
                    when (fileName) {
                        "sql-wasm.wasm" -> {
                            val isNodeRuntime = js(
                                "typeof process !== 'undefined' && process.versions != null && process.versions.node != null"
                            ).unsafeCast<Boolean>()
                            if (isNodeRuntime) {
                                js("require('path').resolve(require.resolve('sql.js/dist/sql-wasm.wasm'))").unsafeCast<String>()
                            } else {
                                "./sql-wasm.wasm"
                            }
                        }
                        else -> "./$fileName"
                    }
                }
                try {
                    console.log("[SqlJs] loading sql.js module…")
                    val module = initSqlJs(config).await()
                    console.log("[SqlJs] sql.js module loaded")
                    moduleDeferred.complete(module)
                } catch (t: Throwable) {
                    console.error("[SqlJs] Failed to load sql.js", t)
                    moduleDeferred.completeExceptionally(t)
                }
            }
        }
    }
    return moduleDeferred.await()
}
