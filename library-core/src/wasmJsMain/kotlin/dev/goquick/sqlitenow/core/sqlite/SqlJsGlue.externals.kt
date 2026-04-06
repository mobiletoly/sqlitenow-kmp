@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)
@file:JsModule("./sqlitenow-sqljs.js")

package dev.goquick.sqlitenow.core.sqlite

import kotlin.js.JsAny
import kotlin.js.JsArray
import kotlin.js.Promise

internal external fun loadSqlJs(config: JsAny? = definedExternally): Promise<JsAny>
internal external fun dbCreate(): Int
internal external fun dbOpen(bytes: JsArray<JsAny?>): Int
internal external fun dbClose(handle: Int)
internal external fun dbExec(handle: Int, sql: String)
internal external fun stmtPrepare(dbHandle: Int, sql: String): Int
internal external fun stmtBind(stmtHandle: Int, params: JsArray<JsAny?>): Boolean
internal external fun stmtStep(stmtHandle: Int): Boolean
internal external fun stmtGetRow(stmtHandle: Int): JsArray<JsAny?>?
internal external fun stmtReset(stmtHandle: Int)
internal external fun stmtClearBindings(stmtHandle: Int)
internal external fun stmtGetColumnCount(stmtHandle: Int): Int
internal external fun stmtGetColumnName(stmtHandle: Int, columnIndex: Int): String
internal external fun stmtGetValue(stmtHandle: Int, columnIndex: Int): JsAny?
internal external fun stmtFinalize(stmtHandle: Int)
internal external fun dbExport(handle: Int): JsArray<JsAny?>
