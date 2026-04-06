@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)
@file:JsModule("./sqlitenow-indexeddb.js")

package dev.goquick.sqlitenow.core.persistence

import kotlin.js.JsAny
import kotlin.js.JsArray
import kotlin.js.Promise

external fun indexedDbLoad(
    storageName: String,
    storeName: String,
    dbName: String,
): Promise<JsAny?>

external fun indexedDbPersist(
    storageName: String,
    storeName: String,
    dbName: String,
    bytes: JsArray<JsAny?>,
): Promise<JsAny?>

external fun indexedDbClear(
    storageName: String,
    storeName: String,
    dbName: String,
): Promise<JsAny?>
