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
@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)

package dev.goquick.sqlitenow.core.persistence

import kotlin.JsFun
import kotlin.js.JsAny
import kotlin.js.JsArray
import kotlin.js.Promise

private val indexedDbBridge: JsAny = createIndexedDbBridge()

internal fun indexedDbLoad(
    storageName: String,
    storeName: String,
    dbName: String,
): Promise<JsAny?> = indexedDbBridgeLoad(indexedDbBridge, storageName, storeName, dbName)

internal fun indexedDbPersist(
    storageName: String,
    storeName: String,
    dbName: String,
    bytes: JsArray<JsAny?>,
): Promise<JsAny?> = indexedDbBridgePersist(indexedDbBridge, storageName, storeName, dbName, bytes)

internal fun indexedDbClear(
    storageName: String,
    storeName: String,
    dbName: String,
): Promise<JsAny?> = indexedDbBridgeClear(indexedDbBridge, storageName, storeName, dbName)

@JsFun(
    """
    () => {
      const openDatabase = (storageName, storeName) => new Promise((resolve, reject) => {
        if (typeof indexedDB === "undefined") {
          reject(new Error("IndexedDB unavailable"));
          return;
        }
        const request = indexedDB.open(storageName, 1);
        request.onupgradeneeded = () => {
          const database = request.result;
          if (!database.objectStoreNames.contains(storeName)) {
            database.createObjectStore(storeName);
          }
        };
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error ?? new Error("IndexedDB open failure"));
        request.onblocked = () => reject(new Error("IndexedDB open request was blocked"));
      });

      const transaction = (database, storeName, mode, operation) => new Promise((resolve, reject) => {
        const tx = database.transaction(storeName, mode);
        const request = operation(tx.objectStore(storeName));
        request.onsuccess = () => resolve(request.result ?? null);
        request.onerror = () => reject(request.error ?? new Error("IndexedDB request failure"));
        tx.onerror = () => reject(tx.error ?? new Error("IndexedDB transaction failure"));
        tx.onabort = () => reject(tx.error ?? new Error("IndexedDB transaction aborted"));
      });

      return {
        load: async (storageName, storeName, dbName) => {
          try {
            if (
              typeof indexedDB === "undefined" ||
              (
                typeof indexedDB.databases === "function" &&
                !(await indexedDB.databases()).some(database => database.name === storageName)
              )
            ) {
              return null;
            }
            const database = await openDatabase(storageName, storeName);
            try {
              const value = await transaction(
                database,
                storeName,
                "readonly",
                store => store.get(dbName),
              );
              if (value == null) return null;
              if (value instanceof Uint8Array) return Array.from(value);
              if (value instanceof ArrayBuffer) return Array.from(new Uint8Array(value));
              if (Array.isArray(value)) return value;
              if (value.buffer instanceof ArrayBuffer) {
                return Array.from(new Uint8Array(value.buffer, value.byteOffset ?? 0, value.byteLength));
              }
              return null;
            } finally {
              database.close();
            }
          } catch (error) {
            console.warn("[SqliteNow][IndexedDB] load failed", error);
            return null;
          }
        },
        persist: async (storageName, storeName, dbName, bytes) => {
          const database = await openDatabase(storageName, storeName);
          try {
            await transaction(
              database,
              storeName,
              "readwrite",
              store => store.put(new Uint8Array(bytes), dbName),
            );
            return null;
          } finally {
            database.close();
          }
        },
        clear: async (storageName, storeName, dbName) => {
          if (
            typeof indexedDB === "undefined" ||
            (
              typeof indexedDB.databases === "function" &&
              !(await indexedDB.databases()).some(database => database.name === storageName)
            )
          ) {
            return null;
          }
          const database = await openDatabase(storageName, storeName);
          try {
            await transaction(database, storeName, "readwrite", store => store.delete(dbName));
            return null;
          } finally {
            database.close();
          }
        },
      };
    }
    """
)
private external fun createIndexedDbBridge(): JsAny

@JsFun("(bridge, storageName, storeName, dbName) => bridge.load(storageName, storeName, dbName)")
private external fun indexedDbBridgeLoad(
    bridge: JsAny,
    storageName: String,
    storeName: String,
    dbName: String,
): Promise<JsAny?>

@JsFun(
    "(bridge, storageName, storeName, dbName, bytes) => " +
        "bridge.persist(storageName, storeName, dbName, bytes)"
)
private external fun indexedDbBridgePersist(
    bridge: JsAny,
    storageName: String,
    storeName: String,
    dbName: String,
    bytes: JsArray<JsAny?>,
): Promise<JsAny?>

@JsFun("(bridge, storageName, storeName, dbName) => bridge.clear(storageName, storeName, dbName)")
private external fun indexedDbBridgeClear(
    bridge: JsAny,
    storageName: String,
    storeName: String,
    dbName: String,
): Promise<JsAny?>
