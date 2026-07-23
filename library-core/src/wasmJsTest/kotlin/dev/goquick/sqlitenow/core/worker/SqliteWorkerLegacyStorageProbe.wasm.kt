@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)

package dev.goquick.sqlitenow.core.worker

import kotlin.JsFun
import kotlin.js.JsAny
import kotlin.js.Promise
import kotlinx.coroutines.await

internal actual suspend fun legacySnapshotExistsForTest(
    dbName: String,
    forceOpfs: Boolean,
): Boolean {
    return isTrue(probeLegacyStorage(dbName, forceOpfs).await<JsAny>())
}

internal actual suspend fun workerStorageArtifactsForTest(dbName: String): Set<String> {
    val artifacts = listOf(
        "target" to "",
        "migration" to ".migration.json",
        "health" to ".health.json",
    )
    return artifacts.filter { (_, suffix) ->
        isTrue(probeWorkerStorageArtifact(dbName, suffix).await<JsAny>())
    }.mapTo(mutableSetOf()) { it.first }
}

@JsFun(
    """
    (dbName, forceOpfs) => {
      if (forceOpfs) {
        return (async () => {
          if (
            typeof navigator === "undefined" ||
            !navigator.storage ||
            typeof navigator.storage.getDirectory !== "function"
          ) {
            return false;
          }
          try {
            const root = await navigator.storage.getDirectory();
            const directory = await root.getDirectoryHandle("SqliteNow");
            await directory.getFileHandle(dbName + ".sqlite3");
            return true;
          } catch (error) {
            if (error && error.name === "NotFoundError") return false;
            throw error;
          }
        })();
      }
      return (async () => {
        if (
          typeof indexedDB === "undefined" ||
          typeof indexedDB.databases !== "function"
        ) {
          throw new Error("Read-only IndexedDB database enumeration is unavailable.");
        }
        const databases = await indexedDB.databases();
        if (!databases.some((database) => database.name === "SqliteNow")) return false;
        const database = await new Promise((resolve, reject) => {
          const request = indexedDB.open("SqliteNow");
          request.onsuccess = () => resolve(request.result);
          request.onerror = () => reject(request.error);
        });
        try {
          if (!database.objectStoreNames.contains("sqlite-databases")) return false;
          return await new Promise((resolve, reject) => {
            const transaction = database.transaction("sqlite-databases", "readonly");
            const request = transaction.objectStore("sqlite-databases").get(dbName);
            request.onsuccess = () => resolve(request.result != null);
            request.onerror = () => reject(request.error);
          });
        } finally {
          database.close();
        }
      })();
    }
    """
)
private external fun probeLegacyStorage(dbName: String, forceOpfs: Boolean): Promise<JsAny>

@JsFun(
    """
    (dbName, suffix) => (async () => {
      if (
        typeof navigator === "undefined" ||
        !navigator.storage ||
        typeof navigator.storage.getDirectory !== "function" ||
        typeof globalThis.crypto?.subtle?.digest !== "function"
      ) {
        throw new Error("Read-only worker storage inspection is unavailable.");
      }
      const digest = await globalThis.crypto.subtle.digest(
        "SHA-256",
        new TextEncoder().encode(dbName)
      );
      const hash = Array.from(new Uint8Array(digest))
        .map((byte) => byte.toString(16).padStart(2, "0"))
        .join("");
      try {
        const root = await navigator.storage.getDirectory();
        await root.getFileHandle(
          "sqlitenow-worker-v1-" + hash + ".sqlite3" + suffix
        );
        return true;
      } catch (error) {
        if (error && error.name === "NotFoundError") return false;
        throw error;
      }
    })()
    """
)
private external fun probeWorkerStorageArtifact(
    dbName: String,
    suffix: String,
): Promise<JsAny>

@JsFun("(value) => value === true")
private external fun isTrue(value: JsAny): Boolean
