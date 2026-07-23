package dev.goquick.sqlitenow.core.worker

import kotlin.js.Promise
import kotlinx.coroutines.await

private val legacyStorageProbe: dynamic = js(
    """
    ({
      opfs: function(dbName) {
        if (
          typeof navigator === "undefined" ||
          !navigator.storage ||
          typeof navigator.storage.getDirectory !== "function"
        ) {
          return Promise.resolve(false);
        }
        return navigator.storage.getDirectory()
          .then(function(root) {
            return root.getDirectoryHandle("SqliteNow");
          })
          .then(function(directory) {
            return directory.getFileHandle(dbName + ".sqlite3");
          })
          .then(
            function() { return true; },
            function(error) {
              if (error && error.name === "NotFoundError") return false;
              throw error;
            }
          );
      },
      indexedDb: function(dbName) {
        if (
          typeof indexedDB === "undefined" ||
          typeof indexedDB.databases !== "function"
        ) {
          return Promise.reject(
            new Error("Read-only IndexedDB database enumeration is unavailable.")
          );
        }
        return indexedDB.databases().then(function(databases) {
          if (!databases.some(function(database) { return database.name === "SqliteNow"; })) {
            return false;
          }
          return new Promise(function(resolve, reject) {
            var request = indexedDB.open("SqliteNow");
            request.onsuccess = function() { resolve(request.result); };
            request.onerror = function() { reject(request.error); };
          }).then(function(database) {
            if (!database.objectStoreNames.contains("sqlite-databases")) {
              database.close();
              return false;
            }
            return new Promise(function(resolve, reject) {
              var transaction = database.transaction("sqlite-databases", "readonly");
              var request = transaction.objectStore("sqlite-databases").get(dbName);
              request.onsuccess = function() { resolve(request.result != null); };
              request.onerror = function() { reject(request.error); };
            }).then(function(result) {
              database.close();
              return result;
            }, function(error) {
              database.close();
              throw error;
            });
          });
        });
      },
      workerArtifact: function(dbName, suffix) {
        if (
          typeof navigator === "undefined" ||
          !navigator.storage ||
          typeof navigator.storage.getDirectory !== "function" ||
          !globalThis.crypto ||
          !globalThis.crypto.subtle ||
          typeof globalThis.crypto.subtle.digest !== "function"
        ) {
          return Promise.reject(
            new Error("Read-only worker storage inspection is unavailable.")
          );
        }
        return globalThis.crypto.subtle.digest(
          "SHA-256",
          new TextEncoder().encode(dbName)
        ).then(function(digest) {
          var hash = Array.from(new Uint8Array(digest))
            .map(function(byte) { return byte.toString(16).padStart(2, "0"); })
            .join("");
          return navigator.storage.getDirectory().then(function(root) {
            return root.getFileHandle(
              "sqlitenow-worker-v1-" + hash + ".sqlite3" + suffix
            );
          });
        }).then(
          function() { return true; },
          function(error) {
            if (error && error.name === "NotFoundError") return false;
            throw error;
          }
        );
      },
    })
    """
)

internal actual suspend fun legacySnapshotExistsForTest(
    dbName: String,
    forceOpfs: Boolean,
): Boolean {
    val result: Promise<Boolean> = if (forceOpfs) {
        legacyStorageProbe.opfs(dbName)
    } else {
        legacyStorageProbe.indexedDb(dbName)
    }
    return result.await()
}

internal actual suspend fun workerStorageArtifactsForTest(dbName: String): Set<String> {
    val artifacts = listOf(
        "target" to "",
        "migration" to ".migration.json",
        "health" to ".health.json",
    )
    return artifacts.filter { (_, suffix) ->
        val result: Promise<Boolean> = legacyStorageProbe.workerArtifact(dbName, suffix)
        result.await()
    }.mapTo(mutableSetOf()) { it.first }
}
