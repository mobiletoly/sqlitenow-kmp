package dev.goquick.sqlitenow.oversqlite

import kotlin.js.Promise
import kotlinx.coroutines.await

internal actual fun suiteEnv(name: String): String? {
    val global = js("globalThis")
    val process = global.process
    val processValue = if (process != null && process.env != null) process.env[name] else null
    if (processValue != null) {
        return processValue.toString()
    }
    val globalValue = global[name]
    return globalValue?.toString()
}

internal actual fun webRuntimeKind(): String =
    if (js("typeof window === 'undefined'").unsafeCast<Boolean>()) {
        "js-node"
    } else {
        "js-browser"
    }

internal actual suspend fun cleanupWorkerDirectWorkerDatabase(dbName: String) {
    cleanupWorkerDirectWorkerDatabaseJs(dbName).await()
}

internal actual suspend fun sameOriginIframeCanAcquireLifecycleLock(dbName: String): Boolean =
    sameOriginIframeCanAcquireLifecycleLockJs(dbName).await()

@Suppress("UNUSED_PARAMETER")
private fun sameOriginIframeCanAcquireLifecycleLockJs(dbName: String): Promise<Boolean> =
    js(
        """
        (() => {
          if (typeof document === 'undefined') return false;
          return crypto.subtle.digest('SHA-256', new TextEncoder().encode(dbName)).then((digest) => {
            const hash = Array.from(new Uint8Array(digest))
              .map((byte) => byte.toString(16).padStart(2, '0')).join('');
            const lockName = 'sqlitenow-worker-v1-lifecycle-' + hash;
            return new Promise((resolve, reject) => {
              const iframe = document.createElement('iframe');
              iframe.style.display = 'none';
              const timer = setTimeout(() => finish(new Error('iframe lifecycle probe timed out')), 10000);
              const finish = (error, value) => {
                clearTimeout(timer);
                iframe.remove();
                if (error) reject(error); else resolve(value);
              };
              iframe.onload = () => {
                const frameLocks = iframe.contentWindow && iframe.contentWindow.navigator.locks;
                if (!frameLocks) {
                  finish(new Error('same-origin iframe lacks Web Locks'));
                  return;
                }
                frameLocks.request(lockName, {mode:'exclusive', ifAvailable:true}, (lock) => {
                  finish(null, !!lock);
                }).catch((error) => finish(error));
              };
              iframe.src = 'about:blank';
              document.body.appendChild(iframe);
            });
          });
        })()
        """,
    ).unsafeCast<Promise<Boolean>>()

internal actual suspend fun directWorkerStorageCensus(dbName: String): String =
    directWorkerStorageCensusJs(dbName).await()

@Suppress("UNUSED_PARAMETER")
private fun directWorkerStorageCensusJs(dbName: String): Promise<String> =
    js(
        """
        (() => {
          if (typeof document === 'undefined') return '';
          return crypto.subtle.digest('SHA-256', new TextEncoder().encode(dbName)).then((digest) => {
            const hash = Array.from(new Uint8Array(digest))
              .map((byte) => byte.toString(16).padStart(2, '0')).join('');
            const prefix = 'sqlitenow-worker-v1-' + hash + '.sqlite3';
            return navigator.storage.getDirectory().then((root) => {
              const iterator = root.entries();
              const tasks = [];
              const visit = () => iterator.next().then((item) => {
                if (item.done) return Promise.all(tasks).then((rows) => rows.sort().join('|'));
                const [name, handle] = item.value;
                if (handle.kind === 'file' && name.startsWith(prefix)) {
                  tasks.push(handle.getFile().then((file) => file.arrayBuffer()).then((buffer) => {
                    const bytes = new Uint8Array(buffer);
                    return crypto.subtle.digest('SHA-256', bytes).then((fileDigest) => {
                      const fileHash = Array.from(new Uint8Array(fileDigest))
                        .map((byte) => byte.toString(16).padStart(2, '0')).join('');
                      return name + ':' + bytes.byteLength + ':' + fileHash;
                    });
                  }));
                }
                return visit();
              });
              return visit();
            });
          });
        })()
        """,
    ).unsafeCast<Promise<String>>()

@Suppress("UNUSED_PARAMETER")
private fun cleanupWorkerDirectWorkerDatabaseJs(dbName: String): Promise<dynamic> =
    js(
        """
        (() => {
          if (typeof window === 'undefined') return Promise.resolve();
          const bytes = new TextEncoder().encode(dbName);
          return crypto.subtle.digest('SHA-256', bytes).then((digest) => {
            const hash = Array.from(new Uint8Array(digest))
              .map((byte) => byte.toString(16).padStart(2, '0')).join('');
            const target = "sqlitenow-worker-v1-" + hash + ".sqlite3";
            const ignoreMissing = (promise) => promise.catch((error) => {
              if (!error || error.name !== 'NotFoundError') throw error;
            });
            return navigator.storage.getDirectory().then((root) =>
              Promise.all(
                [target, target + ".health.json", target + ".migration.json"].map(
                  (name) => ignoreMissing(root.removeEntry(name))
                )
              )
            );
          });
        })()
        """,
    ).unsafeCast<Promise<dynamic>>()

internal actual suspend fun cleanupWorkerLegacyOpfsDatabase(dbName: String) {
    cleanupWorkerLegacyOpfsDatabaseJs(dbName).await()
}

@Suppress("UNUSED_PARAMETER")
private fun cleanupWorkerLegacyOpfsDatabaseJs(dbName: String): Promise<dynamic> =
    js(
        """
        (() => {
          if (typeof window === 'undefined') return;
          return navigator.storage.getDirectory().then((root) =>
            root.getDirectoryHandle('SqliteNow')
              .then((legacyDirectory) =>
                legacyDirectory.removeEntry(dbName + '.sqlite3').catch((error) => {
                  if (!error || error.name !== 'NotFoundError') throw error;
                })
              )
              .catch((error) => {
              if (!error || error.name !== 'NotFoundError') throw error;
              })
          );
        })()
        """,
    ).unsafeCast<Promise<dynamic>>()

internal actual suspend fun cleanupWorkerLegacyIndexedDbDatabase(dbName: String) {
    cleanupWorkerLegacyIndexedDbDatabaseJs(dbName).await()
}

@Suppress("UNUSED_PARAMETER")
private fun cleanupWorkerLegacyIndexedDbDatabaseJs(dbName: String): Promise<dynamic> =
    js(
        """
        new Promise((resolve, reject) => {
          if (typeof window === 'undefined' || !globalThis.indexedDB) {
            resolve();
            return;
          }
          const open = indexedDB.open('SqliteNow');
          open.onerror = () => reject(open.error);
          open.onsuccess = () => {
            const database = open.result;
            if (!database.objectStoreNames.contains('sqlite-databases')) {
              database.close();
              resolve();
              return;
            }
            const tx = database.transaction('sqlite-databases', 'readwrite');
            tx.objectStore('sqlite-databases').delete(dbName);
            tx.oncomplete = () => { database.close(); resolve(); };
            tx.onerror = () => { database.close(); reject(tx.error); };
            tx.onabort = () => { database.close(); reject(tx.error); };
          };
        })
        """,
    ).unsafeCast<Promise<dynamic>>()

internal actual suspend fun prepareStatementReuseleanupRegressionArtifacts(
    directDbName: String,
    legacyOpfsDbName: String,
    legacyIndexedDbName: String,
    sentinelName: String,
) {
    prepareStatementReuseleanupRegressionArtifactsJs(
        directDbName,
        legacyOpfsDbName,
        legacyIndexedDbName,
        sentinelName,
    ).await()
}

@Suppress("UNUSED_PARAMETER")
private fun prepareStatementReuseleanupRegressionArtifactsJs(
    directDbName: String,
    legacyOpfsDbName: String,
    legacyIndexedDbName: String,
    sentinelName: String,
): Promise<dynamic> {
    val body =
        """
        return (async () => {
          const hex = async (value) => {
            const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
            return Array.from(new Uint8Array(digest))
              .map((byte) => byte.toString(16).padStart(2, "0")).join("");
          };
          const writeFile = async (directory, name, text) => {
            const handle = await directory.getFileHandle(name, {create: true});
            const writable = await handle.createWritable();
            await writable.write(text);
            await writable.close();
          };
          const openDatabase = () => new Promise((resolve, reject) => {
            const request = indexedDB.open("SqliteNow");
            request.onupgradeneeded = () => {
              if (!request.result.objectStoreNames.contains("sqlite-databases")) {
                request.result.createObjectStore("sqlite-databases");
              }
            };
            request.onerror = () => reject(request.error);
            request.onsuccess = () => {
              if (request.result.objectStoreNames.contains("sqlite-databases")) {
                resolve(request.result);
                return;
              }
              const version = request.result.version + 1;
              request.result.close();
              const upgrade = indexedDB.open("SqliteNow", version);
              upgrade.onupgradeneeded = () => upgrade.result.createObjectStore("sqlite-databases");
              upgrade.onerror = () => reject(upgrade.error);
              upgrade.onsuccess = () => resolve(upgrade.result);
            };
          });
          const root = await navigator.storage.getDirectory();
          const target = "sqlitenow-worker-v1-" + await hex(directDbName) + ".sqlite3";
          for (const name of [target, target + ".health.json", target + ".migration.json"]) {
            await writeFile(root, name, "owned");
          }
          await writeFile(root, sentinelName + ".direct.sqlite3", "sentinel");
          const legacy = await root.getDirectoryHandle("SqliteNow", {create: true});
          await writeFile(legacy, legacyOpfsDbName + ".sqlite3", "owned");
          await writeFile(legacy, sentinelName + ".sqlite3", "sentinel");
          const database = await openDatabase();
          await new Promise((resolve, reject) => {
            const tx = database.transaction("sqlite-databases", "readwrite");
            const store = tx.objectStore("sqlite-databases");
            store.put(new Uint8Array([1]), legacyIndexedDbName);
            store.put(new Uint8Array([2]), sentinelName);
            tx.oncomplete = resolve;
            tx.onerror = () => reject(tx.error);
            tx.onabort = () => reject(tx.error);
          });
          database.close();
          globalThis.__workerIndexedDbDatabasesDescriptor =
            Object.getOwnPropertyDescriptor(indexedDB, "databases") || null;
          globalThis.__workerIndexedDbDatabasesCalls = 0;
          Object.defineProperty(indexedDB, "databases", {
            configurable: true,
            value: () => {
              globalThis.__workerIndexedDbDatabasesCalls++;
              throw new Error("origin catalog enumeration must not be used by worker migration cleanup");
            },
          });
        })()
        """
    return js(
        "Function('directDbName', 'legacyOpfsDbName', 'legacyIndexedDbName', " +
            "'sentinelName', body)(directDbName, legacyOpfsDbName, legacyIndexedDbName, sentinelName)",
    ).unsafeCast<Promise<dynamic>>()
}

internal actual suspend fun verifyAndClearStatementReuseleanupRegressionArtifacts(
    directDbName: String,
    legacyOpfsDbName: String,
    legacyIndexedDbName: String,
    sentinelName: String,
) {
    verifyAndClearStatementReuseleanupRegressionArtifactsJs(
        directDbName,
        legacyOpfsDbName,
        legacyIndexedDbName,
        sentinelName,
    ).await()
}

@Suppress("UNUSED_PARAMETER")
private fun verifyAndClearStatementReuseleanupRegressionArtifactsJs(
    directDbName: String,
    legacyOpfsDbName: String,
    legacyIndexedDbName: String,
    sentinelName: String,
): Promise<dynamic> {
    val body =
        """
        return (async () => {
          const restoreEnumeration = () => {
            const descriptor = globalThis.__workerIndexedDbDatabasesDescriptor;
            if (descriptor) Object.defineProperty(indexedDB, "databases", descriptor);
            else delete indexedDB.databases;
            delete globalThis.__workerIndexedDbDatabasesDescriptor;
          };
          const exists = async (directory, name) => {
            try { await directory.getFileHandle(name); return true; }
            catch (error) { if (error?.name === "NotFoundError") return false; throw error; }
          };
          const hex = async (value) => {
            const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
            return Array.from(new Uint8Array(digest))
              .map((byte) => byte.toString(16).padStart(2, "0")).join("");
          };
          try {
            if (globalThis.__workerIndexedDbDatabasesCalls !== 0) {
              throw new Error("cleanup invoked origin catalog enumeration");
            }
            const root = await navigator.storage.getDirectory();
            const target = "sqlitenow-worker-v1-" + await hex(directDbName) + ".sqlite3";
            for (const name of [target, target + ".health.json", target + ".migration.json"]) {
              if (await exists(root, name)) throw new Error("owned direct artifact remains: " + name);
            }
            if (!await exists(root, sentinelName + ".direct.sqlite3")) {
              throw new Error("unrelated direct sentinel was removed");
            }
            const legacy = await root.getDirectoryHandle("SqliteNow");
            if (await exists(legacy, legacyOpfsDbName + ".sqlite3")) {
              throw new Error("owned legacy OPFS artifact remains");
            }
            if (!await exists(legacy, sentinelName + ".sqlite3")) {
              throw new Error("unrelated legacy OPFS sentinel was removed");
            }
            const database = await new Promise((resolve, reject) => {
              const request = indexedDB.open("SqliteNow");
              request.onerror = () => reject(request.error);
              request.onsuccess = () => resolve(request.result);
            });
            const values = await new Promise((resolve, reject) => {
              const tx = database.transaction("sqlite-databases", "readonly");
              const store = tx.objectStore("sqlite-databases");
              const owned = store.get(legacyIndexedDbName);
              const sentinel = store.get(sentinelName);
              tx.oncomplete = () => resolve([owned.result, sentinel.result]);
              tx.onerror = () => reject(tx.error);
              tx.onabort = () => reject(tx.error);
            });
            if (values[0] !== undefined) throw new Error("owned IndexedDB key remains");
            if (values[1] === undefined) throw new Error("unrelated IndexedDB sentinel was removed");
            database.close();
            await root.removeEntry(sentinelName + ".direct.sqlite3");
            await legacy.removeEntry(sentinelName + ".sqlite3");
            const cleanupDatabase = await new Promise((resolve, reject) => {
              const request = indexedDB.open("SqliteNow");
              request.onerror = () => reject(request.error);
              request.onsuccess = () => resolve(request.result);
            });
            await new Promise((resolve, reject) => {
              const tx = cleanupDatabase.transaction("sqlite-databases", "readwrite");
              tx.objectStore("sqlite-databases").delete(sentinelName);
              tx.oncomplete = resolve;
              tx.onerror = () => reject(tx.error);
              tx.onabort = () => reject(tx.error);
            });
            cleanupDatabase.close();
          } finally {
            restoreEnumeration();
            delete globalThis.__workerIndexedDbDatabasesCalls;
          }
        })()
        """
    return js(
        "Function('directDbName', 'legacyOpfsDbName', 'legacyIndexedDbName', " +
            "'sentinelName', body)(directDbName, legacyOpfsDbName, legacyIndexedDbName, sentinelName)",
    ).unsafeCast<Promise<dynamic>>()
}

internal actual suspend fun workerWorkerStorageEvidence(dbName: String): String =
    workerWorkerStorageEvidenceJs(dbName).await().toString()

@Suppress("UNUSED_PARAMETER")
private fun workerWorkerStorageEvidenceJs(dbName: String): Promise<dynamic> {
    val body =
        """
        return (async () => {
          const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(dbName));
          const hash = Array.from(new Uint8Array(digest))
            .map((byte) => byte.toString(16).padStart(2, "0")).join("");
          const target = "sqlitenow-worker-v1-" + hash + ".sqlite3";
          if (typeof window === "undefined") return target + "\t0\t0\t";
          const root = await navigator.storage.getDirectory();
          const read = async (name) => {
            try {
              const file = await (await root.getFileHandle(name)).getFile();
              return await file.text();
            } catch (error) {
              if (error?.name === "NotFoundError") return null;
              throw error;
            }
          };
          const targetExists = await read(target) !== null;
          const intentExists = await read(target + ".migration.json") !== null;
          const health = await read(target + ".health.json");
          return target + "\t" + (targetExists ? "1" : "0") + "\t" +
            (intentExists ? "1" : "0") + "\t" + (health || "");
        })()
        """
    return js("Function('dbName', body)(dbName)").unsafeCast<Promise<dynamic>>()
}
