@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)

package dev.goquick.sqlitenow.oversqlite

import kotlin.js.JsAny
import kotlin.JsFun
import kotlin.js.Promise
import kotlinx.coroutines.await

@JsFun(
    """
    (name) => {
      const g = globalThis;
      if (typeof process !== 'undefined' && process?.env && process.env[name] != null) {
        return String(process.env[name]);
      }
      if (g && g[name] != null) {
        return String(g[name]);
      }
      return null;
    }
    """,
)
private external fun readWasmSuiteEnv(name: String): String?

internal actual fun suiteEnv(name: String): String? = readWasmSuiteEnv(name)

internal actual fun webRuntimeKind(): String = "wasm-browser"

internal actual suspend fun cleanupPhase6DirectWorkerDatabase(dbName: String) {
    cleanupPhase6DirectWorkerDatabaseWasm(dbName).await<JsAny?>()
}

@JsFun(
    """
    async (dbName) => {
      const bytes = new TextEncoder().encode(dbName);
      const digest = await crypto.subtle.digest("SHA-256", bytes);
      const hash = Array.from(new Uint8Array(digest))
        .map((byte) => byte.toString(16).padStart(2, "0")).join("");
      const target = "sqlitenow-worker-v1-" + hash + ".sqlite3";
      const root = await navigator.storage.getDirectory();
      for (const name of [target, target + ".health.json", target + ".migration.json"]) {
        try { await root.removeEntry(name); }
        catch (error) { if (error?.name !== "NotFoundError") throw error; }
      }
    }
    """,
)
private external fun cleanupPhase6DirectWorkerDatabaseWasm(
    dbName: String,
): Promise<JsAny?>

internal actual suspend fun cleanupPhase6LegacyOpfsDatabase(dbName: String) {
    cleanupPhase6LegacyOpfsDatabaseWasm(dbName).await<JsAny?>()
}

@JsFun(
    """
    async (dbName) => {
      const root = await navigator.storage.getDirectory();
      try {
        const legacyDirectory = await root.getDirectoryHandle("SqliteNow");
        try { await legacyDirectory.removeEntry(dbName + ".sqlite3"); }
        catch (error) { if (error?.name !== "NotFoundError") throw error; }
      } catch (error) {
        if (error?.name !== "NotFoundError") throw error;
      }
    }
    """,
)
private external fun cleanupPhase6LegacyOpfsDatabaseWasm(
    dbName: String,
): Promise<JsAny?>

internal actual suspend fun cleanupPhase6LegacyIndexedDbDatabase(dbName: String) {
    cleanupPhase6LegacyIndexedDbDatabaseWasm(dbName).await<JsAny?>()
}

@JsFun(
    """
    (dbName) => new Promise((resolve, reject) => {
      if (!globalThis.indexedDB) {
        resolve();
        return;
      }
      const open = indexedDB.open("SqliteNow");
      open.onerror = () => reject(open.error);
      open.onsuccess = () => {
        const database = open.result;
        if (!database.objectStoreNames.contains("sqlite-databases")) {
          database.close();
          resolve();
          return;
        }
        const tx = database.transaction("sqlite-databases", "readwrite");
        tx.objectStore("sqlite-databases").delete(dbName);
        tx.oncomplete = () => { database.close(); resolve(); };
        tx.onerror = () => { database.close(); reject(tx.error); };
        tx.onabort = () => { database.close(); reject(tx.error); };
      };
    })
    """,
)
private external fun cleanupPhase6LegacyIndexedDbDatabaseWasm(
    dbName: String,
): Promise<JsAny?>

internal actual suspend fun preparePhase6CleanupRegressionArtifacts(
    directDbName: String,
    legacyOpfsDbName: String,
    legacyIndexedDbName: String,
    sentinelName: String,
) {
    preparePhase6CleanupRegressionArtifactsWasm(
        directDbName,
        legacyOpfsDbName,
        legacyIndexedDbName,
        sentinelName,
    ).await<JsAny?>()
}

@JsFun(
    """
    async (directDbName, legacyOpfsDbName, legacyIndexedDbName, sentinelName) => {
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
      globalThis.__phase6IndexedDbDatabasesDescriptor =
        Object.getOwnPropertyDescriptor(indexedDB, "databases") || null;
      globalThis.__phase6IndexedDbDatabasesCalls = 0;
      Object.defineProperty(indexedDB, "databases", {
        configurable: true,
        value: () => {
          globalThis.__phase6IndexedDbDatabasesCalls++;
          throw new Error("origin catalog enumeration must not be used by Phase 6 cleanup");
        },
      });
    }
    """,
)
private external fun preparePhase6CleanupRegressionArtifactsWasm(
    directDbName: String,
    legacyOpfsDbName: String,
    legacyIndexedDbName: String,
    sentinelName: String,
): Promise<JsAny?>

internal actual suspend fun verifyAndClearPhase6CleanupRegressionArtifacts(
    directDbName: String,
    legacyOpfsDbName: String,
    legacyIndexedDbName: String,
    sentinelName: String,
) {
    verifyAndClearPhase6CleanupRegressionArtifactsWasm(
        directDbName,
        legacyOpfsDbName,
        legacyIndexedDbName,
        sentinelName,
    ).await<JsAny?>()
}

@JsFun(
    """
    async (directDbName, legacyOpfsDbName, legacyIndexedDbName, sentinelName) => {
      const restoreEnumeration = () => {
        const descriptor = globalThis.__phase6IndexedDbDatabasesDescriptor;
        if (descriptor) Object.defineProperty(indexedDB, "databases", descriptor);
        else delete indexedDB.databases;
        delete globalThis.__phase6IndexedDbDatabasesDescriptor;
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
        if (globalThis.__phase6IndexedDbDatabasesCalls !== 0) {
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
        delete globalThis.__phase6IndexedDbDatabasesCalls;
      }
    }
    """,
)
private external fun verifyAndClearPhase6CleanupRegressionArtifactsWasm(
    directDbName: String,
    legacyOpfsDbName: String,
    legacyIndexedDbName: String,
    sentinelName: String,
): Promise<JsAny?>

internal actual suspend fun phase6WorkerStorageEvidence(dbName: String): String =
    phase6WorkerStorageEvidenceWasm(dbName).await<JsAny?>().toString()

@JsFun(
    """
    async (dbName) => {
      const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(dbName));
      const hash = Array.from(new Uint8Array(digest))
        .map((byte) => byte.toString(16).padStart(2, "0")).join("");
      const target = "sqlitenow-worker-v1-" + hash + ".sqlite3";
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
    }
    """,
)
private external fun phase6WorkerStorageEvidenceWasm(dbName: String): Promise<JsAny?>
