const databaseCache = new Map();

function cacheKey(storageName, storeName) {
  return `${storageName}::${storeName}`;
}

function toUint8Array(bytes) {
  if (bytes == null) {
    return new Uint8Array(0);
  }
  if (bytes instanceof Uint8Array) {
    return bytes;
  }
  if (Array.isArray(bytes)) {
    return new Uint8Array(bytes);
  }
  if (bytes.buffer instanceof ArrayBuffer) {
    return new Uint8Array(bytes.buffer, bytes.byteOffset ?? 0, bytes.byteLength ?? bytes.length);
  }
  if (typeof bytes.length === "number") {
    return new Uint8Array(bytes);
  }
  throw new Error("Unsupported bytes container: " + Object.prototype.toString.call(bytes));
}

function ensureDatabase(storageName, storeName) {
  const key = cacheKey(storageName, storeName);
  const existing = databaseCache.get(key);
  if (existing) {
    return existing;
  }

  const promise = new Promise((resolve, reject) => {
    if (typeof indexedDB === "undefined") {
      reject(new Error("IndexedDB unavailable"));
      return;
    }

    const request = indexedDB.open(storageName, 1);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(storeName)) {
        db.createObjectStore(storeName);
      }
    };
    request.onsuccess = () => {
      const db = request.result;
      db.onversionchange = () => {
        db.close();
        databaseCache.delete(key);
      };
      resolve(db);
    };
    request.onerror = () => {
      reject(request.error ?? new Error("IndexedDB open failure"));
    };
    request.onblocked = () => {
      console.warn(`(sqlitenow) IndexedDB: open request for ${storageName} blocked`);
    };
  });

  databaseCache.set(key, promise);
  return promise;
}

function runTransaction(db, storeName, mode, executor) {
  return new Promise((resolve, reject) => {
    const tx = db.transaction(storeName, mode);
    const store = tx.objectStore(storeName);
    executor(store, resolve, reject);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error ?? new Error("IndexedDB transaction error"));
    tx.onabort = () => reject(tx.error ?? new Error("IndexedDB transaction aborted"));
  });
}

export async function indexedDbLoad(storageName, storeName, dbName) {
  try {
    const db = await ensureDatabase(storageName, storeName);
    console.debug(`[SqliteNow][IndexedDB] load -> ${storageName}/${storeName}/${dbName}`);
    return await new Promise((resolve, reject) => {
      const tx = db.transaction(storeName, "readonly");
      const store = tx.objectStore(storeName);
      const request = store.get(dbName);
      request.onsuccess = () => {
        const value = request.result;
        if (value == null) {
          console.debug("[SqliteNow][IndexedDB] load -> no snapshot");
          resolve(null);
          return;
        }
        if (value instanceof Uint8Array) {
          resolve(Array.from(value));
          return;
        }
        if (value instanceof ArrayBuffer) {
          resolve(Array.from(new Uint8Array(value)));
          return;
        }
        if (Array.isArray(value)) {
          resolve(value);
          return;
        }
        if (value.buffer instanceof ArrayBuffer) {
          resolve(Array.from(new Uint8Array(value.buffer)));
          return;
        }
        console.warn("[SqliteNow][IndexedDB] Unsupported value type", value);
        resolve(null);
      };
      request.onerror = () => reject(request.error ?? new Error("IndexedDB read error"));
      tx.onerror = () => reject(tx.error ?? new Error("IndexedDB transaction error"));
      tx.onabort = () => reject(tx.error ?? new Error("IndexedDB transaction aborted"));
      tx.oncomplete = () => {};
    });
  } catch (error) {
    console.warn("[SqliteNow][IndexedDB] load failed", error);
    return null;
  }
}

export async function indexedDbPersist(storageName, storeName, dbName, bytes) {
  try {
    console.debug(`[SqliteNow][IndexedDB] persist -> ${storageName}/${storeName}/${dbName} (${bytes.length ?? 'unknown'} entries)`);
    const db = await ensureDatabase(storageName, storeName);
    const payload = toUint8Array(bytes);
    await runTransaction(db, storeName, "readwrite", (store, resolve, reject) => {
      const request = store.put(payload, dbName);
      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error ?? new Error("IndexedDB write error"));
    });
  } catch (error) {
    console.error("[SqliteNow][IndexedDB] persist failed", error);
  }
}

export async function indexedDbClear(storageName, storeName, dbName) {
  try {
    const db = await ensureDatabase(storageName, storeName);
    await runTransaction(db, storeName, "readwrite", (store, resolve, reject) => {
      const request = store.delete(dbName);
      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error ?? new Error("IndexedDB delete error"));
    });
  } catch (error) {
    console.warn("[SqliteNow][IndexedDB] clear failed", error);
  }
}
