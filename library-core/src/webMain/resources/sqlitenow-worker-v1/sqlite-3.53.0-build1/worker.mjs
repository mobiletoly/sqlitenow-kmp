/*
 * Copyright 2026 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0.
 *
 * SQLiteNow packaged worker module for @sqlite.org/sqlite-wasm 3.53.0-build1.
 */

const PROTOCOL = "sqlitenow-sqlite-worker-v1";
const COMMANDS = new Set([
  "open",
  "completeOpen",
  "prepare",
  "page",
  "reset",
  "clearBindings",
  "closeStatement",
  "closeDatabase",
  "metrics",
  "shutdown",
]);
const REQUEST_KEYS = new Set([
  "protocol",
  "command",
  "databaseId",
  "statementId",
  "fileName",
  "sql",
  "bindings",
  "pageRows",
  "pageBytes",
  "legacySourceMode",
  "openId",
  "legacySourceStatus",
]);
const COMMAND_KEYS = new Map([
  [
    "open",
    new Set(["protocol", "command", "fileName", "legacySourceMode"]),
  ],
  [
    "completeOpen",
    new Set(["protocol", "command", "openId", "legacySourceStatus"]),
  ],
  ["prepare", new Set(["protocol", "command", "databaseId", "sql"])],
  [
    "page",
    new Set([
      "protocol",
      "command",
      "databaseId",
      "statementId",
      "sql",
      "bindings",
      "pageRows",
      "pageBytes",
    ]),
  ],
  [
    "reset",
    new Set(["protocol", "command", "databaseId", "statementId", "sql"]),
  ],
  [
    "clearBindings",
    new Set(["protocol", "command", "databaseId", "statementId", "sql"]),
  ],
  [
    "closeStatement",
    new Set(["protocol", "command", "databaseId", "statementId", "sql"]),
  ],
  ["closeDatabase", new Set(["protocol", "command", "databaseId"])],
  ["metrics", new Set(["protocol", "command"])],
  ["shutdown", new Set(["protocol", "command"])],
]);
const VALUE_KEYS = new Map([
  ["null", new Set(["type"])],
  ["integer", new Set(["type", "integer"])],
  ["real", new Set(["type", "real"])],
  ["text", new Set(["type", "text"])],
  ["blob", new Set(["type", "blob"])],
]);
const MAX_PAGE_ROWS = 1024;
const MIN_PAGE_BYTES = 2;
const MAX_PAGE_BYTES = 1024 * 1024;
const HARD_ROW_BYTES = 1024 * 1024;
const MIN_REQUEST_ID = -2_147_483_648;
const MAX_REQUEST_ID = 2_147_483_647;
const LEGACY_OPFS_DIRECTORY = "SqliteNow";
const LEGACY_INDEXED_DB = "SqliteNow";
const LEGACY_INDEXED_DB_STORE = "sqlite-databases";
const HEALTH_SCHEMA = "sqlitenow-worker-health-v1";
const INTENT_SCHEMA = "sqlitenow-worker-migration-intent-v1";
const HEALTH_STATE = "healthy";
const INTENT_STATE = "importing";
const SOURCE_KINDS = new Set(["opfs", "indexeddb", "custom"]);
const isNodeRuntime = Boolean(globalThis.process?.versions?.node);
const textEncoder = new TextEncoder();

let parentPort;
if (isNodeRuntime) {
  ({ parentPort } = await import("node:worker_threads"));
}

const modulePath = isNodeRuntime
  ? "./vendor/node.mjs"
  : "./vendor/index.mjs";
const { default: sqlite3InitModule } = await import(modulePath);
if (isNodeRuntime) {
  globalThis.sqlite3ApiConfig = {
    disable: {
      vfs: {
        opfs: true,
        "opfs-vfs": true,
        "opfs-sahpool": true,
        "opfs-wl": true,
      },
    },
  };
}
const sqlite3 = await sqlite3InitModule({
  print: () => {},
  printErr: () => {},
});
const { capi, oo1 } = sqlite3;

const databases = new Map();
const statements = new Map();
const pendingOpens = new Map();
let nextDatabaseId = 1;
let nextStatementId = 1;
let nextOpenId = 1;
let requestQueue = Promise.resolve();
let highestPositiveRequestId = 0;
let lowestNegativeRequestId = 0;
const completedRequests = new Map();
let cancellationCleanupFailuresForTest = null;
let acknowledgementModeForTest = null;
let releaseModeForTest = null;
let negativeReconciliationForTest = false;
let shutdownCleanupFailuresForTest = null;
let holdNextActivePageForTest = false;
let directCapabilityMissingForTest = null;
const migrationInterruptionsForTest = new Map();
let nextMigrationHeapSamplesForTest = null;
const migrationCancellationHoldsForTest = new Map();
let nextCompleteOpenCancellationHoldForTest = null;
const usedCancellationHoldIdsForTest = new Set();

const metrics = {
  runtimeKind: isNodeRuntime ? "js-node-worker" : "browser-worker",
  sqliteVersion: sqlite3.version.libVersion,
  storageMode: "none",
  requestsStarted: 0,
  requestsCompleted: 0,
  requestsCancelled: 0,
  pendingRequests: 0,
  liveDatabases: 0,
  liveStatements: 0,
  transactionsRolledBackOnCancel: 0,
  integerBindingsAsStrings: 0,
  integerResultsAsStrings: 0,
  integerNumberViolations: 0,
  pageRequests: 0,
  steppedRows: 0,
  encodedRows: 0,
  transferredRows: 0,
  transferredBytes: 0,
  maxPageRows: 0,
  maxPageBytes: 0,
  oversizedRows: 0,
  snapshotExports: 0,
  migrationSourceKind: "",
  migrationSourceBytes: 0,
  migrationDurationMillis: 0,
  migrationPeakOwnedBytes: 0,
  migrationTargetFileName: "",
  migrationSourceSha256: "",
  migrationIntegrityCheck: "",
  migrationImportedUserVersion: 0,
  migrationSourceRetained: false,
  migrationHeapAvailable: false,
  migrationHeapStartBytes: 0,
  migrationHeapPeakBytes: 0,
  migrationHeapEndBytes: 0,
  workerStarts: 1,
  workerStops: 0,
};

function post(message) {
  if (isNodeRuntime) parentPort.postMessage(message);
  else globalThis.postMessage(message);
}

function validateExactKeys(value, allowed, label) {
  if (value == null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object.`);
  }
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) throw new TypeError(`Unknown ${label} field: ${key}`);
  }
}

function listen(listener) {
  if (isNodeRuntime) parentPort.on("message", listener);
  else globalThis.addEventListener("message", (event) => listener(event.data));
}

function response(data = {}) {
  return {
    protocol: PROTOCOL,
    done: false,
    oversizedRow: false,
    pageRows: 0,
    pageBytes: 0,
    inTransaction: false,
    ...data,
  };
}

function errorEnvelope(id, operation, error, request, cancelled = false) {
  const database = databases.get(request?.databaseId);
  const workerError = {
    operation,
    message: String(error?.message ?? error),
    cancelled,
    suppressed: error?.workerSuppressed ?? [],
  };
  if (request?.sql != null) workerError.sql = request.sql;
  if (Number.isInteger(error?.resultCode)) workerError.sqliteCode = error.resultCode;
  if (database) workerError.inTransaction = inTransaction(database);
  return {
    id,
    error: workerError,
  };
}

function canonicalInteger(value) {
  if (typeof value !== "string" || !/^-?(0|[1-9][0-9]*)$/.test(value)) {
    return false;
  }
  if (value === "-0") return false;
  try {
    return BigInt.asIntN(64, BigInt(value)).toString() === value;
  } catch (_) {
    return false;
  }
}

function validateValue(value) {
  if (value == null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("SQLite worker values must be tagged objects.");
  }
  const allowedKeys = VALUE_KEYS.get(value.type);
  if (!allowedKeys) {
    throw new TypeError(`Unsupported SQLite value tag: ${String(value.type)}`);
  }
  for (const key of Object.keys(value)) {
    if (!allowedKeys.has(key)) {
      throw new TypeError(`Unknown ${String(value.type)} value field: ${key}`);
    }
  }
  const populated = ["integer", "real", "text", "blob"].filter(
    (key) => value[key] !== undefined,
  );
  switch (value.type) {
    case "null":
      if (populated.length !== 0) throw new TypeError("Invalid null value payload.");
      break;
    case "integer":
      if (populated.join() !== "integer" || !canonicalInteger(value.integer)) {
        metrics.integerNumberViolations++;
        throw new TypeError("SQLite INTEGER must be canonical signed 64-bit decimal text.");
      }
      break;
    case "real":
      if (populated.join() !== "real" || !Number.isFinite(value.real)) {
        throw new TypeError("SQLite REAL must be finite.");
      }
      break;
    case "text":
      if (populated.join() !== "text" || typeof value.text !== "string") {
        throw new TypeError("Invalid SQLite TEXT payload.");
      }
      break;
    case "blob":
      if (
        populated.join() !== "blob" ||
        !Array.isArray(value.blob) ||
        value.blob.some((byte) => !Number.isInteger(byte) || byte < 0 || byte > 255)
      ) {
        throw new TypeError("Invalid SQLite BLOB payload.");
      }
      break;
    default:
      throw new TypeError(`Unsupported SQLite value tag: ${String(value.type)}`);
  }
}

function positiveHandle(value, label) {
  if (!Number.isInteger(value) || value <= 0 || value > MAX_REQUEST_ID) {
    throw new TypeError(`A positive ${label} is required.`);
  }
}

function allocateHandle(kind) {
  const current =
    kind === "database"
      ? nextDatabaseId
      : kind === "statement"
        ? nextStatementId
        : nextOpenId;
  if (current > MAX_REQUEST_ID) {
    throw new Error(`SQLite worker ${kind} handles are exhausted.`);
  }
  if (kind === "database") nextDatabaseId++;
  else if (kind === "statement") nextStatementId++;
  else nextOpenId++;
  return current;
}

function canonicalBindingIndex(rawIndex) {
  if (!/^[1-9][0-9]*$/.test(rawIndex)) {
    throw new TypeError(`Binding index ${rawIndex} is not canonical positive decimal text.`);
  }
  const index = Number(rawIndex);
  if (!Number.isSafeInteger(index) || String(index) !== rawIndex) {
    throw new TypeError(`Binding index ${rawIndex} is outside the safe canonical range.`);
  }
  return index;
}

function requireDirectOpfs() {
  const missing = [];
  if (
    directCapabilityMissingForTest === "web-crypto" ||
    typeof globalThis.crypto?.subtle?.digest !== "function"
  ) {
    missing.push("Web Crypto");
  }
  if (
    directCapabilityMissingForTest === "opfs" ||
    typeof globalThis.navigator?.storage?.getDirectory !== "function"
  ) {
    missing.push("Origin Private File System");
  }
  if (
    directCapabilityMissingForTest === "web-locks" ||
    typeof globalThis.navigator?.locks?.request !== "function"
  ) {
    missing.push("Web Locks");
  }
  if (
    directCapabilityMissingForTest === "opfs-vfs" ||
    typeof oo1.OpfsDb !== "function"
  ) {
    missing.push("SQLite OPFS VFS");
  }
  if (missing.length > 0) {
    throw new Error(
      `SQLite worker direct persistence requires ${missing.join(", ")}; ` +
        "no snapshot or in-memory browser fallback was started.",
    );
  }
}

function validateRequest(request) {
  if (request == null || typeof request !== "object" || Array.isArray(request)) {
    throw new TypeError("SQLite worker request must be an object.");
  }
  if (request.protocol !== PROTOCOL) {
    throw new Error("Unsupported SQLite worker protocol.");
  }
  for (const key of Object.keys(request)) {
    if (!REQUEST_KEYS.has(key)) throw new TypeError(`Unknown request field: ${key}`);
  }
  if (!COMMANDS.has(request.command)) {
    throw new Error(`Unsupported SQLite worker command: ${String(request.command)}`);
  }
  const commandKeys = COMMAND_KEYS.get(request.command);
  for (const key of Object.keys(request)) {
    if (!commandKeys.has(key)) {
      throw new TypeError(
        `Field ${key} is not valid for SQLite worker command ${request.command}.`,
      );
    }
  }
  const bindings = Object.prototype.hasOwnProperty.call(request, "bindings")
    ? request.bindings
    : {};
  if (bindings == null || typeof bindings !== "object" || Array.isArray(bindings)) {
    throw new TypeError("SQLite worker bindings must be an object.");
  }
  for (const [index, value] of Object.entries(bindings)) {
    canonicalBindingIndex(index);
    validateValue(value);
  }
  switch (request.command) {
    case "open":
      if (typeof request.fileName !== "string" || request.fileName.trim() === "") {
        throw new TypeError("SQLite worker database name must be non-empty.");
      }
      if (!["built-in", "custom", "none"].includes(request.legacySourceMode)) {
        throw new TypeError("SQLite worker open requires a supported legacySourceMode.");
      }
      break;
    case "completeOpen":
      positiveHandle(request.openId, "openId");
      if (!["present", "absent"].includes(request.legacySourceStatus)) {
        throw new TypeError(
          "SQLite worker completeOpen requires a supported legacySourceStatus.",
        );
      }
      break;
    case "prepare":
      positiveHandle(request.databaseId, "databaseId");
      if (typeof request.sql !== "string" || request.sql.trim() === "") {
        throw new TypeError("A non-empty SQL statement is required.");
      }
      break;
    case "page":
      positiveHandle(request.databaseId, "databaseId");
      positiveHandle(request.statementId, "statementId");
      if (
        !Number.isInteger(request.pageRows) ||
        request.pageRows < 1 ||
        request.pageRows > MAX_PAGE_ROWS ||
        !Number.isInteger(request.pageBytes) ||
        request.pageBytes < MIN_PAGE_BYTES ||
        request.pageBytes > MAX_PAGE_BYTES
      ) {
        throw new Error("Invalid SQLite worker page limits.");
      }
      break;
    case "reset":
    case "clearBindings":
    case "closeStatement":
      positiveHandle(request.databaseId, "databaseId");
      positiveHandle(request.statementId, "statementId");
      break;
    case "closeDatabase":
      positiveHandle(request.databaseId, "databaseId");
      break;
    default:
      break;
  }
  return bindings;
}

function hexSha256(digest) {
  return Array.from(new Uint8Array(digest))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

async function sha256(bytes) {
  return hexSha256(
    await globalThis.crypto.subtle.digest("SHA-256", bytes),
  );
}

async function databaseIdentity(databaseName) {
  const digest = await globalThis.crypto.subtle.digest(
    "SHA-256",
    textEncoder.encode(databaseName),
  );
  const databaseNameSha256 = hexSha256(digest);
  return {
    databaseNameSha256,
    targetFileName: `sqlitenow-worker-v1-${databaseNameSha256}.sqlite3`,
    lockName: `sqlitenow-worker-v1-migrate-${databaseNameSha256}`,
  };
}

async function rootDirectory() {
  return globalThis.navigator.storage.getDirectory();
}

async function rootFileExists(name) {
  const root = await rootDirectory();
  try {
    await root.getFileHandle(name);
    return true;
  } catch (error) {
    if (error?.name === "NotFoundError") return false;
    throw error;
  }
}

async function readRootFile(name) {
  const root = await rootDirectory();
  try {
    const handle = await root.getFileHandle(name);
    return await handle.getFile();
  } catch (error) {
    if (error?.name === "NotFoundError") return null;
    throw error;
  }
}

async function writeRootText(name, text) {
  const root = await rootDirectory();
  const handle = await root.getFileHandle(name, { create: true });
  const writer = await handle.createWritable();
  try {
    await writer.write(text);
  } finally {
    await writer.close();
  }
}

async function removeRootEntry(name) {
  const root = await rootDirectory();
  try {
    await root.removeEntry(name);
  } catch (error) {
    if (error?.name !== "NotFoundError") throw error;
  }
}

function healthMarkerName(targetFileName) {
  return `${targetFileName}.health.json`;
}

function intentMarkerName(targetFileName) {
  return `${targetFileName}.migration.json`;
}

const INTENT_KEYS = [
  "schema",
  "state",
  "protocol",
  "targetFileName",
  "databaseNameSha256",
  "sourceKind",
  "sourceSha256",
  "sourceBytes",
];
const HEALTH_KEYS = [
  ...INTENT_KEYS,
  "integrityCheck",
  "importedUserVersion",
];

function validLowerSha256(value) {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
}

function parseStrictMarker(text, markerKind, identity) {
  let marker;
  try {
    marker = JSON.parse(text);
  } catch (error) {
    throw new Error(`Malformed ${markerKind} marker JSON: ${String(error?.message ?? error)}`);
  }
  const expectedKeys = markerKind === "health" ? HEALTH_KEYS : INTENT_KEYS;
  validateExactKeys(marker, new Set(expectedKeys), `${markerKind} marker`);
  if (
    JSON.stringify(marker) !== text ||
    JSON.stringify(Object.keys(marker)) !== JSON.stringify(expectedKeys)
  ) {
    throw new Error(`${markerKind} marker is not canonical.`);
  }
  if (
    marker.schema !== (markerKind === "health" ? HEALTH_SCHEMA : INTENT_SCHEMA) ||
    marker.state !== (markerKind === "health" ? HEALTH_STATE : INTENT_STATE) ||
    marker.protocol !== PROTOCOL ||
    marker.targetFileName !== identity.targetFileName ||
    marker.databaseNameSha256 !== identity.databaseNameSha256 ||
    !SOURCE_KINDS.has(marker.sourceKind) ||
    !validLowerSha256(marker.sourceSha256) ||
    !Number.isSafeInteger(marker.sourceBytes) ||
    marker.sourceBytes <= 0
  ) {
    throw new Error(`${markerKind} marker identity or field type is invalid.`);
  }
  if (
    markerKind === "health" &&
    (
      marker.integrityCheck !== "ok" ||
      !Number.isSafeInteger(marker.importedUserVersion)
    )
  ) {
    throw new Error("health marker validation evidence is invalid.");
  }
  return marker;
}

async function readStrictMarker(name, markerKind, identity) {
  const file = await readRootFile(name);
  if (file == null) return null;
  return parseStrictMarker(await file.text(), markerKind, identity);
}

function throwIfCancelled(cancelView) {
  if (cancelView != null && isCancelled(cancelView)) throw cancelledError();
}

async function writeAndVerifyMarker(
  name,
  markerKind,
  marker,
  identity,
  cancelView = null,
) {
  const text = JSON.stringify(marker);
  await writeRootText(name, text);
  throwIfCancelled(cancelView);
  const reread = await readRootFile(name);
  throwIfCancelled(cancelView);
  if (reread == null) throw new Error(`${markerKind} marker disappeared after write.`);
  const rereadText = await reread.text();
  throwIfCancelled(cancelView);
  if (rereadText !== text) throw new Error(`${markerKind} marker reread changed.`);
  return parseStrictMarker(rereadText, markerKind, identity);
}

function markersDescribeSameMigration(health, intent) {
  return INTENT_KEYS.slice(3).every((key) => health[key] === intent[key]);
}

function markersAreExactlyEqual(first, second) {
  return JSON.stringify(first) === JSON.stringify(second);
}

async function restoreRetryableMigrationState(recovery) {
  const { databaseName, identity, intent, health } = recovery;
  const intentName = intentMarkerName(identity.targetFileName);
  const healthName = healthMarkerName(identity.targetFileName);
  let currentIntent = await readStrictMarker(intentName, "intent", identity);
  if (currentIntent == null) {
    currentIntent = await writeAndVerifyMarker(
      intentName,
      "intent",
      intent,
      identity,
    );
  }
  if (!markersAreExactlyEqual(currentIntent, intent)) {
    throw new Error(
      `Cancellation recovery refused mismatched migration intent for ${databaseName}.`,
    );
  }
  const currentHealth = await readStrictMarker(healthName, "health", identity);
  if (currentHealth != null) {
    if (health == null || !markersAreExactlyEqual(currentHealth, health)) {
      throw new Error(
        `Cancellation recovery refused mismatched health marker for ${databaseName}.`,
      );
    }
    await removeRootEntry(healthName);
    if (await rootFileExists(healthName)) {
      throw new Error(
        `Cancellation recovery could not remove exact health marker for ${databaseName}.`,
      );
    }
  }
  const verifiedIntent = await readStrictMarker(intentName, "intent", identity);
  if (!markersAreExactlyEqual(verifiedIntent, intent)) {
    throw new Error(
      `Cancellation recovery could not preserve exact migration intent for ${databaseName}.`,
    );
  }
}

function snapshotBytes(value, label) {
  if (value instanceof Uint8Array) return Uint8Array.from(value);
  if (value instanceof ArrayBuffer) return new Uint8Array(value.slice(0));
  if (ArrayBuffer.isView(value)) {
    return Uint8Array.from(
      new Uint8Array(value.buffer, value.byteOffset, value.byteLength),
    );
  }
  throw new Error(`${label} has an unsupported byte representation.`);
}

async function readLegacyOpfs(databaseName) {
  const root = await rootDirectory();
  let directory;
  try {
    directory = await root.getDirectoryHandle(LEGACY_OPFS_DIRECTORY);
  } catch (error) {
    if (error?.name === "NotFoundError") return null;
    throw error;
  }
  try {
    const handle = await directory.getFileHandle(`${databaseName}.sqlite3`);
    return new Uint8Array(await (await handle.getFile()).arrayBuffer());
  } catch (error) {
    if (error?.name === "NotFoundError") return null;
    throw error;
  }
}

async function readLegacyIndexedDb(databaseName) {
  if (
    typeof globalThis.indexedDB === "undefined" ||
    typeof globalThis.indexedDB.databases !== "function"
  ) {
    throw new Error(
      "Read-only legacy IndexedDB absence detection requires indexedDB.databases().",
    );
  }
  const catalog = await globalThis.indexedDB.databases();
  if (!catalog.some((entry) => entry.name === LEGACY_INDEXED_DB)) return null;
  return new Promise((resolve, reject) => {
    const open = globalThis.indexedDB.open(LEGACY_INDEXED_DB);
    open.onerror = () => reject(open.error ?? new Error("Legacy IndexedDB open failed."));
    open.onupgradeneeded = () => {
      open.transaction?.abort();
      reject(new Error("Legacy IndexedDB changed during read-only discovery."));
    };
    open.onsuccess = () => {
      const db = open.result;
      if (!db.objectStoreNames.contains(LEGACY_INDEXED_DB_STORE)) {
        db.close();
        resolve(null);
        return;
      }
      const transaction = db.transaction(LEGACY_INDEXED_DB_STORE, "readonly");
      const get = transaction.objectStore(LEGACY_INDEXED_DB_STORE).get(databaseName);
      get.onerror = () => {
        db.close();
        reject(get.error ?? new Error("Legacy IndexedDB read failed."));
      };
      get.onsuccess = () => {
        try {
          const value = get.result;
          resolve(value === undefined ? null : snapshotBytes(value, "Legacy IndexedDB snapshot"));
        } catch (error) {
          reject(error);
        } finally {
          db.close();
        }
      };
    };
  });
}

async function readBuiltInSource(sourceKind, databaseName) {
  if (sourceKind === "opfs") return readLegacyOpfs(databaseName);
  if (sourceKind === "indexeddb") return readLegacyIndexedDb(databaseName);
  throw new Error(`Unsupported built-in legacy source kind: ${sourceKind}`);
}

async function discoverBuiltInSource(databaseName) {
  const [opfsBytes, indexedDbBytes] = await Promise.all([
    readLegacyOpfs(databaseName),
    readLegacyIndexedDb(databaseName),
  ]);
  if (opfsBytes == null && indexedDbBytes == null) return null;
  if (opfsBytes != null && indexedDbBytes == null) {
    return { sourceKind: "opfs", bytes: opfsBytes, sourceSha256: await sha256(opfsBytes) };
  }
  if (opfsBytes == null) {
    return {
      sourceKind: "indexeddb",
      bytes: indexedDbBytes,
      sourceSha256: await sha256(indexedDbBytes),
    };
  }
  const [opfsSha256, indexedDbSha256] = await Promise.all([
    sha256(opfsBytes),
    sha256(indexedDbBytes),
  ]);
  if (opfsSha256 !== indexedDbSha256) {
    throw new Error(
      `Ambiguous legacy sources for ${databaseName}: OPFS and IndexedDB hashes differ.`,
    );
  }
  return { sourceKind: "opfs", bytes: opfsBytes, sourceSha256: opfsSha256 };
}

function validateSqliteSnapshot(bytes) {
  const header = [0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x20, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x20, 0x33, 0x00];
  if (
    bytes.byteLength < 512 ||
    header.some((byte, index) => bytes[index] !== byte)
  ) {
    throw new Error("Legacy source is not a plausible SQLite format 3 database.");
  }
  const encodedPageSize = (bytes[16] << 8) | bytes[17];
  const pageSize = encodedPageSize === 1 ? 65_536 : encodedPageSize;
  if (
    pageSize < 512 ||
    pageSize > 65_536 ||
    (pageSize & (pageSize - 1)) !== 0 ||
    bytes.byteLength % pageSize !== 0
  ) {
    throw new Error("Legacy source has an invalid SQLite page-size or file-length boundary.");
  }
}

function sqliteScalar(db, sql) {
  const statement = db.prepare(sql);
  try {
    if (!statement.step()) throw new Error(`SQLite validation returned no row for ${sql}.`);
    return statement.get(0);
  } finally {
    statement.finalize();
  }
}

function requireForeignKeyIntegrity(db) {
  const statement = db.prepare("PRAGMA foreign_key_check");
  try {
    if (statement.step()) {
      const tableName = String(statement.get(0));
      const rowId = String(statement.get(1));
      throw new Error(
        `Imported target failed foreign_key_check: table=${tableName}, rowid=${rowId}.`,
      );
    }
  } finally {
    statement.finalize();
  }
}

function inspectSchemaAndVersion(db, requireIntegrity, requireForeignKeys = false) {
  let integrityCheck = "";
  if (requireIntegrity) {
    integrityCheck = String(sqliteScalar(db, "PRAGMA integrity_check"));
    if (integrityCheck !== "ok") {
      throw new Error(`Imported target failed integrity_check: ${integrityCheck}`);
    }
  }
  if (requireForeignKeys) requireForeignKeyIntegrity(db);
  const userVersionValue = sqliteScalar(db, "PRAGMA user_version");
  const userVersion = Number(userVersionValue);
  if (!Number.isSafeInteger(userVersion)) {
    throw new Error("Imported target user_version is not an integer.");
  }
  const statement = db.prepare(
    "SELECT type, name, tbl_name, rootpage, sql FROM sqlite_schema ORDER BY rowid",
  );
  try {
    while (statement.step()) {
      const sqlType = capi.sqlite3_column_type(statement.pointer, 4);
      if (sqlType !== capi.SQLITE_NULL && sqlType !== capi.SQLITE_TEXT) {
        throw new Error("Imported target contains a non-text sqlite_schema SQL value.");
      }
      if (sqlType === capi.SQLITE_TEXT) statement.get(4);
    }
  } finally {
    statement.finalize();
  }
  return { integrityCheck, userVersion };
}

function openValidatedTarget(targetFileName, requireIntegrity) {
  let first = new oo1.OpfsDb(targetFileName, "c");
  let evidence;
  try {
    evidence = inspectSchemaAndVersion(first, requireIntegrity, requireIntegrity);
  } finally {
    first.close();
  }
  first = new oo1.OpfsDb(targetFileName, "c");
  try {
    inspectSchemaAndVersion(first, false, requireIntegrity);
  } finally {
    first.close();
  }
  return {
    db: new oo1.OpfsDb(targetFileName, "c"),
    integrityCheck: evidence.integrityCheck,
    userVersion: evidence.userVersion,
  };
}

function migrationModeAcceptsSource(legacySourceMode, sourceKind) {
  return legacySourceMode === "custom"
    ? sourceKind === "custom"
    : legacySourceMode === "built-in" && sourceKind !== "custom";
}

function heapSample() {
  const value = globalThis.performance?.memory?.usedJSHeapSize;
  return Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function takeMigrationHeapSampler() {
  const controlled = nextMigrationHeapSamplesForTest;
  nextMigrationHeapSamplesForTest = null;
  if (controlled == null) return heapSample;
  let index = 0;
  return () => controlled[index++] ?? null;
}

function interruptMigrationForTest(databaseName, stage) {
  if (migrationInterruptionsForTest.get(databaseName) !== stage) return;
  migrationInterruptionsForTest.delete(databaseName);
  throw new Error(`controlled Phase 5B interruption at ${stage}`);
}

async function awaitCancellationHoldForTest(control, cancelView) {
  if (usedCancellationHoldIdsForTest.has(control.id)) {
    throw new Error(`Reused cancellation hold test control ${control.id}.`);
  }
  usedCancellationHoldIdsForTest.add(control.id);
  post({
    kind: "test-cancellation-hold",
    protocol: PROTOCOL,
    ...control,
    pendingOpenCount: pendingOpens.size,
  });
  while (!isCancelled(cancelView)) {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

async function holdMigrationForCancellationTest(databaseName, stage, cancelView) {
  const control = migrationCancellationHoldsForTest.get(databaseName);
  if (control?.stage !== stage) return;
  migrationCancellationHoldsForTest.delete(databaseName);
  await awaitCancellationHoldForTest(control, cancelView);
}

function registerDatabase(db, storageMode, cancellationRecovery = null) {
  const databaseId = allocateHandle("database");
  databases.set(databaseId, { db, storageMode, cancellationRecovery });
  metrics.storageMode = storageMode;
  metrics.liveDatabases = databases.size;
  return response({
    openState: "opened",
    databaseId,
    runtimeKind: metrics.runtimeKind,
    sqliteVersion: metrics.sqliteVersion,
  });
}

async function inspectMigrationState(databaseName, identity) {
  const targetExists = await rootFileExists(identity.targetFileName);
  const health = await readStrictMarker(
    healthMarkerName(identity.targetFileName),
    "health",
    identity,
  );
  const intent = await readStrictMarker(
    intentMarkerName(identity.targetFileName),
    "intent",
    identity,
  );
  if (health != null && !targetExists) {
    throw new Error(`Orphan health marker for ${databaseName}.`);
  }
  if (health != null && intent != null && !markersDescribeSameMigration(health, intent)) {
    throw new Error(`Health and intent markers disagree for ${databaseName}.`);
  }
  return { targetExists, health, intent };
}

async function openCommittedTarget(databaseName, identity, state) {
  let opened;
  try {
    opened = openValidatedTarget(identity.targetFileName, false);
  } catch (error) {
    throw new Error(
      `SQLite worker could not open authoritative target for ${databaseName}: ` +
        String(error?.message ?? error),
    );
  }
  if (state.health != null && state.intent != null) {
    await removeRootEntry(intentMarkerName(identity.targetFileName));
  }
  return registerDatabase(opened.db, "direct-opfs");
}

async function resolveBuiltInSource(databaseName, intent) {
  if (intent == null) return discoverBuiltInSource(databaseName);
  if (intent.sourceKind === "custom") {
    throw new Error(
      `Interrupted migration for ${databaseName} requires custom source kind.`,
    );
  }
  const bytes = await readBuiltInSource(intent.sourceKind, databaseName);
  if (bytes == null) {
    throw new Error(
      `Interrupted migration source ${intent.sourceKind} is absent for ${databaseName}.`,
    );
  }
  const sourceSha256 = await sha256(bytes);
  if (
    sourceSha256 !== intent.sourceSha256 ||
    bytes.byteLength !== intent.sourceBytes
  ) {
    throw new Error(
      `Interrupted migration source changed for ${databaseName} (${intent.sourceKind}).`,
    );
  }
  return { sourceKind: intent.sourceKind, bytes, sourceSha256 };
}

async function importLegacySource(
  databaseName,
  identity,
  source,
  existingIntent,
  cancelView,
) {
  const startedAt = globalThis.performance.now();
  const sampleHeap = takeMigrationHeapSampler();
  const heapStart = sampleHeap();
  let heapPeak = heapStart;
  const recordHeapSample = () => {
    const sample = sampleHeap();
    if (sample != null && (heapPeak == null || sample > heapPeak)) {
      heapPeak = sample;
    }
    return sample;
  };
  let stage = "validate-source";
  let intentEstablished = existingIntent != null;
  let cancellationRecovery = null;
  let finalDatabase = null;
  try {
    validateSqliteSnapshot(source.bytes);
    throwIfCancelled(cancelView);
    stage = "validate-source-hash";
    const validatedSourceSha256 = await sha256(source.bytes);
    throwIfCancelled(cancelView);
    if (
      source.bytes.byteLength <= 0 ||
      source.sourceSha256 !== validatedSourceSha256
    ) {
      throw new Error("Legacy source hash or byte count changed before intent.");
    }
    const intent = {
      schema: INTENT_SCHEMA,
      state: INTENT_STATE,
      protocol: PROTOCOL,
      targetFileName: identity.targetFileName,
      databaseNameSha256: identity.databaseNameSha256,
      sourceKind: source.sourceKind,
      sourceSha256: source.sourceSha256,
      sourceBytes: source.bytes.byteLength,
    };
    cancellationRecovery = {
      databaseName,
      identity: Object.freeze({ ...identity }),
      intent: Object.freeze({ ...intent }),
      health: null,
    };
    if (existingIntent == null) {
      if (isCancelled(cancelView)) throw cancelledError();
      interruptMigrationForTest(databaseName, "before-intent");
      stage = "write-intent";
      await holdMigrationForCancellationTest(
        databaseName,
        "before-intent-write",
        cancelView,
      );
      throwIfCancelled(cancelView);
      await writeAndVerifyMarker(
        intentMarkerName(identity.targetFileName),
        "intent",
        intent,
        identity,
        cancelView,
      );
      intentEstablished = true;
      interruptMigrationForTest(databaseName, "after-intent");
    } else if (!markersDescribeSameMigration(intent, existingIntent)) {
      throw new Error("Retry source does not match the migration intent.");
    }
    throwIfCancelled(cancelView);
    stage = "replace-intent-owned-target";
    await removeRootEntry(identity.targetFileName);
    throwIfCancelled(cancelView);
    interruptMigrationForTest(databaseName, "during-import");
    stage = "official-import";
    await oo1.OpfsDb.importDb(identity.targetFileName, source.bytes);
    throwIfCancelled(cancelView);
    recordHeapSample();
    interruptMigrationForTest(databaseName, "after-import");
    stage = "sqlite-validation";
    const validated = openValidatedTarget(identity.targetFileName, true);
    validated.db.close();
    throwIfCancelled(cancelView);
    interruptMigrationForTest(databaseName, "after-integrity");
    await holdMigrationForCancellationTest(
      databaseName,
      "after-integrity",
      cancelView,
    );
    throwIfCancelled(cancelView);

    let retainedSha256 = source.sourceSha256;
    let peakOwnedBytes = source.bytes.byteLength;
    if (source.sourceKind !== "custom") {
      stage = "source-revalidation";
      const retained = await readBuiltInSource(source.sourceKind, databaseName);
      throwIfCancelled(cancelView);
      if (retained == null) throw new Error("Selected built-in legacy source disappeared.");
      peakOwnedBytes = source.bytes.byteLength + retained.byteLength;
      recordHeapSample();
      retainedSha256 = await sha256(retained);
      throwIfCancelled(cancelView);
      if (
        retainedSha256 !== source.sourceSha256 ||
        retained.byteLength !== source.bytes.byteLength
      ) {
        throw new Error("Selected built-in legacy source changed during import.");
      }
    }

    const health = {
      schema: HEALTH_SCHEMA,
      state: HEALTH_STATE,
      protocol: PROTOCOL,
      targetFileName: identity.targetFileName,
      databaseNameSha256: identity.databaseNameSha256,
      sourceKind: source.sourceKind,
      sourceSha256: source.sourceSha256,
      sourceBytes: source.bytes.byteLength,
      integrityCheck: "ok",
      importedUserVersion: validated.userVersion,
    };
    cancellationRecovery = Object.freeze({
      ...cancellationRecovery,
      health: Object.freeze({ ...health }),
    });
    stage = "write-health";
    await writeAndVerifyMarker(
      healthMarkerName(identity.targetFileName),
      "health",
      health,
      identity,
      cancelView,
    );
    throwIfCancelled(cancelView);
    interruptMigrationForTest(databaseName, "after-health");
    await holdMigrationForCancellationTest(databaseName, "after-health", cancelView);
    throwIfCancelled(cancelView);
    stage = "remove-intent";
    interruptMigrationForTest(databaseName, "before-intent-cleanup");
    await removeRootEntry(intentMarkerName(identity.targetFileName));
    throwIfCancelled(cancelView);
    stage = "final-open";
    const finalTarget = openValidatedTarget(identity.targetFileName, false);
    finalDatabase = finalTarget.db;
    throwIfCancelled(cancelView);

    const heapEnd = recordHeapSample();
    metrics.migrationSourceKind = source.sourceKind;
    metrics.migrationSourceBytes = source.bytes.byteLength;
    metrics.migrationDurationMillis = Math.max(
      0,
      Math.ceil(globalThis.performance.now() - startedAt),
    );
    metrics.migrationPeakOwnedBytes = peakOwnedBytes;
    metrics.migrationTargetFileName = identity.targetFileName;
    metrics.migrationSourceSha256 = source.sourceSha256;
    metrics.migrationIntegrityCheck = "ok";
    metrics.migrationImportedUserVersion = validated.userVersion;
    metrics.migrationSourceRetained = retainedSha256 === source.sourceSha256;
    metrics.migrationHeapAvailable =
      heapStart != null && heapPeak != null && heapEnd != null;
    if (metrics.migrationHeapAvailable) {
      metrics.migrationHeapStartBytes = heapStart;
      metrics.migrationHeapPeakBytes = heapPeak;
      metrics.migrationHeapEndBytes = heapEnd;
    } else {
      metrics.migrationHeapStartBytes = 0;
      metrics.migrationHeapPeakBytes = 0;
      metrics.migrationHeapEndBytes = 0;
    }
    const opened = registerDatabase(
      finalDatabase,
      "direct-opfs",
      cancellationRecovery,
    );
    finalDatabase = null;
    return opened;
  } catch (error) {
    if (finalDatabase != null) {
      try {
        finalDatabase.close();
      } catch (closeFailure) {
        appendCleanupFailure(error, closeFailure);
      }
    }
    let cancellationRecoveryFailure = null;
    if (
      (error?.workerCancelled || isCancelled(cancelView)) &&
      intentEstablished &&
      cancellationRecovery != null
    ) {
      try {
        await restoreRetryableMigrationState(cancellationRecovery);
      } catch (recoveryFailure) {
        cancellationRecoveryFailure = recoveryFailure;
        appendCleanupFailure(error, recoveryFailure);
      }
    }
    const failure = new Error(
      `Legacy migration failed for ${databaseName}; source=${source.sourceKind}; ` +
        `bytes=${source.bytes.byteLength}; stage=${stage}: ${String(error?.message ?? error)}`,
    );
    if (error?.workerSuppressed != null) {
      failure.workerSuppressed = [...error.workerSuppressed];
    }
    if (cancellationRecoveryFailure != null) {
      failure.cancellationRecoveryFailure = cancellationRecoveryFailure;
    }
    if (error?.workerCancelled) failure.workerCancelled = true;
    throw failure;
  }
}

async function openBrowserDatabase(request, cancelView) {
  requireDirectOpfs();
  if (request.legacySourceMode === "none") {
    throw new Error("Browser worker opens require built-in or custom legacy-source mode.");
  }
  const identity = await databaseIdentity(request.fileName);
  return globalThis.navigator.locks.request(
    identity.lockName,
    { mode: "exclusive" },
    async () => {
      const state = await inspectMigrationState(request.fileName, identity);
      if (state.health != null || (state.targetExists && state.intent == null)) {
        return openCommittedTarget(request.fileName, identity, state);
      }
      if (
        state.intent != null &&
        !migrationModeAcceptsSource(request.legacySourceMode, state.intent.sourceKind)
      ) {
        throw new Error(
          `Interrupted migration for ${request.fileName} requires ` +
            `${state.intent.sourceKind} source kind.`,
        );
      }
      if (request.legacySourceMode === "custom") {
        const openId = allocateHandle("open");
        pendingOpens.set(openId, {
          databaseName: request.fileName,
          identity,
        });
        return response({
          openState: "legacy-source-required",
          openId,
          runtimeKind: metrics.runtimeKind,
          sqliteVersion: metrics.sqliteVersion,
        });
      }
      const source = await resolveBuiltInSource(request.fileName, state.intent);
      if (source == null) {
        const db = new oo1.OpfsDb(identity.targetFileName, "c");
        return registerDatabase(db, "direct-opfs");
      }
      return importLegacySource(
        request.fileName,
        identity,
        source,
        state.intent,
        cancelView,
      );
    },
  );
}

async function completeBrowserOpen(request, legacy, cancelView) {
  const pending = pendingOpens.get(request.openId);
  if (pending == null) {
    throw new Error(`Unknown, stale, or reused SQLite worker openId ${request.openId}.`);
  }
  pendingOpens.delete(request.openId);
  const { databaseName, identity } = pending;
  return globalThis.navigator.locks.request(
    identity.lockName,
    { mode: "exclusive" },
    async () => {
      const state = await inspectMigrationState(databaseName, identity);
      if (state.health != null || (state.targetExists && state.intent == null)) {
        return openCommittedTarget(databaseName, identity, state);
      }
      if (state.intent != null && state.intent.sourceKind !== "custom") {
        throw new Error(
          `Interrupted migration for ${databaseName} requires ` +
            `${state.intent.sourceKind} source kind.`,
        );
      }
      if (request.legacySourceStatus === "absent") {
        if (state.intent != null) {
          throw new Error(
            `Interrupted custom migration source is absent for ${databaseName}.`,
          );
        }
        return registerDatabase(
          new oo1.OpfsDb(identity.targetFileName, "c"),
          "direct-opfs",
        );
      }
      if (legacy == null) {
        throw new Error("Present custom legacy source requires transferred bytes.");
      }
      const receivedSha256 = await sha256(legacy.bytes);
      if (receivedSha256 !== legacy.sha256) {
        throw new Error(`Transferred custom legacy source hash mismatch for ${databaseName}.`);
      }
      const source = {
        sourceKind: "custom",
        bytes: legacy.bytes,
        sourceSha256: receivedSha256,
      };
      if (
        state.intent != null &&
        (
          state.intent.sourceSha256 !== receivedSha256 ||
          state.intent.sourceBytes !== legacy.bytes.byteLength
        )
      ) {
        throw new Error(`Custom retry source changed for ${databaseName}.`);
      }
      return importLegacySource(
        databaseName,
        identity,
        source,
        state.intent,
        cancelView,
      );
    },
  );
}

async function deleteLegacyIndexedDbValue(databaseName) {
  if (
    typeof globalThis.indexedDB === "undefined" ||
    typeof globalThis.indexedDB.databases !== "function"
  ) {
    return;
  }
  const catalog = await globalThis.indexedDB.databases();
  if (!catalog.some((entry) => entry.name === LEGACY_INDEXED_DB)) return;
  await new Promise((resolve, reject) => {
    const open = globalThis.indexedDB.open(LEGACY_INDEXED_DB);
    open.onerror = () => reject(open.error ?? new Error("Legacy IndexedDB cleanup open failed."));
    open.onsuccess = () => {
      const db = open.result;
      if (!db.objectStoreNames.contains(LEGACY_INDEXED_DB_STORE)) {
        db.close();
        resolve();
        return;
      }
      const transaction = db.transaction(LEGACY_INDEXED_DB_STORE, "readwrite");
      transaction.objectStore(LEGACY_INDEXED_DB_STORE).delete(databaseName);
      transaction.oncomplete = () => {
        db.close();
        resolve();
      };
      transaction.onerror = () => {
        db.close();
        reject(transaction.error ?? new Error("Legacy IndexedDB cleanup failed."));
      };
    };
  });
}

async function cleanupMigrationStateForTest(databaseName) {
  if (isNodeRuntime) return;
  requireDirectOpfs();
  const identity = await databaseIdentity(databaseName);
  await globalThis.navigator.locks.request(
    identity.lockName,
    { mode: "exclusive" },
    async () => {
      await removeRootEntry(identity.targetFileName);
      await removeRootEntry(healthMarkerName(identity.targetFileName));
      await removeRootEntry(intentMarkerName(identity.targetFileName));
      const root = await rootDirectory();
      try {
        const directory = await root.getDirectoryHandle(LEGACY_OPFS_DIRECTORY);
        try {
          await directory.removeEntry(`${databaseName}.sqlite3`);
        } catch (error) {
          if (error?.name !== "NotFoundError") throw error;
        }
      } catch (error) {
        if (error?.name !== "NotFoundError") throw error;
      }
      await deleteLegacyIndexedDbValue(databaseName);
    },
  );
}

async function seedMigrationMarkerForTest(databaseName, mode) {
  if (isNodeRuntime) return;
  requireDirectOpfs();
  const identity = await databaseIdentity(databaseName);
  const sourceSha256 = "0".repeat(64);
  const intent = {
    schema: INTENT_SCHEMA,
    state: INTENT_STATE,
    protocol: PROTOCOL,
    targetFileName: identity.targetFileName,
    databaseNameSha256: identity.databaseNameSha256,
    sourceKind: "custom",
    sourceSha256,
    sourceBytes: 1,
  };
  const health = {
    schema: HEALTH_SCHEMA,
    state: HEALTH_STATE,
    protocol: PROTOCOL,
    targetFileName: identity.targetFileName,
    databaseNameSha256: identity.databaseNameSha256,
    sourceKind: "custom",
    sourceSha256,
    sourceBytes: 1,
    integrityCheck: "ok",
    importedUserVersion: 0,
  };
  await globalThis.navigator.locks.request(
    identity.lockName,
    { mode: "exclusive" },
    async () => {
      switch (mode) {
        case "malformed-health":
          await writeRootText(healthMarkerName(identity.targetFileName), "{");
          break;
        case "duplicate-health":
          await writeRootText(
            healthMarkerName(identity.targetFileName),
            JSON.stringify(health).replace(
              `"state":"${HEALTH_STATE}"`,
              `"state":"${HEALTH_STATE}","state":"${HEALTH_STATE}"`,
            ),
          );
          break;
        case "unknown-health":
          await writeRootText(
            healthMarkerName(identity.targetFileName),
            JSON.stringify({ ...health, schema: "sqlitenow-worker-health-v2" }),
          );
          break;
        case "mismatched-health":
          await writeRootText(
            healthMarkerName(identity.targetFileName),
            JSON.stringify({ ...health, targetFileName: "wrong.sqlite3" }),
          );
          break;
        case "orphan-health":
          await writeRootText(
            healthMarkerName(identity.targetFileName),
            JSON.stringify(health),
          );
          break;
        case "malformed-intent":
          await writeRootText(intentMarkerName(identity.targetFileName), "{}");
          break;
        case "noncanonical-intent":
          await writeRootText(
            intentMarkerName(identity.targetFileName),
            ` ${JSON.stringify(intent)}`,
          );
          break;
        case "mismatched-intent":
          await writeRootText(
            intentMarkerName(identity.targetFileName),
            JSON.stringify({ ...intent, databaseNameSha256: "f".repeat(64) }),
          );
          break;
        default:
          throw new Error(`Unsupported migration marker test mode: ${mode}`);
      }
    },
  );
}

function requireDatabase(databaseId) {
  const record = databases.get(databaseId);
  if (!record) throw new Error(`Unknown SQLite worker database ${databaseId}.`);
  return record;
}

function requireStatement(statementId, databaseId) {
  const record = statements.get(statementId);
  if (!record || record.databaseId !== databaseId) {
    throw new Error(`Unknown SQLite worker statement ${statementId}.`);
  }
  return record;
}

function isCancelled(cancelView) {
  return Atomics.load(cancelView, 0) !== 0;
}

function cancelledError() {
  const error = new Error("SQLite worker request cancelled.");
  error.workerCancelled = true;
  return error;
}

function inTransaction(database) {
  return capi.sqlite3_get_autocommit(database.db.pointer) === 0;
}

function bindStatement(statement, bindings) {
  if (!statement.needsBinding) return;
  for (const [rawIndex, value] of Object.entries(bindings)) {
    const index = canonicalBindingIndex(rawIndex);
    switch (value.type) {
      case "null":
        statement.stmt.bind(index, null);
        break;
      case "integer":
        if (!canonicalInteger(value.integer)) {
          metrics.integerNumberViolations++;
          throw new TypeError("SQLite INTEGER crossed the worker boundary as a Number.");
        }
        statement.stmt.bind(index, BigInt(value.integer));
        metrics.integerBindingsAsStrings++;
        break;
      case "real":
        {
          const rc = capi.sqlite3_bind_double(
            statement.stmt.pointer,
            index,
            value.real,
          );
          if (rc) oo1.DB.checkRc(statement.stmt.db.pointer, rc);
        }
        break;
      case "text":
        statement.stmt.bind(index, value.text);
        break;
      case "blob":
        statement.stmt.bind(index, Uint8Array.from(value.blob));
        break;
      default:
        throw new TypeError(`Unsupported SQLite value tag: ${String(value.type)}`);
    }
  }
  statement.needsBinding = false;
}

function encodeCurrentRow(statement) {
  const row = [];
  for (let index = 0; index < statement.columnNames.length; index++) {
    const type = capi.sqlite3_column_type(statement.stmt.pointer, index);
    switch (type) {
      case capi.SQLITE_INTEGER: {
        const exact = capi.sqlite3_column_int64(statement.stmt.pointer, index);
        if (typeof exact !== "bigint") {
          metrics.integerNumberViolations++;
          throw new TypeError("SQLite INTEGER result crossed the worker boundary as a Number.");
        }
        row.push({ type: "integer", integer: exact.toString() });
        metrics.integerResultsAsStrings++;
        break;
      }
      case capi.SQLITE_FLOAT:
        row.push({ type: "real", real: statement.stmt.get(index) });
        break;
      case capi.SQLITE_TEXT:
        row.push({ type: "text", text: statement.stmt.get(index) });
        break;
      case capi.SQLITE_BLOB:
        row.push({ type: "blob", blob: Array.from(statement.stmt.get(index)) });
        break;
      case capi.SQLITE_NULL:
        row.push({ type: "null" });
        break;
      default:
        throw new TypeError(`Unknown SQLite storage class ${type}.`);
    }
  }
  return {
    row,
    bytes: textEncoder.encode(JSON.stringify(row)).byteLength,
  };
}

function finalizeStatement(statementId) {
  const statement = statements.get(statementId);
  if (!statement) return;
  try {
    statement.stmt.finalize();
  } finally {
    statements.delete(statementId);
    metrics.liveStatements = statements.size;
  }
}

function finalizeOwnedStatement(statementId, databaseId, allowMissing = false) {
  const statement = statements.get(statementId);
  if (!statement) {
    if (allowMissing) return;
    throw new Error(`Unknown SQLite worker statement ${statementId}.`);
  }
  if (statement.databaseId !== databaseId) {
    throw new Error(
      `SQLite worker statement ${statementId} does not belong to database ${databaseId}.`,
    );
  }
  finalizeStatement(statementId);
}

function rollbackIfNeeded(database) {
  if (!inTransaction(database)) return false;
  database.db.exec("ROLLBACK");
  return true;
}

function appendShutdownFailure(failures, stage) {
  const message = shutdownCleanupFailuresForTest?.[stage];
  if (typeof message !== "string") return;
  failures.push(new Error(message));
  delete shutdownCleanupFailuresForTest[stage];
}

function closeDatabase(databaseId, injectShutdownFailures = false) {
  const database = databases.get(databaseId);
  if (!database) return;
  const failures = [];
  for (const [statementId, statement] of statements) {
    if (statement.databaseId !== databaseId) continue;
    try {
      finalizeStatement(statementId);
    } catch (error) {
      failures.push(error);
    }
  }
  if (injectShutdownFailures) appendShutdownFailure(failures, "finalize");
  try {
    rollbackIfNeeded(database);
  } catch (error) {
    failures.push(error);
  }
  if (injectShutdownFailures) appendShutdownFailure(failures, "rollback");
  try {
    database.db.close();
  } catch (error) {
    failures.push(error);
  } finally {
    databases.delete(databaseId);
    metrics.liveDatabases = databases.size;
  }
  if (injectShutdownFailures) appendShutdownFailure(failures, "close");
  if (failures.length > 0) {
    const primary = failures[0];
    primary.workerSuppressed = failures
      .slice(1)
      .map((error) => String(error?.message ?? error));
    throw primary;
  }
}

function appendCleanupFailure(error, cleanupError) {
  error.workerSuppressed = [
    ...(error.workerSuppressed ?? []),
    String(cleanupError?.message ?? cleanupError),
  ];
}

function appendInjectedCleanupFailure(requestId, stage, error) {
  const injection = cancellationCleanupFailuresForTest;
  if (
    !injection ||
    injection.requestId !== requestId ||
    typeof injection[stage] !== "string"
  ) {
    return;
  }
  appendCleanupFailure(error, new Error(injection[stage]));
  delete injection[stage];
  if (
    typeof injection.finalize !== "string" &&
    typeof injection.rollback !== "string"
  ) {
    cancellationCleanupFailuresForTest = null;
  }
}

async function cleanupCancelledRequest(requestId, request, error) {
  if (request?.command === "completeOpen") {
    pendingOpens.delete(request.openId);
    return;
  }
  if (request?.command !== "page") return;
  const database = databases.get(request.databaseId);
  if (!database) return;
  try {
    finalizeOwnedStatement(request.statementId, request.databaseId, true);
  } catch (cleanupError) {
    appendCleanupFailure(error, cleanupError);
  }
  appendInjectedCleanupFailure(requestId, "finalize", error);
  try {
    if (rollbackIfNeeded(database)) metrics.transactionsRolledBackOnCancel++;
  } catch (rollbackError) {
    appendCleanupFailure(error, rollbackError);
  }
  appendInjectedCleanupFailure(requestId, "rollback", error);
}

async function cleanupSuccessfulCancelledRequest(requestId, record, error) {
  const { request, envelope } = record;
  if (!envelope?.data) return;
  switch (request?.command) {
    case "open":
      if (envelope.data.openState === "legacy-source-required") {
        pendingOpens.delete(envelope.data.openId);
      } else {
        const database = databases.get(envelope.data.databaseId);
        const cancellationRecovery = database?.cancellationRecovery ?? null;
        try {
          closeDatabase(envelope.data.databaseId);
        } catch (cleanupError) {
          appendCleanupFailure(error, cleanupError);
        }
        if (cancellationRecovery != null) {
          await globalThis.navigator.locks.request(
            cancellationRecovery.identity.lockName,
            { mode: "exclusive" },
            () => restoreRetryableMigrationState(cancellationRecovery),
          );
        }
      }
      break;
    case "completeOpen": {
      const database = databases.get(envelope.data.databaseId);
      const cancellationRecovery = database?.cancellationRecovery ?? null;
      try {
        closeDatabase(envelope.data.databaseId);
      } catch (cleanupError) {
        appendCleanupFailure(error, cleanupError);
      }
      if (cancellationRecovery != null) {
        await globalThis.navigator.locks.request(
          cancellationRecovery.identity.lockName,
          { mode: "exclusive" },
          () => restoreRetryableMigrationState(cancellationRecovery),
        );
      }
      break;
    }
    case "prepare":
      try {
        finalizeOwnedStatement(
          envelope.data.statementId,
          request.databaseId,
          true,
        );
      } catch (cleanupError) {
        appendCleanupFailure(error, cleanupError);
      }
      appendInjectedCleanupFailure(requestId, "finalize", error);
      break;
    case "page": {
      const database = databases.get(request.databaseId);
      if (!database) break;
      try {
        finalizeOwnedStatement(request.statementId, request.databaseId, true);
      } catch (cleanupError) {
        appendCleanupFailure(error, cleanupError);
      }
      appendInjectedCleanupFailure(requestId, "finalize", error);
      try {
        if (rollbackIfNeeded(database)) metrics.transactionsRolledBackOnCancel++;
      } catch (cleanupError) {
        appendCleanupFailure(error, cleanupError);
      }
      appendInjectedCleanupFailure(requestId, "rollback", error);
      break;
    }
    default:
      break;
  }
}

async function page(requestId, request, cancelView) {
  const database = requireDatabase(request.databaseId);
  const statement = requireStatement(request.statementId, request.databaseId);
  bindStatement(statement, request.bindings);
  metrics.pageRequests++;
  const rows = [];
  let bytes = 2;
  let done = false;
  let oversizedRow = false;
  capi.sqlite3_progress_handler(
    database.db.pointer,
    1000,
    () => (isCancelled(cancelView) ? 1 : 0),
    0,
  );
  try {
    if (holdNextActivePageForTest) {
      holdNextActivePageForTest = false;
      post({
        kind: "test-active-page",
        protocol: PROTOCOL,
        id: requestId,
      });
      await new Promise((resolve) => {
        const poll = () => {
          if (isCancelled(cancelView)) resolve();
          else setTimeout(poll, 1);
        };
        setTimeout(poll, 1);
      });
    }
    while (rows.length < request.pageRows) {
      if (isCancelled(cancelView)) throw cancelledError();
      let encoded = statement.pendingRow;
      if (encoded) {
        statement.pendingRow = null;
      } else {
        if (!statement.stmt.step()) {
          done = true;
          break;
        }
        metrics.steppedRows++;
        encoded = encodeCurrentRow(statement);
        metrics.encodedRows++;
      }
      if (encoded.bytes > HARD_ROW_BYTES) {
        metrics.oversizedRows++;
        throw new Error(
          `Encoded row ${encoded.bytes} exceeds hard cap ${HARD_ROW_BYTES}.`,
        );
      }
      const prospectiveBytes =
        rows.length === 0 ? 2 + encoded.bytes : bytes + 1 + encoded.bytes;
      if (rows.length > 0 && prospectiveBytes > request.pageBytes) {
        statement.pendingRow = encoded;
        break;
      }
      if (rows.length === 0 && prospectiveBytes > request.pageBytes) {
        oversizedRow = true;
        metrics.oversizedRows++;
      }
      rows.push(encoded.row);
      bytes = prospectiveBytes;
    }
  } catch (error) {
    const cancelled = Boolean(error?.workerCancelled || isCancelled(cancelView));
    try {
      finalizeOwnedStatement(request.statementId, request.databaseId, true);
    } catch (cleanupError) {
      appendCleanupFailure(error, cleanupError);
    }
    appendInjectedCleanupFailure(requestId, "finalize", error);
    if (cancelled) {
      error.workerCancelled = true;
      try {
        if (rollbackIfNeeded(database)) metrics.transactionsRolledBackOnCancel++;
      } catch (rollbackError) {
        appendCleanupFailure(error, rollbackError);
      }
      appendInjectedCleanupFailure(requestId, "rollback", error);
    }
    throw error;
  } finally {
    capi.sqlite3_progress_handler(database.db.pointer, 0, 0, 0);
  }
  metrics.transferredRows += rows.length;
  metrics.transferredBytes += bytes;
  metrics.maxPageRows = Math.max(metrics.maxPageRows, rows.length);
  metrics.maxPageBytes = Math.max(metrics.maxPageBytes, bytes);
  return response({
    rows,
    done,
    oversizedRow,
    pageRows: rows.length,
    pageBytes: bytes,
    inTransaction: inTransaction(database),
  });
}

async function handleRequest(requestId, request, cancelView, legacy) {
  if (isCancelled(cancelView)) throw cancelledError();
  switch (request.command) {
    case "open": {
      if (isNodeRuntime) {
        if (request.legacySourceMode === "custom") {
          throw new Error(
            "Explicit custom legacy persistence is browser-only and was not invoked.",
          );
        }
        return registerDatabase(new oo1.DB(":memory:", "c"), "memory");
      }
      return openBrowserDatabase(request, cancelView);
    }
    case "completeOpen":
      if (isNodeRuntime) {
        throw new Error("Node worker cannot complete a browser legacy-source open.");
      }
      return completeBrowserOpen(request, legacy, cancelView);
    case "prepare": {
      const database = requireDatabase(request.databaseId);
      const stmt = database.db.prepare(request.sql);
      if (isCancelled(cancelView)) {
        stmt.finalize();
        throw cancelledError();
      }
      const statementId = allocateHandle("statement");
      const columnCount = capi.sqlite3_column_count(stmt.pointer);
      const columnNames = Array.from(
        { length: columnCount },
        (_, index) => capi.sqlite3_column_name(stmt.pointer, index),
      );
      statements.set(statementId, {
        databaseId: request.databaseId,
        stmt,
        columnNames,
        needsBinding: true,
        pendingRow: null,
      });
      metrics.liveStatements = statements.size;
      return response({
        statementId,
        columnNames,
        inTransaction: inTransaction(database),
      });
    }
    case "page":
      return page(requestId, request, cancelView);
    case "reset": {
      const database = requireDatabase(request.databaseId);
      const statement = requireStatement(request.statementId, request.databaseId);
      statement.stmt.reset();
      statement.pendingRow = null;
      statement.needsBinding = true;
      return response({ inTransaction: inTransaction(database) });
    }
    case "clearBindings": {
      const database = requireDatabase(request.databaseId);
      const statement = requireStatement(request.statementId, request.databaseId);
      statement.stmt.reset();
      statement.stmt.clearBindings();
      statement.pendingRow = null;
      statement.needsBinding = true;
      return response({ inTransaction: inTransaction(database) });
    }
    case "closeStatement": {
      const database = requireDatabase(request.databaseId);
      finalizeOwnedStatement(request.statementId, request.databaseId);
      return response({ inTransaction: inTransaction(database) });
    }
    case "closeDatabase":
      closeDatabase(request.databaseId);
      return response();
    case "metrics":
      return response({
        metrics: {
          ...metrics,
          requestsCompleted: metrics.requestsCompleted + 1,
          pendingRequests: Math.max(0, metrics.pendingRequests - 1),
        },
      });
    case "shutdown": {
      const failures = [];
      for (const databaseId of Array.from(databases.keys())) {
        try {
          closeDatabase(databaseId, true);
        } catch (error) {
          failures.push(error);
        }
      }
      pendingOpens.clear();
      metrics.workerStops++;
      if (failures.length > 0) {
        const primary = failures[0];
        primary.workerSuppressed = [
          ...(primary.workerSuppressed ?? []),
          ...failures.slice(1)
          .flatMap((error) => [
            String(error?.message ?? error),
            ...(error?.workerSuppressed ?? []),
          ]),
        ];
        throw primary;
      }
      return response({
        metrics: {
          ...metrics,
          requestsCompleted: metrics.requestsCompleted + 1,
          pendingRequests: 0,
        },
      });
    }
    default:
      throw new Error(`Unsupported SQLite worker command: ${String(request.command)}`);
  }
}

async function dispatch(message) {
  const { id, request, cancelBuffer } = message;
  let cancelView;
  let legacy = null;
  try {
    const hasLegacyBytes = Object.prototype.hasOwnProperty.call(message, "legacyBytes");
    const hasLegacySha256 = Object.prototype.hasOwnProperty.call(
      message,
      "legacyBytesSha256",
    );
    const expectsLegacyBytes =
      request?.command === "completeOpen" &&
      request?.legacySourceStatus === "present";
    validateExactKeys(
      message,
      new Set(
        expectsLegacyBytes
          ? ["id", "request", "cancelBuffer", "legacyBytes", "legacyBytesSha256"]
          : ["id", "request", "cancelBuffer"],
      ),
      "request envelope",
    );
    if (!(cancelBuffer instanceof SharedArrayBuffer)) {
      throw new TypeError("Every SQLite worker request requires a cancellation flag.");
    }
    cancelView = new Int32Array(cancelBuffer);
    const bindings = validateRequest(request);
    if (request.command === "page") request.bindings = bindings;
    if (expectsLegacyBytes) {
      if (
        !hasLegacyBytes ||
        !hasLegacySha256 ||
        !(message.legacyBytes instanceof Uint8Array) ||
        message.legacyBytes.byteOffset !== 0 ||
        message.legacyBytes.byteLength === 0 ||
        message.legacyBytes.byteLength !== message.legacyBytes.buffer.byteLength ||
        !validLowerSha256(message.legacyBytesSha256)
      ) {
        throw new TypeError("Invalid transferred custom legacy source envelope.");
      }
      legacy = {
        bytes: message.legacyBytes,
        sha256: message.legacyBytesSha256,
      };
    } else if (hasLegacyBytes || hasLegacySha256) {
      throw new TypeError("Legacy bytes are valid only for a present completeOpen source.");
    }
  } catch (error) {
    if (
      request?.command === "completeOpen" &&
      Number.isSafeInteger(request?.openId)
    ) {
      pendingOpens.delete(request.openId);
    }
    const envelope = errorEnvelope(id, request?.command ?? "unknown", error, request);
    completedRequests.set(id, { request, cancelView, envelope });
    post(envelope);
    return;
  }

  metrics.requestsStarted++;
  metrics.pendingRequests++;
  try {
    if (
      request.command === "completeOpen" &&
      nextCompleteOpenCancellationHoldForTest != null
    ) {
      const control = nextCompleteOpenCancellationHoldForTest;
      nextCompleteOpenCancellationHoldForTest = null;
      await awaitCancellationHoldForTest(control, cancelView);
    }
    const data = await handleRequest(id, request, cancelView, legacy);
    metrics.requestsCompleted++;
    const envelope = { id, data };
    completedRequests.set(id, { request, cancelView, envelope });
    post(envelope);
  } catch (error) {
    const cancelled = Boolean(error?.workerCancelled || isCancelled(cancelView));
    if (cancelled) {
      await cleanupCancelledRequest(id, request, error);
      metrics.requestsCancelled++;
    }
    const envelope = errorEnvelope(id, request.command, error, request, cancelled);
    completedRequests.set(id, {
      request,
      cancelView,
      envelope,
      cancellationRecoveryFailure: error?.cancellationRecoveryFailure ?? null,
    });
    post(envelope);
  } finally {
    metrics.pendingRequests--;
  }
}

function validateRequestId(requestId) {
  if (
    !Number.isInteger(requestId) ||
    requestId === 0 ||
    requestId < MIN_REQUEST_ID ||
    requestId > MAX_REQUEST_ID
  ) {
    throw new TypeError("SQLite worker request IDs must be non-zero signed 32-bit integers.");
  }
}

function acceptFreshRequestId(requestId) {
  validateRequestId(requestId);
  if (requestId > 0) {
    if (requestId <= highestPositiveRequestId) {
      throw new Error(`SQLite worker request ID ${requestId} was already used.`);
    }
    highestPositiveRequestId = requestId;
    return;
  }
  if (requestId >= lowestNegativeRequestId) {
    throw new Error(`SQLite worker request ID ${requestId} was already used.`);
  }
  lowestNegativeRequestId = requestId;
}

function acknowledge(message) {
  validateExactKeys(message, new Set(["kind", "protocol", "id"]), "acknowledgement");
  if (message.protocol !== PROTOCOL) {
    throw new Error("Unsupported SQLite worker acknowledgement protocol.");
  }
  validateRequestId(message.id);
  const record = completedRequests.get(message.id);
  if (!record) {
    throw new Error(`SQLite worker request ${message.id} is not awaiting acknowledgement.`);
  }
  if (record.acknowledged) {
    throw new Error(`SQLite worker request ${message.id} was already acknowledged.`);
  }
  const modeSpec = acknowledgementModeForTest;
  const applies =
    modeSpec != null &&
    (modeSpec.command == null || modeSpec.command === record.request?.command);
  const mode = applies ? modeSpec.mode : null;
  if (applies) acknowledgementModeForTest = null;
  if (mode === "throw") {
    throw new Error("controlled acknowledgement failure");
  }
  record.acknowledged = true;
  if (mode === "drop-confirmation") return;
  post({
    kind: "acknowledged",
    protocol: PROTOCOL,
    id: message.id,
  });
}

function releaseResponse(message) {
  validateExactKeys(message, new Set(["kind", "protocol", "id"]), "response release");
  if (message.protocol !== PROTOCOL) {
    throw new Error("Unsupported SQLite worker release protocol.");
  }
  validateRequestId(message.id);
  const mode = releaseModeForTest;
  releaseModeForTest = null;
  if (mode === "throw") {
    throw new Error("controlled response release failure");
  }
  const record = completedRequests.get(message.id);
  if (!record?.acknowledged) {
    throw new Error(`SQLite worker request ${message.id} is not acknowledged for release.`);
  }
  if (
    ["open", "completeOpen"].includes(record.request?.command) &&
    record.envelope?.data?.databaseId != null
  ) {
    const database = databases.get(record.envelope.data.databaseId);
    if (database != null) database.cancellationRecovery = null;
  }
  completedRequests.delete(message.id);
  if (mode === "drop-confirmation") return;
  post({
    kind: "released",
    protocol: PROTOCOL,
    id: message.id,
  });
}

async function reconcileCancellation(message) {
  let envelope;
  try {
    validateExactKeys(
      message,
      new Set(["kind", "protocol", "id"]),
      "cancellation reconciliation",
    );
    if (message.protocol !== PROTOCOL) {
      throw new Error("Unsupported SQLite worker cancellation protocol.");
    }
    validateRequestId(message.id);
    if (message.id <= 0) {
      throw new Error("SQLite worker cancellation reconciliation IDs must be positive.");
    }
    const record = completedRequests.get(message.id);
    if (!record) {
      throw new Error(`SQLite worker request ${message.id} is not reclaimable.`);
    }
    if (!record.cancelView || !isCancelled(record.cancelView)) {
      throw new Error(`SQLite worker request ${message.id} has no authoritative cancellation.`);
    }
    if (record.cancellationRecoveryFailure != null) {
      throw new Error(
        `SQLite worker cancellation recovery failed: ` +
          String(
            record.cancellationRecoveryFailure?.message ??
              record.cancellationRecoveryFailure,
          ),
      );
    }
    if (negativeReconciliationForTest) {
      negativeReconciliationForTest = false;
      const error = new Error("controlled negative cancellation reconciliation");
      envelope = errorEnvelope(
        message.id,
        record.request.command,
        error,
        record.request,
        false,
      );
      completedRequests.delete(message.id);
      post({
        kind: "cancellation-reconciled",
        protocol: PROTOCOL,
        id: message.id,
        envelope,
      });
      return;
    }
    if (record.envelope.data) {
      const error = cancelledError();
      await cleanupSuccessfulCancelledRequest(message.id, record, error);
      metrics.requestsCancelled++;
      envelope = errorEnvelope(
        message.id,
        record.request.command,
        error,
        record.request,
        true,
      );
    } else {
      envelope = record.envelope;
    }
    completedRequests.delete(message.id);
  } catch (error) {
    envelope = errorEnvelope(message?.id ?? -1, "cancel", error, null);
  }
  post({
    kind: "cancellation-reconciled",
    protocol: PROTOCOL,
    id: message?.id ?? -1,
    envelope,
  });
}

listen((message) => {
  if (message?.kind === "test-control") {
    validateExactKeys(
      message,
      new Set([
        "kind",
        "cancellationCleanupFailures",
        "acknowledgementMode",
        "acknowledgementCommand",
        "releaseMode",
        "negativeReconciliation",
        "shutdownCleanupFailures",
        "holdNextActivePage",
        "cleanupMigrationDatabaseName",
        "cleanupMigrationId",
        "migrationInterruptionDatabaseName",
        "migrationInterruptionStage",
        "migrationHeapSamples",
        "migrationMarkerDatabaseName",
        "migrationMarkerMode",
        "cancellationHold",
        "pendingOpenCountId",
        "directCapabilityMissing",
        "crash",
      ]),
      "test control",
    );
    if (typeof message.crash === "string") {
      throw new Error(message.crash);
    }
    if (message.directCapabilityMissing !== undefined) {
      validateExactKeys(
        message,
        new Set(["kind", "directCapabilityMissing"]),
        "direct capability test control envelope",
      );
      if (
        directCapabilityMissingForTest !== null ||
        !["web-crypto", "opfs", "web-locks", "opfs-vfs"].includes(
          message.directCapabilityMissing,
        )
      ) {
        throw new TypeError("Invalid direct capability test control.");
      }
      directCapabilityMissingForTest = message.directCapabilityMissing;
      return;
    }
    if (message.cancellationHold !== undefined) {
      validateExactKeys(
        message,
        new Set(["kind", "cancellationHold"]),
        "cancellation hold test control envelope",
      );
      const control = message.cancellationHold;
      if (
        control == null ||
        typeof control !== "object" ||
        Array.isArray(control) ||
        !Number.isSafeInteger(control.id) ||
        control.id <= 0 ||
        usedCancellationHoldIdsForTest.has(control.id)
      ) {
        throw new TypeError("Invalid cancellation hold test control.");
      }
      if (control.command === "migration") {
        validateExactKeys(
          control,
          new Set(["id", "command", "databaseName", "stage"]),
          "migration cancellation hold test control",
        );
        if (
          typeof control.databaseName !== "string" ||
          control.databaseName.trim() === "" ||
          ![
            "before-intent-write",
            "after-integrity",
            "after-health",
          ].includes(control.stage) ||
          migrationCancellationHoldsForTest.has(control.databaseName)
        ) {
          throw new TypeError("Invalid migration cancellation hold test control.");
        }
        migrationCancellationHoldsForTest.set(
          control.databaseName,
          Object.freeze({ ...control }),
        );
        return;
      }
      if (control.command === "completeOpen") {
        validateExactKeys(
          control,
          new Set(["id", "command", "stage"]),
          "completeOpen cancellation hold test control",
        );
        if (
          control.stage !== "before-dispatch" ||
          nextCompleteOpenCancellationHoldForTest != null
        ) {
          throw new TypeError("Invalid completeOpen cancellation hold test control.");
        }
        nextCompleteOpenCancellationHoldForTest = Object.freeze({ ...control });
        return;
      }
      throw new TypeError("Unknown cancellation hold test command.");
    }
    if (message.pendingOpenCountId !== undefined) {
      validateExactKeys(
        message,
        new Set(["kind", "pendingOpenCountId"]),
        "pending open count test control",
      );
      if (
        !Number.isSafeInteger(message.pendingOpenCountId) ||
        message.pendingOpenCountId <= 0
      ) {
        throw new TypeError("Invalid pending open count test control.");
      }
      post({
        kind: "test-pending-open-count",
        protocol: PROTOCOL,
        id: message.pendingOpenCountId,
        count: pendingOpens.size,
      });
      return;
    }
    if (message.migrationInterruptionDatabaseName !== undefined) {
      if (
        typeof message.migrationInterruptionDatabaseName !== "string" ||
        message.migrationInterruptionDatabaseName.trim() === "" ||
        ![
          "before-intent",
          "after-intent",
          "during-import",
          "after-import",
          "after-integrity",
          "after-health",
          "before-intent-cleanup",
        ].includes(message.migrationInterruptionStage)
      ) {
        throw new TypeError("Invalid migration interruption test control.");
      }
      migrationInterruptionsForTest.set(
        message.migrationInterruptionDatabaseName,
        message.migrationInterruptionStage,
      );
      return;
    }
    if (message.migrationHeapSamples !== undefined) {
      if (
        !Array.isArray(message.migrationHeapSamples) ||
        message.migrationHeapSamples.length < 3 ||
        message.migrationHeapSamples.length > 4 ||
        !message.migrationHeapSamples.every(
          (sample) => Number.isSafeInteger(sample) && sample >= 0,
        ) ||
        nextMigrationHeapSamplesForTest != null
      ) {
        throw new TypeError("Invalid migration heap sample test control.");
      }
      nextMigrationHeapSamplesForTest = Object.freeze([
        ...message.migrationHeapSamples,
      ]);
      return;
    }
    if (message.migrationMarkerDatabaseName !== undefined) {
      if (
        typeof message.migrationMarkerDatabaseName !== "string" ||
        message.migrationMarkerDatabaseName.trim() === "" ||
        ![
          "malformed-health",
          "duplicate-health",
          "unknown-health",
          "mismatched-health",
          "orphan-health",
          "malformed-intent",
          "noncanonical-intent",
          "mismatched-intent",
        ].includes(message.migrationMarkerMode)
      ) {
        throw new TypeError("Invalid migration marker test control.");
      }
      const seed = () => seedMigrationMarkerForTest(
        message.migrationMarkerDatabaseName,
        message.migrationMarkerMode,
      );
      requestQueue = requestQueue.then(seed, seed);
      return;
    }
    if (message.cleanupMigrationDatabaseName !== undefined) {
      if (
        typeof message.cleanupMigrationDatabaseName !== "string" ||
        message.cleanupMigrationDatabaseName.trim() === "" ||
        !Number.isSafeInteger(message.cleanupMigrationId) ||
        message.cleanupMigrationId <= 0
      ) {
        throw new TypeError("Invalid migration cleanup test control.");
      }
      const cleanup = async () => {
        let error = "";
        try {
          await cleanupMigrationStateForTest(message.cleanupMigrationDatabaseName);
        } catch (failure) {
          error = String(failure?.message ?? failure);
        }
        post({
          kind: "test-migration-cleaned",
          protocol: PROTOCOL,
          id: message.cleanupMigrationId,
          error,
        });
      };
      requestQueue = requestQueue.then(cleanup, cleanup);
      return;
    }
    if (message.negativeReconciliation !== undefined) {
      if (message.negativeReconciliation !== true) {
        throw new TypeError("Invalid negative reconciliation test control.");
      }
      negativeReconciliationForTest = true;
      return;
    }
    if (message.holdNextActivePage !== undefined) {
      if (message.holdNextActivePage !== true) {
        throw new TypeError("Invalid active page test control.");
      }
      holdNextActivePageForTest = true;
      return;
    }
    if (message.shutdownCleanupFailures !== undefined) {
      const failures = message.shutdownCleanupFailures;
      validateExactKeys(
        failures,
        new Set(["finalize", "rollback", "close"]),
        "shutdown cleanup failures",
      );
      for (const value of Object.values(failures)) {
        if (typeof value !== "string") {
          throw new TypeError("Shutdown cleanup failures must be strings.");
        }
      }
      shutdownCleanupFailuresForTest = { ...failures };
      return;
    }
    if (message.acknowledgementMode !== undefined) {
      if (!["drop-confirmation", "throw"].includes(message.acknowledgementMode)) {
        throw new TypeError("Invalid SQLite worker acknowledgement test control.");
      }
      if (
        message.acknowledgementCommand !== undefined &&
        typeof message.acknowledgementCommand !== "string"
      ) {
        throw new TypeError("Invalid SQLite worker acknowledgement command filter.");
      }
      acknowledgementModeForTest = {
        mode: message.acknowledgementMode,
        command: message.acknowledgementCommand ?? null,
      };
      return;
    }
    if (message.releaseMode !== undefined) {
      if (!["drop-confirmation", "throw"].includes(message.releaseMode)) {
        throw new TypeError("Invalid SQLite worker release test control.");
      }
      releaseModeForTest = message.releaseMode;
      return;
    }
    const failures = message.cancellationCleanupFailures;
    if (
      failures == null ||
      typeof failures !== "object" ||
      Array.isArray(failures) ||
      !Number.isSafeInteger(failures.requestId) ||
      failures.requestId === 0 ||
      !["undefined", "string"].includes(typeof failures.finalize) ||
      !["undefined", "string"].includes(typeof failures.rollback)
    ) {
      throw new TypeError("Invalid SQLite worker cleanup-failure test control.");
    }
    cancellationCleanupFailuresForTest = { ...failures };
    return;
  }
  if (message?.kind === "acknowledge") {
    acknowledge(message);
    return;
  }
  if (message?.kind === "release-response") {
    releaseResponse(message);
    return;
  }
  if (message?.kind === "reconcile-cancellation") {
    requestQueue = requestQueue.then(
      () => reconcileCancellation(message),
      () => reconcileCancellation(message),
    );
    return;
  }
  const requestId = message?.id;
  try {
    acceptFreshRequestId(requestId);
  } catch (error) {
    const responseId =
      Number.isSafeInteger(requestId) && requestId !== 0 ? requestId : -1;
    post(errorEnvelope(responseId, "request", error, message?.request));
    return;
  }
  requestQueue = requestQueue.then(
    () => dispatch(message),
    () => dispatch(message),
  );
});

post({
  kind: "ready",
  protocol: PROTOCOL,
  runtimeKind: metrics.runtimeKind,
  sqliteVersion: metrics.sqliteVersion,
});
