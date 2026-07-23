/*
 * Copyright 2026 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0.
 *
 * SQLiteNow packaged worker client module.
 */

const PROTOCOL = "sqlitenow-sqlite-worker-v1";
const SQLITE_VERSION = "3.53.0";
const DEFAULT_WORKER_MODULE_URL = "./sqlite-3.53.0-build1/worker.mjs";
const FORBIDDEN_WORKER_MODULE_SCHEMES = new Set(["blob", "data"]);
const MIN_REQUEST_ID = -2_147_483_648;
const MAX_REQUEST_ID = 2_147_483_647;
const MAX_PAGE_ROWS = 1024;
const MAX_ENCODED_PAGE_BYTES = 1024 * 1024 + 2;
const SOURCE_CLIENT_MODULE_URL = import.meta.url;
const isNodeRuntime = Boolean(globalThis.process?.versions?.node);
const BROWSER_POLICY_STARTUP_TEST_MODE = "missing-browser-policy";
const DIRECT_CAPABILITY_STARTUP_TEST_MODES = new Map([
  ["missing-web-crypto", "web-crypto"],
  ["missing-opfs", "opfs"],
  ["missing-web-locks", "web-locks"],
  ["missing-opfs-vfs", "opfs-vfs"],
]);
const globalDiagnostics = {
  workersCreated: 0,
  workersTerminated: 0,
  activeWorkers: 0,
  pendingStartups: 0,
};

function withCleanupDeadline(promise, timeoutMillis, operation) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(
        () => reject(
          new Error(
            `SQLite worker ${operation} exceeded the ${timeoutMillis}ms cleanup deadline.`,
          ),
        ),
        timeoutMillis,
      );
    }),
  ]).finally(() => clearTimeout(timer));
}

function resolveClientModuleUrl() {
  const urlConstructor = globalThis.URL;
  const sourceClientModuleUrl = new urlConstructor(SOURCE_CLIENT_MODULE_URL);
  if (
    isNodeRuntime ||
    (
      sourceClientModuleUrl.protocol !== "file:" &&
      sourceClientModuleUrl.pathname.endsWith("/sqlitenow-worker-v1/client.mjs")
    )
  ) {
    return sourceClientModuleUrl;
  }
  return new URL(
    /* webpackIgnore: true */ "./sqlitenow-worker-v1/client.mjs",
    import.meta.url,
  );
}

function resolveWorkerModuleUrl(workerModuleUrl) {
  const urlConstructor = globalThis.URL;
  const clientModuleUrl = resolveClientModuleUrl();
  let resolvedWorkerModuleUrl;
  if (workerModuleUrl === null || workerModuleUrl === undefined) {
    resolvedWorkerModuleUrl =
      new urlConstructor(DEFAULT_WORKER_MODULE_URL, clientModuleUrl);
  } else if (
    typeof workerModuleUrl !== "string" ||
    workerModuleUrl.trim().length === 0
  ) {
    throw new TypeError("SQLite worker module URL must be a non-empty string.");
  } else {
    resolvedWorkerModuleUrl = new urlConstructor(workerModuleUrl, clientModuleUrl);
  }
  const resolvedScheme = resolvedWorkerModuleUrl.protocol.replace(/:$/, "");
  if (FORBIDDEN_WORKER_MODULE_SCHEMES.has(resolvedScheme)) {
    throw new TypeError(
      `SQLite worker module URL scheme ${resolvedWorkerModuleUrl.protocol} is forbidden.`,
    );
  }
  return resolvedWorkerModuleUrl;
}

async function createNodeWorker(workerModuleUrl) {
  const importModule = new Function("specifier", "return import(specifier)");
  const { Worker: NodeWorker } = await importModule("node:worker_threads");
  return new NodeWorker(workerModuleUrl, { type: "module" });
}

function requireBrowserCapabilities(missingBrowserPolicyForTest = false) {
  const missing = [];
  if (typeof globalThis.Worker !== "function") missing.push("Worker");
  if (typeof globalThis.WebAssembly !== "object") missing.push("WebAssembly");
  if (typeof globalThis.SharedArrayBuffer !== "function") {
    missing.push("SharedArrayBuffer");
  }
  if (typeof globalThis.Atomics !== "object") missing.push("Atomics");
  if (globalThis.isSecureContext !== true) missing.push("secure context");
  if (globalThis.crossOriginIsolated !== true) missing.push("cross-origin isolation");
  if (missingBrowserPolicyForTest) missing.push("required browser policy");
  if (missing.length > 0) {
    throw new Error(
      `SQLite worker requires ${missing.join(", ")}; no transient fallback was started.`,
    );
  }
}

function createBrowserWorker(workerModuleUrl, missingBrowserPolicyForTest) {
  requireBrowserCapabilities(missingBrowserPolicyForTest);
  return new globalThis.Worker(workerModuleUrl, { type: "module" });
}

async function createWorker(workerModuleUrl, missingBrowserPolicyForTest) {
  return isNodeRuntime
    ? createNodeWorker(workerModuleUrl)
    : createBrowserWorker(workerModuleUrl, missingBrowserPolicyForTest);
}

function addWorkerListener(worker, type, listener) {
  if (isNodeRuntime) {
    worker.on(type, listener);
    return () => worker.off(type, listener);
  }
  const wrapped = type === "message" ? (event) => listener(event.data) : listener;
  worker.addEventListener(type, wrapped);
  return () => worker.removeEventListener(type, wrapped);
}

function postMessage(worker, message, transfer = undefined) {
  if (transfer === undefined) worker.postMessage(message);
  else worker.postMessage(message, transfer);
}

function confirmTermination(owner) {
  if (owner.terminationConfirmed) return;
  owner.terminationConfirmed = true;
  if (owner.countedWorker) {
    globalDiagnostics.workersTerminated++;
    globalDiagnostics.activeWorkers =
      Math.max(0, globalDiagnostics.activeWorkers - 1);
  }
}

function initializeTerminationOwner(owner, timeoutMillis = 5_000) {
  owner.cleanupTimeoutMillis ??= timeoutMillis;
  owner.terminationSequencePromise ??= null;
  owner.terminationFailure ??= null;
  owner.terminationConfirmed ??= false;
  owner.terminationAttempts ??= 0;
  owner.countedWorker ??= true;
  return owner;
}

function runTerminationAttempt(owner, forced) {
  owner.terminationAttempts++;
  const terminationMode = owner.terminationModeForTest;
  owner.terminationModeForTest = null;
  const underlying = Promise.resolve()
    .then(() => {
      if (terminationMode === "reject-once") {
        throw new Error("controlled worker termination rejection");
      }
      if (terminationMode === "hang-once") return new Promise(() => {});
      return owner.worker.terminate();
    })
    .then(() => confirmTermination(owner));
  return withCleanupDeadline(
    underlying,
    owner.cleanupTimeoutMillis,
    forced ? "forced termination" : "worker termination",
  );
}

function appendTerminationFailure(primary, additional) {
  const normalized = normalizeWorkerError(additional);
  if (normalized !== primary) primary.cause ??= normalized;
  return primary;
}

function beginTermination(rawOwner) {
  const owner = initializeTerminationOwner(rawOwner);
  if (owner.terminationConfirmed) return Promise.resolve();
  if (owner.terminationSequencePromise) return owner.terminationSequencePromise;
  owner.terminationSequencePromise = (async () => {
    let primary;
    try {
      await runTerminationAttempt(owner, false);
      return;
    } catch (error) {
      primary = normalizeWorkerError(error);
    }
    if (owner.terminationConfirmed) return;
    try {
      await runTerminationAttempt(owner, true);
      return;
    } catch (retryFailure) {
      owner.terminationFailure = appendTerminationFailure(primary, retryFailure);
      throw owner.terminationFailure;
    }
  })();
  return owner.terminationSequencePromise;
}

function makeCancellationBuffer() {
  return new globalThis.SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT);
}

function normalizeWorkerError(error, prefix = "SQLite worker failed") {
  if (error instanceof Error) return error;
  return new Error(`${prefix}: ${String(error?.message ?? error)}`);
}

function validatedBindings(request) {
  if (!hasOwn(request, "bindings")) return {};
  const bindings = request.bindings;
  if (bindings == null || typeof bindings !== "object" || Array.isArray(bindings)) {
    throw new TypeError("SQLite worker bindings must be an object.");
  }
  return bindings;
}

function validateIntegerBoundary(request) {
  for (const value of Object.values(validatedBindings(request))) {
    if (value?.type === "integer" && typeof value.integer !== "string") {
      throw new TypeError(
        "SQLite INTEGER bindings must cross the worker boundary as decimal strings.",
      );
    }
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

function acceptFreshRequestId(client, requestId) {
  validateRequestId(requestId);
  if (requestId > 0) {
    if (requestId <= client.highestPositiveRequestId) {
      throw new Error(`SQLite worker request ID ${requestId} was already used.`);
    }
    client.highestPositiveRequestId = requestId;
    return;
  }
  if (requestId >= client.lowestNegativeRequestId) {
    throw new Error(`SQLite worker request ID ${requestId} was already used.`);
  }
  client.lowestNegativeRequestId = requestId;
}

function rejectEntries(entries, error) {
  for (const entry of entries.values()) entry.reject(error);
  entries.clear();
}

function validateExactKeys(value, allowed, label) {
  if (value == null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object.`);
  }
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) throw new TypeError(`Unknown ${label} field: ${key}`);
  }
}

function hasOwn(value, key) {
  return Object.prototype.hasOwnProperty.call(value, key);
}

function validateWorkerError(error, label) {
  validateExactKeys(
    error,
    new Set([
      "operation",
      "message",
      "sql",
      "sqliteCode",
      "cancelled",
      "suppressed",
      "inTransaction",
    ]),
    label,
  );
  if (
    !hasOwn(error, "operation") ||
    !hasOwn(error, "message") ||
    !hasOwn(error, "cancelled") ||
    !hasOwn(error, "suppressed") ||
    typeof error.operation !== "string" ||
    typeof error.message !== "string" ||
    (hasOwn(error, "sql") && typeof error.sql !== "string") ||
    (hasOwn(error, "sqliteCode") && !Number.isInteger(error.sqliteCode)) ||
    (hasOwn(error, "cancelled") && typeof error.cancelled !== "boolean") ||
    (hasOwn(error, "inTransaction") && typeof error.inTransaction !== "boolean") ||
    (
      hasOwn(error, "suppressed") &&
      (
        !Array.isArray(error.suppressed) ||
        error.suppressed.some((message) => typeof message !== "string")
      )
    )
  ) {
    throw new TypeError(`Invalid ${label}.`);
  }
}

const RESPONSE_COMMON_KEYS = new Set([
  "protocol",
  "done",
  "oversizedRow",
  "pageRows",
  "pageBytes",
  "inTransaction",
]);
const RESPONSE_COMMAND_KEYS = new Map([
  [
    "open",
    new Set(["databaseId", "openState", "openId", "runtimeKind", "sqliteVersion"]),
  ],
  [
    "completeOpen",
    new Set(["databaseId", "openState", "openId", "runtimeKind", "sqliteVersion"]),
  ],
  ["prepare", new Set(["statementId", "columnNames"])],
  ["page", new Set(["rows"])],
  ["reset", new Set()],
  ["clearBindings", new Set()],
  ["closeStatement", new Set()],
  ["closeDatabase", new Set()],
  ["metrics", new Set(["metrics"])],
  ["shutdown", new Set(["metrics"])],
]);
const METRIC_KEYS = [
  "runtimeKind",
  "sqliteVersion",
  "storageMode",
  "requestsStarted",
  "requestsCompleted",
  "requestsCancelled",
  "pendingRequests",
  "liveDatabases",
  "liveStatements",
  "transactionsRolledBackOnCancel",
  "integerBindingsAsStrings",
  "integerResultsAsStrings",
  "integerNumberViolations",
  "pageRequests",
  "steppedRows",
  "encodedRows",
  "transferredRows",
  "transferredBytes",
  "maxPageRows",
  "maxPageBytes",
  "oversizedRows",
  "snapshotExports",
  "migrationSourceKind",
  "migrationSourceBytes",
  "migrationDurationMillis",
  "migrationPeakOwnedBytes",
  "migrationTargetFileName",
  "migrationSourceSha256",
  "migrationIntegrityCheck",
  "migrationImportedUserVersion",
  "migrationSourceRetained",
  "migrationHeapAvailable",
  "migrationHeapStartBytes",
  "migrationHeapPeakBytes",
  "migrationHeapEndBytes",
  "workerStarts",
  "workerStops",
];

function positiveInt(value) {
  return Number.isInteger(value) && value > 0 && value <= MAX_REQUEST_ID;
}

function validateResponseValue(value) {
  validateExactKeys(
    value,
    new Set(
      value?.type === "null"
        ? ["type"]
        : ["type", String(value?.type)],
    ),
    "SQLite worker response value",
  );
  switch (value.type) {
    case "null":
      return;
    case "integer":
      if (
        typeof value.integer !== "string" ||
        !/^-?(0|[1-9][0-9]*)$/.test(value.integer) ||
        value.integer === "-0"
      ) {
        throw new TypeError("SQLite worker INTEGER response must be decimal text.");
      }
      try {
        if (BigInt.asIntN(64, BigInt(value.integer)).toString() !== value.integer) {
          throw new TypeError("SQLite worker INTEGER response is outside signed 64-bit range.");
        }
      } catch (error) {
        if (error instanceof TypeError && error.message.includes("outside")) throw error;
        throw new TypeError("SQLite worker INTEGER response must be decimal text.");
      }
      return;
    case "real":
      if (!Number.isFinite(value.real)) {
        throw new TypeError("SQLite worker REAL response must be finite.");
      }
      return;
    case "text":
      if (typeof value.text !== "string") {
        throw new TypeError("SQLite worker TEXT response must be a string.");
      }
      return;
    case "blob":
      if (
        !Array.isArray(value.blob) ||
        value.blob.some((byte) => !Number.isInteger(byte) || byte < 0 || byte > 255)
      ) {
        throw new TypeError("SQLite worker BLOB response must contain octets.");
      }
      return;
    default:
      throw new TypeError(`Unsupported SQLite worker response value ${String(value.type)}.`);
  }
}

function validateMetrics(value) {
  validateExactKeys(value, new Set(METRIC_KEYS), "SQLite worker metrics");
  for (const key of METRIC_KEYS) {
    if (!hasOwn(value, key)) {
      throw new TypeError(`Missing SQLite worker metrics field: ${key}`);
    }
  }
  if (
    typeof value.runtimeKind !== "string" ||
    value.runtimeKind === "" ||
    typeof value.sqliteVersion !== "string" ||
    value.sqliteVersion === "" ||
    typeof value.storageMode !== "string" ||
    value.storageMode === ""
  ) {
    throw new TypeError("Invalid SQLite worker metric identity.");
  }
  for (const key of [
    "requestsStarted",
    "requestsCompleted",
    "requestsCancelled",
    "pendingRequests",
    "liveDatabases",
    "liveStatements",
    "transactionsRolledBackOnCancel",
    "integerBindingsAsStrings",
    "integerResultsAsStrings",
    "integerNumberViolations",
    "pageRequests",
    "steppedRows",
    "encodedRows",
    "transferredRows",
    "transferredBytes",
    "maxPageRows",
    "maxPageBytes",
    "oversizedRows",
    "snapshotExports",
    "migrationSourceBytes",
    "migrationDurationMillis",
    "migrationPeakOwnedBytes",
    "migrationHeapStartBytes",
    "migrationHeapPeakBytes",
    "migrationHeapEndBytes",
    "workerStarts",
    "workerStops",
  ]) {
    if (!Number.isSafeInteger(value[key]) || value[key] < 0) {
      throw new TypeError(`SQLite worker metric ${key} must be a non-negative integer.`);
    }
  }
  if (!Number.isSafeInteger(value.migrationImportedUserVersion)) {
    throw new TypeError(
      "SQLite worker metric migrationImportedUserVersion must be an integer.",
    );
  }
  for (const key of [
    "migrationSourceKind",
    "migrationTargetFileName",
    "migrationSourceSha256",
    "migrationIntegrityCheck",
  ]) {
    if (typeof value[key] !== "string") {
      throw new TypeError(`SQLite worker metric ${key} must be a string.`);
    }
  }
  if (
    typeof value.migrationSourceRetained !== "boolean" ||
    typeof value.migrationHeapAvailable !== "boolean"
  ) {
    throw new TypeError("SQLite worker migration boolean metrics are invalid.");
  }
}

function validateSuccessResponse(data, request) {
  const commandKeys = RESPONSE_COMMAND_KEYS.get(request.command);
  if (!commandKeys) {
    throw new TypeError(`Unsupported SQLite worker response command ${String(request.command)}.`);
  }
  const allowed = new Set([...RESPONSE_COMMON_KEYS, ...commandKeys]);
  validateExactKeys(data, allowed, `${request.command} response`);
  for (const key of RESPONSE_COMMON_KEYS) {
    if (!hasOwn(data, key)) {
      throw new TypeError(`Missing ${request.command} response field: ${key}`);
    }
  }
  if (
    data.protocol !== PROTOCOL ||
    typeof data.done !== "boolean" ||
    typeof data.oversizedRow !== "boolean" ||
    !Number.isInteger(data.pageRows) ||
    data.pageRows < 0 ||
    data.pageRows > MAX_PAGE_ROWS ||
    !Number.isInteger(data.pageBytes) ||
    data.pageBytes < 0 ||
    data.pageBytes > MAX_ENCODED_PAGE_BYTES ||
    typeof data.inTransaction !== "boolean"
  ) {
    throw new TypeError(`Invalid ${request.command} response primitives.`);
  }
  if (request.command !== "page") {
    if (data.done || data.oversizedRow || data.pageRows !== 0 || data.pageBytes !== 0) {
      throw new TypeError(`Invalid ${request.command} response page fields.`);
    }
  }
  switch (request.command) {
    case "open":
    case "completeOpen":
      if (
        !hasOwn(data, "openState") ||
        typeof data.runtimeKind !== "string" ||
        data.runtimeKind === "" ||
        typeof data.sqliteVersion !== "string" ||
        data.sqliteVersion === "" ||
        data.inTransaction
      ) {
        throw new TypeError("Invalid open response.");
      }
      if (data.openState === "opened") {
        if (!positiveInt(data.databaseId) || hasOwn(data, "openId")) {
          throw new TypeError("Invalid opened response.");
        }
      } else if (
        request.command === "open" &&
        request.legacySourceMode === "custom" &&
        data.openState === "legacy-source-required"
      ) {
        if (hasOwn(data, "databaseId") || !positiveInt(data.openId)) {
          throw new TypeError("Invalid legacy-source-required response.");
        }
      } else {
        throw new TypeError("Invalid SQLite worker open state.");
      }
      break;
    case "prepare":
      if (
        !positiveInt(data.statementId) ||
        !Array.isArray(data.columnNames) ||
        data.columnNames.some((name) => typeof name !== "string")
      ) {
        throw new TypeError("Invalid prepare response.");
      }
      break;
    case "page": {
      if (
        !Array.isArray(data.rows) ||
        data.rows.some((row) => !Array.isArray(row))
      ) {
        throw new TypeError("Invalid page response rows.");
      }
      for (const row of data.rows) row.forEach(validateResponseValue);
      if (
        data.pageRows !== data.rows.length ||
        (data.oversizedRow && data.rows.length !== 1)
      ) {
        throw new TypeError("Invalid page response accounting.");
      }
      const exactBytes = new TextEncoder().encode(JSON.stringify(data.rows)).byteLength;
      if (data.pageBytes !== exactBytes) {
        throw new TypeError("Invalid page response byte count.");
      }
      break;
    }
    case "closeDatabase":
      if (data.inTransaction) {
        throw new TypeError("Closed database response cannot be in a transaction.");
      }
      break;
    case "metrics":
    case "shutdown":
      validateMetrics(data.metrics);
      if (data.inTransaction) {
        throw new TypeError(`${request.command} response cannot be in a transaction.`);
      }
      break;
    default:
      break;
  }
}

function validateResponseForEntry(message, entry) {
  if (message.id !== entry.id) {
    throw new TypeError(
      `SQLite worker response ID ${message.id} does not match request ID ${entry.id}.`,
    );
  }
  if (hasOwn(message, "error")) {
    validateWorkerError(message.error, "response error");
    if (message.error.operation !== entry.request.command) {
      throw new TypeError(
        `SQLite worker error operation ${message.error.operation} does not match ` +
          `${entry.request.command}.`,
      );
    }
    return;
  }
  validateSuccessResponse(message.data, entry.request);
}

function failClient(client, rawError) {
  if (client.terminalError) return client.terminalError;
  const error = normalizeWorkerError(rawError);
  client.terminalError = error;
  client.closed = true;
  client.readyReject?.(error);
  client.cancelStartupDelay();
  rejectEntries(client.pending, error);
  rejectEntries(client.completed, error);
  rejectEntries(client.reconciliations, error);
  rejectEntries(client.acknowledgements, error);
  rejectEntries(client.releases, error);
  rejectEntries(client.activePageWaiters, error);
  rejectEntries(client.migrationCleanupWaiters, error);
  rejectEntries(client.cancellationHoldWaiters, error);
  rejectEntries(client.pendingOpenCountWaiters, error);
  client.activePageAnyWaiter?.reject(error);
  client.activePageAnyWaiter = null;
  client.activePagesObserved.clear();
  client.cancellationHolds.clear();
  client.cancellationHoldsObserved.clear();
  client.cancellationViews.clear();
  client.detachListeners();
  void beginTermination(client).catch((terminationFailure) => {
    client.terminalCleanupFailure ??= terminationFailure;
    error.cause ??= terminationFailure;
  });
  return error;
}

function clientUnavailableError(client) {
  return client.terminalError ??
    new Error(client.closed ? "SQLite worker client is closed." : "SQLite worker client unavailable.");
}

function requestRaw(client, requestId, requestJson, oneWay = false, legacy = null) {
  if (client.closed || client.terminalError) {
    return Promise.reject(clientUnavailableError(client));
  }
  try {
    acceptFreshRequestId(client, requestId);
    const request = JSON.parse(requestJson);
    validateIntegerBoundary(request);
    const cancelBuffer = makeCancellationBuffer();
    const cancelView = new Int32Array(cancelBuffer);
    let resolveRequest;
    let rejectRequest;
    const promise = new Promise((resolve, reject) => {
      resolveRequest = resolve;
      rejectRequest = reject;
    });
    const entry = {
      id: requestId,
      request,
      cancelView,
      resolve: resolveRequest,
      reject: rejectRequest,
      oneWay,
      promise,
      settled: false,
    };
    if (!oneWay) client.cancellationViews.set(requestId, cancelView);
    client.pending.set(requestId, entry);
    try {
      const message = { id: requestId, request, cancelBuffer };
      if (legacy != null) {
        message.legacyBytes = legacy.bytes;
        message.legacyBytesSha256 = legacy.sha256;
        postMessage(client.worker, message, [legacy.bytes.buffer]);
      } else {
        postMessage(client.worker, message);
      }
    } catch (error) {
      client.pending.delete(requestId);
      client.cancellationViews.delete(requestId);
      rejectRequest(failClient(client, error));
    }
    return promise;
  } catch (error) {
    return Promise.reject(error);
  }
}

function responseModeFor(client, entry) {
  const mode = client.responseModeForTest;
  if (!mode || mode.command !== entry.request.command) return { mode: "normal" };
  client.responseModeForTest = null;
  return mode;
}

function responseForMode(message, mode) {
  if (mode.mode === "omit-field") {
    const payloadName = mode.payload ?? (hasOwn(message, "data") ? "data" : "error");
    const payload = { ...message[payloadName] };
    if (
      payloadName === "data" &&
      !hasOwn(payload, mode.field) &&
      payload.metrics != null &&
      hasOwn(payload.metrics, mode.field)
    ) {
      payload.metrics = { ...payload.metrics };
      delete payload.metrics[mode.field];
    } else {
      delete payload[mode.field];
    }
    return { id: message.id, [payloadName]: payload };
  }
  if (mode.mode === "malform-field") {
    const payloadName = mode.payload ?? (hasOwn(message, "data") ? "data" : "error");
    const payload = { ...message[payloadName], [mode.field]: mode.value };
    return { id: message.id, [payloadName]: payload };
  }
  if (mode.mode !== "malformed") return message;
  const data = { ...message.data };
  switch (mode.command) {
    case "open":
      data.databaseId = 0;
      break;
    case "prepare":
      data.statementId = 0;
      break;
    case "page":
      data.pageBytes = Number(data.pageBytes ?? 0) + 1;
      break;
    case "metrics":
      data.metrics = null;
      break;
    case "reset":
    case "clearBindings":
    case "closeStatement":
    case "closeDatabase":
      data.inTransaction = "false";
      break;
    default:
      throw new TypeError(
        `No controlled malformed response for ${String(mode.command)}.`,
      );
  }
  return { id: message.id, data };
}

async function acknowledgeCompleted(client, requestId) {
  const entry = client.completed.get(requestId);
  if (!entry) return;
  if (client.closed || client.terminalError) throw clientUnavailableError(client);
  validateResponseForEntry(entry.response, entry);
  let resolveAcknowledgement;
  let rejectAcknowledgement;
  const acknowledgement = new Promise((resolve, reject) => {
    resolveAcknowledgement = resolve;
    rejectAcknowledgement = reject;
  });
  client.acknowledgements.set(requestId, {
    resolve: resolveAcknowledgement,
    reject: rejectAcknowledgement,
  });
  try {
    postMessage(client.worker, {
      kind: "acknowledge",
      protocol: PROTOCOL,
      id: requestId,
    });
  } catch (error) {
    client.acknowledgements.delete(requestId);
    throw failClient(client, error);
  }
  try {
    await withCleanupDeadline(
      acknowledgement,
      client.cleanupTimeoutMillis,
      "response acknowledgement",
    );
  } catch (error) {
    client.acknowledgements.delete(requestId);
    const terminal = failClient(client, error);
    await beginTermination(client).catch((terminationFailure) => {
      terminal.cause ??= terminationFailure;
    });
    throw terminal;
  }
}

function releaseCompleted(client, requestId) {
  const entry = client.completed.get(requestId);
  if (!entry) throw new Error(`SQLite worker request ${requestId} is not retained.`);
  if (client.closed || client.terminalError) throw clientUnavailableError(client);
  if (client.releases.has(requestId)) {
    throw new Error(`SQLite worker request ${requestId} is already awaiting release.`);
  }
  let resolveRelease;
  let rejectRelease;
  const released = new Promise((resolve, reject) => {
    resolveRelease = resolve;
    rejectRelease = reject;
  });
  client.releases.set(requestId, {
    resolve: resolveRelease,
    reject: rejectRelease,
  });
  try {
    postMessage(client.worker, {
      kind: "release-response",
      protocol: PROTOCOL,
      id: requestId,
    });
  } catch (error) {
    client.releases.delete(requestId);
    throw failClient(client, error);
  }
  return withCleanupDeadline(
    released,
    client.cleanupTimeoutMillis,
    "response release",
  ).catch(async (error) => {
    client.releases.delete(requestId);
    const terminal = failClient(client, error);
    await beginTermination(client).catch((terminationFailure) => {
      terminal.cause ??= terminationFailure;
    });
    throw terminal;
  });
}

function delayStartupForTest(mode, registerCancellation) {
  if (mode !== "delay-ready") return Promise.resolve();
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, 60_000);
    registerCancellation(() => {
      clearTimeout(timer);
      reject(new Error("SQLite worker startup cancelled."));
    });
  });
}

function startupModeParts(mode) {
  for (const terminationMode of ["reject-once", "hang-once"]) {
    const suffix = `-${terminationMode}`;
    if (mode.endsWith(suffix)) {
      return {
        mode: mode.slice(0, -suffix.length),
        terminationMode,
      };
    }
  }
  return { mode, terminationMode: null };
}

function validateStartupMode(mode) {
  const parts = startupModeParts(mode);
  if (
    ![
      "normal",
      "protocol-mismatch",
      "delay-ready",
      "hold-ready",
      BROWSER_POLICY_STARTUP_TEST_MODE,
      ...DIRECT_CAPABILITY_STARTUP_TEST_MODES.keys(),
    ].includes(parts.mode)
  ) {
    throw new Error(`Unknown SQLite worker startup test mode: ${String(mode)}.`);
  }
  return parts;
}

export function createSqliteWorkerClient(
  configJson,
  workerModuleUrl = null,
  startupModeForTest = "normal",
  cleanupTimeoutMillis = 5_000,
) {
  const startupTest = validateStartupMode(startupModeForTest);
  const resolvedWorkerModuleUrl = resolveWorkerModuleUrl(workerModuleUrl);
  if (!Number.isInteger(cleanupTimeoutMillis) || cleanupTimeoutMillis <= 0) {
    throw new TypeError("SQLite worker cleanup timeout must be a positive integer.");
  }
  globalDiagnostics.pendingStartups++;
  let cancelRequested = false;
  let cancelDelay = () => {};
  let client;
  const workerPromise = createWorker(
    resolvedWorkerModuleUrl,
    startupTest.mode === BROWSER_POLICY_STARTUP_TEST_MODE,
  ).then((worker) => {
    globalDiagnostics.workersCreated++;
    globalDiagnostics.activeWorkers++;
    return worker;
  });

  const startupPromise = (async () => {
    let worker;
    try {
      worker = await workerPromise;
      let readyResolve;
      let readyReject;
      const ready = new Promise((resolve, reject) => {
        readyResolve = resolve;
        readyReject = reject;
      });
      client = {
        worker,
        configJson,
        cleanupTimeoutMillis,
        pending: new Map(),
        completed: new Map(),
        reconciliations: new Map(),
        acknowledgements: new Map(),
        releases: new Map(),
        activePageWaiters: new Map(),
        migrationCleanupWaiters: new Map(),
        cancellationHoldWaiters: new Map(),
        pendingOpenCountWaiters: new Map(),
        cancellationHolds: new Map(),
        cancellationHoldsObserved: new Map(),
        nextTestControlId: 1,
        nextMigrationCleanupId: 1,
        activePagesObserved: new Set(),
        activePageAnyWaiter: null,
        cancellationViews: new Map(),
        oneWays: [],
        nextOneWayId: -1,
        highestPositiveRequestId: 0,
        lowestNegativeRequestId: 0,
        runtimeKind: isNodeRuntime ? "js-node-worker" : "browser-worker",
        ignoredResponses: 0,
        responseModeForTest: null,
        terminalError: null,
        terminalCleanupFailure: null,
        terminationModeForTest: startupTest.terminationMode,
        terminationSequencePromise: null,
        terminationFailure: null,
        terminationConfirmed: false,
        terminationAttempts: 0,
        countedWorker: true,
        closed: false,
        readyReject,
        cancelStartupDelay: () => cancelDelay(),
        detachListeners: () => {},
      };

      client.handleMessage = (message) => {
        try {
          if (message?.kind === "test-cancellation-hold") {
            if (message.command === "migration") {
              validateExactKeys(
                message,
                new Set([
                  "kind",
                  "protocol",
                  "id",
                  "command",
                  "databaseName",
                  "stage",
                  "pendingOpenCount",
                ]),
                "migration cancellation hold test sideband",
              );
            } else if (message.command === "completeOpen") {
              validateExactKeys(
                message,
                new Set([
                  "kind",
                  "protocol",
                  "id",
                  "command",
                  "stage",
                  "pendingOpenCount",
                ]),
                "completeOpen cancellation hold test sideband",
              );
            } else {
              throw new TypeError("Unknown cancellation hold test command.");
            }
            const expected = client.cancellationHolds.get(message.id);
            if (
              message.protocol !== PROTOCOL ||
              !positiveInt(message.id) ||
              !Number.isSafeInteger(message.pendingOpenCount) ||
              message.pendingOpenCount < 0 ||
              expected == null ||
              expected.command !== message.command ||
              expected.stage !== message.stage ||
              (
                message.command === "migration" &&
                (
                  typeof message.databaseName !== "string" ||
                  expected.databaseName !== message.databaseName
                )
              ) ||
              client.cancellationHoldsObserved.has(message.id)
            ) {
              throw new TypeError("Invalid cancellation hold test sideband.");
            }
            const waiter = client.cancellationHoldWaiters.get(message.id);
            if (waiter) {
              client.cancellationHoldWaiters.delete(message.id);
              client.cancellationHolds.delete(message.id);
              waiter.resolve(String(message.pendingOpenCount));
            } else {
              client.cancellationHoldsObserved.set(
                message.id,
                message.pendingOpenCount,
              );
            }
            return;
          }
          if (message?.kind === "test-pending-open-count") {
            validateExactKeys(
              message,
              new Set(["kind", "protocol", "id", "count"]),
              "pending open count test sideband",
            );
            if (
              message.protocol !== PROTOCOL ||
              !positiveInt(message.id) ||
              !Number.isSafeInteger(message.count) ||
              message.count < 0
            ) {
              throw new TypeError("Invalid pending open count test sideband.");
            }
            const waiter = client.pendingOpenCountWaiters.get(message.id);
            if (!waiter) {
              throw new TypeError(`Unexpected pending open count ${message.id}.`);
            }
            client.pendingOpenCountWaiters.delete(message.id);
            waiter.resolve(String(message.count));
            return;
          }
          if (message?.kind === "test-migration-cleaned") {
            validateExactKeys(
              message,
              new Set(["kind", "protocol", "id", "error"]),
              "migration cleanup test sideband",
            );
            if (
              message.protocol !== PROTOCOL ||
              !positiveInt(message.id) ||
              typeof message.error !== "string"
            ) {
              throw new TypeError("Invalid migration cleanup test sideband.");
            }
            const waiter = client.migrationCleanupWaiters.get(message.id);
            if (!waiter) {
              throw new TypeError(`Unexpected migration cleanup ${message.id}.`);
            }
            client.migrationCleanupWaiters.delete(message.id);
            if (message.error === "") waiter.resolve();
            else waiter.reject(new Error(message.error));
            return;
          }
          if (message?.kind === "test-active-page") {
            validateExactKeys(
              message,
              new Set(["kind", "protocol", "id"]),
              "active page test sideband",
            );
            if (message.protocol !== PROTOCOL) {
              throw new TypeError("Unsupported SQLite worker active page protocol.");
            }
            validateRequestId(message.id);
            if (message.id <= 0) {
              throw new TypeError("Active page test sideband IDs must be positive.");
            }
            const waiter = client.activePageWaiters.get(message.id);
            if (client.activePageAnyWaiter) {
              const anyWaiter = client.activePageAnyWaiter;
              client.activePageAnyWaiter = null;
              anyWaiter.resolve();
            } else if (waiter) {
              client.activePageWaiters.delete(message.id);
              waiter.resolve();
            } else {
              client.activePagesObserved.add(message.id);
            }
            return;
          }
          if (message?.kind === "ready") {
            validateExactKeys(
              message,
              new Set(["kind", "protocol", "runtimeKind", "sqliteVersion"]),
              "ready message",
            );
            if (
              message.protocol !== PROTOCOL ||
              typeof message.runtimeKind !== "string" ||
              typeof message.sqliteVersion !== "string"
            ) {
              throw new TypeError("Invalid SQLite worker ready message.");
            }
            const missingDirectCapability =
              DIRECT_CAPABILITY_STARTUP_TEST_MODES.get(startupTest.mode);
            if (missingDirectCapability !== undefined) {
              postMessage(client.worker, {
                kind: "test-control",
                directCapabilityMissing: missingDirectCapability,
              });
            }
            if (startupTest.mode !== "hold-ready") readyResolve(message);
            return;
          }
          if (message?.kind === "acknowledged") {
            validateExactKeys(
              message,
              new Set(["kind", "protocol", "id"]),
              "acknowledgement confirmation",
            );
            if (message.protocol !== PROTOCOL) {
              throw new TypeError("Unsupported SQLite worker acknowledgement protocol.");
            }
            validateRequestId(message.id);
            const acknowledgement = client.acknowledgements.get(message.id);
            if (!acknowledgement) {
              throw new TypeError(
                `Unexpected SQLite worker acknowledgement ${message.id}.`,
              );
            }
            client.acknowledgements.delete(message.id);
            acknowledgement.resolve();
            return;
          }
          if (message?.kind === "released") {
            validateExactKeys(
              message,
              new Set(["kind", "protocol", "id"]),
              "release confirmation",
            );
            if (message.protocol !== PROTOCOL) {
              throw new TypeError("Unsupported SQLite worker release protocol.");
            }
            validateRequestId(message.id);
            const release = client.releases.get(message.id);
            if (!release || !client.completed.has(message.id)) {
              throw new TypeError(`Unexpected SQLite worker release ${message.id}.`);
            }
            client.releases.delete(message.id);
            client.completed.delete(message.id);
            client.cancellationViews.delete(message.id);
            release.resolve();
            return;
          }
          if (message?.kind === "cancellation-reconciled") {
            validateExactKeys(
              message,
              new Set(["kind", "protocol", "id", "envelope"]),
              "cancellation reconciliation",
            );
            if (message.protocol !== PROTOCOL) {
              throw new TypeError("Unsupported SQLite worker cancellation protocol.");
            }
            validateRequestId(message.id);
            if (message.id <= 0) {
              throw new TypeError(
                "SQLite worker cancellation reconciliation IDs must be positive.",
              );
            }
            validateExactKeys(
              message.envelope,
              new Set(["id", "error"]),
              "reconciliation envelope",
            );
            if (
              message.envelope.id !== message.id ||
              !hasOwn(message.envelope, "error")
            ) {
              throw new TypeError("Invalid SQLite worker reconciliation envelope.");
            }
            validateWorkerError(
              message.envelope.error,
              "reconciliation error",
            );
            const reconciliation = client.reconciliations.get(message.id);
            if (!reconciliation) {
              throw new TypeError(
                `Unexpected SQLite worker cancellation reconciliation ${message.id}.`,
              );
            }
            const request = reconciliation.request;
            if (
              message.envelope.error.operation !== request.command ||
              message.envelope.error.cancelled !== true ||
              (
                hasOwn(request, "databaseId") &&
                !hasOwn(message.envelope.error, "inTransaction")
              ) ||
              (
                !hasOwn(request, "databaseId") &&
                hasOwn(message.envelope.error, "inTransaction")
              )
            ) {
              throw new TypeError(
                `Invalid SQLite worker cancellation reconciliation ${message.id}: ` +
                  `${message.envelope.error.message}`,
              );
            }
            client.reconciliations.delete(message.id);
            const acknowledgement = client.acknowledgements.get(message.id);
            if (acknowledgement) {
              client.acknowledgements.delete(message.id);
              acknowledgement.resolve();
            }
            const completed = client.completed.get(message.id);
            client.completed.delete(message.id);
            client.cancellationViews.delete(message.id);
            if (completed && !completed.settled) {
              completed.settled = true;
              completed.resolve(completed.response);
            }
            reconciliation.resolve(message.envelope);
            return;
          }
          validateExactKeys(
            message,
            new Set(["id", "data", "error"]),
            "response envelope",
          );
          validateRequestId(message.id);
          const hasData = hasOwn(message, "data");
          const hasError = hasOwn(message, "error");
          if (hasData === hasError) {
            throw new TypeError("SQLite worker response must contain exactly one payload.");
          }
          const entry = client.pending.get(message.id);
          if (!entry || client.completed.has(message.id)) {
            client.ignoredResponses++;
            return;
          }
          const mode = responseModeFor(client, entry);
          const deliveredMessage = responseForMode(message, mode);
          validateResponseForEntry(deliveredMessage, entry);
          client.pending.delete(message.id);
          entry.response = deliveredMessage;
          client.completed.set(message.id, entry);
          if (mode.mode !== "hold" && mode.mode !== "drop") {
            entry.settled = true;
            entry.resolve(deliveredMessage);
          }
        } catch (error) {
          failClient(client, error);
        }
      };

      const detach = [
        addWorkerListener(worker, "message", client.handleMessage),
        addWorkerListener(worker, "error", (error) => failClient(client, error)),
      ];
      if (isNodeRuntime) {
        detach.push(
          addWorkerListener(worker, "messageerror", (error) =>
            failClient(client, normalizeWorkerError(error, "SQLite worker message error"))),
          addWorkerListener(worker, "exit", (code) => {
            confirmTermination(client);
            if (!client.closed && !client.terminalError) {
              failClient(client, new Error(`SQLite worker exited unexpectedly with code ${code}.`));
            }
          }),
        );
      } else {
        detach.push(
          addWorkerListener(worker, "messageerror", (error) =>
            failClient(client, normalizeWorkerError(error, "SQLite worker message error"))),
        );
      }
      client.detachListeners = () => {
        while (detach.length > 0) detach.pop()();
      };

      if (cancelRequested) throw new Error("SQLite worker startup cancelled.");
      let startup = await ready;
      await delayStartupForTest(startupTest.mode, (cancel) => {
        cancelDelay = cancel;
        if (cancelRequested) cancel();
      });
      if (startupTest.mode === "protocol-mismatch") {
        startup = { ...startup, protocol: "sqlitenow-sqlite-worker-v0" };
      }
      if (
        startup?.protocol !== PROTOCOL ||
        startup?.sqliteVersion !== SQLITE_VERSION
      ) {
        throw new Error(
          `SQLite worker startup mismatch: protocol=${String(startup?.protocol)} ` +
            `sqlite=${String(startup?.sqliteVersion)}.`,
        );
      }
      if (cancelRequested) throw new Error("SQLite worker startup cancelled.");
      return client;
    } catch (error) {
      if (client) client.closed = true;
      if (!cancelRequested && worker) {
        client?.detachListeners?.();
        try {
          await beginTermination(
            client ?? initializeTerminationOwner({
              worker,
              cleanupTimeoutMillis,
              countedWorker: true,
            }),
          );
        } catch (terminationFailure) {
          error.cause ??= terminationFailure;
        }
      }
      throw error;
    } finally {
      globalDiagnostics.pendingStartups =
        Math.max(0, globalDiagnostics.pendingStartups - 1);
    }
  })();

  startupPromise.cancel = async () => {
    cancelRequested = true;
    cancelDelay();
    let worker;
    try {
      worker = await workerPromise;
    } catch (_) {
      // The startup promise preserves the worker-creation failure.
    }
    if (client) {
      client.closed = true;
      client.readyReject?.(new Error("SQLite worker startup cancelled."));
      client.detachListeners?.();
      await beginTermination(client);
    } else if (worker) {
      await beginTermination(initializeTerminationOwner({
        worker,
        cleanupTimeoutMillis,
        countedWorker: true,
      }));
    }
    try {
      await startupPromise;
    } catch (_) {
      // The cancellation owner waits for startup to relinquish every reference.
    }
  };
  startupPromise.cleanupTimeoutMillis = cleanupTimeoutMillis;
  return startupPromise;
}

export async function sqliteWorkerRequest(client, requestId, requestJson) {
  return JSON.stringify(await requestRaw(client, requestId, requestJson));
}

export async function sqliteWorkerRequestWithLegacyBytes(
  client,
  requestId,
  requestJson,
  legacyBytes,
) {
  if (!(legacyBytes instanceof Uint8Array)) {
    throw new TypeError("SQLite worker legacy bytes must be a Uint8Array.");
  }
  const standalone =
    legacyBytes.byteOffset === 0 &&
    legacyBytes.byteLength === legacyBytes.buffer.byteLength
      ? legacyBytes
      : Uint8Array.from(legacyBytes);
  if (standalone.byteLength === 0) {
    throw new TypeError("SQLite worker legacy bytes must not be empty.");
  }
  const digest = await globalThis.crypto.subtle.digest("SHA-256", standalone);
  const sha256 = Array.from(new Uint8Array(digest))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
  return JSON.stringify(
    await requestRaw(
      client,
      requestId,
      requestJson,
      false,
      { bytes: standalone, sha256 },
    ),
  );
}

export async function cancelSqliteWorkerRequest(client, requestId) {
  if (client.terminalError) throw client.terminalError;
  const entry = client.pending.get(requestId) ?? client.completed.get(requestId);
  const cancelView = client.cancellationViews.get(requestId) ?? entry?.cancelView;
  if (!entry) {
    throw new Error(`SQLite worker request ${requestId} is not reclaimable.`);
  }
  if (cancelView) Atomics.store(cancelView, 0, 1);
  let resolveReconciliation;
  let rejectReconciliation;
  const reconciliation = new Promise((resolve, reject) => {
    resolveReconciliation = resolve;
    rejectReconciliation = reject;
  });
  client.reconciliations.set(requestId, {
    resolve: resolveReconciliation,
    reject: rejectReconciliation,
    request: entry.request,
  });
  try {
    postMessage(client.worker, {
      kind: "reconcile-cancellation",
      protocol: PROTOCOL,
      id: requestId,
    });
  } catch (error) {
    client.reconciliations.delete(requestId);
    throw failClient(client, error);
  }
  try {
    return JSON.stringify(await withCleanupDeadline(
      reconciliation,
      client.cleanupTimeoutMillis,
      "cancellation reconciliation",
    ));
  } catch (error) {
    client.reconciliations.delete(requestId);
    const terminal = failClient(client, error);
    await beginTermination(client).catch((terminationFailure) => {
      terminal.cause ??= terminationFailure;
    });
    throw terminal;
  }
}

export async function acknowledgeSqliteWorkerRequest(client, requestId) {
  await acknowledgeCompleted(client, requestId);
  return "";
}

export function releaseSqliteWorkerRequest(client, requestId) {
  void releaseCompleted(client, requestId).catch(() => {
    // releaseCompleted records the sticky terminal failure and owns bounded cleanup.
  });
}

export async function cancelSqliteWorkerStartup(startup) {
  if (typeof startup?.cancel !== "function") {
    throw new Error("SQLite worker startup has no cancellation owner.");
  }
  await startup.cancel();
  return "";
}

function allocateOneWayId(client) {
  const requestId = client.nextOneWayId;
  if (requestId < MIN_REQUEST_ID) {
    throw new Error("SQLite worker one-way request IDs are exhausted.");
  }
  client.nextOneWayId = requestId - 1;
  return requestId;
}

export function sendSqliteWorkerOneWay(client, requestJson) {
  const requestId = allocateOneWayId(client);
  const promise = requestRaw(client, requestId, requestJson, true);
  const entry = {
    requestId,
    response: null,
    error: null,
    settled: null,
  };
  entry.settled = promise.then(
    (response) => {
      entry.response = response;
    },
    (error) => {
      entry.error = normalizeWorkerError(error);
    },
  );
  client.oneWays.push(entry);
  return requestId;
}

export async function flushSqliteWorkerOneWays(client) {
  const pending = client.oneWays.splice(0);
  if (pending.length === 0) {
    return JSON.stringify({ envelopes: [], barrierFailures: [] });
  }
  const barrierFailures = [];
  try {
    await withCleanupDeadline(
      Promise.all(pending.map((entry) => entry.settled)),
      client.cleanupTimeoutMillis,
      "one-way flush",
    );
  } catch (error) {
    barrierFailures.push(String(error?.message ?? error));
    failClient(client, error);
    await Promise.all(pending.map((entry) => entry.settled));
  }
  const envelopes = [];
  for (const entry of pending) {
    if (entry.response) {
      envelopes.push(entry.response);
      if (client.closed || client.terminalError) continue;
      try {
        await acknowledgeCompleted(client, entry.requestId);
        releaseCompleted(client, entry.requestId).catch(() => {
          // The sticky terminal failure is returned by this or the next barrier.
        });
      } catch (error) {
        barrierFailures.push(String(error?.message ?? error));
      }
    } else if (entry.error) {
      const message = String(entry.error.message ?? entry.error);
      if (!barrierFailures.includes(message)) barrierFailures.push(message);
    }
  }
  return JSON.stringify({ envelopes, barrierFailures });
}

export async function shutdownSqliteWorker(client, requestId, requestJson) {
  if (client.closed || client.terminalError) throw clientUnavailableError(client);
  let response;
  let primary;
  try {
    response = await withCleanupDeadline(
      requestRaw(client, requestId, requestJson),
      client.cleanupTimeoutMillis,
      "shutdown",
    );
    await acknowledgeCompleted(client, requestId);
    await releaseCompleted(client, requestId);
  } catch (error) {
    primary = error;
  }
  client.closed = true;
  const closedError = new Error("SQLite worker client closed.");
  rejectEntries(client.pending, closedError);
  rejectEntries(client.completed, closedError);
  rejectEntries(client.reconciliations, closedError);
  rejectEntries(client.acknowledgements, closedError);
  rejectEntries(client.releases, closedError);
  rejectEntries(client.activePageWaiters, closedError);
  rejectEntries(client.cancellationHoldWaiters, closedError);
  rejectEntries(client.pendingOpenCountWaiters, closedError);
  client.activePageAnyWaiter?.reject(closedError);
  client.activePageAnyWaiter = null;
  client.activePagesObserved.clear();
  client.cancellationHolds.clear();
  client.cancellationHoldsObserved.clear();
  client.cancellationViews.clear();
  client.oneWays.splice(0);
  client.detachListeners();
  try {
    await beginTermination(client);
  } catch (error) {
    if (primary) {
      primary.cause = primary.cause ?? error;
    } else {
      primary = error;
    }
  }
  if (primary) throw primary;
  return JSON.stringify(response);
}

export async function forceTerminateSqliteWorker(client) {
  if (!client.closed && !client.terminalError) {
    failClient(client, new Error("SQLite worker was force-terminated during cleanup."));
  }
  await beginTermination(client);
  return "";
}

export function waitForSqliteWorkerCleanupDeadline(client) {
  return new Promise((resolve) => {
    setTimeout(resolve, client.cleanupTimeoutMillis);
  });
}

export function sqliteWorkerRuntimeKind(client) {
  return client.runtimeKind;
}

export function sqliteWorkerClientDiagnostics(client) {
  return JSON.stringify({
    pendingRequests: client.pending.size,
    completedResponses: client.completed.size,
    completedCommands: Array.from(
      client.completed.values(),
      (entry) => `${entry.request.command}:${entry.request.sql ?? ""}`,
    ),
    completedTransactionStates: Array.from(
      client.completed.values(),
      (entry) => entry.response?.data?.inTransaction ?? null,
    ),
    reconciliationRequests: client.reconciliations.size,
    acknowledgementRequests: client.acknowledgements.size,
    releaseRequests: client.releases.size,
    queuedOneWays: client.oneWays.length,
    ignoredResponses: client.ignoredResponses,
    highestPositiveRequestId: client.highestPositiveRequestId,
    lowestNegativeRequestId: client.lowestNegativeRequestId,
    terminal: client.terminalError !== null,
    terminationAttempts: client.terminationAttempts,
    terminationConfirmed: client.terminationConfirmed ? 1 : 0,
  });
}

export function injectSqliteWorkerResponseForTest(client, responseJson) {
  client.handleMessage(JSON.parse(responseJson));
}

export function setSqliteWorkerCleanupFailuresForTest(client, failuresJson) {
  postMessage(client.worker, {
    kind: "test-control",
    cancellationCleanupFailures: JSON.parse(failuresJson),
  });
}

export function setSqliteWorkerNegativeReconciliationForTest(client) {
  postMessage(client.worker, {
    kind: "test-control",
    negativeReconciliation: true,
  });
}

export function setSqliteWorkerShutdownFailuresForTest(client, failuresJson) {
  postMessage(client.worker, {
    kind: "test-control",
    shutdownCleanupFailures: JSON.parse(failuresJson),
  });
}

export function holdSqliteWorkerActivePageForTest(client) {
  postMessage(client.worker, {
    kind: "test-control",
    holdNextActivePage: true,
  });
}

export async function awaitSqliteWorkerActivePageForTest(client) {
  const observed = client.activePagesObserved.values().next();
  if (!observed.done) {
    client.activePagesObserved.delete(observed.value);
    return "";
  }
  if (client.activePageAnyWaiter) {
    throw new Error("An active page test barrier is already awaited.");
  }
  let resolveBarrier;
  let rejectBarrier;
  const barrier = new Promise((resolve, reject) => {
    resolveBarrier = resolve;
    rejectBarrier = reject;
  });
  client.activePageAnyWaiter = {
    resolve: resolveBarrier,
    reject: rejectBarrier,
  };
  try {
    await withCleanupDeadline(
      barrier,
      client.cleanupTimeoutMillis,
      "active page test barrier",
    );
  } catch (error) {
    client.activePageAnyWaiter = null;
    throw error;
  }
  return "";
}

export function setSqliteWorkerAcknowledgementModeForTest(client, mode) {
  if (
    ![
      "drop-confirmation",
      "throw",
      "drop-page-confirmation",
      "drop-release-confirmation",
      "throw-release",
    ].includes(mode)
  ) {
    throw new TypeError("Invalid SQLite worker acknowledgement test mode.");
  }
  if (mode === "drop-release-confirmation" || mode === "throw-release") {
    postMessage(client.worker, {
      kind: "test-control",
      releaseMode: mode === "throw-release" ? "throw" : "drop-confirmation",
    });
  } else {
    postMessage(client.worker, {
      kind: "test-control",
      acknowledgementMode:
        mode === "drop-page-confirmation" ? "drop-confirmation" : mode,
      acknowledgementCommand:
        mode === "drop-page-confirmation" ? "page" : undefined,
    });
  }
}

export function setSqliteWorkerTerminationModeForTest(client, mode) {
  if (!["reject-once", "hang-once"].includes(mode)) {
    throw new TypeError("Invalid SQLite worker termination test mode.");
  }
  client.terminationModeForTest = mode;
}

export function setSqliteWorkerResponseModeForTest(client, modeJson) {
  const mode = JSON.parse(modeJson);
  if (
    mode == null ||
    typeof mode !== "object" ||
    Array.isArray(mode) ||
    !["hold", "drop", "malformed", "omit-field", "malform-field"].includes(mode.mode) ||
    typeof mode.command !== "string"
  ) {
    throw new TypeError("Invalid SQLite worker response test mode.");
  }
  if (
    mode.mode === "malformed" &&
    ![
      "open",
      "prepare",
      "page",
      "reset",
      "clearBindings",
      "closeStatement",
      "closeDatabase",
      "metrics",
    ].includes(mode.command)
  ) {
    throw new TypeError("Unsupported malformed SQLite worker response command.");
  }
  if (
    ["omit-field", "malform-field"].includes(mode.mode) &&
    (
      typeof mode.field !== "string" ||
      (mode.payload !== undefined && !["data", "error"].includes(mode.payload))
    )
  ) {
    throw new TypeError("Response field test controls require a field name.");
  }
  client.responseModeForTest = { ...mode };
}

export function failSqliteWorkerForTest(client, message) {
  postMessage(client.worker, {
    kind: "test-control",
    crash: String(message),
  });
}

export function setSqliteWorkerNextOneWayIdForTest(client, requestId) {
  validateRequestId(requestId);
  if (requestId >= 0 || requestId >= client.lowestNegativeRequestId) {
    throw new Error("The next one-way request ID must be fresh and negative.");
  }
  client.nextOneWayId = requestId;
}

export async function cleanupSqliteWorkerMigrationForTest(client, databaseName) {
  if (client.closed || client.terminalError) throw clientUnavailableError(client);
  if (typeof databaseName !== "string" || databaseName.trim() === "") {
    throw new TypeError("Migration cleanup database name must be non-empty.");
  }
  const id = client.nextMigrationCleanupId++;
  let resolveCleanup;
  let rejectCleanup;
  const cleanup = new Promise((resolve, reject) => {
    resolveCleanup = resolve;
    rejectCleanup = reject;
  });
  client.migrationCleanupWaiters.set(id, {
    resolve: resolveCleanup,
    reject: rejectCleanup,
  });
  try {
    postMessage(client.worker, {
      kind: "test-control",
      cleanupMigrationDatabaseName: databaseName,
      cleanupMigrationId: id,
    });
  } catch (error) {
    client.migrationCleanupWaiters.delete(id);
    throw error;
  }
  return withCleanupDeadline(
    cleanup,
    client.cleanupTimeoutMillis,
    "migration test cleanup",
  );
}

export function setSqliteWorkerMigrationInterruptionForTest(
  client,
  databaseName,
  stage,
) {
  if (client.closed || client.terminalError) throw clientUnavailableError(client);
  postMessage(client.worker, {
    kind: "test-control",
    migrationInterruptionDatabaseName: databaseName,
    migrationInterruptionStage: stage,
  });
}

function allocateTestControlId(client) {
  const id = client.nextTestControlId;
  if (!Number.isSafeInteger(id) || id <= 0) {
    throw new Error("SQLite worker test control IDs are exhausted.");
  }
  client.nextTestControlId = id + 1;
  return id;
}

export function holdSqliteWorkerMigrationCancellationForTest(
  client,
  databaseName,
  stage,
) {
  if (client.closed || client.terminalError) throw clientUnavailableError(client);
  if (
    typeof databaseName !== "string" ||
    databaseName.trim() === "" ||
    !["before-intent-write", "after-integrity", "after-health"].includes(stage)
  ) {
    throw new TypeError("Invalid migration cancellation hold test control.");
  }
  const id = allocateTestControlId(client);
  const control = Object.freeze({
    id,
    command: "migration",
    databaseName,
    stage,
  });
  client.cancellationHolds.set(id, control);
  try {
    postMessage(client.worker, {
      kind: "test-control",
      cancellationHold: control,
    });
  } catch (error) {
    client.cancellationHolds.delete(id);
    throw error;
  }
  return id;
}

export function holdSqliteWorkerNextCompleteOpenCancellationForTest(client) {
  if (client.closed || client.terminalError) throw clientUnavailableError(client);
  const id = allocateTestControlId(client);
  const control = Object.freeze({
    id,
    command: "completeOpen",
    stage: "before-dispatch",
  });
  client.cancellationHolds.set(id, control);
  try {
    postMessage(client.worker, {
      kind: "test-control",
      cancellationHold: control,
    });
  } catch (error) {
    client.cancellationHolds.delete(id);
    throw error;
  }
  return id;
}

export async function awaitSqliteWorkerCancellationHoldForTest(client, id) {
  if (client.closed || client.terminalError) throw clientUnavailableError(client);
  if (!positiveInt(id) || !client.cancellationHolds.has(id)) {
    throw new Error(`Unknown or reused cancellation hold test control ${String(id)}.`);
  }
  if (client.cancellationHoldWaiters.has(id)) {
    throw new Error(`Cancellation hold test control ${id} already has a waiter.`);
  }
  const observed = client.cancellationHoldsObserved.get(id);
  if (observed !== undefined) {
    client.cancellationHoldsObserved.delete(id);
    client.cancellationHolds.delete(id);
    return String(observed);
  }
  let resolveBarrier;
  let rejectBarrier;
  const barrier = new Promise((resolve, reject) => {
    resolveBarrier = resolve;
    rejectBarrier = reject;
  });
  client.cancellationHoldWaiters.set(id, {
    resolve: resolveBarrier,
    reject: rejectBarrier,
  });
  try {
    return await withCleanupDeadline(
      barrier,
      client.cleanupTimeoutMillis,
      "cancellation hold test barrier",
    );
  } catch (error) {
    client.cancellationHoldWaiters.delete(id);
    throw error;
  }
}

export async function sqliteWorkerPendingOpenCountForTest(client) {
  if (client.closed || client.terminalError) throw clientUnavailableError(client);
  const id = allocateTestControlId(client);
  let resolveCount;
  let rejectCount;
  const count = new Promise((resolve, reject) => {
    resolveCount = resolve;
    rejectCount = reject;
  });
  client.pendingOpenCountWaiters.set(id, {
    resolve: resolveCount,
    reject: rejectCount,
  });
  try {
    postMessage(client.worker, {
      kind: "test-control",
      pendingOpenCountId: id,
    });
  } catch (error) {
    client.pendingOpenCountWaiters.delete(id);
    throw error;
  }
  return withCleanupDeadline(
    count,
    client.cleanupTimeoutMillis,
    "pending open count test query",
  );
}

export function setSqliteWorkerMigrationHeapSamplesForTest(client, samplesJson) {
  if (client.closed || client.terminalError) throw clientUnavailableError(client);
  const samples = JSON.parse(samplesJson);
  if (
    !Array.isArray(samples) ||
    samples.length < 3 ||
    samples.length > 4 ||
    !samples.every((sample) => Number.isSafeInteger(sample) && sample >= 0)
  ) {
    throw new TypeError(
      "Migration heap test samples must contain three or four non-negative safe integers.",
    );
  }
  postMessage(client.worker, {
    kind: "test-control",
    migrationHeapSamples: samples,
  });
}

export function seedSqliteWorkerMigrationMarkerForTest(
  client,
  databaseName,
  mode,
) {
  if (client.closed || client.terminalError) throw clientUnavailableError(client);
  postMessage(client.worker, {
    kind: "test-control",
    migrationMarkerDatabaseName: databaseName,
    migrationMarkerMode: mode,
  });
}

export function sqliteWorkerGlobalDiagnosticsForTest() {
  return JSON.stringify(globalDiagnostics);
}
