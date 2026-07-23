import initSqlJs from "sql.js/dist/sql-wasm.js";

const sqlWasmUrl = new URL("./sql-wasm.wasm", import.meta.url).toString();

const databaseHandles = new Map();
const statementHandles = new Map();

let nextHandle = 1;
let sqlModule = null;

const HANDLE_DB_PREFIX = 1_000_000;
const SQLITE_DATA_INTEGER = 1;
const SQLITE_DATA_FLOAT = 2;
const SQLITE_DATA_TEXT = 3;
const SQLITE_DATA_BLOB = 4;
const SQLITE_DATA_NULL = 5;

function allocateHandle(map, value, offset = 0) {
  const handle = nextHandle++ + offset;
  map.set(handle, value);
  return handle;
}

function getHandle(map, handle) {
  const value = map.get(handle);
  if (!value) {
    throw new Error(`Handle ${handle} is not registered.`);
  }
  return value;
}

function releaseHandle(map, handle) {
  map.delete(handle);
}

export async function loadSqlJs(config) {
  if (sqlModule) {
    return sqlModule;
  }
  const finalConfig = { ...(config ?? {}) };
  const userLocateFile = finalConfig.locateFile;
  finalConfig.locateFile = (fileName) => {
    if (fileName === "sql-wasm.wasm") {
      return sqlWasmUrl;
    }
    return typeof userLocateFile === "function" ? userLocateFile(fileName) : fileName;
  };
  sqlModule = await initSqlJs(finalConfig);
  return sqlModule;
}

export function dbCreate() {
  if (!sqlModule) {
    throw new Error("SQL.js fixture module has not been loaded. Call loadSqlJs first.");
  }
  const db = new sqlModule.Database();
  return allocateHandle(databaseHandles, db, HANDLE_DB_PREFIX);
}

export function dbOpen(bytes) {
  if (!sqlModule) {
    throw new Error("SQL.js module has not been loaded. Call loadSqlJs first.");
  }
  const source = Array.isArray(bytes) ? new Uint8Array(bytes) : bytes;
  const db = new sqlModule.Database(source);
  const handle = allocateHandle(databaseHandles, db, HANDLE_DB_PREFIX);
  return handle;
}

export function dbClose(dbHandle) {
  const db = getHandle(databaseHandles, dbHandle);
  db.close?.();
  releaseHandle(databaseHandles, dbHandle);
}

export function dbExec(dbHandle, sql, onStatementExecuted) {
  const db = getHandle(databaseHandles, dbHandle);
  const iterator = db.iterateStatements(sql);
  let failure = null;

  try {
    while (true) {
      const next = iterator.next();
      if (next.done) {
        break;
      }

      const statement = next.value;
      const normalizedSql = statement.getNormalizedSQL();
      while (statement.step()) {
        // execSQL intentionally discards result rows.
      }
      onStatementExecuted(normalizedSql);
    }
  } catch (error) {
    failure = error;
  }

  failure = appendCleanupFailure(failure, () => {
    while (!iterator.next().done) {
      // Advancing frees the active statement and eventually releases the iterator SQL buffer.
    }
  });
  if (failure !== null) {
    throw failure;
  }
}

export function stmtPrepare(dbHandle, sql) {
  const db = getHandle(databaseHandles, dbHandle);
  const stmt = db.prepare(sql);
  return allocateHandle(statementHandles, stmt);
}

export function stmtBind(stmtHandle, params) {
  const stmt = getHandle(statementHandles, stmtHandle);
  const normalized = Array.from(params, value => {
    if (value == null) {
      return null;
    }

    if (Array.isArray(value)) {
      return new Uint8Array(value);
    }

    if (typeof value === "bigint") {
      return Number(value);
    }

    if (typeof value === "object") {
      if (value instanceof Uint8Array) {
        return value;
      }
      if (typeof value.valueOf === "function") {
        const primitive = value.valueOf();
        if (typeof primitive === "number" || typeof primitive === "string") {
          return primitive;
        }
        if (typeof primitive === "bigint") {
          return Number(primitive);
        }
      }
    }

    return value;
  });
  return stmt.bind(normalized);
}

export function stmtStep(stmtHandle) {
  const stmt = getHandle(statementHandles, stmtHandle);
  return stmt.step();
}

export function stmtGetRow(stmtHandle) {
  const stmt = getHandle(statementHandles, stmtHandle);
  const row = stmt.get(null, { useBigInt: true });
  if (!row) {
    return null;
  }
  return row.map(value => {
    if (Array.isArray(value)) {
      return value;
    }
    if (value instanceof Uint8Array) {
      return Array.from(value);
    }
    if (typeof value === 'bigint') {
      return value.toString();
    }
    return value;
  });
}

export function stmtGetNormalizedSql(stmtHandle) {
  const stmt = getHandle(statementHandles, stmtHandle);
  return stmt.getNormalizedSQL();
}

export function stmtReset(stmtHandle) {
  const stmt = getHandle(statementHandles, stmtHandle);
  stmt.reset();
}

export function stmtClearBindings(stmtHandle) {
  const stmt = getHandle(statementHandles, stmtHandle);
  stmt.reset();
  stmt.bind([]);
}

export function stmtGetColumnCount(stmtHandle) {
  const stmt = getHandle(statementHandles, stmtHandle);
  if (typeof stmt.getColumnCount === "function") {
    return stmt.getColumnCount();
  }
  if (typeof stmt.columnCount === "function") {
    return stmt.columnCount();
  }
  if (typeof stmt.getColumnNames === "function") {
    const names = stmt.getColumnNames();
    if (Array.isArray(names)) {
      return names.length;
    }
  }
  console.warn("[SqliteNow][sql.js] Unable to determine column count; falling back to 0");
  return 0;
}

export function stmtGetColumnName(stmtHandle, columnIndex) {
  const stmt = getHandle(statementHandles, stmtHandle);
  if (typeof stmt.getColumnName === "function") {
    return stmt.getColumnName(columnIndex);
  }
  if (typeof stmt.getColumnNames === "function") {
    const names = stmt.getColumnNames();
    if (Array.isArray(names) && columnIndex >= 0 && columnIndex < names.length) {
      return names[columnIndex];
    }
  }
  throw new Error("Unable to determine column name for index " + columnIndex);
}

export function stmtGetColumnType(stmtHandle, columnIndex) {
  const stmt = getHandle(statementHandles, stmtHandle);
  const row = stmt.get(null, { useBigInt: true });
  if (!row || columnIndex < 0 || columnIndex >= row.length) {
    const size = row?.length ?? 0;
    throw new Error(`Column ${columnIndex} out of bounds (size=${size})`);
  }
  const value = row[columnIndex];
  if (value == null) {
    return SQLITE_DATA_NULL;
  }
  if (value instanceof Uint8Array || Array.isArray(value)) {
    return SQLITE_DATA_BLOB;
  }
  if (typeof value === "bigint") {
    return SQLITE_DATA_INTEGER;
  }
  if (typeof value === "number") {
    return SQLITE_DATA_FLOAT;
  }
  if (typeof value === "string") {
    return SQLITE_DATA_TEXT;
  }
  throw new Error(`Column ${columnIndex} has unsupported SQL.js type ${typeof value}`);
}

export function stmtGetValue(stmtHandle, columnIndex) {
  const stmt = getHandle(statementHandles, stmtHandle);
  return stmt.get(columnIndex);
}

export function stmtFinalize(stmtHandle) {
  const stmt = getHandle(statementHandles, stmtHandle);
  try {
    stmt.free();
  } finally {
    releaseHandle(statementHandles, stmtHandle);
  }
}

export function dbExport(dbHandle) {
  const db = getHandle(databaseHandles, dbHandle);
  return Array.from(db.export());
}

function appendCleanupFailure(primary, cleanup) {
  try {
    cleanup();
    return primary;
  } catch (additional) {
    if (primary === null) {
      return additional;
    }
    if (primary !== additional && typeof primary === "object" && primary !== null) {
      const suppressed = Array.isArray(primary.sqlitenowSuppressed)
        ? primary.sqlitenowSuppressed
        : [];
      suppressed.push(additional);
      primary.sqlitenowSuppressed = suppressed;
    }
    return primary;
  }
}

export function takeSqliteNowSuppressed(error) {
  if (typeof error !== "object" || error === null || !Array.isArray(error.sqlitenowSuppressed)) {
    return [];
  }
  const suppressed = error.sqlitenowSuppressed;
  delete error.sqlitenowSuppressed;
  return suppressed;
}

export function throwSqliteNowFailure(error) {
  throw error;
}
