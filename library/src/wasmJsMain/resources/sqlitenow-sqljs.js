import initSqlJs from "sql.js/dist/sql-wasm.js";

const sqlWasmUrl = new URL("./sql-wasm.wasm", import.meta.url).toString();

const databaseHandles = new Map();
const statementHandles = new Map();

let nextHandle = 1;
let sqlModule = null;

const HANDLE_DB_PREFIX = 1_000_000;

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
    throw new Error("SQL.js module has not been loaded. Call loadSqlJs first.");
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

export function dbExec(dbHandle, sql) {
  const db = getHandle(databaseHandles, dbHandle);
  db.exec(sql);
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

export function stmtGetValue(stmtHandle, columnIndex) {
  const stmt = getHandle(statementHandles, stmtHandle);
  return stmt.get(columnIndex);
}

export function stmtFinalize(stmtHandle) {
  const stmt = getHandle(statementHandles, stmtHandle);
  stmt.free();
  releaseHandle(statementHandles, stmtHandle);
}

export function dbExport(dbHandle) {
  const db = getHandle(databaseHandles, dbHandle);
  return Array.from(db.export());
}
