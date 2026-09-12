// W1_TEST_FIXTURE_ONLY. Authored generator; never writes shipped resources.
import {readFile, writeFile, mkdir, cp, rm, readdir} from 'node:fs/promises';
import {resolve, dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../../../..');
const out = join(root, 'library-oversqlite/build/generated/w1-test-fixtures');
const source = join(root, 'library-core/src/webMain/resources/sqlitenow-worker-v1');
const generated = join(root, 'library-core/build/generated/sqlite-worker/resources/sqlitenow-worker-v1');
const runtime = 'sqlite-3.53.0-build1';
// Exact test-owned SQL pins; capture-size is checked against ReplayTableSql by the test.
const replayStatementTargets = Object.freeze({
  "plan-save": "INSERT INTO _sync_apply_plan(singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,row_cursor,entries,bytes,edge_entries,edge_bytes,operation_kind,preserve_schema,preserve_table,preserve_key,atomic_work,preservation_complete)\n        VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)\n        ON CONFLICT(singleton_key) DO UPDATE SET unit=excluded.unit,guard=excluded.guard,\n        phase=excluded.phase,root_cursor=excluded.root_cursor,visit_cursor=excluded.visit_cursor,\n        table_cursor=excluded.table_cursor,fk_cursor=excluded.fk_cursor,row_cursor=excluded.row_cursor,\n        entries=excluded.entries,bytes=excluded.bytes,edge_entries=excluded.edge_entries,edge_bytes=excluded.edge_bytes,\n        operation_kind=excluded.operation_kind,preserve_schema=excluded.preserve_schema,\n        preserve_table=excluded.preserve_table,preserve_key=excluded.preserve_key,\n        atomic_work=excluded.atomic_work,preservation_complete=excluded.preservation_complete",
  "capture-size": "SELECT 2+92+CASE typeof(\"live_row\".\"id\") WHEN 'blob' THEN 2*length(\"live_row\".\"id\")+2 WHEN 'text' THEN 6*length(CAST(\"live_row\".\"id\" AS BLOB))+2 ELSE 32 END+104+CASE typeof(\"live_row\".\"name\") WHEN 'blob' THEN 2*length(\"live_row\".\"name\")+2 WHEN 'text' THEN 6*length(CAST(\"live_row\".\"name\" AS BLOB))+2 ELSE 32 END+134+CASE typeof(\"live_row\".\"parent_id\") WHEN 'blob' THEN 2*length(\"live_row\".\"parent_id\")+2 WHEN 'text' THEN 6*length(CAST(\"live_row\".\"parent_id\" AS BLOB))+2 ELSE 32 END FROM \"nodes\" live_row WHERE \"id\"=?"
});
const digest = raw => createHash('sha256').update(raw).digest('hex');
const anchors = [];
const inputs = {};
function replaceOnce(text, before, after, owner) {
  const count = text.split(before).length - 1;
  if (count !== 1) throw new Error(`${owner}: expected one anchor, got ${count}`);
  anchors.push({owner, anchorSHA256: digest(before), count});
  return text.replace(before, after);
}
async function input(path, name) {
  const raw = await readFile(path, 'utf8'); inputs[name] = digest(raw); return raw;
}
await rm(out, {recursive: true, force: true});
await mkdir(join(out, 'sqlitenow-worker-v1'), {recursive: true});
let client = await input(join(source, 'client.mjs'), 'client.mjs');
client = `// W1_TEST_FIXTURE_ONLY: private test resource, never a publication input.
function w1Event(client, event) {
  const fixture = globalThis.__sqlitenowW1Fixture;
  if (fixture?.client === client) fixture.events.push(event);
}
` + client;
client = replaceOnce(client, 'function postMessage(worker, message, transfer = undefined) {',
  `function postMessage(worker, message, transfer = undefined) {
  const fixture = globalThis.__sqlitenowW1Fixture;
  if (fixture?.client?.worker === worker) {
    const request = message.request;
    fixture.events.push({phase:"dispatch", kind:message.kind ?? "request", id:message.id,
      command:request?.command, sql:request?.sql, statementId:request?.statementId});
  }`, 'client dispatch telemetry');
client = replaceOnce(client, '      client.handleMessage = (message) => {\n        try {',
  `      if (String(workerModuleUrl).includes("w1-test-fixtures/")) {
        globalThis.__sqlitenowW1Fixture = {client, events:[],
          snapshot:() => JSON.parse(sqliteWorkerClientDiagnostics(client)),
          configuration:globalThis.__w1NextConfiguration ?? {}};
      }
      client.handleMessage = (message) => {
        try {
          if (message?.kind === "w1-fixture-evidence") {
            w1Event(client, message.event); return;
          }
          const fixture = globalThis.__sqlitenowW1Fixture;
          if (fixture?.client === client && fixture.configuration.holdCommand) {
            const selected = fixture.events.find(event => event.phase === "selected");
            const sent = fixture.events.find(event => event.phase === "dispatch" && event.id === message?.id);
            if (sent?.statementId === selected?.statementId && sent?.command === fixture.configuration.holdCommand) {
              fixture.held = message;
              w1Event(client, {phase:"selectedResponseHeld", id:message.id, command:sent.command}); return;
            }
          }
          w1Event(client, {phase:"response", kind:message?.kind, id:message?.id,
            error:message?.error?.message});`, 'client response telemetry');
client = replaceOnce(client, '  owner.terminationConfirmed = true;',
  '  owner.terminationConfirmed = true;\n  w1Event(owner, {phase:"terminationConfirmed"});', 'termination telemetry');
client = replaceOnce(client, 'export async function flushSqliteWorkerOneWays(client) {',
  'export async function flushSqliteWorkerOneWays(client) {\n  w1Event(client, {phase:"flushEnter", ids:client.oneWays.map(entry => entry.requestId)});', 'flush entry');
client = replaceOnce(client, '  return JSON.stringify({ envelopes, barrierFailures });',
  `  w1Event(client, {phase:"flushReturn", envelopeIds:envelopes.map(entry => entry.id),
    errors:envelopes.filter(entry => entry.error).map(entry => entry.error.message), barrierFailures});
  return JSON.stringify({ envelopes, barrierFailures });`, 'flush acknowledgement');
await writeFile(join(out, 'sqlitenow-worker-v1/client.mjs'), client);

await cp(join(root, 'library-oversqlite/src/webTest/fixtures/w1-control.mjs'), join(out, 'w1-test-fixtures/control.mjs'));
const originalWorker = await input(join(source, runtime, 'worker.mjs'), 'worker.mjs');
for (const scenario of ['normal', 'reset', 'clear', 'close-before', 'close-after', 'reset-terminal', 'reset-rollback-denied']) {
const target = join(out, 'w1-test-fixtures', scenario, runtime);
await mkdir(target, {recursive: true});
await cp(join(generated, runtime, 'vendor'), join(target, 'vendor'), {recursive: true});
let worker = originalWorker;
worker = replaceOnce(worker, 'const databases = new Map();', `
// W1_TEST_FIXTURE_ONLY: selected handle, real native ownership, no product hook.
const w1Scenario = ${JSON.stringify(scenario)};
let w1SelectedId = null;
let w1SelectedPointer = null;
let w1SelectedDisposed = false;
let w1FaultFired = false;
let w1SettlementArmed = false;
let w1TargetFamily = "workspace";
const w1ReplayStatementTargets = ${JSON.stringify(replayStatementTargets)};
function w1Emit(event) { post({kind:"w1-fixture-evidence", event}); }
function w1NativeAlive(database) {
  if (typeof capi.sqlite3_next_stmt !== "function") throw new Error("native statement oracle unavailable");
  // sqlite3 can reuse a pointer for a later allocation. Follow the selected allocation's lifetime.
  if (w1SelectedDisposed) return false;
  for (let pointer = capi.sqlite3_next_stmt(database.db.pointer, 0); pointer;
       pointer = capi.sqlite3_next_stmt(database.db.pointer, pointer)) {
    if (pointer === w1SelectedPointer) return true;
  }
  return false;
}
function w1FailCleanup(operation, request, statement, database) {
  const matches = operation === "reset" ? w1Scenario.startsWith("reset") : w1Scenario === "clear";
  if (matches && request.statementId === w1SelectedId && !w1FaultFired) {
    w1FaultFired = true;
    w1Emit({phase:operation + "BeforeFault", statementId:w1SelectedId,
      selectedNativeAlive:w1NativeAlive(database), selectedLogicalAlive:statements.has(w1SelectedId)});
    throw new Error("W1 selected workspace " + operation + " before native call");
  }
}
globalThis.__w1NativeFinalize = (pointer, call) => {
  if (pointer !== w1SelectedPointer || w1SelectedDisposed) return call();
  w1Emit({phase:"nativeFinalizeEnter", pointer});
  const result = call();
  w1Emit({phase:"nativeFinalizeReturn", pointer, result});
  if (result !== capi.SQLITE_OK) throw new Error("W1 selected native finalization did not succeed");
  w1SelectedDisposed = true;
  return result;
};
const databases = new Map();`, 'selected native ownership oracle');
worker = replaceOnce(worker, '      const columnCount = capi.sqlite3_column_count(stmt.pointer);',
  `      const workspaceTarget = w1TargetFamily === "workspace" &&
        (request.sql.includes("/* w1-workspace */") || (w1SettlementArmed &&
          request.sql.includes("FROM _sync_operation_state o") && request.sql.includes("NULL,0,0,0,0,0") &&
          request.sql.includes("LEFT JOIN _sync_push_plan")));
      const replayTarget = w1SettlementArmed && w1TargetFamily !== "workspace" &&
        request.sql === w1ReplayStatementTargets[w1TargetFamily];
      if (w1SelectedId === null && (workspaceTarget || replayTarget)) {
        w1SelectedId = statementId; w1SelectedPointer = stmt.pointer;
        w1Emit({phase:"selected", statementId, pointer:stmt.pointer, sql:request.sql, targetFamily:w1TargetFamily});
      }
      const columnCount = capi.sqlite3_column_count(stmt.pointer);`, 'closed-family SQL selection');
worker = replaceOnce(worker, '    statement.stmt.finalize();', `    if (w1Scenario === "close-before" && statementId === w1SelectedId && !w1FaultFired) {
      w1FaultFired = true;
      w1Emit({phase:"finalizeBeforeFault", statementId, pointer:statement.stmt.pointer,
        selectedNativeAlive:w1NativeAlive(databases.get(statement.databaseId))});
      throw new Error("W1 selected workspace finalize before native call");
    }
    statement.stmt.finalize();
    if (w1Scenario === "close-after" && statementId === w1SelectedId && !w1FaultFired) {
      w1FaultFired = true;
      w1Emit({phase:"finalizeAfterFault", statementId,
        selectedNativeAlive:w1NativeAlive(databases.get(statement.databaseId))});
      throw new Error("W1 selected workspace finalize after native call");
    }`, 'actual selected finalize fault');
worker = replaceOnce(worker, '    case "reset": {\n      const database = requireDatabase(request.databaseId);\n      const statement = requireStatement(request.statementId, request.databaseId);',
  `    case "reset": {
      const database = requireDatabase(request.databaseId);
      const statement = requireStatement(request.statementId, request.databaseId);
      w1FailCleanup("reset", request, statement, database);`, 'actual selected reset fault');
worker = replaceOnce(worker, '      statement.stmt.clearBindings();',
  '      w1FailCleanup("clear", request, statement, database);\n      statement.stmt.clearBindings();', 'actual selected clear fault');
worker = replaceOnce(worker, '      const stmt = database.db.prepare(request.sql);', `      let stmt;
      const deny = w1Scenario === "reset-rollback-denied" && w1FaultFired && request.sql.trim() === "ROLLBACK";
      if (deny) {
        if (typeof capi.sqlite3_set_authorizer !== "function") throw new Error("authorizer unavailable");
        const rc = capi.sqlite3_set_authorizer(database.db.pointer, (_p, code, operation) => {
          if (code === capi.SQLITE_TRANSACTION && operation === "ROLLBACK") {
            w1Emit({phase:"authorizerDeniedRollback", selectedNativeAlive:w1NativeAlive(database)});
            return capi.SQLITE_DENY;
          }
          return capi.SQLITE_OK;
        }, 0);
        if (rc !== capi.SQLITE_OK) throw new Error("authorizer installation failed");
      }
      try { stmt = database.db.prepare(request.sql); }
      finally { if (deny) capi.sqlite3_set_authorizer(database.db.pointer, 0, 0); }`, 'real rollback authorizer denial');
worker = replaceOnce(worker, '    statements.delete(statementId);\n    metrics.liveStatements = statements.size;',
  `    statements.delete(statementId);
    metrics.liveStatements = statements.size;
    if (statementId === w1SelectedId) w1Emit({phase:"logicalRemoval", statementId});`, 'logical removal witness');
worker = replaceOnce(worker, '  metrics.transferredRows += rows.length;',
  `  if (w1SelectedId !== null && request.sql.trim() === "ROLLBACK") {
    w1Emit({phase:"rollbackResult", inTransaction:inTransaction(database),
      selectedNativeAlive:w1NativeAlive(database), selectedLogicalAlive:statements.has(w1SelectedId)});
  }
  metrics.transferredRows += rows.length;`, 'actual rollback result and native handle oracle');
worker = replaceOnce(worker, '    database.db.close();',
  `    if (w1SelectedId !== null) w1Emit({phase:"databaseCloseEnter", selectedNativeAlive:w1NativeAlive(database)});
    database.db.close();
    if (w1SelectedId !== null) w1Emit({phase:"databaseCloseReturn"});`, 'owned database close');
worker = replaceOnce(worker, 'listen((message) => {\n  if (message?.kind === "test-control") {',
  `listen((message) => {
  if (message?.kind === "w1-fixture-control") {
    const targetFamily = message.targetFamily === undefined ? "workspace" : message.targetFamily;
    if (!["workspace", "plan-save", "capture-size"].includes(targetFamily)) {
      w1SettlementArmed = false;
      w1Emit({phase:"targetFamilyRejected"}); return;
    }
    w1TargetFamily = targetFamily;
    w1SettlementArmed = true;
    w1Emit({phase:"settlementArmed", targetFamily}); return;
  }
  if (message?.kind === "test-control") {`, 'private activated settlement arming');
await writeFile(join(target, 'worker.mjs'), worker);
for (const name of ['index.mjs', 'node.mjs']) {
  const path = join(target, 'vendor', name);
  let vendor = await input(path, name);
  vendor = replaceOnce(vendor, 'capi.sqlite3_finalize(ptr);',
    '(globalThis.__w1NativeFinalize ? globalThis.__w1NativeFinalize(ptr, () => capi.sqlite3_finalize(ptr)) : capi.sqlite3_finalize(ptr));', `${name} native finalize witness`);
  await writeFile(path, vendor);
}
}
async function hashes(dir, prefix = '') {
  const result = {};
  for (const item of await readdir(dir, {withFileTypes: true})) {
    const path = prefix + item.name;
    if (item.isDirectory()) Object.assign(result, await hashes(join(dir, item.name), path + '/'));
    else result[path] = digest(await readFile(join(dir, item.name)));
  }
  return result;
}
await writeFile(join(out, 'fixture-manifest.json'), JSON.stringify({marker:'W1_TEST_FIXTURE_ONLY', inputs, anchors, outputs:await hashes(out)}, null, 2) + '\n');
console.log(JSON.stringify({output:out, marker:'W1_TEST_FIXTURE_ONLY', anchors:anchors.length, inputs}));
