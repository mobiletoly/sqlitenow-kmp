// Test-only authored generator. Never modifies main assets or vendor inputs.
import {readFile, writeFile, mkdir, cp, rm, readdir} from 'node:fs/promises';
import {resolve, dirname, join} from 'node:path';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
const root = resolve(dirname(fileURLToPath(import.meta.url)), '../../../..');
const out = resolve(process.argv[2] ?? join(root, 'library-core/build/generated/bound-write-test-fixtures'));
const source = join(root, 'library-core/src/webMain/resources/sqlitenow-worker-v1');
const generated = join(root, 'library-core/build/generated/sqlite-worker/resources/sqlitenow-worker-v1');
const runtime = 'sqlite-3.53.0-build1';
const digest = text => createHash('sha256').update(text).digest('hex');
const bindings = [];
function replaceOnce(text, before, after, owner) {
  const count = text.split(before).length - 1;
  if (count !== 1) throw new Error(`${owner}: expected one anchor, got ${count}`);
  bindings.push({owner, anchorSHA256: digest(before), count});
  return text.replace(before, after);
}
// Refuse output paths outside the dedicated generated test tree.
if (out !== join(root, 'library-core/build/generated/bound-write-test-fixtures')) throw new Error('Unexpected fixture output root');
await rm(out, {recursive:true, force:true});
await mkdir(out, {recursive:true});
let client = await readFile(join(source,'client.mjs'),'utf8');
const originalClient = client;
client = replaceOnce(client, '      client.handleMessage = (message) => {\n        try {', `      client.fixtureEvents = [];
      client.handleMessage = (message) => {
        try {
          if (message?.kind === "bound-write-fixture-evidence") {
            client.fixtureEvents.push(message.event);
            return;
          }`, 'client evidence sideband');
await writeFile(join(out,'client.mjs'),client);
await cp(join(root,'library-core/src/webTest/fixtures/bound-write-runner.mjs'),join(out,'bound-write-runner.mjs'));
const inputs = {'client.mjs':digest(originalClient)};
const scenarios = ['normal','finalize-before','finalize-after','rollback-denied','active'];
for (const scenario of scenarios) {
  const target = join(out,scenario,runtime);await mkdir(target,{recursive:true});
  await cp(join(generated,runtime,'vendor'),join(target,'vendor'),{recursive:true});
  let worker = await readFile(join(source,runtime,'worker.mjs'),'utf8');
  inputs['worker.mjs'] = digest(worker);
  const prelude = `
// BOUND_WRITE_TEST_FIXTURE_ONLY: generated, unpublished evidence seam.
const fixtureScenario = ${JSON.stringify(scenario)};
const fixturePointers = new Set();
const fixtureStatementIds = new Set();
let fixtureArmed = false;
let fixtureFaultFired = false;
function fixtureEmit(event) { post({kind:"bound-write-fixture-evidence", event}); }
globalThis.__boundWriteNativeFinalize = (pointer, call) => {
  if (!fixturePointers.has(pointer)) return call();
  fixtureEmit({phase:"nativeFinalizeEnter", pointer});
  const result = call();
  fixtureEmit({phase:"nativeFinalizeReturn", pointer, result});
  fixturePointers.delete(pointer);
  return result;
};
`;
  worker = replaceOnce(worker,'const databases = new Map();',prelude+'\nconst databases = new Map();','worker fixture owner');
  worker = replaceOnce(worker,'    statement.stmt.finalize();',`    if (fixturePointers.has(statement.stmt.pointer)) {
      fixtureEmit({phase:"oo1FinalizeEnter", statementId});
      if (fixtureScenario === "finalize-before" && !fixtureFaultFired) {
        fixtureFaultFired=true; throw new Error("fixture thrown before native finalize");
      }
      statement.stmt.finalize();
      if (fixtureScenario === "finalize-after" && !fixtureFaultFired) {
        fixtureFaultFired=true; throw new Error("fixture thrown after native finalize");
      }
    } else statement.stmt.finalize();`,'actual finalize call');
  worker = replaceOnce(worker,'    statements.delete(statementId);\n    metrics.liveStatements = statements.size;',`    statements.delete(statementId);
    metrics.liveStatements = statements.size;
    if (fixtureStatementIds.delete(statementId)) fixtureEmit({phase:"logicalRemoval", statementId, liveStatements:statements.size});`,'logical removal');
  worker = replaceOnce(worker,'  database.db.exec("ROLLBACK");\n  return true;',`  if (!fixtureArmed) { database.db.exec("ROLLBACK"); return true; }
  fixtureEmit({phase:"rollbackAttempt", active:inTransaction(database)});
  if (fixtureScenario === "rollback-denied") {
    if (typeof capi.sqlite3_set_authorizer !== "function") throw new Error("authorizer unavailable");
    const rc = capi.sqlite3_set_authorizer(database.db.pointer, (_p, code, operation) => {
      if (code === capi.SQLITE_TRANSACTION && operation === "ROLLBACK") {
        fixtureEmit({phase:"authorizerDeniedRollback"}); return capi.SQLITE_DENY;
      }
      return capi.SQLITE_OK;
    }, 0);
    if (rc !== capi.SQLITE_OK) throw new Error("authorizer installation failed");
  }
  try {
    database.db.exec("ROLLBACK");
    fixtureEmit({phase:"rollbackSuccess", active:inTransaction(database)});
    return true;
  } catch(error) {
    fixtureEmit({phase:"rollbackThrown", resultCode:error.resultCode, message:String(error.message)});
    throw error;
  } finally {
    if (fixtureScenario === "rollback-denied") capi.sqlite3_set_authorizer(database.db.pointer, 0, 0);
  }`,'actual rollback call');
  worker = replaceOnce(worker,'  bindStatement(statement, request.bindings);\n  metrics.pageRequests++;',`  if (request.sql.includes("bound-write-fixture")) {
    fixtureArmed=true;fixturePointers.add(statement.stmt.pointer);fixtureStatementIds.add(request.statementId);
    fixtureEmit({phase:"bindEnter", statementId:request.statementId});
  }
  bindStatement(statement, request.bindings);
  metrics.pageRequests++;`,'binding phase');
  worker = replaceOnce(worker,'    () => (isCancelled(cancelView) ? 1 : 0),',`    () => {
      if (fixtureScenario === "active" && fixturePointers.has(statement.stmt.pointer) && !fixtureFaultFired) {
        fixtureFaultFired=true;fixtureEmit({phase:"vmProgressEnter", statementId:request.statementId});
        const deadline=Date.now()+5000;
        while (!isCancelled(cancelView) && Date.now()<deadline) Atomics.wait(cancelView,0,0,10);
        if (!isCancelled(cancelView)) throw new Error("fixture active cancellation not delivered");
      }
      const interrupted=isCancelled(cancelView);
      if (fixtureScenario === "active" && interrupted) fixtureEmit({phase:"vmInterruptReturn"});
      return interrupted ? 1 : 0;
    },`,'active VM callback');
  worker = replaceOnce(worker,'  } catch (error) {\n    const cancelled = Boolean(error?.workerCancelled || isCancelled(cancelView));\n    try {\n      finalizeOwnedStatement(request.statementId, request.databaseId, true);',`  } catch (error) {
    if (fixtureArmed) fixtureEmit({phase:"stepThrown", resultCode:error.resultCode, message:String(error.message), active:inTransaction(database)});
    const cancelled = Boolean(error?.workerCancelled || isCancelled(cancelView));
    try {
      finalizeOwnedStatement(request.statementId, request.databaseId, true);`,'step failure evidence');
  await writeFile(join(target,'worker.mjs'),worker);
  for (const file of ['index.mjs','node.mjs']) {
    const path=join(target,'vendor',file);let vendor=await readFile(path,'utf8');inputs[file]=digest(vendor);
    vendor=replaceOnce(vendor,'capi.sqlite3_finalize(ptr);','(globalThis.__boundWriteNativeFinalize ? globalThis.__boundWriteNativeFinalize(ptr, () => capi.sqlite3_finalize(ptr)) : capi.sqlite3_finalize(ptr));',`${scenario}/${file} native call`);
    await writeFile(path,vendor);
  }
}
async function outputHashes(dir, prefix='') {
  const hashes={};
  for(const item of await readdir(dir,{withFileTypes:true})) {
    const relative=prefix+item.name;
    if(item.isDirectory()) Object.assign(hashes,await outputHashes(join(dir,item.name),relative+'/'));
    else hashes[relative]=digest(await readFile(join(dir,item.name)));
  }
  return hashes;
}
const outputs=await outputHashes(out);
await writeFile(join(out,'fixture-manifest.json'),JSON.stringify({marker:'BOUND_WRITE_TEST_FIXTURE_ONLY',inputs,scenarios,anchors:bindings,outputs},null,2)+'\n');
console.log(JSON.stringify({output:out, scenarios, anchors:bindings.length, inputs}));
