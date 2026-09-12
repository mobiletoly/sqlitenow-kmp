// BOUND_WRITE_TEST_FIXTURE_ONLY: raw-worker prerequisite proof, not Safe owner proof.
import * as api from './client.mjs';
const protocol='sqlitenow-sqlite-worker-v1';
const runtime='sqlite-3.53.0-build1';
function check(value, message) {if (!value) throw new Error(message);}
async function until(test, message) {
  const end=Date.now()+5000;
  while (!test()) {if(Date.now()>end) throw new Error(message);await new Promise(r=>setTimeout(r,1));}
}
export async function runBoundWriteFixture(scenario) {
  check(['normal','finalize-before','finalize-after','rollback-denied','active'].includes(scenario),'unknown scenario');
  const node=Boolean(globalThis.process?.versions?.node);
  const url=node ? new URL(`./${scenario}/${runtime}/worker.mjs`,import.meta.url).href
    : new URL(`/bound-write-test-fixtures/${scenario}/${runtime}/worker.mjs`,globalThis.location.href).href;
  const databaseName=`bound-write-fixture-${scenario}-${Date.now()}-${Math.random()}`;
  const globalBefore=JSON.parse(api.sqliteWorkerGlobalDiagnosticsForTest());
  const client=await api.createSqliteWorkerClient('{}',url,'normal',5000);
  let id=1;let databaseId;let outcome;let cleanup;
  const request=(command,fields={})=>({protocol,command,...fields});
  async function raw(command,fields={}) {
    const requestId=id++;const envelope=JSON.parse(await api.sqliteWorkerRequest(client,requestId,JSON.stringify(request(command,fields))));
    await api.acknowledgeSqliteWorkerRequest(client,requestId);api.releaseSqliteWorkerRequest(client,requestId);
    return envelope;
  }
  async function success(command,fields={}) {const e=await raw(command,fields);check(!e.error,JSON.stringify(e.error));return e.data;}
  async function execute(sql) {
    const prepared=await success('prepare',{databaseId,sql});
    const e=await raw('page',{databaseId,statementId:prepared.statementId,sql,bindings:{},pageRows:64,pageBytes:65536});
    check(!e.error,JSON.stringify(e.error));await success('closeStatement',{databaseId,statementId:prepared.statementId,sql});
    return e.data;
  }
  try {
    databaseId=(await success('open',{fileName:databaseName,legacySourceMode:'built-in'})).databaseId;
    await execute('CREATE TABLE fixture_values(value INTEGER)');
    await execute('BEGIN');await execute('INSERT INTO fixture_values VALUES (1)');
    const sql=scenario==='active'
      ? '/* bound-write-fixture */ WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000000) INSERT INTO fixture_values SELECT x FROM n'
      : '/* bound-write-fixture */ INSERT INTO fixture_values VALUES (7)';
    const statementId=(await success('prepare',{databaseId,sql})).statementId;
    if (scenario==='active' || scenario==='rollback-denied') {
      const requestId=id++;
      const pending=api.sqliteWorkerRequest(client,requestId,JSON.stringify(request('page',{databaseId,statementId,sql,bindings:{},pageRows:64,pageBytes:65536})));
      if(scenario==='active') await until(()=>client.fixtureEvents.some(e=>e.phase==='vmProgressEnter'),'no active VM callback');
      else {const completed=JSON.parse(await pending);check(completed.data?.done===true,'write did not complete');check(completed.data.inTransaction===true,'no transaction before late cancel');}
      const reconciled=JSON.parse(await api.cancelSqliteWorkerRequest(client,requestId));
      const original=JSON.parse(await pending);
      check(reconciled.error?.cancelled===true,'missing authoritative cancellation');
      const metrics=(await success('metrics')).metrics;
      if(scenario==='active') {
        check(reconciled.error.inTransaction===false,'active interrupt did not roll back');
        check(client.fixtureEvents.some(e=>e.phase==='stepThrown' && e.resultCode===9),'no actual SQLITE_INTERRUPT');
        check(client.fixtureEvents.some(e=>e.phase==='vmInterruptReturn'),'no interruption callback return');
        const read=await execute('SELECT COUNT(*) FROM fixture_values');check(read.rows[0][0].integer==='0','cancelled transaction rows survived');
      } else {
        check(reconciled.error.inTransaction===true,'denied rollback did not preserve active transaction');
        check(client.fixtureEvents.filter(e=>e.phase==='rollbackAttempt').length===1,'wrong late-cancel rollback attempt count');
        check(client.fixtureEvents.filter(e=>e.phase==='rollbackSuccess').length===0,'denied rollback reported success');
        check(client.fixtureEvents.some(e=>e.phase==='rollbackThrown' && e.resultCode===23),'no SQLite authorization failure');
        check(reconciled.error.suppressed?.some(s=>String(s).includes('not authorized')),'rollback failure missing from envelope');
      }
      check(metrics.liveStatements===0,'logical statement leaked');
      outcome={original,reconciled,metrics,events:[...client.fixtureEvents]};
    } else {
      await success('page',{databaseId,statementId,sql,bindings:{},pageRows:64,pageBytes:65536});
      const closed=await raw('closeStatement',{databaseId,statementId,sql});
      const metrics=(await success('metrics')).metrics;
      check(metrics.liveStatements===0,'logical removal missing');
      const native=client.fixtureEvents.filter(e=>e.phase==='nativeFinalizeEnter').length;
      check(native===(scenario==='finalize-before'?0:1),'wrong native finalize call count');
      check(client.fixtureEvents.filter(e=>e.phase==='nativeFinalizeReturn').length===native,'native finalize did not return');
      check(client.fixtureEvents.filter(e=>e.phase==='oo1FinalizeEnter').length===1,'wrong OO1 finalize invocation count');
      if(scenario==='normal') check(!closed.error,'normal finalize failed');
      else check(closed.error?.message?.includes('fixture thrown'),'actual finalize exception missing');
      outcome={closed,metrics,events:[...client.fixtureEvents]};
    }
  } finally {
    await api.forceTerminateSqliteWorker(client);
    check(api.isSqliteWorkerTerminationConfirmed(client),'termination not confirmed');
    const globals=JSON.parse(api.sqliteWorkerGlobalDiagnosticsForTest());
    check(globals.activeWorkers===globalBefore.activeWorkers,'worker leak');
    const diagnostics=JSON.parse(api.sqliteWorkerClientDiagnostics(client));
    for(const field of ['pendingRequests','completedResponses','reconciliationRequests','acknowledgementRequests','releaseRequests','queuedOneWays']) check(diagnostics[field]===0,`cleanup retained ${field}`);
    check(diagnostics.terminationAttempts===1,'wrong termination attempt count');
    cleanup={terminationConfirmed:true,diagnostics,globals};
    if(!node) {
      const clean=await api.createSqliteWorkerClient('{}',url,'normal',5000);
      try {
        let cleanId=1;
        async function cleanRequest(command,fields={}) {
          const i=cleanId++;const e=JSON.parse(await api.sqliteWorkerRequest(clean,i,JSON.stringify(request(command,fields))));
          await api.acknowledgeSqliteWorkerRequest(clean,i);api.releaseSqliteWorkerRequest(clean,i);
          check(!e.error,JSON.stringify(e.error));return e.data;
        }
        const reopened=(await cleanRequest('open',{fileName:databaseName,legacySourceMode:'built-in'})).databaseId;
        const sql='SELECT COUNT(*) FROM fixture_values';
        const statementId=(await cleanRequest('prepare',{databaseId:reopened,sql})).statementId;
        const read=await cleanRequest('page',{databaseId:reopened,statementId,sql,bindings:{},pageRows:64,pageBytes:65536});
        check(read.rows[0][0].integer==='0','uncommitted rows survived worker termination/reopen');
        cleanup.durableReopenRows=read.rows[0][0].integer;
        await cleanRequest('closeStatement',{databaseId:reopened,statementId,sql});
        await cleanRequest('closeDatabase',{databaseId:reopened});
        await api.cleanupSqliteWorkerMigrationForTest(clean,databaseName);
      } finally {await api.forceTerminateSqliteWorker(clean);}
      const final=JSON.parse(api.sqliteWorkerGlobalDiagnosticsForTest());
      check(final.activeWorkers===globalBefore.activeWorkers,'cleanup worker leak');
      cleanup.finalGlobals=final;
    }
  }
  return JSON.stringify({scenario,runtime:node?'js-node-worker':'browser-worker',outcome,cleanup});
}
