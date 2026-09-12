// W1_TEST_FIXTURE_ONLY: telemetry for the generated private test client.
import {setSqliteWorkerResponseModeForTest} from '../sqlitenow-worker-v1/client.mjs';
export function workerModuleUrl(scenario) {
  if (!['normal','reset','clear','close-before','close-after','reset-terminal','reset-rollback-denied'].includes(scenario)) {
    throw new Error('Unknown W1 worker scenario');
  }
  const path = 'w1-test-fixtures/' + scenario + '/sqlite-3.53.0-build1/worker.mjs';
  return globalThis.process?.versions?.node
    ? new globalThis.URL('./' + scenario + '/sqlite-3.53.0-build1/worker.mjs', import.meta.url).href
    : new globalThis.URL('/' + path, globalThis.location.href).href;
}
function fixture() {
  const value = globalThis.__sqlitenowW1Fixture;
  if (!value?.client || !value.snapshot) throw new Error('W1 fixture client is unavailable');
  return value;
}
export function configure(configuration) {
  const value = JSON.parse(configuration);
  const targetFamily = value.targetFamily === undefined ? 'workspace' : value.targetFamily;
  if (!['workspace', 'plan-save', 'capture-size'].includes(targetFamily)) {
    throw new Error('Unknown replay statement target family');
  }
  globalThis.__w1NextConfiguration = {...value, targetFamily};
}
export function mark(phase) {
  const value = fixture(); if (phase === 'armCommittedSettlement') value.events.length = 0; value.events.push({phase});
  if (phase === 'armCommittedSettlement') {
    value.client.worker.postMessage({kind:'w1-fixture-control', targetFamily:value.configuration.targetFamily ?? 'workspace'});
  }
  if (phase === 'armSelectedCleanup' && value.configuration.dropClear) {
    setSqliteWorkerResponseModeForTest(value.client, JSON.stringify({mode:'drop', command:'clearBindings'}));
  }
}
export async function awaitHeld() {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    const value = fixture();
    if (value.held && value.events.some(event => event.phase === 'flushEnter' && event.ids.includes(value.held.id))) return;
    await new Promise(resolve => setTimeout(resolve, 1));
  }
  throw new Error('W1 selected response did not reach its flush barrier');
}
export function releaseHeld() {
  const value = fixture(); const held = value.held;
  if (!held) throw new Error('No W1 selected response is held');
  value.configuration.holdCommand = null; value.held = null;
  value.events.push({phase:'selectedResponseReleased', id:held.id});
  value.client.handleMessage(held);
}
export function snapshot() {
  const value = fixture();
  return JSON.stringify({events: value.events, diagnostics: value.snapshot(), runtime: value.client.runtimeKind});
}
