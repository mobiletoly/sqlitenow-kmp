const browserTestTimeoutMillis = 180000;
const baselineLiveSettings = {};
for (const key of ['OVERSQLITE_REALSERVER_TESTS', 'OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL']) {
  if (process.env[key] != null) baselineLiveSettings[key] = process.env[key];
}
const liveOrigin = baselineLiveSettings.OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL
  ? new URL(baselineLiveSettings.OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL).origin
  : '';

config.set({
  client: {
    baselineLiveSettings,
    mocha: {
      timeout: browserTestTimeoutMillis,
    },
  },
  customHeaders: [
    {
      match: '.*',
      name: 'Cross-Origin-Opener-Policy',
      value: 'same-origin',
    },
    {
      match: '.*',
      name: 'Cross-Origin-Embedder-Policy',
      value: 'require-corp',
    },
    {
      match: '.*',
      name: 'Content-Security-Policy',
      value:
        "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval' 'wasm-unsafe-eval'; worker-src 'self'; connect-src 'self' ws: " + liveOrigin,
    },
  ],
  browserNoActivityTimeout: browserTestTimeoutMillis,
  captureTimeout: browserTestTimeoutMillis,
});

const path = require('path');
const resourceRoot = path.resolve(__dirname, 'kotlin');
const workerResources = [
  'sqlitenow-worker-v1/sqlite-3.53.0-build1/worker.mjs',
  'sqlitenow-worker-v1/sqlite-3.53.0-build1/vendor/index.mjs',
  'sqlitenow-worker-v1/sqlite-3.53.0-build1/vendor/sqlite3.wasm',
  'sqlitenow-worker-v1/sqlite-3.53.0-build1/vendor/sqlite3-opfs-async-proxy.js',
];

config.files = config.files || [];
workerResources.forEach((resource) => {
  config.files.push({
    pattern: path.resolve(resourceRoot, resource),
    included: false,
    served: true,
    watched: false,
  });
});
config.proxies = Object.assign(config.proxies || {}, {
  '/sqlitenow-worker-v1/':
    `/absolute${path.resolve(resourceRoot, 'sqlitenow-worker-v1')}/`,
});

// Generated, unpublished W1 worker fault variants. Normal resources are untouched.
config.files.push({
  pattern: path.resolve(resourceRoot, 'w1-test-fixtures/**'),
  included: false, served: true, watched: false,
});
config.proxies['/w1-test-fixtures/'] =
  `/absolute${path.resolve(resourceRoot, 'w1-test-fixtures')}/`;
