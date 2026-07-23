const browserTestTimeoutMillis = 180000;

config.set({
  client: {
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
        "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval' 'wasm-unsafe-eval'; worker-src 'self'; connect-src 'self' ws:",
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
