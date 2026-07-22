const sqlWasmPath = require.resolve("sql.js/dist/sql-wasm.wasm");

config.files = config.files || [];
config.files.push({
  pattern: sqlWasmPath,
  included: false,
  served: true,
  watched: false,
});
config.proxies = Object.assign(config.proxies || {}, {
  "/sql-wasm.wasm": `/absolute${sqlWasmPath}`,
});
