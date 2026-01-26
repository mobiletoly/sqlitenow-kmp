const webpackVersion = (() => {
  try {
    return require("webpack/package.json").version || "";
  } catch (_) {
    return "";
  }
})();
const isWebpack5 = webpackVersion.startsWith("5");

const configName = String(config.name ?? "").toLowerCase();
const outputPath = String(config.output?.path ?? "").toLowerCase();
const isWasmBuild =
  configName.includes("wasm") ||
  outputPath.includes("/wasmjs/") ||
  outputPath.includes("\\wasmjs\\");

config.resolve = config.resolve || {};
config.resolve.fallback = Object.assign({}, config.resolve.fallback, {
  fs: false,
  path: false,
  crypto: false,
});
config.resolve.alias = Object.assign({}, config.resolve.alias, {
  "sql.js": "sql.js/dist/sql-wasm.js",
});

config.module = config.module || {};
config.module.rules = Array.isArray(config.module.rules) ? config.module.rules : [];
config.module.rules.push({
  test: /sql\.js\/dist\/sql-wasm\.wasm$/,
  type: "asset/resource",
});

if (isWebpack5) {
  config.experiments = Object.assign({}, config.experiments, {
    asyncWebAssembly: true,
  });
}

if (isWasmBuild) {
  config.target = "web";
}
