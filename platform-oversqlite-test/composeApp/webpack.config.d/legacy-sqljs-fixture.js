const isLegacySqlJsFixtureTest = String(__dirname).includes(
  "sqlitenow-kmp-platform-oversqlite-test-composeApp-test",
);

if (isLegacySqlJsFixtureTest) {
  // Shared JS fixtures retain lazy Node-only filesystem helpers; browser tests never call them.
  config.externalsPresets = Object.assign({}, config.externalsPresets, { node: true });
  config.resolve = config.resolve || {};
  config.resolve.fallback = Object.assign({}, config.resolve.fallback, {
    fs: false,
    path: false,
    crypto: false,
  });

  config.module = config.module || {};
  config.module.rules = Array.isArray(config.module.rules) ? config.module.rules : [];
  config.module.rules.push({
    test: /sql-wasm\.wasm$/,
    type: "asset/resource",
  });

  config.experiments = Object.assign({}, config.experiments, {
    asyncWebAssembly: true,
  });
}
