const isLegacySqlJsFixtureTest = String(__dirname).includes(
  "sqlitenow-kmp-platform-oversqlite-test-composeApp-test",
);

if (isLegacySqlJsFixtureTest) {
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
