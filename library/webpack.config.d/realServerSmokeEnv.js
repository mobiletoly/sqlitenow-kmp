const webpack = require("webpack");

const realServerEnabled =
  process.env.OVERSQLITE_REALSERVER_TESTS ?? null;
const realServerHeavyEnabled =
  process.env.OVERSQLITE_REALSERVER_HEAVY ?? null;
const smokeBaseUrl =
  process.env.OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL ?? null;

config.plugins = Array.isArray(config.plugins) ? config.plugins : [];
config.plugins.push(
  new webpack.BannerPlugin({
    raw: true,
    entryOnly: true,
    banner: `
globalThis.OVERSQLITE_REALSERVER_TESTS = ${JSON.stringify(realServerEnabled)};
globalThis.OVERSQLITE_REALSERVER_HEAVY = ${JSON.stringify(realServerHeavyEnabled)};
globalThis.OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL = ${JSON.stringify(smokeBaseUrl)};
`,
  }),
);
