const path = require("path");
const CopyWebpackPlugin = require("copy-webpack-plugin");

config.resolve = config.resolve || {};
config.resolve.fallback = Object.assign(config.resolve.fallback || {}, {
  fs: false,
  path: false,
  crypto: false,
  "wasi_snapshot_preview1": false,
  env: false,
});

config.experiments = config.experiments || {};
config.experiments.asyncWebAssembly = true;

config.module = config.module || {};
config.module.rules = Array.isArray(config.module.rules) ? config.module.rules : [];
config.module.rules.push({
  test: /\.wasm$/,
  type: "asset/resource",
  generator: {
    filename: "[name][ext]"
  }
});

config.plugins = config.plugins || [];
config.plugins.push(
  new CopyWebpackPlugin({
    patterns: [
      {
        from: path.resolve(__dirname, "../../node_modules/sql.js/dist/sql-wasm.wasm"),
        to: ".",
      },
    ],
  })
);
