config.resolve = config.resolve || {};
config.resolve.fallback = Object.assign(config.resolve.fallback || {}, {
  fs: "commonjs fs",
  path: "commonjs path",
  crypto: "commonjs crypto",
});

config.target = "node";

config.experiments = config.experiments || {};
config.experiments.asyncWebAssembly = true;

config.module = config.module || {};
config.module.rules = Array.isArray(config.module.rules) ? config.module.rules : [];
config.module.rules.push({
  test: /\.wasm$/,
  type: "webassembly/async",
});
