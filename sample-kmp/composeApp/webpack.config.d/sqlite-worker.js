config.devServer = config.devServer || {};
config.devServer.headers = Object.assign(config.devServer.headers || {}, {
  "Cross-Origin-Opener-Policy": "same-origin",
  "Cross-Origin-Embedder-Policy": "require-corp",
  "Content-Security-Policy":
    "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval' 'wasm-unsafe-eval'; worker-src 'self'; connect-src 'self' ws:",
});
