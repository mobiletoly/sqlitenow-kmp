config.client = config.client || {};
config.client.mocha = Object.assign({}, config.client.mocha, {
  timeout: 120000,
});

config.browserNoActivityTimeout = Math.max(
  Number(config.browserNoActivityTimeout || 0),
  120000,
);
config.browserDisconnectTimeout = Math.max(
  Number(config.browserDisconnectTimeout || 0),
  30000,
);
config.captureTimeout = Math.max(Number(config.captureTimeout || 0), 120000);
