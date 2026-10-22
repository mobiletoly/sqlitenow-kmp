// This directory is merged with the library's normal Karma configuration only
// for the exact SettlementTest.backlogBound qualification invocation.
config.client = config.client || {};
config.client.mocha = Object.assign({}, config.client.mocha, { timeout: 480000 });
config.browserNoActivityTimeout = Math.max(Number(config.browserNoActivityTimeout || 0), 480000);
