// Gradle's TeamCity parser buffers one physical line up to 1 MiB. The stock
// Kotlin Karma reporter writes messages without line endings, so complete
// receipt chunks can accumulate across tests and overflow that buffer.
const teamcityReporterKey = 'reporter:karma-kotlin-reporter';
let framedReporterRegistrations = 0;

config.plugins = (config.plugins || []).map((plugin) => {
  if (!plugin || typeof plugin !== 'object' || !Object.hasOwn(plugin, teamcityReporterKey)) {
    return plugin;
  }
  const registration = plugin[teamcityReporterKey];
  if (!Array.isArray(registration) || registration[0] !== 'type' || typeof registration[1] !== 'function') {
    throw new Error('Expected the stock Kotlin Karma reporter constructor');
  }
  const StockReporter = registration[1];
  function LineDelimitedReporter(...args) {
    StockReporter.apply(this, args);
    const originalWrite = this.write;
    this.write = function (message, ...formatArgs) {
      return originalWrite.call(this, message.endsWith('\n') ? message : message + '\n', ...formatArgs);
    };
  }
  LineDelimitedReporter.$inject = StockReporter.$inject;
  framedReporterRegistrations += 1;
  return { ...plugin, [teamcityReporterKey]: ['type', LineDelimitedReporter] };
});

if (framedReporterRegistrations !== 1) {
  throw new Error('Expected exactly one Kotlin Karma reporter registration');
}
