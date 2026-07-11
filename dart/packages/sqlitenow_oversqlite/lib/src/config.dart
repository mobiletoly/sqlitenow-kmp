final class SyncTable {
  const SyncTable({
    required this.tableName,
    required this.syncKeyColumnName,
    this.numericColumns = const {},
  });

  final String tableName;
  final String syncKeyColumnName;
  final Map<String, NumericColumnKind> numericColumns;
}

enum NumericColumnKind { exactInt64, exactDecimal, approximate }

enum BundleChangeWatchMode { off, auto }

final class OversqliteConfig {
  const OversqliteConfig({
    required this.schema,
    required this.syncTables,
    this.uploadLimit = 200,
    this.downloadLimit = 1000,
    this.verboseLogs = false,
    this.automaticDownloadInterval = const Duration(seconds: 60),
    this.bundleChangeWatchMode = BundleChangeWatchMode.off,
    this.bundleChangeWatchReconnectMin = const Duration(seconds: 1),
    this.bundleChangeWatchReconnectMax = const Duration(seconds: 60),
  });

  final String schema;
  final List<SyncTable> syncTables;
  final int uploadLimit;
  final int downloadLimit;
  final bool verboseLogs;
  final Duration automaticDownloadInterval;
  final BundleChangeWatchMode bundleChangeWatchMode;
  final Duration bundleChangeWatchReconnectMin;
  final Duration bundleChangeWatchReconnectMax;
}
