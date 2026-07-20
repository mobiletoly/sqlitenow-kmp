final class SyncTable {
  const SyncTable({required this.tableName, required this.syncKeyColumnName});

  final String tableName;
  final String syncKeyColumnName;
}

enum BundleChangeWatchMode { off, auto }

final class OversqliteConfig {
  OversqliteConfig({
    required this.schema,
    required this.syncTables,
    this.uploadLimit = 200,
    this.downloadLimit = 1000,
    this.snapshotChunkRows = 1000,
    this.snapshotChunkBytes = 4 * 1024 * 1024,
    this.snapshotApplyBatchRows = 256,
    this.snapshotApplyBatchBytes = 4 * 1024 * 1024,
    OversqliteSnapshotCapacityRetryPolicy? snapshotCapacityRetryPolicy,
    this.verboseLogs = false,
    this.automaticDownloadInterval = const Duration(seconds: 60),
    this.bundleChangeWatchMode = BundleChangeWatchMode.off,
    this.bundleChangeWatchReconnectMin = const Duration(seconds: 1),
    this.bundleChangeWatchReconnectMax = const Duration(seconds: 60),
  }) : snapshotCapacityRetryPolicy =
           snapshotCapacityRetryPolicy ??
           OversqliteSnapshotCapacityRetryPolicy() {
    _requirePositiveInt(snapshotChunkRows, 'snapshotChunkRows');
    _requirePositiveInt(snapshotChunkBytes, 'snapshotChunkBytes');
    _requirePositiveInt(snapshotApplyBatchRows, 'snapshotApplyBatchRows');
    _requirePositiveInt(snapshotApplyBatchBytes, 'snapshotApplyBatchBytes');
  }

  final String schema;
  final List<SyncTable> syncTables;
  final int uploadLimit;
  final int downloadLimit;
  final int snapshotChunkRows;
  final int snapshotChunkBytes;
  final int snapshotApplyBatchRows;
  final int snapshotApplyBatchBytes;
  final OversqliteSnapshotCapacityRetryPolicy snapshotCapacityRetryPolicy;
  final bool verboseLogs;
  final Duration automaticDownloadInterval;
  final BundleChangeWatchMode bundleChangeWatchMode;
  final Duration bundleChangeWatchReconnectMin;
  final Duration bundleChangeWatchReconnectMax;
}

final class OversqliteSnapshotCapacityRetryPolicy {
  OversqliteSnapshotCapacityRetryPolicy({
    this.enabled = true,
    this.maxWait = const Duration(seconds: 30),
    this.fallbackDelay = const Duration(seconds: 1),
    this.positiveJitterRatio = 1.0,
  }) {
    _requirePositiveDuration(maxWait, 'maxWait');
    _requirePositiveDuration(fallbackDelay, 'fallbackDelay');
    if (!positiveJitterRatio.isFinite ||
        positiveJitterRatio < 0 ||
        positiveJitterRatio > 1) {
      throw ArgumentError.value(
        positiveJitterRatio,
        'positiveJitterRatio',
        'must be finite and between 0 and 1',
      );
    }
  }

  final bool enabled;
  final Duration maxWait;
  final Duration fallbackDelay;
  final double positiveJitterRatio;
}

void _requirePositiveInt(int value, String name) {
  if (value <= 0) {
    throw ArgumentError.value(value, name, 'must be positive');
  }
}

void _requirePositiveDuration(Duration value, String name) {
  if (value <= Duration.zero) {
    throw ArgumentError.value(value, name, 'must be positive');
  }
}
