final class SyncTable {
  const SyncTable({required this.tableName, required this.syncKeyColumnName});

  final String tableName;
  final String syncKeyColumnName;
}

final class OversqliteConfig {
  const OversqliteConfig({
    required this.schema,
    required this.syncTables,
    this.uploadLimit = 200,
    this.downloadLimit = 1000,
    this.verboseLogs = false,
  });

  final String schema;
  final List<SyncTable> syncTables;
  final int uploadLimit;
  final int downloadLimit;
  final bool verboseLogs;
}
