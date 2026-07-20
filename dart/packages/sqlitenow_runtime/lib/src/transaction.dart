enum TransactionMode {
  deferred('BEGIN'),
  immediate('BEGIN IMMEDIATE'),
  exclusive('BEGIN EXCLUSIVE');

  const TransactionMode(this.beginSql);

  final String beginSql;
}

final class SqliteNowFatalTransactionException implements Exception {
  const SqliteNowFatalTransactionException({
    required this.primaryFailure,
    required this.cleanupFailure,
    this.connectionCloseFailure,
  });

  final Object primaryFailure;
  final Object cleanupFailure;
  final Object? connectionCloseFailure;

  @override
  String toString() =>
      'SQLite transaction cleanup failed; the connection was closed';
}

final class SqliteNowFatalStatementCleanupException implements Exception {
  const SqliteNowFatalStatementCleanupException({
    required this.primaryFailure,
    required this.cleanupFailure,
    this.connectionCloseFailure,
  });

  final Object? primaryFailure;
  final Object cleanupFailure;
  final Object? connectionCloseFailure;

  @override
  String toString() =>
      'SQLite statement cleanup failed; the connection was closed';
}
