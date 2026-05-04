enum TransactionMode {
  deferred('BEGIN'),
  immediate('BEGIN IMMEDIATE'),
  exclusive('BEGIN EXCLUSIVE');

  const TransactionMode(this.beginSql);

  final String beginSql;
}
