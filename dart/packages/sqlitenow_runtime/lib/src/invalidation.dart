import 'dart:async';

final class TableInvalidation {
  TableInvalidation(Iterable<String> tables)
    : tables = Set.unmodifiable(_normalizeTables(tables));

  final Set<String> tables;
}

final class TableInvalidationTracker {
  final _controller = StreamController<TableInvalidation>.broadcast();

  Stream<void> watchTables(Set<String> affectedTables) {
    final normalized = _normalizeTables(affectedTables);
    if (normalized.isEmpty) return const Stream.empty();

    return _controller.stream
        .where((event) => event.tables.any(normalized.contains))
        .map((_) {});
  }

  void reportTablesChanged(Set<String> affectedTables) {
    final normalized = _normalizeTables(affectedTables);
    if (normalized.isEmpty || _controller.isClosed) return;
    _controller.add(TableInvalidation(normalized));
  }

  Future<void> close() => _controller.close();
}

Set<String> _normalizeTables(Iterable<String> tables) {
  return {
    for (final table in tables)
      if (table.trim().isNotEmpty) table.trim().toLowerCase(),
  };
}
