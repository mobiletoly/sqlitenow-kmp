import 'dart:async';

import 'database.dart';

final class SelectRunner<T> {
  const SelectRunner({
    required SqliteNowDatabase database,
    required Set<String> affectedTables,
    required Future<List<T>> Function() query,
  }) : _database = database,
       _affectedTables = affectedTables,
       _query = query;

  final SqliteNowDatabase _database;
  final Set<String> _affectedTables;
  final Future<List<T>> Function() _query;

  Future<List<T>> asList() => _query();

  Future<T> asOne() async {
    final rows = await asList();
    if (rows.length != 1) {
      throw StateError('Expected exactly one row, got ${rows.length}');
    }
    return rows.single;
  }

  Future<T?> asOneOrNull() async {
    final rows = await asList();
    if (rows.length > 1) {
      throw StateError('Expected zero or one row, got ${rows.length}');
    }
    if (rows.isEmpty) return null;
    return rows.single;
  }

  Stream<List<T>> watch() {
    late StreamController<List<T>> controller;
    StreamSubscription<void>? subscription;
    var active = false;

    Future<void> emitLatest() async {
      if (!active) return;
      try {
        final rows = await asList();
        if (active) controller.add(rows);
      } catch (error, stackTrace) {
        if (active) controller.addError(error, stackTrace);
      }
    }

    controller = StreamController<List<T>>(
      onListen: () {
        active = true;
        subscription = _database.invalidationTracker
            .watchTables(_affectedTables)
            .listen((_) {
              unawaited(emitLatest());
            });
        unawaited(emitLatest());
      },
      onCancel: () async {
        active = false;
        await subscription?.cancel();
      },
    );

    return controller.stream;
  }
}
