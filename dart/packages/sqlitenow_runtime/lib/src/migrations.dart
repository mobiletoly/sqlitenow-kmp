import 'dart:async';

import 'sqlite_connection.dart';
import 'sqlite_row_reader.dart';

typedef SqliteNowMigrationBody =
    FutureOr<void> Function(SqliteNowConnection connection);

typedef SqliteNowMigrationStepCallback =
    FutureOr<void> Function(SqliteNowMigrationScope scope);

final class SqliteNowMigrationScope {
  SqliteNowMigrationScope({
    required SqliteNowConnection connection,
    required this.originalVersion,
    required this.fromVersion,
    required this.toVersion,
    required this.targetVersion,
  }) : connection = SqliteNowMigrationConnection._(connection);

  final int originalVersion;
  final int fromVersion;
  final int toVersion;
  final int targetVersion;
  final SqliteNowMigrationConnection connection;
}

final class SqliteNowMigrationConnection {
  SqliteNowMigrationConnection._(this._connection);

  final SqliteNowConnection _connection;
  var _active = true;
  final _operations = <_TrackedMigrationOperation>[];
  var _nextFailureOrder = 0;

  Future<void> execute(
    String sql, {
    List<Object?> parameters = const [],
    Set<String> affectedTables = const {},
  }) {
    _ensureActive();
    _requireMigrationSqlAllowed(sql);
    return _track(
      _connection.execute(
        sql,
        parameters: parameters,
        affectedTables: affectedTables,
      ),
    );
  }

  Future<List<T>> select<T>(
    String sql,
    T Function(SqliteRowReader row) read, {
    List<Object?> parameters = const [],
  }) {
    _ensureActive();
    _requireMigrationSqlAllowed(sql);
    return _track(_connection.select(sql, read, parameters: parameters));
  }

  Future<T> usePrepared<T>(
    String sql,
    FutureOr<T> Function(SqliteNowPreparedStatement statement) block,
  ) {
    _ensureActive();
    _requireMigrationSqlAllowed(sql);
    return _track(_connection.usePrepared(sql, block));
  }

  Future<T> _track<T>(Future<T> operation) {
    final tracked = _TrackedMigrationOperation();
    _operations.add(tracked);
    final rootBranch = _TrackedMigrationBranch();
    tracked.branches.add(rootBranch);
    tracked.completion = operation.then<void>(
      (_) {
        tracked.completed = true;
        rootBranch.completed = true;
      },
      onError: (Object error, StackTrace stackTrace) {
        tracked.completed = true;
        tracked.error = error;
        tracked.stackTrace = stackTrace;
        tracked.failureOrder = _nextFailureOrder++;
        rootBranch.completed = true;
        rootBranch.error = error;
        rootBranch.stackTrace = stackTrace;
      },
    );
    return _MigrationOperationFuture(operation, tracked, rootBranch, this);
  }

  Future<({Object error, StackTrace stackTrace})?> _expireAndDrain() async {
    _active = false;
    final operations = [..._operations];
    for (final operation in operations) {
      operation.pendingAtExpiration = !operation.completed;
      for (final branch in operation.branches) {
        branch.pendingAtExpiration = !branch.completed;
      }
    }
    await Future.wait([
      for (final operation in operations) operation.completion,
    ]);

    final failures = operations.where(_hasUnhandledFailure).toList()
      ..sort(
        (left, right) => left.failureOrder!.compareTo(right.failureOrder!),
      );
    if (failures.isEmpty) return null;
    final firstFailure = failures.first;
    final branchFailure = firstFailure.branches
        .where((branch) => !branch.hasActiveChild && branch.error != null)
        .firstOrNull;
    return (
      error: branchFailure?.error ?? firstFailure.error!,
      stackTrace: branchFailure?.stackTrace ?? firstFailure.stackTrace!,
    );
  }

  bool _hasUnhandledFailure(_TrackedMigrationOperation operation) {
    if (operation.error == null) return false;
    if (operation.pendingAtExpiration) return true;
    return operation.branches
        .where((branch) => !branch.hasActiveChild)
        .any((branch) => branch.pendingAtExpiration || branch.error != null);
  }

  _TrackedMigrationBranch? _trackDerived<T>(
    _TrackedMigrationOperation operation,
    _TrackedMigrationBranch? parent,
    Future<T> future,
  ) {
    if (!_active || parent == null) return null;
    parent.hasActiveChild = true;
    final branch = _TrackedMigrationBranch();
    operation.branches.add(branch);
    future.then<void>(
      (_) {
        branch.completed = true;
      },
      onError: (Object error, StackTrace stackTrace) {
        branch.completed = true;
        branch.error = error;
        branch.stackTrace = stackTrace;
      },
    );
    return branch;
  }

  void _ensureActive() {
    if (!_active) {
      throw StateError('Migration-step connection is no longer active');
    }
  }
}

final class _TrackedMigrationOperation {
  late final Future<void> completion;
  final branches = <_TrackedMigrationBranch>[];
  var completed = false;
  var pendingAtExpiration = false;
  Object? error;
  StackTrace? stackTrace;
  int? failureOrder;
}

final class _TrackedMigrationBranch {
  var completed = false;
  var pendingAtExpiration = false;
  var hasActiveChild = false;
  Object? error;
  StackTrace? stackTrace;
}

final class _MigrationOperationFuture<T> implements Future<T> {
  _MigrationOperationFuture(
    this._delegate,
    this._operation,
    this._branch,
    this._owner,
  );

  final Future<T> _delegate;
  final _TrackedMigrationOperation _operation;
  final _TrackedMigrationBranch? _branch;
  final SqliteNowMigrationConnection _owner;

  @override
  Future<R> then<R>(
    FutureOr<R> Function(T value) onValue, {
    Function? onError,
  }) {
    final derived = _delegate.then<R>(onValue, onError: onError);
    return _MigrationOperationFuture(
      derived,
      _operation,
      _owner._trackDerived(_operation, _branch, derived),
      _owner,
    );
  }

  @override
  Future<T> catchError(Function onError, {bool Function(Object error)? test}) {
    final derived = _delegate.catchError(onError, test: test);
    return _MigrationOperationFuture(
      derived,
      _operation,
      _owner._trackDerived(_operation, _branch, derived),
      _owner,
    );
  }

  @override
  Future<T> whenComplete(FutureOr<void> Function() action) {
    final derived = _delegate.whenComplete(action);
    return _MigrationOperationFuture(
      derived,
      _operation,
      _owner._trackDerived(_operation, _branch, derived),
      _owner,
    );
  }

  @override
  Stream<T> asStream() => _MigrationOperationStream(
    _delegate.asStream(),
    _operation,
    _branch,
    _owner,
  );

  @override
  Future<T> timeout(Duration timeLimit, {FutureOr<T> Function()? onTimeout}) {
    final derived = _delegate.timeout(timeLimit, onTimeout: onTimeout);
    return _MigrationOperationFuture(
      derived,
      _operation,
      _owner._trackDerived(_operation, _branch, derived),
      _owner,
    );
  }
}

final class _MigrationOperationStream<T> extends Stream<T> {
  _MigrationOperationStream(
    this._delegate,
    this._operation,
    this._branch,
    this._owner,
  );

  final Stream<T> _delegate;
  final _TrackedMigrationOperation _operation;
  final _TrackedMigrationBranch? _branch;
  final SqliteNowMigrationConnection _owner;

  Future<R> _track<R>(Future<R> future) => _MigrationOperationFuture(
    future,
    _operation,
    _owner._trackDerived(_operation, _branch, future),
    _owner,
  );

  Stream<R> _wrap<R>(Stream<R> stream) =>
      _MigrationOperationStream(stream, _operation, _branch, _owner);

  @override
  bool get isBroadcast => _delegate.isBroadcast;

  @override
  Stream<T> asBroadcastStream({
    void Function(StreamSubscription<T> subscription)? onListen,
    void Function(StreamSubscription<T> subscription)? onCancel,
  }) => _wrap(
    _delegate.asBroadcastStream(onListen: onListen, onCancel: onCancel),
  );

  @override
  Stream<R> cast<R>() => _wrap(_delegate.cast<R>());

  @override
  Stream<T> where(bool Function(T event) test) => _wrap(_delegate.where(test));

  @override
  Stream<R> map<R>(R Function(T event) convert) =>
      _wrap(_delegate.map(convert));

  @override
  Stream<R> asyncMap<R>(FutureOr<R> Function(T event) convert) =>
      _wrap(_delegate.asyncMap(convert));

  @override
  Stream<R> asyncExpand<R>(Stream<R>? Function(T event) convert) =>
      _wrap(_delegate.asyncExpand(convert));

  @override
  Stream<T> handleError(Function onError, {bool Function(dynamic)? test}) {
    final void Function(Object, StackTrace) trackedOnError;
    if (onError is void Function(Object, StackTrace)) {
      trackedOnError = (Object error, StackTrace stackTrace) {
        _trackAsyncErrorHandlerResult(
          Function.apply(onError, [error, stackTrace]),
        );
      };
    } else if (onError is void Function(Object)) {
      trackedOnError = (Object error, StackTrace stackTrace) {
        _trackAsyncErrorHandlerResult(Function.apply(onError, [error]));
      };
    } else {
      throw ArgumentError.value(
        onError,
        'onError',
        'Error handler must accept one Object or one Object and a StackTrace '
            'as arguments.',
      );
    }
    return _wrap(_delegate.handleError(trackedOnError, test: test));
  }

  void _trackAsyncErrorHandlerResult(Object? result) {
    if (result is Future) {
      _owner._trackDerived(_operation, _branch, result);
    }
  }

  @override
  Stream<R> expand<R>(Iterable<R> Function(T element) convert) =>
      _wrap(_delegate.expand(convert));

  @override
  Stream<R> transform<R>(StreamTransformer<T, R> streamTransformer) =>
      _wrap(_delegate.transform(streamTransformer));

  @override
  Stream<T> take(int count) {
    final stream = _delegate.take(count);
    return _MigrationOperationStream(
      stream,
      _operation,
      count == 0 ? null : _branch,
      _owner,
    );
  }

  @override
  Stream<T> takeWhile(bool Function(T element) test) =>
      _wrap(_delegate.takeWhile(test));

  @override
  Stream<T> skip(int count) => _wrap(_delegate.skip(count));

  @override
  Stream<T> skipWhile(bool Function(T element) test) =>
      _wrap(_delegate.skipWhile(test));

  @override
  Stream<T> distinct([bool Function(T previous, T next)? equals]) =>
      _wrap(_delegate.distinct(equals));

  @override
  Stream<T> timeout(
    Duration timeLimit, {
    void Function(EventSink<T> sink)? onTimeout,
  }) => _wrap(_delegate.timeout(timeLimit, onTimeout: onTimeout));

  @override
  Future<T> reduce(T Function(T previous, T element) combine) =>
      _track(_delegate.reduce(combine));

  @override
  Future<R> fold<R>(
    R initialValue,
    R Function(R previous, T element) combine,
  ) => _track(_delegate.fold(initialValue, combine));

  @override
  Future<String> join([String separator = '']) =>
      _track(_delegate.join(separator));

  @override
  Future<bool> contains(Object? needle) => _track(_delegate.contains(needle));

  @override
  Future<void> forEach(void Function(T element) action) =>
      _track(_delegate.forEach(action));

  @override
  Future<bool> every(bool Function(T element) test) =>
      _track(_delegate.every(test));

  @override
  Future<bool> any(bool Function(T element) test) =>
      _track(_delegate.any(test));

  @override
  Future<int> get length => _track(_delegate.length);

  @override
  Future<bool> get isEmpty => _track(_delegate.isEmpty);

  @override
  Future<List<T>> toList() => _track(_delegate.toList());

  @override
  Future<Set<T>> toSet() => _track(_delegate.toSet());

  @override
  Future<R> drain<R>([R? futureValue]) => _track(_delegate.drain(futureValue));

  @override
  Future<T> firstWhere(bool Function(T element) test, {T Function()? orElse}) =>
      _track(_delegate.firstWhere(test, orElse: orElse));

  @override
  Future<T> lastWhere(bool Function(T element) test, {T Function()? orElse}) =>
      _track(_delegate.lastWhere(test, orElse: orElse));

  @override
  Future<T> singleWhere(
    bool Function(T element) test, {
    T Function()? orElse,
  }) => _track(_delegate.singleWhere(test, orElse: orElse));

  @override
  Future<T> elementAt(int index) => _track(_delegate.elementAt(index));

  @override
  Future<dynamic> pipe(StreamConsumer<T> streamConsumer) =>
      _track(_delegate.pipe(streamConsumer));

  @override
  Future<T> get first => _track(_delegate.first);

  @override
  Future<T> get last => _track(_delegate.last);

  @override
  Future<T> get single => _track(_delegate.single);

  @override
  StreamSubscription<T> listen(
    void Function(T event)? onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) {
    if (onError != null &&
        onError is! void Function(Object, StackTrace) &&
        onError is! void Function(Object)) {
      throw ArgumentError.value(onError, 'onError');
    }

    if (!_owner._active || _branch == null) {
      return _delegate.listen(
        onData,
        onError: onError,
        onDone: onDone,
        cancelOnError: cancelOnError,
      );
    }

    final outcome = Completer<void>();
    var errorObserved = false;

    void trackedOnError(Object error, StackTrace stackTrace) {
      errorObserved = true;
      if (onError == null) {
        if (!outcome.isCompleted) outcome.completeError(error, stackTrace);
        Zone.current.handleUncaughtError(error, stackTrace);
        return;
      }
      try {
        final result = onError is void Function(Object, StackTrace)
            ? Function.apply(onError, [error, stackTrace])
            : Function.apply(onError, [error]);
        if (result is Future) {
          result.then<void>(
            (_) {
              if (!outcome.isCompleted) outcome.complete();
            },
            onError: (Object failure, StackTrace failureStackTrace) {
              if (!outcome.isCompleted) {
                outcome.completeError(failure, failureStackTrace);
              }
            },
          );
        } else if (!outcome.isCompleted) {
          outcome.complete();
        }
      } catch (failure, failureStackTrace) {
        if (!outcome.isCompleted) {
          outcome.completeError(failure, failureStackTrace);
        }
        rethrow;
      }
    }

    void trackedOnDone() {
      if (!errorObserved && !outcome.isCompleted) outcome.complete();
      onDone?.call();
    }

    final subscription = _delegate.listen(
      onData,
      onError: trackedOnError,
      onDone: trackedOnDone,
      cancelOnError: cancelOnError,
    );
    _owner._trackDerived(_operation, _branch, outcome.future);
    return subscription;
  }
}

final class SqliteNowMigrationStep {
  const SqliteNowMigrationStep(this.version, this.migrate)
    : freshOnly = false,
      assert(version >= 0, 'Migration version must be non-negative');

  const SqliteNowMigrationStep.fresh(this.version, this.migrate)
    : freshOnly = true,
      assert(version >= 0, 'Migration version must be non-negative');

  final int version;
  final SqliteNowMigrationBody migrate;
  final bool freshOnly;
}

final class SqliteNowMigrationPlan {
  SqliteNowMigrationPlan(Iterable<SqliteNowMigrationStep> steps)
    : steps = List.unmodifiable(_validateSteps(steps));

  final List<SqliteNowMigrationStep> steps;

  int get latestVersion {
    if (steps.isEmpty) return 0;
    return steps.map((step) => step.version).reduce((a, b) => a > b ? a : b);
  }

  Future<int> apply(
    SqliteNowConnection connection,
    int currentVersion, {
    SqliteNowMigrationStepCallback? onMigrationStep,
  }) async {
    if (currentVersion == -1) {
      final freshSteps = steps.where((step) => step.freshOnly);
      if (freshSteps.isNotEmpty) {
        for (final step in freshSteps) {
          await step.migrate(connection);
        }
        return latestVersion;
      }
    }

    final incrementalSteps = {
      for (final step in steps)
        if (!step.freshOnly) step.version: step,
    };
    final targetVersion = latestVersion;
    if (currentVersion >= 0 && currentVersion < targetVersion) {
      for (
        var toVersion = currentVersion + 1;
        toVersion <= targetVersion;
        toVersion++
      ) {
        final step = incrementalSteps[toVersion];
        if (step != null) {
          await step.migrate(connection);
        }
        if (onMigrationStep != null) {
          final scope = SqliteNowMigrationScope(
            connection: connection,
            originalVersion: currentVersion,
            fromVersion: toVersion - 1,
            toVersion: toVersion,
            targetVersion: targetVersion,
          );
          Object? callbackError;
          StackTrace? callbackStackTrace;
          try {
            final callbackResult = onMigrationStep(scope);
            if (callbackResult is Future<void>) {
              await callbackResult;
            }
          } catch (error, stackTrace) {
            callbackError = error;
            callbackStackTrace = stackTrace;
          }
          final operationFailure = await scope.connection._expireAndDrain();
          if (callbackError != null) {
            Error.throwWithStackTrace(callbackError, callbackStackTrace!);
          }
          if (operationFailure != null) {
            Error.throwWithStackTrace(
              operationFailure.error,
              operationFailure.stackTrace,
            );
          }
        }
      }
      return targetVersion;
    }

    var newVersion = currentVersion;
    for (final step in steps) {
      if (!step.freshOnly && step.version > currentVersion) {
        await step.migrate(connection);
        newVersion = step.version;
      }
    }
    if (newVersion == -1) return latestVersion;
    return newVersion;
  }
}

const _forbiddenMigrationKeywords = {
  'BEGIN',
  'COMMIT',
  'END',
  'ROLLBACK',
  'SAVEPOINT',
  'RELEASE',
};

void _requireMigrationSqlAllowed(String sql) {
  final sanitized = _stripMigrationSqlCommentsAndStrings(sql);
  for (final range in _migrationSqlStatementRanges(sanitized)) {
    final sanitizedStatement = sanitized.substring(range.start, range.end);
    final originalStatement = sql.substring(range.start, range.end);
    final keywordMatch = RegExp(r'[A-Za-z_]+').firstMatch(sanitizedStatement);
    if (keywordMatch == null) continue;
    final keyword = keywordMatch.group(0)!.toUpperCase();
    if (_forbiddenMigrationKeywords.contains(keyword)) {
      throw ArgumentError(
        '$keyword is not allowed from a migration-step callback',
      );
    }
    if (keyword == 'PRAGMA') {
      final pragmaName = _readMigrationPragmaName(
        originalStatement,
        keywordMatch.end,
      );
      if (pragmaName?.toLowerCase() == 'user_version') {
        throw ArgumentError(
          'PRAGMA user_version is owned by SQLiteNow during migration',
        );
      }
    }
  }
}

List<({int start, int end})> _migrationSqlStatementRanges(String sql) {
  final ranges = <({int start, int end})>[];
  var statementStart = 0;
  var index = 0;
  var trigger = false;
  var triggerBody = false;
  var triggerEndSeen = false;
  var caseDepth = 0;
  final leadingWords = <String>[];

  void resetStatement() {
    trigger = false;
    triggerBody = false;
    triggerEndSeen = false;
    caseDepth = 0;
    leadingWords.clear();
  }

  while (index < sql.length) {
    if (RegExp(r'[A-Za-z_]').hasMatch(sql[index])) {
      final wordStart = index;
      index++;
      while (index < sql.length &&
          RegExp(r'[A-Za-z0-9_]').hasMatch(sql[index])) {
        index++;
      }
      final word = sql.substring(wordStart, index).toUpperCase();
      if (!triggerBody) {
        if (leadingWords.length < 4) leadingWords.add(word);
        trigger =
            trigger ||
            _wordsEqual(leadingWords, const ['CREATE', 'TRIGGER']) ||
            _wordsEqual(leadingWords, const ['CREATE', 'TEMP', 'TRIGGER']) ||
            _wordsEqual(leadingWords, const ['CREATE', 'TEMPORARY', 'TRIGGER']);
        if (trigger && word == 'BEGIN') triggerBody = true;
      } else if (word == 'CASE') {
        caseDepth++;
      } else if (word == 'END') {
        if (caseDepth > 0) {
          caseDepth--;
        } else {
          triggerEndSeen = true;
        }
      }
      continue;
    }

    if (sql[index] == ';' && (!triggerBody || triggerEndSeen)) {
      ranges.add((start: statementStart, end: index));
      statementStart = index + 1;
      resetStatement();
    }
    index++;
  }
  if (statementStart < sql.length) {
    ranges.add((start: statementStart, end: sql.length));
  }
  return ranges;
}

bool _wordsEqual(List<String> actual, List<String> expected) {
  if (actual.length != expected.length) return false;
  for (var index = 0; index < actual.length; index++) {
    if (actual[index] != expected[index]) return false;
  }
  return true;
}

String? _readMigrationPragmaName(String statement, int startIndex) {
  var index = _skipMigrationSqlTrivia(statement, startIndex);
  final first = _readMigrationSqlIdentifier(statement, index);
  if (first == null) return null;
  index = _skipMigrationSqlTrivia(statement, first.end);
  if (index >= statement.length || statement[index] != '.') return first.value;
  index = _skipMigrationSqlTrivia(statement, index + 1);
  return _readMigrationSqlIdentifier(statement, index)?.value;
}

int _skipMigrationSqlTrivia(String sql, int startIndex) {
  var index = startIndex;
  while (index < sql.length) {
    if (RegExp(r'\s').hasMatch(sql[index])) {
      index++;
    } else if (sql[index] == '-' &&
        index + 1 < sql.length &&
        sql[index + 1] == '-') {
      index += 2;
      while (index < sql.length && sql[index] != '\n') {
        index++;
      }
    } else if (sql[index] == '/' &&
        index + 1 < sql.length &&
        sql[index + 1] == '*') {
      index += 2;
      while (index < sql.length) {
        if (sql[index] == '*' &&
            index + 1 < sql.length &&
            sql[index + 1] == '/') {
          index += 2;
          break;
        }
        index++;
      }
    } else {
      return index;
    }
  }
  return index;
}

({String value, int end})? _readMigrationSqlIdentifier(
  String sql,
  int startIndex,
) {
  if (startIndex >= sql.length) return null;
  final opening = sql[startIndex];
  if (opening == "'" || opening == '"' || opening == '`' || opening == '[') {
    final closing = opening == '[' ? ']' : opening;
    final value = StringBuffer();
    var index = startIndex + 1;
    while (index < sql.length) {
      if (sql[index] == closing) {
        if (index + 1 < sql.length && sql[index + 1] == closing) {
          value.write(closing);
          index += 2;
          continue;
        }
        return (value: value.toString(), end: index + 1);
      }
      value.write(sql[index]);
      index++;
    }
    return (value: value.toString(), end: index);
  }

  var index = startIndex;
  while (index < sql.length && RegExp(r'[A-Za-z0-9_]').hasMatch(sql[index])) {
    index++;
  }
  if (index == startIndex) return null;
  return (value: sql.substring(startIndex, index), end: index);
}

String _stripMigrationSqlCommentsAndStrings(String sql) {
  final result = StringBuffer();
  var index = 0;
  while (index < sql.length) {
    if (sql[index] == "'" ||
        sql[index] == '"' ||
        sql[index] == '`' ||
        sql[index] == '[') {
      final opening = sql[index];
      final closing = opening == '[' ? ']' : opening;
      result.write(' ');
      index++;
      while (index < sql.length) {
        if (sql[index] == closing &&
            index + 1 < sql.length &&
            sql[index + 1] == closing) {
          result.write('  ');
          index += 2;
        } else if (sql[index] == closing) {
          result.write(' ');
          index++;
          break;
        } else {
          result.write(sql[index] == '\n' ? '\n' : ' ');
          index++;
        }
      }
    } else if (sql[index] == '-' &&
        index + 1 < sql.length &&
        sql[index + 1] == '-') {
      result.write('  ');
      index += 2;
      while (index < sql.length && sql[index] != '\n') {
        result.write(' ');
        index++;
      }
    } else if (sql[index] == '/' &&
        index + 1 < sql.length &&
        sql[index + 1] == '*') {
      result.write('  ');
      index += 2;
      while (index < sql.length) {
        if (sql[index] == '*' &&
            index + 1 < sql.length &&
            sql[index + 1] == '/') {
          result.write('  ');
          index += 2;
          break;
        }
        result.write(sql[index] == '\n' ? '\n' : ' ');
        index++;
      }
    } else {
      result.write(sql[index]);
      index++;
    }
  }
  return result.toString();
}

List<SqliteNowMigrationStep> _validateSteps(
  Iterable<SqliteNowMigrationStep> steps,
) {
  final sorted = [...steps]..sort((a, b) => a.version.compareTo(b.version));
  int? previous;
  for (final step in sorted) {
    if (step.freshOnly) continue;
    if (previous == step.version) {
      throw ArgumentError('Duplicate migration version ${step.version}');
    }
    previous = step.version;
  }
  return sorted;
}
