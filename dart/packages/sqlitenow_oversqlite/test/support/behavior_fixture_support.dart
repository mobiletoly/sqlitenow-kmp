import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';
import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

Directory repoRoot() {
  var current = Directory.current;
  while (true) {
    if (File.fromUri(current.uri.resolve('settings.gradle.kts')).existsSync()) {
      return current;
    }
    final parent = current.parent;
    if (parent.path == current.path) {
      throw StateError(
        'Could not locate repository root from ${Directory.current.path}',
      );
    }
    current = parent;
  }
}

Future<SqliteNowDatabase> openUsersDatabase() async {
  final database = SqliteNowDatabase.inMemory();
  await database.open(
    preInit: (connection) {
      return connection.execute(
        'CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)',
      );
    },
  );
  return database;
}

Future<SqliteNowDatabase> openBehaviorDatabase(String schema) {
  switch (schema) {
    case 'users':
      return openUsersDatabase();
    case 'users-posts':
      return _openUsersPostsDatabase();
    case 'immediate-authors-profiles-cycle':
      return _openAuthorProfileDatabase();
    case 'typed-rows':
      return _openTypedRowsDatabase();
    default:
      throw StateError('unknown behavior fixture schema $schema');
  }
}

Future<SqliteNowDatabase> _openUsersPostsDatabase() async {
  final database = SqliteNowDatabase.inMemory();
  await database.open(
    preInit: (connection) async {
      await connection.execute(
        'CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)',
      );
      await connection.execute('''CREATE TABLE posts (
  id TEXT PRIMARY KEY NOT NULL,
  user_id TEXT NOT NULL REFERENCES users(id),
  title TEXT NOT NULL
)''');
    },
  );
  return database;
}

Future<SqliteNowDatabase> _openAuthorProfileDatabase() async {
  final database = SqliteNowDatabase.inMemory();
  await database.open(
    preInit: (connection) async {
      await connection.execute('''CREATE TABLE authors (
  id TEXT PRIMARY KEY NOT NULL,
  profile_id TEXT NOT NULL REFERENCES profiles(id),
  name TEXT NOT NULL
)''');
      await connection.execute('''CREATE TABLE profiles (
  id TEXT PRIMARY KEY NOT NULL,
  author_id TEXT NOT NULL REFERENCES authors(id),
  bio TEXT NOT NULL
)''');
    },
  );
  return database;
}

Future<SqliteNowDatabase> _openTypedRowsDatabase() async {
  final database = SqliteNowDatabase.inMemory();
  await database.open(
    preInit: (connection) {
      return connection.execute('''CREATE TABLE typed_rows (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,
  note TEXT NULL,
  count_value INTEGER NULL,
  enabled_flag INTEGER NOT NULL,
  rating REAL NULL,
  data BLOB NULL,
  created_at TEXT NULL
)''');
    },
  );
  return database;
}

DefaultOversqliteClient newUsersClient(
  SqliteNowDatabase database,
  OversqliteHttpClient http, {
  Resolver resolver = const ServerWinsResolver(),
}) {
  return DefaultOversqliteClient(
    database: database,
    config: usersConfig,
    httpClient: http,
    resolver: resolver,
  );
}

DefaultOversqliteClient newBehaviorClient(
  SqliteNowDatabase database,
  OversqliteHttpClient http,
  Map<String, Object?> fixture, {
  Resolver resolver = const ServerWinsResolver(),
}) {
  return DefaultOversqliteClient(
    database: database,
    config: behaviorConfig(fixture),
    httpClient: http,
    resolver: resolver,
  );
}

const usersConfig = OversqliteConfig(
  schema: 'main',
  syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
  automaticDownloadInterval: Duration(seconds: 10),
  bundleChangeWatchMode: BundleChangeWatchMode.auto,
  bundleChangeWatchReconnectMin: Duration(milliseconds: 10),
  bundleChangeWatchReconnectMax: Duration(milliseconds: 20),
);

OversqliteConfig behaviorConfig(Map<String, Object?> fixture) {
  final syncTables =
      (fixture['syncTables'] as List<Object?>?)
          ?.cast<Map<String, Object?>>()
          .map(
            (table) => SyncTable(
              tableName: table['tableName']! as String,
              syncKeyColumnName: table['syncKeyColumnName']! as String,
            ),
          )
          .toList() ??
      const [SyncTable(tableName: 'users', syncKeyColumnName: 'id')];
  return OversqliteConfig(schema: 'main', syncTables: syncTables);
}

const heartbeatWatchConfig = OversqliteConfig(
  schema: 'main',
  syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
  automaticDownloadInterval: Duration(seconds: 10),
  bundleChangeWatchMode: BundleChangeWatchMode.auto,
  bundleChangeWatchReconnectMin: Duration(seconds: 1),
  bundleChangeWatchReconnectMax: Duration(seconds: 1),
);

Future<int> scalarInt(SqliteNowDatabase database, String sql) async {
  final rows = await database.connection.select(sql, (row) => row.readInt(0));
  return rows.single;
}

Future<String> scalarText(SqliteNowDatabase database, String sql) async {
  final rows = await database.connection.select(
    sql,
    (row) => row.readString(0),
  );
  return rows.single;
}

Future<void> executeSetupSql(
  SqliteNowDatabase database,
  List<String> statements,
) async {
  if (statements.isEmpty) {
    return;
  }
  await database.connection.transaction(() async {
    await database.connection.execute('PRAGMA defer_foreign_keys = ON');
    for (final sql in statements) {
      await database.connection.execute(sql);
    }
  }, mode: TransactionMode.immediate);
}

Future<void> executeApplyModeSql(
  SqliteNowDatabase database,
  List<String> statements,
) async {
  if (statements.isEmpty) {
    return;
  }
  await database.connection.transaction(() async {
    await database.connection.execute('PRAGMA defer_foreign_keys = ON');
    await database.connection.execute(
      'UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1',
    );
    try {
      for (final sql in statements) {
        await database.connection.execute(sql);
      }
    } finally {
      await database.connection.execute(
        'UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1',
      );
    }
  }, mode: TransactionMode.immediate);
}

Future<void> expectAppState(
  SqliteNowDatabase database,
  String name,
  Map<String, Object?> expected,
) async {
  if (expected.containsKey('users')) {
    final rows = await database.connection.select(
      'SELECT id, name FROM users ORDER BY id',
      (row) => {'id': row.readString(0), 'name': row.readString(1)},
    );
    expect(rows, expected['users'], reason: '$name users');
  }
  if (expected.containsKey('posts')) {
    final rows = await database.connection.select(
      'SELECT id, user_id, title FROM posts ORDER BY id',
      (row) => {
        'id': row.readString(0),
        'user_id': row.readString(1),
        'title': row.readString(2),
      },
    );
    expect(rows, expected['posts'], reason: '$name posts');
  }
  if (expected.containsKey('authors')) {
    final rows = await database.connection.select(
      'SELECT id, profile_id, name FROM authors ORDER BY id',
      (row) => {
        'id': row.readString(0),
        'profile_id': row.readString(1),
        'name': row.readString(2),
      },
    );
    expect(rows, expected['authors'], reason: '$name authors');
  }
  if (expected.containsKey('profiles')) {
    final rows = await database.connection.select(
      'SELECT id, author_id, bio FROM profiles ORDER BY id',
      (row) => {
        'id': row.readString(0),
        'author_id': row.readString(1),
        'bio': row.readString(2),
      },
    );
    expect(rows, expected['profiles'], reason: '$name profiles');
  }
  if (expected.containsKey('typedRows')) {
    final rows = await database.connection.select(
      '''SELECT id, name, note, count_value, enabled_flag, rating, data, created_at
FROM typed_rows
ORDER BY id''',
      (row) => {
        'id': row.readString(0),
        'name': row.readString(1),
        'note': row.readNullableString(2),
        'count_value': row.readNullableInt(3),
        'enabled_flag': row.readInt(4),
        'rating': row.readNullableDouble(5),
        'data': _nullableHex(row.readNullableBlob(6)),
        'created_at': row.readNullableString(7),
      },
    );
    expect(rows, expected['typedRows'], reason: '$name typed_rows');
  }
}

Future<void> eventually(
  Future<bool> Function() condition, {
  Duration timeout = const Duration(seconds: 5),
}) async {
  final deadline = DateTime.now().add(timeout);
  while (DateTime.now().isBefore(deadline)) {
    if (await condition()) {
      return;
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  if (!await condition()) {
    throw TimeoutException('condition was not met within $timeout');
  }
}

final class PushFixtureServer implements OversqliteHttpClient {
  PushFixtureServer({
    this.failFirstCommit = false,
    this.failFirstCommittedFetch = false,
    this.pruneFirstCommittedFetch = false,
    this.badCommittedBundleHash = false,
    this.committedBundleNotFoundFailures = 0,
    this.alreadyCommitted = false,
    this.sourceRetiredOnCreate = false,
    this.replacementSourceId = 'replacement-source',
    this.conflictOnce,
    this.conflictOnceOnRowCount,
    this.createResponse,
    this.commitResponse,
    this.committedRowsResponse,
    this.committedBundleSeq = 1,
  }) : _nextBundleSeq = committedBundleSeq;

  final bool failFirstCommit;
  final bool failFirstCommittedFetch;
  final bool pruneFirstCommittedFetch;
  final bool badCommittedBundleHash;
  final int committedBundleNotFoundFailures;
  final bool alreadyCommitted;
  final bool sourceRetiredOnCreate;
  final String replacementSourceId;
  final Map<String, Object?>? conflictOnce;
  final int? conflictOnceOnRowCount;
  final Map<String, Object?>? createResponse;
  final Map<String, Object?>? commitResponse;
  final Map<String, Object?>? committedRowsResponse;
  final int committedBundleSeq;
  final List<Map<String, Object?>> createRequests = [];
  final List<Map<String, Object?>> uploadedRows = [];
  final Map<int, List<Map<String, Object?>>> _uploadedRowsByBundleSeq = {};
  final Map<int, int> _sourceBundleIdByBundleSeq = {};
  final Map<int, String> _bundleHashByBundleSeq = {};
  var _commitAttempts = 0;
  var _committedFetchAttempts = 0;
  var _conflictDelivered = false;
  var _activeSourceBundleId = 1;
  var _nextBundleSeq = 1;
  var _sourceId = '';

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
  }) async {
    _sourceId = sourceId;
    if (path == 'sync/capabilities') {
      return _json({
        'protocol_version': 'v1',
        'schema_version': 1,
        'features': {'connect_lifecycle': true},
      });
    }
    if (path.startsWith('sync/committed-bundles/')) {
      _committedFetchAttempts++;
      final requestedBundleSeq = int.tryParse(
        path.substring('sync/committed-bundles/'.length).split('/').first,
      );
      final bundleSeq = requestedBundleSeq ?? committedBundleSeq;
      if (failFirstCommittedFetch && _committedFetchAttempts == 1) {
        return _json({
          'error': 'temporary',
          'message': 'retry',
        }, statusCode: 500);
      }
      if (_committedFetchAttempts <= committedBundleNotFoundFailures) {
        return _json({
          'error': 'committed_bundle_not_found',
          'message': 'committed bundle not visible yet',
        }, statusCode: 404);
      }
      if (pruneFirstCommittedFetch && _committedFetchAttempts == 1) {
        return _json({
          'error': 'history_pruned',
          'message': 'committed bundle replay pruned',
        }, statusCode: 409);
      }
      if (committedRowsResponse != null) {
        return _json({...committedRowsResponse!, 'source_id': _sourceId});
      }
      final rows =
          _uploadedRowsByBundleSeq[bundleSeq] ??
          _defaultCommittedRows(bundleSeq);
      final bundleHash =
          _bundleHashByBundleSeq[bundleSeq] ??
          _committedBundleHashForFixtureRows(rows);
      return _json({
        'bundle_seq': bundleSeq,
        'source_id': _sourceId,
        'source_bundle_id':
            _sourceBundleIdByBundleSeq[bundleSeq] ?? _activeSourceBundleId,
        'row_count': rows.length,
        'bundle_hash': bundleHash,
        'rows': rows,
        'next_row_ordinal': rows.isEmpty ? -1 : rows.length - 1,
        'has_more': false,
      });
    }
    return _json({'error': 'not_found', 'message': path}, statusCode: 404);
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
  }) async {
    _sourceId = sourceId;
    if (path == 'sync/connect') {
      return _json({'resolution': 'initialize_empty'});
    }
    if (path == 'sync/push-sessions') {
      final request = (body! as Map).cast<String, Object?>();
      createRequests.add(request);
      _activeSourceBundleId = request['source_bundle_id']! as int;
      if (sourceRetiredOnCreate) {
        return _json({
          'error': 'source_retired',
          'message': 'source retired',
          'source_id': sourceId,
          'replaced_by_source_id': replacementSourceId,
        }, statusCode: 409);
      }
      if (alreadyCommitted) {
        return _json(
          createResponse ??
              {
                'status': 'already_committed',
                'bundle_seq': committedBundleSeq,
                'source_id': sourceId,
                'source_bundle_id': request['source_bundle_id'],
                'row_count': uploadedRows.length,
                'bundle_hash': 'test-hash',
              },
          sourceIdOverride: sourceId,
        );
      }
      uploadedRows.clear();
      return _json({
        'status': 'staging',
        'push_id': 'push-1',
        'planned_row_count': request['planned_row_count'],
        'next_expected_row_ordinal': 0,
      });
    }
    if (path == 'sync/push-sessions/push-1/chunks') {
      final request = (body! as Map).cast<String, Object?>();
      uploadedRows.addAll(
        (request['rows']! as List<Object?>).cast<Map<String, Object?>>(),
      );
      return _json({
        'push_id': 'push-1',
        'next_expected_row_ordinal': uploadedRows.length,
      });
    }
    if (path == 'sync/push-sessions/push-1/commit') {
      _commitAttempts++;
      if (conflictOnce != null &&
          !_conflictDelivered &&
          (conflictOnceOnRowCount == null ||
              conflictOnceOnRowCount == uploadedRows.length)) {
        _conflictDelivered = true;
        return _json({
          'error': 'push_conflict',
          'message': 'fixture conflict',
          'conflict': conflictOnce,
        }, statusCode: 409);
      }
      if (failFirstCommit && _commitAttempts == 1) {
        return _json({
          'error': 'temporary',
          'message': 'retry',
        }, statusCode: 500);
      }
      final bundleSeq =
          commitResponse?['bundle_seq'] as int? ?? _nextBundleSeq++;
      final committedRows = _defaultCommittedRows(bundleSeq);
      _uploadedRowsByBundleSeq[bundleSeq] = committedRows;
      _sourceBundleIdByBundleSeq[bundleSeq] =
          commitResponse?['source_bundle_id'] as int? ?? _activeSourceBundleId;
      final bundleHash = badCommittedBundleHash
          ? 'bad-hash'
          : _committedBundleHashForFixtureRows(committedRows);
      _bundleHashByBundleSeq[bundleSeq] = bundleHash;
      return _json(
        commitResponse ??
            {
              'bundle_seq': bundleSeq,
              'source_id': _sourceId,
              'source_bundle_id': _activeSourceBundleId,
              'row_count': uploadedRows.length,
              'bundle_hash': bundleHash,
            },
        sourceIdOverride: _sourceId,
      );
    }
    return _json({'error': 'not_found', 'message': path}, statusCode: 404);
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
  }) async {
    return const OversqliteHttpResponse(statusCode: 204, body: '');
  }

  OversqliteHttpResponse _json(
    Map<String, Object?> body, {
    int statusCode = 200,
    String? sourceIdOverride,
  }) {
    final responseBody = sourceIdOverride == null
        ? body
        : {...body, 'source_id': sourceIdOverride};
    return OversqliteHttpResponse(
      statusCode: statusCode,
      body: jsonEncode(responseBody),
    );
  }

  List<Map<String, Object?>> _defaultCommittedRows(int bundleSeq) {
    return [
      for (final row in uploadedRows)
        {
          'schema': row['schema'],
          'table': row['table'],
          'key': row['key'],
          'op': row['op'],
          'row_version': bundleSeq,
          'payload': row['payload'],
        },
    ];
  }
}

String committedBundleHashForFixtureRows(List<Map<String, Object?>> rows) {
  return sha256
      .convert(
        utf8.encode(
          _canonicalizeFixtureJson([
            for (var i = 0; i < rows.length; i++)
              {
                'row_ordinal': i,
                'schema': rows[i]['schema'],
                'table': rows[i]['table'],
                'key': rows[i]['key'],
                'op': rows[i]['op'],
                'row_version': rows[i]['row_version'],
                'payload': rows[i]['payload'],
              },
          ]),
        ),
      )
      .bytes
      .map((byte) => byte.toRadixString(16).padLeft(2, '0'))
      .join();
}

String _committedBundleHashForFixtureRows(List<Map<String, Object?>> rows) =>
    committedBundleHashForFixtureRows(rows);

String canonicalizeFixtureJson(Object? value) =>
    _canonicalizeFixtureJson(value);

String _canonicalizeFixtureJson(Object? value) {
  if (value == null) {
    return 'null';
  }
  if (value is String || value is bool) {
    return jsonEncode(value);
  }
  if (value is num) {
    return _canonicalizeFixtureJsonNumber(value.toString());
  }
  if (value is List) {
    return '[${value.map(_canonicalizeFixtureJson).join(',')}]';
  }
  if (value is Map) {
    final entries = value.entries.toList()
      ..sort(
        (left, right) => left.key.toString().compareTo(right.key.toString()),
      );
    return '{${entries.map((entry) => '${jsonEncode(entry.key.toString())}:${_canonicalizeFixtureJson(entry.value)}').join(',')}}';
  }
  return jsonEncode(value);
}

final _fixtureJsonNumberPattern = RegExp(
  r'^-?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?$',
);

String _canonicalizeFixtureJsonNumber(String value) {
  if (!_fixtureJsonNumberPattern.hasMatch(value)) {
    throw ArgumentError('invalid JSON number: $value');
  }
  final number = double.parse(value);
  if (!number.isFinite) {
    throw ArgumentError('JSON number must be finite: $value');
  }
  if (number == 0) {
    return '0';
  }

  final absNumber = number.abs();
  final rendered = number.toString().toLowerCase();
  if (absNumber >= 1e-6 && absNumber < 1e21) {
    return _normalizeFixturePlainNumber(_fixtureScientificToPlain(rendered));
  }
  return _normalizeFixtureScientificNumber(rendered);
}

String _fixtureScientificToPlain(String raw) {
  if (!raw.contains('e')) {
    return raw;
  }
  final sign = raw.startsWith('-') ? '-' : '';
  final unsigned = sign.isEmpty ? raw : raw.substring(1);
  final parts = unsigned.split('e');
  if (parts.length != 2) {
    throw ArgumentError('invalid scientific notation: $raw');
  }

  final mantissa = parts[0];
  final exponent = int.parse(parts[1]);
  final dotIndex = mantissa.indexOf('.');
  final digits = mantissa.replaceAll('.', '');
  final fractionalDigits = dotIndex >= 0 ? mantissa.length - dotIndex - 1 : 0;
  final decimalIndex = digits.length + exponent - fractionalDigits;

  if (decimalIndex <= 0) {
    return '${sign}0.${_zeroes(-decimalIndex)}$digits';
  }
  if (decimalIndex >= digits.length) {
    return '$sign$digits${_zeroes(decimalIndex - digits.length)}';
  }
  return '$sign${digits.substring(0, decimalIndex)}.'
      '${digits.substring(decimalIndex)}';
}

String _normalizeFixturePlainNumber(String raw) {
  final sign = raw.startsWith('-') ? '-' : '';
  final unsigned = sign.isEmpty ? raw : raw.substring(1);
  final dotIndex = unsigned.indexOf('.');
  final integerRaw = dotIndex >= 0 ? unsigned.substring(0, dotIndex) : unsigned;
  final fractionRaw = dotIndex >= 0 ? unsigned.substring(dotIndex + 1) : '';
  final integerPart = integerRaw.replaceFirst(RegExp(r'^0+'), '');
  final normalizedInteger = integerPart.isEmpty ? '0' : integerPart;
  final fractionalPart = fractionRaw.replaceFirst(RegExp(r'0+$'), '');
  if (normalizedInteger == '0' && fractionalPart.isEmpty) {
    return '0';
  }
  if (fractionalPart.isEmpty) {
    return '$sign$normalizedInteger';
  }
  return '$sign$normalizedInteger.$fractionalPart';
}

String _normalizeFixtureScientificNumber(String raw) {
  if (!raw.contains('e')) {
    return _fixturePlainToScientific(_normalizeFixturePlainNumber(raw));
  }
  final sign = raw.startsWith('-') ? '-' : '';
  final unsigned = sign.isEmpty ? raw : raw.substring(1);
  final parts = unsigned.split('e');
  if (parts.length != 2) {
    throw ArgumentError('invalid scientific notation: $raw');
  }

  final mantissa = _normalizeFixturePlainNumber(parts[0]);
  final exponent = int.parse(parts[1]);
  return '$sign$mantissa'
      'e${exponent >= 0 ? '+' : ''}$exponent';
}

String _fixturePlainToScientific(String raw) {
  final sign = raw.startsWith('-') ? '-' : '';
  final unsigned = sign.isEmpty ? raw : raw.substring(1);
  final dotIndex = unsigned.indexOf('.');
  final integerPart = dotIndex >= 0
      ? unsigned.substring(0, dotIndex)
      : unsigned;
  final fractionalPart = dotIndex >= 0 ? unsigned.substring(dotIndex + 1) : '';

  final firstIntegerNonZero = _firstNonZeroIndex(integerPart);
  if (firstIntegerNonZero >= 0) {
    final digits = (integerPart + fractionalPart).replaceFirst(
      RegExp(r'^0+'),
      '',
    );
    final exponent = integerPart.length - 1;
    return '$sign${_fixtureScientificMantissa(digits)}'
        'e+$exponent';
  }

  final firstFractionNonZero = _firstNonZeroIndex(fractionalPart);
  if (firstFractionNonZero < 0) {
    throw ArgumentError('plainToScientific requires a non-zero number: $raw');
  }
  final digits = fractionalPart.substring(firstFractionNonZero);
  final exponent = -(firstFractionNonZero + 1);
  return '$sign${_fixtureScientificMantissa(digits)}'
      'e$exponent';
}

int _firstNonZeroIndex(String value) {
  for (var i = 0; i < value.length; i++) {
    if (value.codeUnitAt(i) != 0x30) {
      return i;
    }
  }
  return -1;
}

String _fixtureScientificMantissa(String digits) {
  final head = digits.substring(0, 1);
  final tail = digits.substring(1);
  return tail.isEmpty ? head : '$head.$tail';
}

String _zeroes(int count) => List.filled(count, '0').join();

String? _nullableHex(Uint8List? bytes) {
  if (bytes == null) {
    return null;
  }
  return bytes
      .map((byte) => (byte & 0xff).toRadixString(16).padLeft(2, '0'))
      .join();
}

final class PullFixtureServer implements OversqliteHttpClient {
  PullFixtureServer({required this.pullResponse});

  final Map<String, Object?> pullResponse;
  var pullRequestCount = 0;

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
  }) async {
    if (path == 'sync/capabilities') {
      return _json({
        'protocol_version': 'v1',
        'schema_version': 1,
        'features': {'connect_lifecycle': true},
      });
    }
    if (path.startsWith('sync/pull')) {
      pullRequestCount++;
      return _json(pullResponse);
    }
    return _json({'error': 'not_found', 'message': path}, statusCode: 404);
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
  }) async {
    if (path == 'sync/connect') {
      return _json({'resolution': 'initialize_empty'});
    }
    return _json({'error': 'not_found', 'message': path}, statusCode: 404);
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
  }) async {
    return const OversqliteHttpResponse(statusCode: 204, body: '');
  }

  OversqliteHttpResponse _json(
    Map<String, Object?> body, {
    int statusCode = 200,
  }) {
    return OversqliteHttpResponse(
      statusCode: statusCode,
      body: jsonEncode(body),
    );
  }
}

final class WatchFixtureEnv {
  WatchFixtureEnv({
    required this.database,
    required this.server,
    required this.client,
  });

  final SqliteNowDatabase database;
  final WatchFixtureServer server;
  final DefaultOversqliteClient client;

  Future<void> close() async {
    await client.close();
    await database.close();
  }
}

Future<WatchFixtureEnv> newWatchFixtureEnv({
  OversqliteConfig config = usersConfig,
  bool bundleChangeWatchSupported = true,
}) async {
  final database = await openUsersDatabase();
  final server = WatchFixtureServer(
    bundleChangeWatchSupported: bundleChangeWatchSupported,
  );
  final client = DefaultOversqliteClient(
    database: database,
    config: config,
    httpClient: server,
  );
  await client.open();
  await client.attach('user-1');
  return WatchFixtureEnv(database: database, server: server, client: client);
}

final class WatchFixtureServer
    implements OversqliteHttpClient, OversqliteBundleChangeWatchTransport {
  WatchFixtureServer({required this.bundleChangeWatchSupported});

  final bool bundleChangeWatchSupported;
  final bundles = <Map<String, Object?>>[];
  final watchAfterBundleSeqs = <int>[];
  final capabilitySourceIds = <String>[];
  final watchSourceIds = <String>[];
  final _watchControllers = <StreamController<String>>[];
  final _watchResponses = <WatchFixtureResponse>[];
  var watchCloseCount = 0;
  var pullRequestCount = 0;
  var stableBundleSeq = 0;
  var _nextBundleSeq = 1;

  void enqueueWatchResponse({required int statusCode, required String body}) {
    _watchResponses.add(
      WatchFixtureResponse(statusCode: statusCode, body: body),
    );
  }

  void enqueueWatchLines(List<String> lines) {
    _watchResponses.add(WatchFixtureResponse(statusCode: 200, lines: lines));
  }

  void addRemoteBundle(List<Map<String, Object?>> rows) {
    final bundleSeq = _nextBundleSeq++;
    stableBundleSeq = bundleSeq;
    bundles.add({
      'bundle_seq': bundleSeq,
      'source_id': 'remote-source',
      'source_bundle_id': bundleSeq,
      'rows': [
        for (var i = 0; i < rows.length; i++)
          {...rows[i], 'row_version': rows[i]['row_version'] ?? i + 1},
      ],
    });
    final data = jsonEncode({
      'bundle_seq': bundleSeq,
      'source_id': 'remote-source',
      'source_bundle_id': bundleSeq,
    });
    for (final controller in _watchControllers) {
      if (!controller.isClosed) {
        controller
          ..add('event: bundle')
          ..add('data: $data')
          ..add('');
      }
    }
  }

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
  }) async {
    if (path == 'sync/capabilities') {
      capabilitySourceIds.add(sourceId);
      return _json({
        'protocol_version': 'v1',
        'schema_version': 1,
        'features': {
          'connect_lifecycle': true,
          if (bundleChangeWatchSupported) 'bundle_change_watch': true,
        },
      });
    }
    if (path.startsWith('sync/pull')) {
      pullRequestCount += 1;
      final uri = Uri.parse('http://local/$path');
      final after = int.parse(uri.queryParameters['after_bundle_seq'] ?? '0');
      final visible = bundles
          .where((bundle) => (bundle['bundle_seq']! as int) > after)
          .toList();
      return _json({
        'stable_bundle_seq': stableBundleSeq,
        'bundles': visible,
        'has_more': false,
      });
    }
    return _json({'error': 'not_found', 'message': path}, statusCode: 404);
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
  }) async {
    if (path == 'sync/connect') {
      return _json({'resolution': 'initialize_empty'});
    }
    return _json({'error': 'not_found', 'message': path}, statusCode: 404);
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
  }) async {
    return const OversqliteHttpResponse(statusCode: 204, body: '');
  }

  @override
  Future<OversqliteBundleChangeWatchResponse> watchBundleChanges({
    required String sourceId,
    required int afterBundleSeq,
  }) async {
    watchSourceIds.add(sourceId);
    watchAfterBundleSeqs.add(afterBundleSeq);
    if (_watchResponses.isNotEmpty) {
      final response = _watchResponses.removeAt(0);
      var closed = false;
      return OversqliteBundleChangeWatchResponse(
        statusCode: response.statusCode,
        body: response.body,
        lines: Stream<String>.fromIterable(response.lines),
        close: () async {
          if (!closed) {
            closed = true;
            watchCloseCount += 1;
          }
        },
      );
    }
    final controller = StreamController<String>();
    var closed = false;
    _watchControllers.add(controller);
    return OversqliteBundleChangeWatchResponse(
      statusCode: 200,
      lines: controller.stream,
      close: () async {
        if (!closed) {
          closed = true;
          watchCloseCount += 1;
        }
        if (!controller.isClosed) {
          await controller.close();
        }
      },
    );
  }

  OversqliteHttpResponse _json(
    Map<String, Object?> body, {
    int statusCode = 200,
  }) {
    return OversqliteHttpResponse(
      statusCode: statusCode,
      body: jsonEncode(body),
    );
  }
}

final class WatchFixtureResponse {
  const WatchFixtureResponse({
    required this.statusCode,
    this.body = '',
    this.lines = const [],
  });

  final int statusCode;
  final String body;
  final List<String> lines;
}
