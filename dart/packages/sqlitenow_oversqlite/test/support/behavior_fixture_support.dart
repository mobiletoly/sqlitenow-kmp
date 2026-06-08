import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

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

const usersConfig = OversqliteConfig(
  schema: 'main',
  syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
  automaticDownloadInterval: Duration(seconds: 10),
  bundleChangeWatchMode: BundleChangeWatchMode.auto,
  bundleChangeWatchReconnectMin: Duration(milliseconds: 10),
  bundleChangeWatchReconnectMax: Duration(milliseconds: 20),
);

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
    this.alreadyCommitted = false,
    this.createResponse,
    this.commitResponse,
    this.committedRowsResponse,
  });

  final bool failFirstCommit;
  final bool failFirstCommittedFetch;
  final bool pruneFirstCommittedFetch;
  final bool alreadyCommitted;
  final Map<String, Object?>? createResponse;
  final Map<String, Object?>? commitResponse;
  final Map<String, Object?>? committedRowsResponse;
  final List<Map<String, Object?>> createRequests = [];
  final List<Map<String, Object?>> uploadedRows = [];
  var _commitAttempts = 0;
  var _committedFetchAttempts = 0;
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
    if (path.startsWith('sync/committed-bundles/1/rows')) {
      _committedFetchAttempts++;
      if (failFirstCommittedFetch && _committedFetchAttempts == 1) {
        return _json({
          'error': 'temporary',
          'message': 'retry',
        }, statusCode: 500);
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
      return _json({
        'bundle_seq': 1,
        'source_id': _sourceId,
        'source_bundle_id': 1,
        'row_count': uploadedRows.length,
        'bundle_hash': 'test-hash',
        'rows': [
          for (var i = 0; i < uploadedRows.length; i++)
            {
              'schema': uploadedRows[i]['schema'],
              'table': uploadedRows[i]['table'],
              'key': uploadedRows[i]['key'],
              'op': uploadedRows[i]['op'],
              'row_version': i + 1,
              'payload': uploadedRows[i]['payload'],
            },
        ],
        'next_row_ordinal': uploadedRows.isEmpty ? -1 : uploadedRows.length - 1,
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
      if (alreadyCommitted) {
        return _json(
          createResponse ??
              {
                'status': 'already_committed',
                'bundle_seq': 1,
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
      if (failFirstCommit && _commitAttempts == 1) {
        return _json({
          'error': 'temporary',
          'message': 'retry',
        }, statusCode: 500);
      }
      return _json(
        commitResponse ??
            {
              'bundle_seq': 1,
              'source_id': _sourceId,
              'source_bundle_id': 1,
              'row_count': uploadedRows.length,
              'bundle_hash': 'test-hash',
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
}) async {
  final database = await openUsersDatabase();
  final server = WatchFixtureServer(bundleChangeWatchSupported: true);
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
