import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';

void main() {
  test('automatic download config validates when worker starts', () async {
    final database = SqliteNowDatabase.inMemory();
    final client = DefaultOversqliteClient(
      database: database,
      config: OversqliteConfig(
        schema: 'main',
        syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
        automaticDownloadInterval: Duration.zero,
      ),
    );
    addTearDown(client.close);
    addTearDown(database.close);

    expect(() => client.startAutomaticDownloads(), throwsArgumentError);
  });

  test('automatic downloads repeated start returns active handle', () async {
    final env = await _newEnv();
    addTearDown(env.close);

    final first = env.client.startAutomaticDownloads();
    final second = env.client.startAutomaticDownloads();
    addTearDown(first.stop);

    expect(identical(first, second), isTrue);
  });

  test(
    'automatic downloads polling pulls through authoritative path',
    () async {
      final env = await _newEnv();
      addTearDown(env.close);
      final handle = env.client.startAutomaticDownloads();
      addTearDown(handle.stop);

      env.server.addRemoteBundle([
        _bundleRow('INSERT', {'id': 'u1'}, {'id': 'u1', 'name': 'Ada'}),
      ]);

      await _eventually(() async {
        return await _scalarInt(
              env.database,
              "SELECT COUNT(*) FROM users WHERE id = 'u1'",
            ) ==
            1;
      });

      expect(
        await _scalarText(
          env.database,
          "SELECT name FROM users WHERE id = 'u1'",
        ),
        'Ada',
      );
      expect(env.server.pullRequestCount, greaterThan(0));
      expect(env.server.watchAfterBundleSeqs, isEmpty);
    },
  );

  test(
    'automatic downloads polling propagates protocol mismatch without retry',
    () async {
      final env = await _newEnv();
      addTearDown(env.close);
      env.server.protocolVersion = 'v0';
      final initialCapabilityAttempts = env.server.capabilitySourceIds.length;
      final initialPullAttempts = env.server.pullRequestCount;

      final handle = env.client.startAutomaticDownloads();
      await expectLater(
        handle.done,
        throwsA(
          isA<ProtocolVersionMismatchException>()
              .having((error) => error.expected, 'expected', 'v1')
              .having((error) => error.actual, 'actual', 'v0'),
        ),
      );

      expect(
        env.server.capabilitySourceIds.length,
        initialCapabilityAttempts + 1,
      );
      expect(env.server.pullRequestCount, initialPullAttempts);
      expect(env.server.watchAfterBundleSeqs, isEmpty);
    },
  );

  test(
    'automatic downloads propagate table contract mismatch without retry',
    () async {
      final env = await _newEnv();
      addTearDown(env.close);
      env.server.registeredTableSpecs = phase4RegisteredTableSpecs([
        'monitoring_focus',
        'users',
      ]);
      final initialCapabilityAttempts = env.server.capabilitySourceIds.length;
      final initialPullAttempts = env.server.pullRequestCount;

      final handle = env.client.startAutomaticDownloads();
      await expectLater(
        handle.done,
        throwsA(
          isA<SyncTableContractMismatchException>().having(
            (error) => error.serverOnlyTables,
            'serverOnlyTables',
            ['main.monitoring_focus'],
          ),
        ),
      );

      expect(
        env.server.capabilitySourceIds.length,
        initialCapabilityAttempts + 1,
      );
      expect(env.server.pullRequestCount, initialPullAttempts);
      expect(env.server.watchAfterBundleSeqs, isEmpty);
    },
  );

  test('automatic downloads pause suppresses background pulls only', () async {
    final env = await _newEnv();
    addTearDown(env.close);
    await env.client.pauseDownloads();
    final handle = env.client.startAutomaticDownloads();
    addTearDown(handle.stop);

    env.server.addRemoteBundle([
      _bundleRow('INSERT', {'id': 'u1'}, {'id': 'u1', 'name': 'Ada'}),
    ]);
    await Future<void>.delayed(const Duration(milliseconds: 100));

    expect(
      await _scalarInt(
        env.database,
        "SELECT COUNT(*) FROM users WHERE id = 'u1'",
      ),
      0,
    );
    expect(env.server.pullRequestCount, 0);

    await env.client.pullToStable();
    expect(
      await _scalarText(env.database, "SELECT name FROM users WHERE id = 'u1'"),
      'Ada',
    );

    env.server.addRemoteBundle([
      _bundleRow('INSERT', {'id': 'u2'}, {'id': 'u2', 'name': 'Grace'}),
    ]);
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(
      await _scalarInt(
        env.database,
        "SELECT COUNT(*) FROM users WHERE id = 'u2'",
      ),
      0,
    );

    await env.client.resumeDownloads();
    await _eventually(() async {
      return await _scalarInt(
            env.database,
            "SELECT COUNT(*) FROM users WHERE id = 'u2'",
          ) ==
          1;
    });
  });

  test('automatic download stop cancels an active capacity delay', () async {
    final env = await _newEnv(
      config: OversqliteConfig(
        schema: 'main',
        syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
        automaticDownloadInterval: const Duration(milliseconds: 25),
        snapshotCapacityRetryPolicy: OversqliteSnapshotCapacityRetryPolicy(
          maxWait: const Duration(hours: 1),
          fallbackDelay: const Duration(minutes: 30),
          positiveJitterRatio: 0,
        ),
      ),
    );
    addTearDown(env.close);
    env.server
      ..historyPrunedOnPull = true
      ..snapshotCapacityOnCreate = true;
    final capacityTimerScheduled = Completer<void>();
    final handle = runZoned(
      env.client.startAutomaticDownloads,
      zoneSpecification: ZoneSpecification(
        createTimer: (self, parent, zone, duration, callback) {
          if (duration == const Duration(minutes: 30)) {
            if (!capacityTimerScheduled.isCompleted) {
              capacityTimerScheduled.complete();
            }
            return _NeverTimer();
          }
          return parent.createTimer(zone, duration, callback);
        },
      ),
    );

    await capacityTimerScheduled.future.timeout(const Duration(seconds: 2));
    final elapsed = Stopwatch()..start();
    await handle.stop().timeout(const Duration(seconds: 1));
    elapsed.stop();

    expect(env.server.snapshotSessionCreateCount, 1);
    expect(elapsed.elapsed, lessThan(const Duration(seconds: 1)));
    await expectLater(handle.done, completes);
  });

  test(
    'automatic downloads auto watch wakes and pulls authoritatively',
    () async {
      final env = await _newEnv(
        config: OversqliteConfig(
          schema: 'main',
          syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
          automaticDownloadInterval: Duration(seconds: 60),
          bundleChangeWatchMode: BundleChangeWatchMode.auto,
          bundleChangeWatchReconnectMin: Duration(milliseconds: 10),
          bundleChangeWatchReconnectMax: Duration(milliseconds: 20),
        ),
        bundleChangeWatchSupported: true,
      );
      addTearDown(env.close);
      final handle = env.client.startAutomaticDownloads();
      addTearDown(handle.stop);

      await _eventually(() async => env.server.watchAfterBundleSeqs.isNotEmpty);

      env.server.addRemoteBundle([
        _bundleRow('INSERT', {'id': 'u1'}, {'id': 'u1', 'name': 'Ada'}),
      ]);

      await _eventually(() async {
        return await _scalarInt(
              env.database,
              "SELECT COUNT(*) FROM users WHERE id = 'u1'",
            ) ==
            1;
      });

      expect(
        await _scalarText(
          env.database,
          "SELECT name FROM users WHERE id = 'u1'",
        ),
        'Ada',
      );
      expect(env.server.watchAfterBundleSeqs, contains(0));
      expect(env.server.pullRequestCount, greaterThan(0));
    },
  );

  test('automatic downloads stop closes active watch stream', () async {
    final env = await _newEnv(
      config: OversqliteConfig(
        schema: 'main',
        syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
        automaticDownloadInterval: Duration(seconds: 60),
        bundleChangeWatchMode: BundleChangeWatchMode.auto,
        bundleChangeWatchReconnectMin: Duration(milliseconds: 10),
        bundleChangeWatchReconnectMax: Duration(milliseconds: 20),
      ),
      bundleChangeWatchSupported: true,
    );
    addTearDown(env.close);
    final handle = env.client.startAutomaticDownloads();

    await _eventually(() async => env.server.watchAfterBundleSeqs.isNotEmpty);

    await handle.stop();

    expect(env.server.watchCloseCount, 1);
  });

  test(
    'automatic downloads do not poll while watch stream is healthy',
    () async {
      final env = await _newEnv(
        config: OversqliteConfig(
          schema: 'main',
          syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
          automaticDownloadInterval: Duration(milliseconds: 20),
          bundleChangeWatchMode: BundleChangeWatchMode.auto,
          bundleChangeWatchReconnectMin: Duration(milliseconds: 10),
          bundleChangeWatchReconnectMax: Duration(milliseconds: 20),
        ),
        bundleChangeWatchSupported: true,
      );
      addTearDown(env.close);
      final handle = env.client.startAutomaticDownloads();
      addTearDown(handle.stop);

      await _eventually(() async => env.server.watchAfterBundleSeqs.isNotEmpty);
      await Future<void>.delayed(const Duration(milliseconds: 80));

      expect(env.server.watchAfterBundleSeqs.length, 1);
      expect(env.server.pullRequestCount, 0);
    },
  );

  test('automatic downloads watch setup errors fall back to pull', () async {
    final setupErrors = <int, String>{
      400: 'invalid_request',
      401: 'authentication_failed',
      403: 'bundle_change_watch_forbidden',
      409: 'scope_uninitialized',
      503: 'bundle_change_watch_disabled',
      500: 'bundle_change_watch_failed',
    };

    for (final entry in setupErrors.entries) {
      final env = await _newEnv(
        config: _autoWatchConfig,
        bundleChangeWatchSupported: true,
      );
      AutomaticDownloadsHandle? handle;
      try {
        env.server.enqueueWatchResponse(
          statusCode: entry.key,
          body: jsonEncode({
            'error': entry.value,
            'message': 'test setup error',
          }),
        );

        final rowId = 'u-${entry.key}';
        env.server.addRemoteBundle([
          _bundleRow(
            'INSERT',
            {'id': rowId},
            {'id': rowId, 'name': 'Status ${entry.key}'},
          ),
        ]);

        handle = env.client.startAutomaticDownloads();

        await _eventually(() async {
          return await _scalarInt(
                env.database,
                "SELECT COUNT(*) FROM users WHERE id = '$rowId'",
              ) ==
              1;
        });

        expect(
          await _scalarText(
            env.database,
            "SELECT name FROM users WHERE id = '$rowId'",
          ),
          'Status ${entry.key}',
        );
        expect(env.server.watchAfterBundleSeqs, contains(0));
        expect(env.server.watchCloseCount, greaterThanOrEqualTo(1));
        expect(env.server.pullRequestCount, greaterThan(0));
      } finally {
        await handle?.stop();
        await env.close();
      }
    }
  });

  test(
    'automatic downloads close non-ok watch response before fallback',
    () async {
      final contract = _readWatchRuntimeCase(
        'non-ok-watch-response-closes-before-fallback',
      );
      final env = await _newEnv(
        config: _autoWatchConfig,
        bundleChangeWatchSupported: true,
      );
      addTearDown(env.close);
      env.server.enqueueWatchResponse(
        statusCode: contract.status,
        body: jsonEncode(contract.body),
      );
      env.server.addRemoteBundle([
        _bundleRow('INSERT', {'id': 'u1'}, {'id': 'u1', 'name': 'Ada'}),
      ]);

      final handle = env.client.startAutomaticDownloads();
      addTearDown(handle.stop);

      await _eventually(() async {
        return await _scalarInt(
              env.database,
              "SELECT COUNT(*) FROM users WHERE id = 'u1'",
            ) ==
            1;
      });

      expect(env.server.watchCloseCount, contract.expectedCloseCount);
      expect(
        env.server.pullRequestCount,
        greaterThanOrEqualTo(contract.expectedFallbackPullsAtLeast),
      );
    },
  );

  test(
    'automatic downloads watch reconnect uses fresh capabilities and latest durable state',
    () async {
      final env = await _newEnv(
        config: _autoWatchConfig,
        bundleChangeWatchSupported: true,
      );
      addTearDown(env.close);

      env.server.addRemoteBundle([
        _bundleRow('INSERT', {'id': 'u1'}, {'id': 'u1', 'name': 'Ada'}),
      ]);
      env.server.enqueueWatchLines([
        'event: bundle',
        'data: {"bundle_seq":1,"source_id":"remote-source","source_bundle_id":1}',
        '',
      ]);
      final initialCapabilityRequests = env.server.capabilitySourceIds.length;

      final handle = env.client.startAutomaticDownloads();
      addTearDown(handle.stop);

      await _eventually(() async {
        return env.server.watchAfterBundleSeqs.contains(1) &&
            env.server.capabilitySourceIds.length >=
                initialCapabilityRequests + 2;
      });

      expect(env.server.watchAfterBundleSeqs.first, 0);
      expect(env.server.watchAfterBundleSeqs, contains(1));
      expect(
        env.server.watchSourceIds.every((sourceId) => sourceId.isNotEmpty),
        isTrue,
      );
      expect(
        env.server.capabilitySourceIds.last,
        env.server.watchSourceIds.last,
      );
    },
  );

  test(
    'automatic downloads heartbeats do not trigger pulls by themselves',
    () async {
      final env = await _newEnv(
        config: OversqliteConfig(
          schema: 'main',
          syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
          automaticDownloadInterval: Duration(seconds: 60),
          bundleChangeWatchMode: BundleChangeWatchMode.auto,
          bundleChangeWatchReconnectMin: Duration(seconds: 1),
          bundleChangeWatchReconnectMax: Duration(seconds: 1),
        ),
        bundleChangeWatchSupported: true,
      );
      addTearDown(env.close);
      env.server.enqueueWatchLines([
        ': heartbeat',
        '',
        ': heartbeat',
        '',
        ': heartbeat',
        '',
      ]);

      final handle = env.client.startAutomaticDownloads();
      addTearDown(handle.stop);

      await _eventually(
        () async =>
            env.server.watchAfterBundleSeqs.length == 1 &&
            env.server.pullRequestCount == 1,
      );
      await Future<void>.delayed(const Duration(milliseconds: 100));

      expect(env.server.watchAfterBundleSeqs.length, 1);
      expect(env.server.pullRequestCount, 1);
    },
  );

  test(
    'automatic downloads stream EOF and malformed stream reconnect without stalling pulls',
    () async {
      final streamLines = <List<String>>[
        [': stream ended', ''],
        ['event: bundle', 'data: {"bundle_seq":', ''],
      ];

      for (final lines in streamLines) {
        final env = await _newEnv(
          config: _autoWatchConfig,
          bundleChangeWatchSupported: true,
        );
        AutomaticDownloadsHandle? handle;
        try {
          env.server.enqueueWatchLines(lines);
          env.server.addRemoteBundle([
            _bundleRow('INSERT', {'id': 'u1'}, {'id': 'u1', 'name': 'Ada'}),
          ]);

          handle = env.client.startAutomaticDownloads();

          await _eventually(() async {
            return env.server.watchAfterBundleSeqs.length >= 2 &&
                await _scalarInt(
                      env.database,
                      "SELECT COUNT(*) FROM users WHERE id = 'u1'",
                    ) ==
                    1;
          });
          expect(env.server.pullRequestCount, greaterThan(0));
        } finally {
          await handle?.stop();
          await env.close();
        }
      }
    },
  );
}

final class _NeverTimer implements Timer {
  var _active = true;

  @override
  bool get isActive => _active;

  @override
  int get tick => 0;

  @override
  void cancel() => _active = false;
}

Future<_Env> _newEnv({
  OversqliteConfig? config,
  bool bundleChangeWatchSupported = false,
}) async {
  final database = await _openUsersDatabase();
  final server = _WatchSyncServer(
    bundleChangeWatchSupported: bundleChangeWatchSupported,
  );
  final client = DefaultOversqliteClient(
    database: database,
    config: config ?? _usersConfig,
    httpClient: server,
  );
  await client.open();
  await client.attach('user-1');
  return _Env(database: database, server: server, client: client);
}

final _usersConfig = OversqliteConfig(
  schema: 'main',
  syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
  automaticDownloadInterval: Duration(milliseconds: 25),
  bundleChangeWatchReconnectMin: Duration(milliseconds: 10),
  bundleChangeWatchReconnectMax: Duration(milliseconds: 20),
);

final _autoWatchConfig = OversqliteConfig(
  schema: 'main',
  syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
  automaticDownloadInterval: Duration(seconds: 60),
  bundleChangeWatchMode: BundleChangeWatchMode.auto,
  bundleChangeWatchReconnectMin: Duration(milliseconds: 10),
  bundleChangeWatchReconnectMax: Duration(milliseconds: 20),
);

final class _Env {
  const _Env({
    required this.database,
    required this.server,
    required this.client,
  });

  final SqliteNowDatabase database;
  final _WatchSyncServer server;
  final DefaultOversqliteClient client;

  Future<void> close() async {
    await client.close();
    await database.close();
  }
}

final class _WatchSyncServer
    implements OversqliteHttpClient, OversqliteBundleChangeWatchTransport {
  _WatchSyncServer({required this.bundleChangeWatchSupported});

  final bool bundleChangeWatchSupported;
  final bundles = <Map<String, Object?>>[];
  final watchAfterBundleSeqs = <int>[];
  final capabilitySourceIds = <String>[];
  final watchSourceIds = <String>[];
  final _watchControllers = <StreamController<String>>[];
  final _watchResponses = <_WatchResponse>[];
  var watchCloseCount = 0;
  var protocolVersion = 'v1';
  var registeredTableSpecs = phase4RegisteredTableSpecs(['users']);
  var historyPrunedOnPull = false;
  var snapshotCapacityOnCreate = false;
  var pullRequestCount = 0;
  var snapshotSessionCreateCount = 0;
  var stableBundleSeq = 0;
  var _nextBundleSeq = 1;
  final snapshotCapacityReached = Completer<void>();

  void enqueueWatchResponse({required int statusCode, required String body}) {
    _watchResponses.add(_WatchResponse(statusCode: statusCode, body: body));
  }

  void enqueueWatchLines(List<String> lines) {
    _watchResponses.add(_WatchResponse(statusCode: 200, lines: lines));
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
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    if (path == 'sync/capabilities') {
      capabilitySourceIds.add(sourceId);
      return _json(
        phase4CapabilitiesResponse(
          protocolVersion: protocolVersion,
          registeredTableSpecs: registeredTableSpecs,
          features: {
            'connect_lifecycle': true,
            if (bundleChangeWatchSupported) 'bundle_change_watch': true,
          },
        ),
      );
    }
    if (path.startsWith('sync/pull')) {
      pullRequestCount += 1;
      if (historyPrunedOnPull) {
        return _json({
          'error': 'history_pruned',
          'message': 'history pruned',
        }, statusCode: 409);
      }
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
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    if (path == 'sync/connect') {
      return _json({'resolution': 'initialize_empty'});
    }
    if (path == 'sync/snapshot-sessions') {
      snapshotSessionCreateCount++;
      if (snapshotCapacityOnCreate) {
        if (!snapshotCapacityReached.isCompleted) {
          snapshotCapacityReached.complete();
        }
        return _json({
          'error': 'snapshot_build_capacity',
          'message': 'busy',
        }, statusCode: 429);
      }
    }
    return _json({'error': 'not_found', 'message': path}, statusCode: 404);
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
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

final class _WatchResponse {
  const _WatchResponse({
    required this.statusCode,
    this.body = '',
    this.lines = const [],
  });

  final int statusCode;
  final String body;
  final List<String> lines;
}

_WatchRuntimeContract _readWatchRuntimeCase(String name) {
  final file = _repoRoot().uri
      .resolve('oversqlite-contracts/watch/basic.json')
      .toFilePath();
  final raw = jsonDecode(File(file).readAsStringSync()) as Map<String, Object?>;
  final cases = (raw['runtimeCases']! as List<Object?>)
      .cast<Map<String, Object?>>();
  final item = cases.singleWhere((item) => item['name'] == name);
  final response = (item['response']! as Map).cast<String, Object?>();
  return _WatchRuntimeContract(
    status: response['status']! as int,
    body: (response['body']! as Map).cast<String, Object?>(),
    expectedCloseCount: item['expectedCloseCount']! as int,
    expectedFallbackPullsAtLeast: item['expectedFallbackPullsAtLeast']! as int,
  );
}

Directory _repoRoot() {
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

final class _WatchRuntimeContract {
  const _WatchRuntimeContract({
    required this.status,
    required this.body,
    required this.expectedCloseCount,
    required this.expectedFallbackPullsAtLeast,
  });

  final int status;
  final Map<String, Object?> body;
  final int expectedCloseCount;
  final int expectedFallbackPullsAtLeast;
}

Future<SqliteNowDatabase> _openUsersDatabase() async {
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

Map<String, Object?> _bundleRow(
  String op,
  Map<String, String> key,
  Map<String, Object?>? payload,
) {
  return {
    'schema': 'main',
    'table': 'users',
    'key': key,
    'op': op,
    'row_version': 1,
    'payload': payload,
  };
}

Future<int> _scalarInt(SqliteNowDatabase database, String sql) async {
  final rows = await database.connection.select(sql, (row) => row.readInt(0));
  return rows.single;
}

Future<String> _scalarText(SqliteNowDatabase database, String sql) async {
  final rows = await database.connection.select(
    sql,
    (row) => row.readString(0),
  );
  return rows.single;
}

Future<void> _eventually(
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
  fail('condition was not met within $timeout');
}
