import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';

void main() {
  final spec = _readFixture();
  final cases = (spec['cases']! as List<Object?>).cast<Map<String, Object?>>();

  test(
    'Dart shared invalidation behavior fixtures execute against runtime',
    () async {
      for (final fixture in cases) {
        await _runCase(fixture);
      }
    },
  );
}

Future<void> _runCase(Map<String, Object?> fixture) async {
  switch (fixture['action']) {
    case 'incrementalPull':
      await _incrementalPull(fixture);
    case 'snapshotHydrate':
      await _snapshotHydrate(fixture);
    case 'committedReplay':
      await _committedReplay(fixture);
    case 'conflictRewrite':
      await _conflictRewrite(fixture);
    case 'syncUnion':
      await _syncUnion(fixture);
    case 'successfulDetach':
      await _successfulDetach(fixture);
    case 'blockedDetach':
      await _blockedDetach(fixture);
    case 'noopDetach':
      await _noopDetach(fixture);
    default:
      fail('${fixture['name']}: unknown action ${fixture['action']}');
  }
}

Future<void> _incrementalPull(Map<String, Object?> fixture) async {
  await _withClient((database, client, server) async {
    await client.open();
    await client.attach('user-1');
    server.addRemoteBundle([
      _bundleRow('INSERT', {'id': 'u1'}, {'id': 'u1', 'name': 'Ada'}),
    ]);

    await _expectInvalidation(database, fixture, () async {
      await client.pullToStable();
    });
  });
}

Future<void> _snapshotHydrate(Map<String, Object?> fixture) async {
  final server = _InvalidationServer(historyPrunedOnPull: true)
    ..stableBundleSeq = 7
    ..snapshotRows = [
      _snapshotRow(
        {'id': 'u1'},
        {'id': 'u1', 'name': 'Snapshot'},
        rowVersion: 7,
      ),
    ];
  await _withClient((database, client, _) async {
    await client.open();
    await client.attach('user-1');

    await _expectInvalidation(database, fixture, () async {
      await client.pullToStable();
    });
  }, server: server);
}

Future<void> _committedReplay(Map<String, Object?> fixture) async {
  await _withClient((database, client, _) async {
    await client.open();
    await client.attach('user-1');

    await _expectInvalidation(database, fixture, () async {
      await database.connection.execute(
        "INSERT INTO users(id, name) VALUES('u1', 'Ada')",
      );
      await client.pushPending();
    });
  });
}

Future<void> _conflictRewrite(Map<String, Object?> fixture) async {
  await _withClient((database, client, server) async {
    await client.open();
    await client.attach('user-1');
    server.addRemoteBundle([
      _bundleRow('INSERT', {'id': 'u1'}, {'id': 'u1', 'name': 'Original'}),
    ]);
    await client.pullToStable();

    await _expectInvalidation(database, fixture, () async {
      await database.connection.execute(
        "UPDATE users SET name = 'Local Edit' WHERE id = 'u1'",
      );
      server.conflictOnce = {
        'schema': 'main',
        'table': 'users',
        'key': {'id': 'u1'},
        'op': 'UPDATE',
        'base_row_version': 1,
        'server_row_version': 99,
        'server_row_deleted': false,
        'server_row': {'id': 'u1', 'name': 'Server Wins'},
      };
      await client.pushPending();
    });
  });
}

Future<void> _syncUnion(Map<String, Object?> fixture) async {
  await _withClient((database, client, server) async {
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('local-user', 'Local User')",
    );
    await database.connection.execute(
      "INSERT INTO posts(id, user_id, title) VALUES('p1', 'local-user', 'Local Post')",
    );
    server.addRemoteBundle([
      _bundleRow(
        'INSERT',
        {'id': 'remote-user'},
        {'id': 'remote-user', 'name': 'Remote User'},
      ),
    ]);

    await _expectInvalidation(database, fixture, () async {
      await client.sync();
    });
  });
}

Future<void> _successfulDetach(Map<String, Object?> fixture) async {
  await _withClient((database, client, server) async {
    await client.open();
    await client.attach('user-1');
    server.addRemoteBundle([
      _bundleRow('INSERT', {'id': 'u1'}, {'id': 'u1', 'name': 'Ada'}),
    ]);
    await client.pullToStable();

    await _expectInvalidation(database, fixture, () async {
      expect(await client.detach(), DetachOutcome.detached);
    });
  });
}

Future<void> _blockedDetach(Map<String, Object?> fixture) async {
  await _withClient((database, client, _) async {
    await client.open();
    await client.attach('user-1');

    await _expectInvalidation(database, fixture, () async {
      await database.connection.execute(
        "INSERT INTO users(id, name) VALUES('u1', 'Unsynced')",
      );
      expect(await client.detach(), DetachOutcome.blockedUnsyncedData);
    });
  });
}

Future<void> _noopDetach(Map<String, Object?> fixture) async {
  await _withClient((database, client, _) async {
    await client.open();
    await database.connection.execute('''UPDATE _sync_operation_state
SET kind = 'remote_replace',
    target_user_id = 'user-1',
    staged_snapshot_id = '',
    snapshot_bundle_seq = 0,
    snapshot_row_count = 0
WHERE singleton_key = 1''');

    await _expectInvalidation(database, fixture, () async {
      expect(await client.detach(), DetachOutcome.detached);
    });
  });
}

Future<void> _expectInvalidation(
  SqliteNowDatabase database,
  Map<String, Object?> fixture,
  Future<void> Function() action,
) async {
  final watchTables = (fixture['watchTables']! as List<Object?>)
      .cast<String>()
      .toSet();
  final emitted = Completer<void>();
  final subscription = database.invalidationTracker
      .watchTables(watchTables)
      .listen((_) {
        if (!emitted.isCompleted) {
          emitted.complete();
        }
      });
  try {
    await Future<void>.delayed(Duration.zero);
    await action();
    final expectedEmission = fixture['expectedEmission']! as bool;
    if (expectedEmission) {
      await emitted.future.timeout(const Duration(seconds: 5));
    } else {
      final didEmit = await Future.any([
        emitted.future.then((_) => true),
        Future<bool>.delayed(const Duration(milliseconds: 300), () => false),
      ]);
      expect(didEmit, isFalse, reason: fixture['name']! as String);
    }
  } finally {
    await subscription.cancel();
  }
}

Future<void> _withClient(
  Future<void> Function(
    SqliteNowDatabase database,
    DefaultOversqliteClient client,
    _InvalidationServer server,
  )
  block, {
  _InvalidationServer? server,
}) async {
  final database = await openBehaviorDatabase('users-posts');
  final fixtureServer = server ?? _InvalidationServer();
  final client = DefaultOversqliteClient(
    database: database,
    config: const OversqliteConfig(
      schema: 'main',
      syncTables: [
        SyncTable(tableName: 'users', syncKeyColumnName: 'id'),
        SyncTable(tableName: 'posts', syncKeyColumnName: 'id'),
      ],
    ),
    httpClient: fixtureServer,
  );
  try {
    await block(database, client, fixtureServer);
  } finally {
    await client.close();
    await database.close();
  }
}

Map<String, Object?> _bundleRow(
  String op,
  Map<String, String> key,
  Map<String, Object?>? payload, {
  String table = 'users',
  int? rowVersion,
}) {
  return {
    'schema': 'main',
    'table': table,
    'key': key,
    'op': op,
    'row_version': rowVersion ?? 1,
    'payload': payload,
  };
}

Map<String, Object?> _snapshotRow(
  Map<String, String> key,
  Map<String, Object?> payload, {
  String table = 'users',
  int? rowVersion,
}) {
  return {
    'schema': 'main',
    'table': table,
    'key': key,
    'row_version': rowVersion ?? 1,
    'payload': payload,
  };
}

Map<String, Object?> _readFixture() {
  final file = File.fromUri(
    repoRoot().uri.resolve(
      'oversqlite-contracts/invalidation/behavior/basic.json',
    ),
  );
  return jsonDecode(file.readAsStringSync()) as Map<String, Object?>;
}

final class _InvalidationServer implements OversqliteHttpClient {
  _InvalidationServer({this.historyPrunedOnPull = false});

  final bool historyPrunedOnPull;
  final bundles = <Map<String, Object?>>[];
  final uploadedRows = <Map<String, Object?>>[];
  List<Map<String, Object?>> snapshotRows = [];
  Map<String, Object?>? conflictOnce;
  var stableBundleSeq = 0;
  var _sourceId = '';
  var _nextBundleSeq = 1;
  var _conflictDelivered = false;

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
  }

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
    if (path.startsWith('sync/pull')) {
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
    if (path.startsWith('sync/snapshot-sessions/snapshot-1')) {
      final uri = Uri.parse('http://local/$path');
      final after = int.parse(uri.queryParameters['after_row_ordinal'] ?? '0');
      final maxRows = int.parse(uri.queryParameters['max_rows'] ?? '1000');
      final rows = snapshotRows.skip(after).take(maxRows).toList();
      return _json({
        'snapshot_id': 'snapshot-1',
        'snapshot_bundle_seq': stableBundleSeq,
        'rows': rows,
        'next_row_ordinal': after + rows.length,
        'has_more': after + rows.length < snapshotRows.length,
      });
    }
    if (path.startsWith('sync/committed-bundles/')) {
      final bundleSeq = int.parse(
        path.substring('sync/committed-bundles/'.length).split('/').first,
      );
      final rows = [
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
      return _json({
        'bundle_seq': bundleSeq,
        'source_id': _sourceId,
        'source_bundle_id': 1,
        'row_count': rows.length,
        'bundle_hash': committedBundleHashForFixtureRows(rows),
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
    if (path == 'sync/snapshot-sessions') {
      return _json({
        'snapshot_id': 'snapshot-1',
        'snapshot_bundle_seq': stableBundleSeq,
        'row_count': snapshotRows.length,
        'byte_count': 0,
        'expires_at': '2099-01-01T00:00:00Z',
      });
    }
    if (path == 'sync/push-sessions') {
      uploadedRows.clear();
      final request = (body! as Map).cast<String, Object?>();
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
      if (conflictOnce != null && !_conflictDelivered) {
        _conflictDelivered = true;
        return _json({
          'error': 'push_conflict',
          'message': 'fixture conflict',
          'conflict': conflictOnce,
        }, statusCode: 409);
      }
      stableBundleSeq = stableBundleSeq == 0 ? 1 : stableBundleSeq + 1;
      final committedRows = [
        for (final row in uploadedRows)
          {
            'schema': row['schema'],
            'table': row['table'],
            'key': row['key'],
            'op': row['op'],
            'row_version': stableBundleSeq,
            'payload': row['payload'],
          },
      ];
      bundles.add({
        'bundle_seq': stableBundleSeq,
        'source_id': _sourceId,
        'source_bundle_id': 1,
        'rows': committedRows,
      });
      snapshotRows = [
        for (final row in uploadedRows)
          if (row['op'] != 'DELETE')
            {
              'schema': row['schema'],
              'table': row['table'],
              'key': row['key'],
              'row_version': stableBundleSeq,
              'payload': row['payload'],
            },
      ];
      return _json({
        'bundle_seq': stableBundleSeq,
        'source_id': _sourceId,
        'source_bundle_id': 1,
        'row_count': uploadedRows.length,
        'bundle_hash': committedBundleHashForFixtureRows(committedRows),
      });
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
