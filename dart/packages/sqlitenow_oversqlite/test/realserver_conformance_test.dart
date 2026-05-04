import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

void main() {
  final realserverEnabled = _flagEnabled('OVERSQLITE_REALSERVER_TESTS');
  group(
    'realserver conformance',
    skip: realserverEnabled
        ? null
        : 'Set OVERSQLITE_REALSERVER_TESTS=true to run live realserver tests.',
    () {
      late _RealServerConfig config;

      setUpAll(() async {
        config = await _requireRealServerConfig();
      });

      test('open connect push pull and fresh attach converge', () async {
        await _resetRealServerState(config.baseUrl);
        final userId = _randomId('dart-realserver-user');
        final dbA = await _openBusinessDatabase();
        final dbB = await _openBusinessDatabase();
        final dbC = await _openBusinessDatabase();
        addTearDown(dbA.close);
        addTearDown(dbB.close);
        addTearDown(dbC.close);

        final sourceA = await _bootstrapManagedSourceId(dbA);
        final sourceB = await _bootstrapManagedSourceId(dbB);
        final sourceC = await _bootstrapManagedSourceId(dbC);
        final httpA = await _authenticatedHttp(config.baseUrl, userId, sourceA);
        final httpB = await _authenticatedHttp(config.baseUrl, userId, sourceB);
        final httpC = await _authenticatedHttp(config.baseUrl, userId, sourceC);
        addTearDown(httpA.close);
        addTearDown(httpB.close);
        addTearDown(httpC.close);

        final clientA = _newRealServerClient(dbA, httpA);
        final clientB = _newRealServerClient(dbB, httpB);
        final clientC = _newRealServerClient(dbC, httpC);
        addTearDown(clientA.close);
        addTearDown(clientB.close);
        addTearDown(clientC.close);

        await clientA.open();
        await _expectConnected(
          clientA.attach(userId),
          AttachOutcome.startedEmpty,
        );
        await clientB.open();
        await _expectConnected(
          clientB.attach(userId),
          AttachOutcome.usedRemoteState,
        );

        final rowUserId = _uuid();
        final rowPostId = _uuid();
        await _insertBusinessUserAndPost(dbA, rowUserId, rowPostId, 'smoke');

        expect((await clientA.pushPending()).outcome, PushOutcome.committed);
        expect(
          (await clientB.pullToStable()).outcome,
          RemoteSyncOutcome.appliedIncremental,
        );

        await clientC.open();
        await _expectConnected(
          clientC.attach(userId),
          AttachOutcome.usedRemoteState,
        );

        expect((await clientA.syncStatus()).lastBundleSeqSeen, 1);
        expect((await clientB.syncStatus()).lastBundleSeqSeen, 1);
        expect((await clientC.syncStatus()).lastBundleSeqSeen, 1);
        expect(
          await _scalarText(
            dbB,
            "SELECT name FROM users WHERE id = '$rowUserId'",
          ),
          'User smoke',
        );
        expect(
          await _scalarText(
            dbC,
            "SELECT content FROM posts WHERE id = '$rowPostId'",
          ),
          'Payload smoke',
        );
      });

      test('retry later pending sync status and blocked detach work', () async {
        await _resetRealServerState(config.baseUrl);
        final userId = _randomId('dart-lease-user');
        final dbA = await _openBusinessDatabase();
        final dbB = await _openBusinessDatabase();
        addTearDown(dbA.close);
        addTearDown(dbB.close);

        final sourceA = await _bootstrapManagedSourceId(dbA);
        final sourceB = await _bootstrapManagedSourceId(dbB);
        final httpA = await _authenticatedHttp(config.baseUrl, userId, sourceA);
        final httpB = await _authenticatedHttp(config.baseUrl, userId, sourceB);
        addTearDown(httpA.close);
        addTearDown(httpB.close);
        final clientA = _newRealServerClient(dbA, httpA);
        final clientB = _newRealServerClient(dbB, httpB);
        addTearDown(clientA.close);
        addTearDown(clientB.close);

        await _insertBusinessUserAndPost(dbA, _uuid(), _uuid(), 'local-seed');
        await clientA.open();
        await _expectConnected(
          clientA.attach(userId),
          AttachOutcome.seededFromLocal,
        );

        final pending = (await clientA.syncStatus()).pending;
        expect(pending.hasPendingSyncData, isTrue);
        expect(pending.pendingRowCount, greaterThan(0));
        expect(pending.blocksDetach, isTrue);
        expect(await clientA.detach(), DetachOutcome.blockedUnsyncedData);

        await clientB.open();
        final retry = await clientB.attach(userId);
        expect(retry, isA<AttachRetryLater>());

        expect((await clientA.pushPending()).outcome, PushOutcome.committed);
        await _expectConnected(
          clientB.attach(userId),
          AttachOutcome.usedRemoteState,
        );
        expect(
          await _scalarText(dbB, 'SELECT name FROM users LIMIT 1'),
          'User local-seed',
        );
      });

      test('local seed then remote authoritative restore works', () async {
        await _resetRealServerState(config.baseUrl);
        final seedUserId = _randomId('dart-seed-user');
        final restoredUserId = _randomId('dart-restore-user');
        final installDb = await _openBusinessDatabase();
        final remoteSeedDb = await _openBusinessDatabase();
        final verifyDb = await _openBusinessDatabase();
        addTearDown(installDb.close);
        addTearDown(remoteSeedDb.close);
        addTearDown(verifyDb.close);

        final installSeedSource = await _bootstrapManagedSourceId(installDb);
        final remoteSeedSource = await _bootstrapManagedSourceId(remoteSeedDb);
        final verifySource = await _bootstrapManagedSourceId(verifyDb);

        final installSeedHttp = await _authenticatedHttp(
          config.baseUrl,
          seedUserId,
          installSeedSource,
        );
        addTearDown(installSeedHttp.close);
        final installSeedClient = _newRealServerClient(
          installDb,
          installSeedHttp,
        );
        addTearDown(installSeedClient.close);

        final localOnlyUserId = _uuid();
        await _insertBusinessUserAndPost(
          installDb,
          localOnlyUserId,
          _uuid(),
          'install-local-seed',
        );
        await installSeedClient.open();
        await _expectConnected(
          installSeedClient.attach(seedUserId),
          AttachOutcome.seededFromLocal,
        );
        expect(
          (await installSeedClient.pushPending()).outcome,
          PushOutcome.committed,
        );
        expect(await installSeedClient.detach(), DetachOutcome.detached);

        final installRestoreSource = await _currentSourceId(installDb);
        expect(installRestoreSource, isNot(installSeedSource));

        final remoteSeedHttp = await _authenticatedHttp(
          config.baseUrl,
          restoredUserId,
          remoteSeedSource,
        );
        addTearDown(remoteSeedHttp.close);
        final remoteSeedClient = _newRealServerClient(
          remoteSeedDb,
          remoteSeedHttp,
        );
        addTearDown(remoteSeedClient.close);
        final remoteUserId = _uuid();
        await remoteSeedClient.open();
        await _expectConnected(
          remoteSeedClient.attach(restoredUserId),
          AttachOutcome.startedEmpty,
        );
        await _insertBusinessUserAndPost(
          remoteSeedDb,
          remoteUserId,
          _uuid(),
          'remote-authoritative-seed',
        );
        expect(
          (await remoteSeedClient.pushPending()).outcome,
          PushOutcome.committed,
        );

        final restoreHttp = await _authenticatedHttp(
          config.baseUrl,
          restoredUserId,
          installRestoreSource,
        );
        addTearDown(restoreHttp.close);
        final restoreClient = _newRealServerClient(installDb, restoreHttp);
        addTearDown(restoreClient.close);
        await restoreClient.open();
        await _expectConnected(
          restoreClient.attach(restoredUserId),
          AttachOutcome.usedRemoteState,
        );
        expect(
          await _scalarInt(
            installDb,
            "SELECT COUNT(*) FROM users WHERE id = '$localOnlyUserId'",
          ),
          0,
        );
        expect(
          await _scalarText(
            installDb,
            "SELECT name FROM users WHERE id = '$remoteUserId'",
          ),
          'User remote-authoritative-seed',
        );

        final verifyHttp = await _authenticatedHttp(
          config.baseUrl,
          restoredUserId,
          verifySource,
        );
        addTearDown(verifyHttp.close);
        final verifyClient = _newRealServerClient(verifyDb, verifyHttp);
        addTearDown(verifyClient.close);
        await verifyClient.open();
        await _expectConnected(
          verifyClient.attach(restoredUserId),
          AttachOutcome.usedRemoteState,
        );
        expect(
          await _scalarText(
            verifyDb,
            "SELECT name FROM users WHERE id = '$remoteUserId'",
          ),
          'User remote-authoritative-seed',
        );
      });

      test('history pruned pull rebuilds from snapshot', () async {
        await _resetRealServerState(config.baseUrl);
        final userId = _randomId('dart-prune-user');
        final leaderDb = await _openBusinessDatabase();
        final followerDb = await _openBusinessDatabase();
        addTearDown(leaderDb.close);
        addTearDown(followerDb.close);

        final leaderSource = await _bootstrapManagedSourceId(leaderDb);
        final followerSource = await _bootstrapManagedSourceId(followerDb);
        final leaderHttp = await _authenticatedHttp(
          config.baseUrl,
          userId,
          leaderSource,
        );
        final followerHttp = await _authenticatedHttp(
          config.baseUrl,
          userId,
          followerSource,
        );
        addTearDown(leaderHttp.close);
        addTearDown(followerHttp.close);
        final leader = _newRealServerClient(leaderDb, leaderHttp);
        final follower = _newRealServerClient(followerDb, followerHttp);
        addTearDown(leader.close);
        addTearDown(follower.close);

        await leader.open();
        await _expectConnected(
          leader.attach(userId),
          AttachOutcome.startedEmpty,
        );
        await follower.open();
        await _expectConnected(
          follower.attach(userId),
          AttachOutcome.usedRemoteState,
        );

        for (var round = 1; round <= 4; round++) {
          await _insertBusinessUserAndPost(
            leaderDb,
            _uuid(),
            _uuid(),
            'prune-$round',
          );
          expect((await leader.pushPending()).outcome, PushOutcome.committed);
        }
        final leaderSeq = (await leader.syncStatus()).lastBundleSeqSeen;
        expect(leaderSeq, 4);

        await _setRetainedBundleFloor(config.baseUrl, userId, leaderSeq);

        final beforeSource = await _currentSourceId(followerDb);
        final report = await follower.pullToStable();

        expect(report.outcome, RemoteSyncOutcome.appliedSnapshot);
        expect((await follower.syncStatus()).lastBundleSeqSeen, leaderSeq);
        expect(await _currentSourceId(followerDb), beforeSource);
        expect(await _scalarInt(followerDb, 'SELECT COUNT(*) FROM users'), 4);
        expect(
          await _scalarInt(followerDb, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
          0,
        );
        expect(
          await _scalarInt(
            followerDb,
            'SELECT COUNT(*) FROM _sync_snapshot_stage',
          ),
          0,
        );
      });

      test('client-wins conflict converges through real server', () async {
        await _resetRealServerState(config.baseUrl);
        final userId = _randomId('dart-conflict-user');
        final dbA = await _openBusinessDatabase();
        final dbB = await _openBusinessDatabase();
        final observerDb = await _openBusinessDatabase();
        addTearDown(dbA.close);
        addTearDown(dbB.close);
        addTearDown(observerDb.close);

        final sourceA = await _bootstrapManagedSourceId(dbA);
        final sourceB = await _bootstrapManagedSourceId(dbB);
        final observerSource = await _bootstrapManagedSourceId(observerDb);
        final httpA = await _authenticatedHttp(config.baseUrl, userId, sourceA);
        final httpB = await _authenticatedHttp(config.baseUrl, userId, sourceB);
        final observerHttp = await _authenticatedHttp(
          config.baseUrl,
          userId,
          observerSource,
        );
        addTearDown(httpA.close);
        addTearDown(httpB.close);
        addTearDown(observerHttp.close);

        final clientA = _newRealServerClient(dbA, httpA);
        final clientB = _newRealServerClient(
          dbB,
          httpB,
          resolver: const ClientWinsResolver(),
        );
        final observer = _newRealServerClient(observerDb, observerHttp);
        addTearDown(clientA.close);
        addTearDown(clientB.close);
        addTearDown(observer.close);

        await clientA.open();
        await _expectConnected(
          clientA.attach(userId),
          AttachOutcome.startedEmpty,
        );
        await clientB.open();
        await _expectConnected(
          clientB.attach(userId),
          AttachOutcome.usedRemoteState,
        );
        await observer.open();
        await _expectConnected(
          observer.attach(userId),
          AttachOutcome.usedRemoteState,
        );

        final rowId = _uuid();
        await _insertBusinessUserAndPost(dbA, rowId, _uuid(), 'conflict-base');
        expect((await clientA.pushPending()).outcome, PushOutcome.committed);
        await clientB.pullToStable();
        await observer.pullToStable();

        await dbA.connection.execute(
          "UPDATE users SET name = 'Server Winner' WHERE id = '$rowId'",
        );
        await dbB.connection.execute(
          "UPDATE users SET name = 'Client Winner' WHERE id = '$rowId'",
        );

        expect((await clientA.pushPending()).outcome, PushOutcome.committed);
        expect((await clientB.pushPending()).outcome, PushOutcome.committed);
        await observer.pullToStable();

        expect(
          await _scalarText(dbB, "SELECT name FROM users WHERE id = '$rowId'"),
          'Client Winner',
        );
        expect(
          await _scalarText(
            observerDb,
            "SELECT name FROM users WHERE id = '$rowId'",
          ),
          'Client Winner',
        );
        expect(
          await _scalarInt(dbB, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
          0,
        );
      });

      test('source recovery rotates source and retires old source', () async {
        await _resetRealServerState(config.baseUrl);
        final userId = _randomId('dart-retired-user');
        final seedDb = await _openBusinessDatabase();
        final recoverDb = await _openBusinessDatabase();
        final verifyDb = await _openBusinessDatabase();
        addTearDown(seedDb.close);
        addTearDown(recoverDb.close);
        addTearDown(verifyDb.close);

        final seedSource = await _bootstrapManagedSourceId(seedDb);
        final recoverSource = await _bootstrapManagedSourceId(recoverDb);
        final verifySource = await _bootstrapManagedSourceId(verifyDb);
        final rotatedSource = _randomId('dart-rotated-source');

        final seedHttp = await _authenticatedHttp(
          config.baseUrl,
          userId,
          seedSource,
        );
        final recoverHttp = await _authenticatedHttp(
          config.baseUrl,
          userId,
          recoverSource,
        );
        final verifyHttp = await _authenticatedHttp(
          config.baseUrl,
          userId,
          verifySource,
        );
        addTearDown(seedHttp.close);
        addTearDown(recoverHttp.close);
        addTearDown(verifyHttp.close);

        final seed = _newRealServerClient(seedDb, seedHttp);
        final recover = _newRealServerClient(recoverDb, recoverHttp);
        final verify = _newRealServerClient(verifyDb, verifyHttp);
        addTearDown(seed.close);
        addTearDown(recover.close);
        addTearDown(verify.close);

        await seed.open();
        await _expectConnected(seed.attach(userId), AttachOutcome.startedEmpty);
        await _insertBusinessUserAndPost(
          seedDb,
          _uuid(),
          _uuid(),
          'rotated-seed',
        );
        expect((await seed.pushPending()).outcome, PushOutcome.committed);

        await recover.open();
        await _expectConnected(
          recover.attach(userId),
          AttachOutcome.usedRemoteState,
        );
        await _markSourceRecoveryRequired(recoverDb, rotatedSource);
        expect(
          (await recover.rebuild()).outcome,
          RemoteSyncOutcome.appliedSnapshot,
        );
        expect(await _currentSourceId(recoverDb), rotatedSource);
        expect(
          await _scalarText(
            recoverDb,
            "SELECT replaced_by_source_id FROM _sync_source_state WHERE source_id = '$recoverSource'",
          ),
          rotatedSource,
        );

        final oldSourceHttp = await _authenticatedHttp(
          config.baseUrl,
          userId,
          recoverSource,
        );
        addTearDown(oldSourceHttp.close);
        final oldSourceResponse = await oldSourceHttp.postJson(
          'sync/push-sessions',
          sourceId: recoverSource,
          body: {'source_bundle_id': 1, 'planned_row_count': 1},
        );
        expect(oldSourceResponse.statusCode, HttpStatus.conflict);
        final oldSourceBody =
            jsonDecode(oldSourceResponse.body) as Map<String, Object?>;
        expect(oldSourceBody['error'], 'source_retired');
        expect(oldSourceBody['source_id'], recoverSource);
        expect(oldSourceBody['replaced_by_source_id'], rotatedSource);

        final rotatedHttp = await _authenticatedHttp(
          config.baseUrl,
          userId,
          rotatedSource,
        );
        addTearDown(rotatedHttp.close);
        final rotatedClient = _newRealServerClient(recoverDb, rotatedHttp);
        addTearDown(rotatedClient.close);
        await rotatedClient.open();
        await _expectConnected(
          rotatedClient.attach(userId),
          AttachOutcome.resumedAttachedState,
        );

        final followupUserId = _uuid();
        await _insertBusinessUserAndPost(
          recoverDb,
          followupUserId,
          _uuid(),
          'rotated-followup',
        );
        expect(
          (await rotatedClient.pushPending()).outcome,
          PushOutcome.committed,
        );

        await verify.open();
        await _expectConnected(
          verify.attach(userId),
          AttachOutcome.usedRemoteState,
        );
        await verify.pullToStable();
        expect(
          await _scalarText(
            verifyDb,
            "SELECT name FROM users WHERE id = '$followupUserId'",
          ),
          'User rotated-followup',
        );
      });
    },
    timeout: const Timeout(Duration(minutes: 2)),
  );
}

const _businessConfig = OversqliteConfig(
  schema: 'business',
  syncTables: [
    SyncTable(tableName: 'users', syncKeyColumnName: 'id'),
    SyncTable(tableName: 'posts', syncKeyColumnName: 'id'),
  ],
  uploadLimit: 8,
  downloadLimit: 8,
);

final _random = Random();

Future<_RealServerConfig> _requireRealServerConfig() async {
  final baseUrl =
      Platform.environment['OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL']
              ?.trim()
              .isNotEmpty ==
          true
      ? Platform.environment['OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL']!.trim()
      : 'http://localhost:8080';
  final health = await _sendJson('GET', baseUrl, 'syncx/health');
  if (health.statusCode != HttpStatus.ok) {
    throw StateError(
      'realserver unavailable at $baseUrl: HTTP ${health.statusCode} ${health.body}',
    );
  }
  final status = await _sendJson('GET', baseUrl, 'syncx/status');
  if (status.statusCode != HttpStatus.ok) {
    throw StateError(
      'realserver status failed at $baseUrl: HTTP ${status.statusCode} ${status.body}',
    );
  }
  final body = jsonDecode(status.body) as Map<String, Object?>;
  if (body['app_name'] != 'nethttp-server-example') {
    throw StateError(
      "realserver requires app_name='nethttp-server-example', got '${body['app_name']}'",
    );
  }
  return _RealServerConfig(baseUrl: baseUrl);
}

Future<SqliteNowDatabase> _openBusinessDatabase() async {
  final database = SqliteNowDatabase.inMemory();
  await database.open(
    preInit: (connection) async {
      await connection.execute('''CREATE TABLE users (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)''');
      await connection.execute('''CREATE TABLE posts (
  id TEXT PRIMARY KEY NOT NULL,
  title TEXT NOT NULL,
  content TEXT NOT NULL,
  author_id TEXT REFERENCES users(id) DEFERRABLE INITIALLY DEFERRED,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)''');
    },
  );
  return database;
}

DefaultOversqliteClient _newRealServerClient(
  SqliteNowDatabase database,
  OversqliteHttpClient? http, {
  Resolver resolver = const ServerWinsResolver(),
}) {
  return DefaultOversqliteClient(
    database: database,
    config: _businessConfig,
    httpClient: http,
    resolver: resolver,
  );
}

Future<String> _bootstrapManagedSourceId(SqliteNowDatabase database) async {
  final client = _newRealServerClient(database, null);
  await client.open();
  final source = (await client.sourceInfo()).currentSourceId;
  await client.close();
  return source;
}

Future<IoOversqliteHttpClient> _authenticatedHttp(
  String baseUrl,
  String userId,
  String sourceId,
) async {
  final token = await _issueDummySigninToken(baseUrl, userId, sourceId);
  return IoOversqliteHttpClient(
    baseUri: Uri.parse(baseUrl),
    defaultHeaders: {HttpHeaders.authorizationHeader: 'Bearer $token'},
  );
}

Future<String> _issueDummySigninToken(
  String baseUrl,
  String userId,
  String sourceId,
) async {
  final response = await _sendJson(
    'POST',
    baseUrl,
    'dummy-signin',
    body: {'user': userId, 'password': 'anything', 'device': sourceId},
  );
  if (response.statusCode != HttpStatus.ok) {
    throw StateError(
      'dummy-signin failed: HTTP ${response.statusCode} ${response.body}',
    );
  }
  final decoded = jsonDecode(response.body) as Map<String, Object?>;
  return decoded['token']! as String;
}

Future<void> _resetRealServerState(String baseUrl) async {
  final response = await _sendJson('POST', baseUrl, 'test/reset', body: {});
  if (response.statusCode != HttpStatus.ok) {
    throw StateError(
      'server reset failed: HTTP ${response.statusCode} ${response.body}',
    );
  }
}

Future<void> _setRetainedBundleFloor(
  String baseUrl,
  String userId,
  int retainedBundleFloor,
) async {
  final response = await _sendJson(
    'POST',
    baseUrl,
    'test/retention-floor',
    body: {'user_id': userId, 'retained_bundle_floor': retainedBundleFloor},
  );
  if (response.statusCode != HttpStatus.ok) {
    throw StateError(
      'retention floor update failed: HTTP ${response.statusCode} ${response.body}',
    );
  }
}

Future<_HttpResponseBody> _sendJson(
  String method,
  String baseUrl,
  String path, {
  Object? body,
}) async {
  final http = HttpClient();
  try {
    final request = await http.openUrl(method, _resolve(baseUrl, path));
    if (body != null) {
      request.headers.contentType = ContentType.json;
      request.write(jsonEncode(body));
    }
    final response = await request.close();
    return _HttpResponseBody(
      statusCode: response.statusCode,
      body: await utf8.decoder.bind(response).join(),
    );
  } finally {
    http.close(force: true);
  }
}

Uri _resolve(String baseUrl, String path) {
  final normalizedBase = baseUrl.endsWith('/') ? baseUrl : '$baseUrl/';
  final normalizedPath = path.startsWith('/') ? path.substring(1) : path;
  return Uri.parse(normalizedBase).resolve(normalizedPath);
}

Future<AttachConnected> _expectConnected(
  Future<AttachResult> future,
  AttachOutcome outcome,
) async {
  final result = await future;
  expect(result, isA<AttachConnected>());
  final connected = result as AttachConnected;
  expect(connected.outcome, outcome);
  return connected;
}

Future<void> _insertBusinessUserAndPost(
  SqliteNowDatabase database,
  String userId,
  String postId,
  String suffix,
) async {
  await database.connection.execute(
    'INSERT INTO users(id, name, email) VALUES(?, ?, ?)',
    parameters: [userId, 'User $suffix', '$suffix@example.com'],
  );
  await database.connection.execute(
    'INSERT INTO posts(id, title, content, author_id) VALUES(?, ?, ?, ?)',
    parameters: [postId, 'Title $suffix', 'Payload $suffix', userId],
  );
}

Future<void> _markSourceRecoveryRequired(
  SqliteNowDatabase database,
  String replacementSourceId,
) async {
  await database.connection.execute(
    '''INSERT INTO _sync_source_state(source_id, next_source_bundle_id, replaced_by_source_id)
VALUES(?, 1, '')
ON CONFLICT(source_id) DO NOTHING''',
    parameters: [replacementSourceId],
  );
  await database.connection.execute(
    'UPDATE _sync_attachment_state SET rebuild_required = 1 WHERE singleton_key = 1',
  );
  await database.connection.execute(
    '''UPDATE _sync_operation_state
SET kind = 'source_recovery',
    reason = 'source_sequence_changed',
    replacement_source_id = ?
WHERE singleton_key = 1''',
    parameters: [replacementSourceId],
  );
}

Future<String> _currentSourceId(SqliteNowDatabase database) {
  return _scalarText(
    database,
    'SELECT current_source_id FROM _sync_attachment_state',
  );
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

String _uuid() {
  String hex(int count) {
    const chars = '0123456789abcdef';
    return List.generate(count, (_) => chars[_random.nextInt(16)]).join();
  }

  return '${hex(8)}-${hex(4)}-${hex(4)}-${hex(4)}-${hex(12)}';
}

String _randomId(String prefix) {
  final micros = DateTime.now().microsecondsSinceEpoch;
  return '$prefix-$micros-${_random.nextInt(1 << 32)}';
}

bool _flagEnabled(String name) {
  return switch (Platform.environment[name]?.trim().toLowerCase()) {
    '1' || 'true' || 'yes' || 'on' => true,
    _ => false,
  };
}

final class _RealServerConfig {
  const _RealServerConfig({required this.baseUrl});

  final String baseUrl;
}

final class _HttpResponseBody {
  const _HttpResponseBody({required this.statusCode, required this.body});

  final int statusCode;
  final String body;
}
