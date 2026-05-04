import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

void main() {
  final fixtures = _readHandshakeFixtures();

  test('protocol handshake fixture models decode', () {
    for (final fixture in fixtures) {
      final capabilities = CapabilitiesResponse.fromJson(
        fixture.capabilities.body,
      );
      expect(
        capabilities.connectLifecycleSupported,
        fixture.expected.connectLifecycleSupported,
        reason: fixture.name,
      );

      final connect = fixture.connect;
      if (connect != null) {
        final response = ConnectResponse.fromJson(connect.body);
        expect(
          response.resolution,
          fixture.connect!.body['resolution'],
          reason: fixture.name,
        );
      }
    }
  });

  test('fetchCapabilities sends Oversync source header', () async {
    final fixture = fixtures.singleWhere(
      (item) => item.name == 'initialize-empty',
    );
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final http = _RecordingHttpClient(fixture);
    final client = _newClient(database, http);
    addTearDown(client.close);
    await client.open();
    final sourceId = (await client.sourceInfo()).currentSourceId;

    final capabilities = await client.fetchCapabilities();

    expect(capabilities.connectLifecycleSupported, isTrue);
    expect(http.requests, hasLength(1));
    expect(http.requests.single.method, 'GET');
    expect(http.requests.single.path, 'sync/capabilities');
    expect(http.requests.single.sourceId, sourceId);
  });

  test('handshake HTTP errors decode structured error bodies', () async {
    final api = OversqliteRemoteApi(
      _StaticHttpClient(
        OversqliteHttpResponse(
          statusCode: 409,
          body: jsonEncode({
            'error': 'connect_rejected',
            'message': 'connect was rejected',
          }),
        ),
      ),
    );

    await expectLater(
      () => api.fetchCapabilities('source-1'),
      throwsA(
        isA<OversqliteHttpException>()
            .having((error) => error.statusCode, 'statusCode', 409)
            .having(
              (error) => error.errorResponse?.error,
              'error code',
              'connect_rejected',
            )
            .having(
              (error) => error.errorResponse?.message,
              'message',
              'connect was rejected',
            ),
      ),
    );
  });

  test('attach follows shared protocol handshake fixtures', () async {
    for (final fixture in fixtures) {
      final database = await _openUsersDatabase();
      final http = _RecordingHttpClient(fixture);
      final client = _newClient(database, http);
      await client.open();
      final sourceId = (await client.sourceInfo()).currentSourceId;
      if (fixture.local.hasLocalPendingRows) {
        await database.connection.execute(
          "INSERT INTO users(id, name) VALUES('user-1', 'Ada')",
        );
      }

      final attachKind = fixture.expected.attachKind;
      switch (attachKind) {
        case 'unsupportedCapability':
          await expectLater(
            () => client.attach('user-1'),
            throwsA(isA<ConnectLifecycleUnsupportedException>()),
          );
        case 'retryLater':
          final result = await client.attach('user-1');
          expect(result, isA<AttachRetryLater>(), reason: fixture.name);
          expect(
            (result as AttachRetryLater).retryAfterSeconds,
            fixture.expected.retryAfterSeconds,
            reason: fixture.name,
          );
        case 'connected':
          final result = await client.attach('user-1');
          expect(result, isA<AttachConnected>(), reason: fixture.name);
          expect(
            (result as AttachConnected).outcome.name,
            fixture.expected.attachOutcome,
            reason: fixture.name,
          );
          final attachment = await _attachmentRow(database);
          expect(
            attachment['bindingState'],
            fixture.expected.bindingState,
            reason: fixture.name,
          );
          expect(
            attachment['pendingInitializationId'],
            fixture.expected.pendingInitializationId,
            reason: fixture.name,
          );
        case 'deferredDataTransfer':
          await expectLater(
            () => client.attach('user-1'),
            throwsA(isA<RemoteDataTransferDeferredException>()),
          );
          final attachment = await _attachmentRow(database);
          final operation = await _operationRow(database);
          expect(
            attachment['bindingState'],
            fixture.expected.bindingState,
            reason: fixture.name,
          );
          expect(
            operation['kind'],
            fixture.expected.durableOperationKind,
            reason: fixture.name,
          );
        default:
          fail('unknown fixture attachKind $attachKind');
      }

      expect(http.requests.first.method, 'GET', reason: fixture.name);
      expect(
        http.requests.first.path,
        'sync/capabilities',
        reason: fixture.name,
      );
      expect(http.requests.first.sourceId, sourceId, reason: fixture.name);
      final connectRequests = http.requests
          .where((request) => request.path == 'sync/connect')
          .toList();
      if (fixture.expected.connectCalled) {
        expect(connectRequests, hasLength(1), reason: fixture.name);
        expect(connectRequests.single.method, 'POST', reason: fixture.name);
        expect(connectRequests.single.sourceId, sourceId, reason: fixture.name);
        expect(connectRequests.single.body, {
          'has_local_pending_rows': fixture.local.hasLocalPendingRows,
        }, reason: fixture.name);
      } else {
        expect(connectRequests, isEmpty, reason: fixture.name);
      }

      await client.close();
      await database.close();
    }
  });
}

DefaultOversqliteClient _newClient(
  SqliteNowDatabase database,
  OversqliteHttpClient http,
) {
  return DefaultOversqliteClient(
    database: database,
    config: const OversqliteConfig(
      schema: 'main',
      syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
    ),
    httpClient: http,
  );
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

List<_HandshakeFixture> _readHandshakeFixtures() {
  final file = _repoRoot().uri
      .resolve('oversqlite-contracts/protocol-handshake/connect.json')
      .toFilePath();
  final raw = jsonDecode(File(file).readAsStringSync()) as Map<String, Object?>;
  return (raw['cases']! as List<Object?>)
      .cast<Map<String, Object?>>()
      .map(_HandshakeFixture.fromJson)
      .toList();
}

Future<Map<String, Object?>> _attachmentRow(SqliteNowDatabase database) async {
  final rows = await database.connection.select(
    '''SELECT binding_state, attached_user_id, pending_initialization_id
FROM _sync_attachment_state
WHERE singleton_key = 1''',
    (row) => {
      'bindingState': row.readString(0),
      'attachedUserId': row.readString(1),
      'pendingInitializationId': row.readString(2),
    },
  );
  return rows.single;
}

Future<Map<String, Object?>> _operationRow(SqliteNowDatabase database) async {
  final rows = await database.connection.select(
    'SELECT kind, target_user_id FROM _sync_operation_state WHERE singleton_key = 1',
    (row) => {'kind': row.readString(0), 'targetUserId': row.readString(1)},
  );
  return rows.single;
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

final class _StaticHttpClient implements OversqliteHttpClient {
  const _StaticHttpClient(this.response);

  final OversqliteHttpResponse response;

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
  }) async {
    return response;
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
  }) async {
    return response;
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
  }) async {
    return response;
  }
}

final class _RecordingHttpClient implements OversqliteHttpClient {
  _RecordingHttpClient(this.fixture);

  final _HandshakeFixture fixture;
  final List<_RecordedRequest> requests = [];

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
  }) async {
    requests.add(
      _RecordedRequest(method: 'GET', path: path, sourceId: sourceId),
    );
    if (path == 'sync/capabilities') {
      return fixture.capabilities.toHttpResponse();
    }
    if (path.startsWith('sync/snapshot-sessions/snapshot-1')) {
      return OversqliteHttpResponse(
        statusCode: 200,
        body: jsonEncode({
          'snapshot_id': 'snapshot-1',
          'snapshot_bundle_seq': 0,
          'rows': <Object?>[],
          'next_row_ordinal': 0,
          'has_more': false,
        }),
      );
    }
    fail('unexpected GET $path for ${fixture.name}');
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
  }) async {
    requests.add(
      _RecordedRequest(
        method: 'POST',
        path: path,
        sourceId: sourceId,
        body: body,
      ),
    );
    if (path == 'sync/connect') {
      final connect = fixture.connect;
      if (connect == null) {
        fail('fixture ${fixture.name} did not expect a connect request');
      }
      return connect.toHttpResponse();
    }
    if (path == 'sync/snapshot-sessions') {
      return OversqliteHttpResponse(
        statusCode: 200,
        body: jsonEncode({
          'snapshot_id': 'snapshot-1',
          'snapshot_bundle_seq': 0,
          'row_count': 0,
          'byte_count': 0,
          'expires_at': '2099-01-01T00:00:00Z',
        }),
      );
    }
    fail('unexpected POST $path for ${fixture.name}');
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
  }) async {
    requests.add(
      _RecordedRequest(method: 'DELETE', path: path, sourceId: sourceId),
    );
    return const OversqliteHttpResponse(statusCode: 204, body: '');
  }
}

final class _RecordedRequest {
  const _RecordedRequest({
    required this.method,
    required this.path,
    required this.sourceId,
    this.body,
  });

  final String method;
  final String path;
  final String sourceId;
  final Object? body;
}

final class _HandshakeFixture {
  const _HandshakeFixture({
    required this.name,
    required this.capabilities,
    required this.connect,
    required this.local,
    required this.expected,
  });

  final String name;
  final _FixtureHttpResponse capabilities;
  final _FixtureHttpResponse? connect;
  final _FixtureLocal local;
  final _FixtureExpected expected;

  factory _HandshakeFixture.fromJson(Map<String, Object?> json) {
    return _HandshakeFixture(
      name: json['name']! as String,
      capabilities: _FixtureHttpResponse.fromJson(
        json['capabilities']! as Map<String, Object?>,
      ),
      connect: json['connect'] == null
          ? null
          : _FixtureHttpResponse.fromJson(
              json['connect']! as Map<String, Object?>,
            ),
      local: _FixtureLocal.fromJson(json['local']! as Map<String, Object?>),
      expected: _FixtureExpected.fromJson(
        json['expected']! as Map<String, Object?>,
      ),
    );
  }
}

final class _FixtureHttpResponse {
  const _FixtureHttpResponse({required this.status, required this.body});

  final int status;
  final Map<String, Object?> body;

  factory _FixtureHttpResponse.fromJson(Map<String, Object?> json) {
    return _FixtureHttpResponse(
      status: json['status']! as int,
      body: (json['body']! as Map).cast<String, Object?>(),
    );
  }

  OversqliteHttpResponse toHttpResponse() {
    return OversqliteHttpResponse(statusCode: status, body: jsonEncode(body));
  }
}

final class _FixtureLocal {
  const _FixtureLocal({required this.hasLocalPendingRows});

  final bool hasLocalPendingRows;

  factory _FixtureLocal.fromJson(Map<String, Object?> json) {
    return _FixtureLocal(
      hasLocalPendingRows: json['hasLocalPendingRows']! as bool,
    );
  }
}

final class _FixtureExpected {
  const _FixtureExpected({
    required this.connectLifecycleSupported,
    required this.connectCalled,
    required this.attachKind,
    this.attachOutcome,
    this.retryAfterSeconds,
    this.bindingState,
    this.pendingInitializationId,
    this.durableOperationKind,
  });

  final bool connectLifecycleSupported;
  final bool connectCalled;
  final String attachKind;
  final String? attachOutcome;
  final int? retryAfterSeconds;
  final String? bindingState;
  final String? pendingInitializationId;
  final String? durableOperationKind;

  factory _FixtureExpected.fromJson(Map<String, Object?> json) {
    return _FixtureExpected(
      connectLifecycleSupported: json['connectLifecycleSupported']! as bool,
      connectCalled: json['connectCalled']! as bool,
      attachKind: json['attachKind']! as String,
      attachOutcome: json['attachOutcome'] as String?,
      retryAfterSeconds: json['retryAfterSeconds'] as int?,
      bindingState: json['bindingState'] as String?,
      pendingInitializationId: json['pendingInitializationId'] as String?,
      durableOperationKind: json['durableOperationKind'] as String?,
    );
  }
}
