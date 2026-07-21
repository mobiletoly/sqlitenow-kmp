import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

void main() {
  final fixtures = _readHandshakeFixtures();
  final contractCases = _readHandshakeContractCases();

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

  test('root-only API constructs download runtime from capabilities', () {
    final fixture = fixtures.singleWhere(
      (item) => item.name == 'initialize-empty',
    );
    final database = SqliteNowDatabase.inMemory();
    final config = OversqliteConfig(
      schema: 'main',
      syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
    );

    final runtime = OversqliteDownloadRuntime(
      database: database,
      config: config,
      remoteApi: OversqliteRemoteApi(
        _StaticHttpClient(fixture.capabilities.toHttpResponse()),
      ),
      capabilities: CapabilitiesResponse.fromJson(fixture.capabilities.body),
    );

    expect(runtime, isA<OversqliteDownloadRuntime>());
  });

  test(
    'shared table contract cases enforce exact advertised metadata',
    () async {
      final base = fixtures.singleWhere(
        (item) => item.name == 'initialize-empty',
      );
      for (final contractCase in contractCases) {
        final database = await _openContractDatabase(contractCase.localSpecs);
        final body = Map<String, Object?>.from(base.capabilities.body);
        if (contractCase.hasAdvertised) {
          body['registered_table_specs'] = contractCase.advertised;
        } else {
          body.remove('registered_table_specs');
        }
        final client = DefaultOversqliteClient(
          database: database,
          config: OversqliteConfig(
            schema: contractCase.localSpecs.first.schema,
            syncTables: [
              for (final spec in contractCase.localSpecs)
                SyncTable(
                  tableName: spec.table,
                  syncKeyColumnName: spec.syncKeyColumns.single,
                ),
            ],
          ),
          httpClient: _StaticHttpClient(
            OversqliteHttpResponse(statusCode: 200, body: jsonEncode(body)),
          ),
        );
        try {
          await client.open();
          switch (contractCase.kind) {
            case 'match':
              await client.fetchCapabilities();
            case 'invalid':
              await expectLater(
                client.fetchCapabilities,
                throwsA(isA<SnapshotCapabilitiesException>()),
                reason: contractCase.name,
              );
            case 'mismatch':
              try {
                await client.fetchCapabilities();
                fail('${contractCase.name} did not reject mismatched tables');
              } on SyncTableContractMismatchException catch (error) {
                expect(
                  error.serverOnlyTables,
                  contractCase.serverOnlyTables,
                  reason: contractCase.name,
                );
                expect(
                  error.clientOnlyTables,
                  contractCase.clientOnlyTables,
                  reason: contractCase.name,
                );
                expect(
                  error.syncKeyMismatches,
                  hasLength(contractCase.syncKeyMismatches.length),
                  reason: contractCase.name,
                );
                for (
                  var index = 0;
                  index < error.syncKeyMismatches.length;
                  index++
                ) {
                  final actual = error.syncKeyMismatches[index];
                  final expected = contractCase.syncKeyMismatches[index];
                  expect(
                    actual.qualifiedTable,
                    expected.qualifiedTable,
                    reason: contractCase.name,
                  );
                  expect(
                    actual.clientSyncKeyColumns,
                    expected.clientSyncKeyColumns,
                    reason: contractCase.name,
                  );
                  expect(
                    actual.serverSyncKeyColumns,
                    expected.serverSyncKeyColumns,
                    reason: contractCase.name,
                  );
                }
              }
            default:
              fail('unknown contract fixture kind ${contractCase.kind}');
          }
        } finally {
          await client.close();
          await database.close();
        }
      }
    },
  );

  test(
    'table contract mismatch blocks attach before connect and binding',
    () async {
      final base = fixtures.singleWhere(
        (item) => item.name == 'initialize-empty',
      );
      final body = Map<String, Object?>.from(base.capabilities.body)
        ..['registered_table_specs'] = [
          {
            'schema': 'business',
            'table': 'users',
            'sync_key_columns': ['id'],
          },
          {
            'schema': 'business',
            'table': 'monitoring_focus',
            'sync_key_columns': ['id'],
          },
        ];
      final fixture = _HandshakeFixture(
        name: 'server-only-before-connect',
        capabilities: _FixtureHttpResponse(status: 200, body: body),
        connect: base.connect,
        local: base.local,
        expected: base.expected,
      );
      final database = await _openUsersDatabase();
      final http = _RecordingHttpClient(fixture);
      final client = _newClient(database, http, schema: 'business');
      try {
        await client.open();
        await expectLater(
          () => client.attach('user-1'),
          throwsA(
            isA<SyncTableContractMismatchException>().having(
              (error) => error.serverOnlyTables,
              'serverOnlyTables',
              ['business.monitoring_focus'],
            ),
          ),
        );
        expect(http.requests.map((request) => request.path), [
          'sync/capabilities',
        ]);
        final attachment = await _attachmentRow(database);
        expect(attachment['bindingState'], 'anonymous');
        expect(attachment['attachedUserId'], '');
      } finally {
        await client.close();
        await database.close();
      }
    },
  );

  test(
    'table contract mismatch blocks push pull sync and snapshot work',
    () async {
      final base = fixtures.singleWhere(
        (item) => item.name == 'initialize-empty',
      );
      final http = _MutableContractHttpClient(base.capabilities.body);
      final database = await _openUsersDatabase();
      final client = _newClient(database, http);
      try {
        await client.open();
        await client.attach('user-1');
        await database.connection.execute(
          "INSERT INTO users(id, name) VALUES('user-1', 'Ada')",
        );
        http
          ..registeredTableSpecs = [
            {
              'schema': 'main',
              'table': 'users',
              'sync_key_columns': ['id'],
            },
            {
              'schema': 'main',
              'table': 'monitoring_focus',
              'sync_key_columns': ['id'],
            },
          ]
          ..requests.clear();
        final initialDirty = await _scalar(
          database,
          'SELECT COUNT(*) FROM _sync_dirty_rows',
        );
        final initialOutbox = await _scalar(
          database,
          'SELECT COUNT(*) FROM _sync_outbox_rows',
        );

        for (final operation in <Future<void> Function()>[
          () async => client.pushPending(),
          () async => client.pullToStable(),
          () async => client.sync(),
          () async => client.rebuild(),
        ]) {
          await expectLater(
            operation,
            throwsA(isA<SyncTableContractMismatchException>()),
          );
        }

        expect(http.requests, List.filled(4, 'sync/capabilities'));
        expect(
          await _scalar(database, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
          initialDirty,
        );
        expect(
          await _scalar(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
          initialOutbox,
        );
        expect((await _operationRow(database))['kind'], 'none');
      } finally {
        await client.close();
        await database.close();
      }
    },
  );

  test('handshake HTTP errors are redacted snapshot errors', () async {
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
        isA<SnapshotHttpException>()
            .having((error) => error.statusCode, 'statusCode', 409)
            .having(
              (error) => error.errorCode,
              'error code',
              'invalid_error_response',
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
      expect(
        http.requests.where((request) => request.path == 'sync/capabilities'),
        hasLength(1),
        reason: '${fixture.name} must reuse its validated capabilities',
      );
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
  OversqliteHttpClient http, {
  String schema = 'main',
}) {
  return DefaultOversqliteClient(
    database: database,
    config: OversqliteConfig(
      schema: schema,
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

List<_ContractCase> _readHandshakeContractCases() {
  final file = _repoRoot().uri
      .resolve('oversqlite-contracts/protocol-handshake/connect.json')
      .toFilePath();
  final raw = jsonDecode(File(file).readAsStringSync()) as Map<String, Object?>;
  return (raw['contractCases']! as List<Object?>)
      .cast<Map<String, Object?>>()
      .map(_ContractCase.fromJson)
      .toList();
}

Future<SqliteNowDatabase> _openContractDatabase(
  List<RegisteredTableSpec> specs,
) async {
  final database = SqliteNowDatabase.inMemory();
  await database.open(
    preInit: (connection) async {
      for (final spec in specs) {
        await connection.execute(
          'CREATE TABLE ${spec.table} (${spec.syncKeyColumns.single} TEXT PRIMARY KEY NOT NULL)',
        );
      }
    },
  );
  return database;
}

Future<int> _scalar(SqliteNowDatabase database, String sql) async {
  final rows = await database.connection.select(sql, (row) => row.readInt(0));
  return rows.single;
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
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    return response;
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    return response;
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    return response;
  }
}

final class _MutableContractHttpClient implements OversqliteHttpClient {
  _MutableContractHttpClient(Map<String, Object?> baseCapabilities)
    : _baseCapabilities = Map<String, Object?>.from(baseCapabilities),
      registeredTableSpecs = List<Map<String, Object?>>.from(
        (baseCapabilities['registered_table_specs']! as List).map(
          (value) => Map<String, Object?>.from(value as Map),
        ),
      );

  final Map<String, Object?> _baseCapabilities;
  List<Map<String, Object?>> registeredTableSpecs;
  final List<String> requests = [];

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    requests.add(path);
    if (path == 'sync/capabilities') {
      final body = Map<String, Object?>.from(_baseCapabilities)
        ..['registered_table_specs'] = registeredTableSpecs;
      return OversqliteHttpResponse(statusCode: 200, body: jsonEncode(body));
    }
    fail('unexpected GET $path');
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    requests.add(path);
    if (path == 'sync/connect') {
      return OversqliteHttpResponse(
        statusCode: 200,
        body: jsonEncode({
          'resolution': 'initialize_empty',
          'server_has_synced_data': false,
        }),
      );
    }
    fail('unexpected POST $path');
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    requests.add(path);
    fail('unexpected DELETE $path');
  }
}

final class _ContractCase {
  const _ContractCase({
    required this.name,
    required this.localSpecs,
    required this.hasAdvertised,
    required this.advertised,
    required this.kind,
    required this.serverOnlyTables,
    required this.clientOnlyTables,
    required this.syncKeyMismatches,
  });

  final String name;
  final List<RegisteredTableSpec> localSpecs;
  final bool hasAdvertised;
  final Object? advertised;
  final String kind;
  final List<String> serverOnlyTables;
  final List<String> clientOnlyTables;
  final List<SyncKeyContractMismatch> syncKeyMismatches;

  factory _ContractCase.fromJson(Map<String, Object?> json) {
    final expected = (json['expected']! as Map).cast<String, Object?>();
    return _ContractCase(
      name: json['name']! as String,
      localSpecs: List.unmodifiable(
        (json['localSpecs']! as List).map(
          (value) => RegisteredTableSpec.fromJson(
            (value as Map).cast<String, Object?>(),
          ),
        ),
      ),
      hasAdvertised: json.containsKey('advertised'),
      advertised: json['advertised'],
      kind: expected['kind']! as String,
      serverOnlyTables: List<String>.unmodifiable(
        (expected['serverOnlyTables'] as List? ?? const []).cast<String>(),
      ),
      clientOnlyTables: List<String>.unmodifiable(
        (expected['clientOnlyTables'] as List? ?? const []).cast<String>(),
      ),
      syncKeyMismatches: List.unmodifiable(
        (expected['syncKeyMismatches'] as List? ?? const []).map((value) {
          final mismatch = (value as Map).cast<String, Object?>();
          return SyncKeyContractMismatch(
            qualifiedTable: mismatch['qualifiedTable']! as String,
            clientSyncKeyColumns: (mismatch['clientSyncKeyColumns']! as List)
                .cast<String>(),
            serverSyncKeyColumns: (mismatch['serverSyncKeyColumns']! as List)
                .cast<String>(),
          );
        }),
      ),
    );
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
    required String operation,
    required OversqliteHttpRequestBounds bounds,
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
          'byte_count': 0,
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
    required String operation,
    required OversqliteHttpRequestBounds bounds,
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
    required String operation,
    required OversqliteHttpRequestBounds bounds,
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
