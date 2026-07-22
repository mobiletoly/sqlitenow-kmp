import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

import 'generated/rich_real_server_db.dart';

const businessSyncTables = RichRealServerDb.syncTables;

final realserverRandom = Random();

bool flagEnabled(String name) {
  return switch (Platform.environment[name]?.trim().toLowerCase()) {
    '1' || 'true' || 'yes' || 'on' => true,
    _ => false,
  };
}

Future<RealServerConfig> requireRealServerConfig() async {
  final configuredBaseUrl =
      Platform.environment['OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL'];
  final baseUrl =
      configuredBaseUrl != null && configuredBaseUrl.trim().isNotEmpty
      ? configuredBaseUrl.trim()
      : 'http://localhost:8080';
  final health = await sendJson('GET', baseUrl, 'syncx/health');
  if (health.statusCode != HttpStatus.ok) {
    throw StateError(
      'realserver unavailable at $baseUrl: HTTP ${health.statusCode} ${health.body}',
    );
  }
  final status = await sendJson('GET', baseUrl, 'syncx/status');
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
  return RealServerConfig(baseUrl: baseUrl);
}

Future<SqliteNowDatabase> openBusinessDatabase() async {
  final database = RichRealServerDb.inMemory();
  await database.open();
  return database.runtimeDatabase;
}

DefaultOversqliteClient newRealServerClient(
  SqliteNowDatabase database,
  OversqliteHttpClient? http, {
  int uploadLimit = 8,
  int downloadLimit = 8,
  Resolver resolver = const ServerWinsResolver(),
}) {
  return DefaultOversqliteClient(
    database: database,
    config: OversqliteConfig(
      schema: 'business',
      syncTables: businessSyncTables,
      uploadLimit: uploadLimit,
      downloadLimit: downloadLimit,
    ),
    httpClient: http,
    resolver: resolver,
  );
}

Future<String> bootstrapManagedSourceId(SqliteNowDatabase database) async {
  final client = newRealServerClient(database, null);
  await client.open();
  final source = (await client.sourceInfo()).currentSourceId;
  await client.close();
  return source;
}

Future<IoOversqliteHttpClient> authenticatedHttp(
  String baseUrl,
  String userId,
  String sourceId,
) async {
  final token = await issueDummySigninToken(baseUrl, userId, sourceId);
  return IoOversqliteHttpClient(
    baseUri: Uri.parse(baseUrl),
    defaultHeaders: {HttpHeaders.authorizationHeader: 'Bearer $token'},
  );
}

Future<String> issueDummySigninToken(
  String baseUrl,
  String userId,
  String sourceId,
) async {
  final response = await sendJson(
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

Future<void> resetRealServerState(String baseUrl) async {
  final response = await sendJson('GET', baseUrl, 'syncx/status');
  if (response.statusCode != HttpStatus.ok) {
    throw StateError(
      'fresh realserver status probe failed: HTTP ${response.statusCode} ${response.body}',
    );
  }
}

Future<void> setRetainedBundleFloor(
  String baseUrl,
  String userId,
  int retainedBundleFloor,
) async {
  final response = await sendJson(
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

Future<HttpResponseBody> sendJson(
  String method,
  String baseUrl,
  String path, {
  Object? body,
}) async {
  final http = HttpClient();
  try {
    final request = await http.openUrl(method, resolve(baseUrl, path));
    if (body != null) {
      request.headers.contentType = ContentType.json;
      request.write(jsonEncode(body));
    }
    final response = await request.close();
    return HttpResponseBody(
      statusCode: response.statusCode,
      body: await utf8.decoder.bind(response).join(),
    );
  } finally {
    http.close(force: true);
  }
}

Uri resolve(String baseUrl, String path) {
  final normalizedBase = baseUrl.endsWith('/') ? baseUrl : '$baseUrl/';
  final normalizedPath = path.startsWith('/') ? path.substring(1) : path;
  return Uri.parse(normalizedBase).resolve(normalizedPath);
}

Future<AttachConnected> expectConnected(
  Future<AttachResult> future,
  AttachOutcome outcome,
) async {
  final result = await future;
  expect(result, isA<AttachConnected>());
  final connected = result as AttachConnected;
  expect(connected.outcome, outcome);
  return connected;
}

Future<void> insertBusinessUserAndPost(
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

Future<void> markSourceRecoveryRequired(
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

Future<String> currentSourceId(SqliteNowDatabase database) {
  return scalarText(
    database,
    'SELECT current_source_id FROM _sync_attachment_state',
  );
}

Future<void> expectCleanSyncTables(
  SqliteNowDatabase database, {
  bool includeSnapshotStage = true,
}) async {
  expect(await scalarInt(database, 'SELECT COUNT(*) FROM _sync_dirty_rows'), 0);
  expect(
    await scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
    0,
  );
  if (includeSnapshotStage) {
    expect(
      await scalarInt(database, 'SELECT COUNT(*) FROM _sync_snapshot_stage'),
      0,
    );
  }
}

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

String realserverUuid() {
  String hex(int count) {
    const chars = '0123456789abcdef';
    return List.generate(
      count,
      (_) => chars[realserverRandom.nextInt(16)],
    ).join();
  }

  return '${hex(8)}-${hex(4)}-${hex(4)}-${hex(4)}-${hex(12)}';
}

String randomRealserverId(String prefix) {
  final micros = DateTime.now().microsecondsSinceEpoch;
  return '$prefix-$micros-${realserverRandom.nextInt(1 << 32)}';
}

final class RealServerConfig {
  const RealServerConfig({required this.baseUrl});

  final String baseUrl;
}

final class HttpResponseBody {
  const HttpResponseBody({required this.statusCode, required this.body});

  final int statusCode;
  final String body;
}
