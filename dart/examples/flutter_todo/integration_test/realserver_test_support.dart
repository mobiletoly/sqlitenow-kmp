import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:path_provider/path_provider.dart';
import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

import 'generated/rich_real_server_db.dart';

const realserverEnabled = bool.fromEnvironment('OVERSQLITE_REALSERVER_TESTS');
const heavyRealserverEnabled = bool.fromEnvironment(
  'OVERSQLITE_REALSERVER_HEAVY',
);
const realserverBaseUrl = String.fromEnvironment(
  'OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL',
  defaultValue: 'http://10.0.2.2:8080',
);

final businessSyncTables = RichRealServerDb.syncTables;
final richSyncTables = RichRealServerDb.syncTables;

final realserverRandom = Random();

Future<RealServerConfig> requireRealServerConfig() async {
  final health = await sendJson('GET', realserverBaseUrl, 'syncx/health');
  if (health.statusCode != HttpStatus.ok) {
    throw StateError(
      'realserver unavailable at $realserverBaseUrl: '
      'HTTP ${health.statusCode} ${health.body}',
    );
  }
  final status = await sendJson('GET', realserverBaseUrl, 'syncx/status');
  if (status.statusCode != HttpStatus.ok) {
    throw StateError(
      'realserver status failed at $realserverBaseUrl: '
      'HTTP ${status.statusCode} ${status.body}',
    );
  }
  final body = jsonDecode(status.body) as Map<String, Object?>;
  if (body['app_name'] != 'nethttp-server-example') {
    throw StateError(
      "realserver requires app_name='nethttp-server-example', "
      "got '${body['app_name']}'",
    );
  }
  return const RealServerConfig(baseUrl: realserverBaseUrl);
}

Future<Directory> createRealserverTempDir() async {
  final supportDir = await getApplicationSupportDirectory();
  final tempDir = await supportDir.createTemp('sqlitenow-flutter-realserver-');
  addTearDown(() async {
    if (await tempDir.exists()) {
      await tempDir.delete(recursive: true);
    }
  });
  return tempDir;
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
      'retention floor update failed: '
      'HTTP ${response.statusCode} ${response.body}',
    );
  }
}

Future<int> watchSubscriberCount(String baseUrl, String userId) async {
  final response = await sendJson(
    'GET',
    baseUrl,
    'test/watch-subscribers?user_id=${Uri.encodeQueryComponent(userId)}',
  );
  if (response.statusCode != HttpStatus.ok) {
    throw StateError(
      'watch subscriber count failed: '
      'HTTP ${response.statusCode} ${response.body}',
    );
  }
  final decoded = jsonDecode(response.body) as Map<String, Object?>;
  return decoded['subscriber_count']! as int;
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

Future<BusinessDevice> openBusinessDevice({
  required Directory tempDir,
  required String fileName,
  required String baseUrl,
  required String userId,
  int uploadLimit = 8,
  int downloadLimit = 8,
  Resolver resolver = const ServerWinsResolver(),
  Duration automaticDownloadInterval = const Duration(seconds: 60),
  BundleChangeWatchMode bundleChangeWatchMode = BundleChangeWatchMode.off,
  Duration bundleChangeWatchReconnectMin = const Duration(milliseconds: 25),
  Duration bundleChangeWatchReconnectMax = const Duration(milliseconds: 50),
}) async {
  final db = await openBusinessDatabase('${tempDir.path}/$fileName');
  final sourceId = await bootstrapBusinessSourceId(db);
  final http = await authenticatedHttp(baseUrl, userId, sourceId);
  final client = newBusinessClient(
    db,
    http,
    uploadLimit: uploadLimit,
    downloadLimit: downloadLimit,
    resolver: resolver,
    automaticDownloadInterval: automaticDownloadInterval,
    bundleChangeWatchMode: bundleChangeWatchMode,
    bundleChangeWatchReconnectMin: bundleChangeWatchReconnectMin,
    bundleChangeWatchReconnectMax: bundleChangeWatchReconnectMax,
  );
  return BusinessDevice(
    database: db,
    client: client,
    http: http,
    sourceId: sourceId,
    userId: userId,
  );
}

Future<RichDevice> openRichDevice({
  required Directory tempDir,
  required String fileName,
  required String baseUrl,
  required String userId,
  int uploadLimit = 8,
  int downloadLimit = 8,
  BundleChangeWatchMode bundleChangeWatchMode = BundleChangeWatchMode.off,
}) async {
  final db = RichRealServerDb(path: '${tempDir.path}/$fileName');
  await db.open();
  final sourceId = await bootstrapRichSourceId(db);
  final http = await authenticatedHttp(baseUrl, userId, sourceId);
  final client = newRichClient(
    db,
    http,
    uploadLimit: uploadLimit,
    downloadLimit: downloadLimit,
    bundleChangeWatchMode: bundleChangeWatchMode,
  );
  return RichDevice(
    database: db,
    client: client,
    http: http,
    sourceId: sourceId,
    userId: userId,
  );
}

Future<SqliteNowDatabase> openBusinessDatabase(String path) async {
  final database = RichRealServerDb(path: path);
  await database.open();
  return database.runtimeDatabase;
}

DefaultOversqliteClient newBusinessClient(
  SqliteNowDatabase database,
  OversqliteHttpClient? http, {
  int uploadLimit = 8,
  int downloadLimit = 8,
  Resolver resolver = const ServerWinsResolver(),
  Duration automaticDownloadInterval = const Duration(seconds: 60),
  BundleChangeWatchMode bundleChangeWatchMode = BundleChangeWatchMode.off,
  Duration bundleChangeWatchReconnectMin = const Duration(milliseconds: 25),
  Duration bundleChangeWatchReconnectMax = const Duration(milliseconds: 50),
}) {
  return DefaultOversqliteClient(
    database: database,
    config: OversqliteConfig(
      schema: 'business',
      syncTables: businessSyncTables,
      uploadLimit: uploadLimit,
      downloadLimit: downloadLimit,
      automaticDownloadInterval: automaticDownloadInterval,
      bundleChangeWatchMode: bundleChangeWatchMode,
      bundleChangeWatchReconnectMin: bundleChangeWatchReconnectMin,
      bundleChangeWatchReconnectMax: bundleChangeWatchReconnectMax,
    ),
    httpClient: http,
    resolver: resolver,
  );
}

DefaultOversqliteClient newRichClient(
  RichRealServerDb database,
  OversqliteHttpClient http, {
  int uploadLimit = 8,
  int downloadLimit = 8,
  BundleChangeWatchMode bundleChangeWatchMode = BundleChangeWatchMode.off,
}) {
  return database.newOversqliteClient(
    schema: 'business',
    httpClient: http,
    syncTables: richSyncTables,
    uploadLimit: uploadLimit,
    downloadLimit: downloadLimit,
    bundleChangeWatchMode: bundleChangeWatchMode,
    bundleChangeWatchReconnectMin: const Duration(milliseconds: 25),
    bundleChangeWatchReconnectMax: const Duration(milliseconds: 50),
  );
}

Future<String> bootstrapBusinessSourceId(SqliteNowDatabase database) async {
  final client = newBusinessClient(database, null);
  await client.open();
  final source = (await client.sourceInfo()).currentSourceId;
  await client.close();
  return source;
}

Future<String> bootstrapRichSourceId(RichRealServerDb database) async {
  final client = DefaultOversqliteClient(
    database: database.runtimeDatabase,
    config: database.buildOversqliteConfig(
      schema: 'business',
      syncTables: richSyncTables,
      uploadLimit: 8,
      downloadLimit: 8,
    ),
    httpClient: null,
  );
  await client.open();
  final source = (await client.sourceInfo()).currentSourceId;
  await client.close();
  return source;
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

Future<void> insertBusinessUserAndPostBatch(
  SqliteNowDatabase database,
  String batchPrefix,
  int count,
) async {
  for (var index = 0; index < count; index++) {
    await insertBusinessUserAndPost(
      database,
      realserverUuid(),
      realserverUuid(),
      '$batchPrefix-$index',
    );
  }
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

Future<void> expectForeignKeyIntegrity(SqliteNowDatabase database) async {
  final rows = await database.connection.select(
    'PRAGMA foreign_key_check',
    (row) => row.readString(0),
  );
  expect(rows, isEmpty);
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

Future<void> eventually(
  Future<bool> Function() condition, {
  Duration timeout = const Duration(seconds: 5),
}) async {
  final deadline = DateTime.now().add(timeout);
  Object? lastError;
  while (DateTime.now().isBefore(deadline)) {
    try {
      if (await condition()) {
        return;
      }
    } catch (error) {
      lastError = error;
    }
    await Future<void>.delayed(const Duration(milliseconds: 25));
  }
  fail(
    'condition was not met within $timeout'
    '${lastError == null ? '' : ': $lastError'}',
  );
}

Future<CategoryGraphFixture> insertCategoryGraph(
  RichRealServerDb database,
  String label,
) async {
  final rootId = realserverUuid();
  final childId = realserverUuid();
  final leafId = realserverUuid();
  await database.connection.execute(
    'INSERT INTO categories(id, name, parent_id) VALUES(?, ?, NULL)',
    parameters: [rootId, 'Category root $label'],
  );
  await database.connection.execute(
    'INSERT INTO categories(id, name, parent_id) VALUES(?, ?, ?)',
    parameters: [childId, 'Category child $label', rootId],
  );
  await database.connection.execute(
    'INSERT INTO categories(id, name, parent_id) VALUES(?, ?, ?)',
    parameters: [leafId, 'Category leaf $label', childId],
  );
  return CategoryGraphFixture(
    rootId: rootId,
    childId: childId,
    leafId: leafId,
    label: label,
  );
}

Future<TeamGraphFixture> insertTeamGraph(
  RichRealServerDb database,
  String label,
) async {
  final teamId = realserverUuid();
  final captainId = realserverUuid();
  final memberId = realserverUuid();
  await database.transaction(() async {
    await database.connection.execute(
      'INSERT INTO teams(id, name, captain_member_id) VALUES(?, ?, ?)',
      parameters: [teamId, 'Team $label', captainId],
    );
    await database.connection.execute(
      'INSERT INTO team_members(id, name, team_id) VALUES(?, ?, ?)',
      parameters: [captainId, 'Captain $label', teamId],
    );
    await database.connection.execute(
      'INSERT INTO team_members(id, name, team_id) VALUES(?, ?, ?)',
      parameters: [memberId, 'Member $label', teamId],
    );
  });
  return TeamGraphFixture(
    teamId: teamId,
    captainId: captainId,
    memberId: memberId,
    label: label,
  );
}

Future<void> insertTypedRow(
  RichRealServerDb database,
  TypedRowFixture row,
) async {
  await database.connection.execute(
    '''INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)''',
    parameters: [
      row.id,
      row.name,
      row.note,
      row.countValue,
      row.enabledFlag,
      row.rating,
      row.data,
      row.createdAt,
    ],
  );
}

Future<void> insertBlobKeyPair(
  RichRealServerDb database,
  BlobKeyFixture row,
) async {
  await database.connection.execute(
    'INSERT INTO files(id, name, data) VALUES(?, ?, ?)',
    parameters: [row.fileId, 'File ${row.label}', row.data],
  );
  await database.connection.execute(
    'INSERT INTO file_reviews(id, review, file_id) VALUES(?, ?, ?)',
    parameters: [row.reviewId, 'Review ${row.label}', row.fileId],
  );
}

Future<void> assertTopologyState(
  RichRealServerDb database,
  CategoryGraphFixture category,
  TeamGraphFixture team,
  String location,
) async {
  expect(
    await scalarInt(
      database.runtimeDatabase,
      "SELECT COUNT(*) FROM categories WHERE id IN ('${category.rootId}', '${category.childId}', '${category.leafId}')",
    ),
    3,
  );
  expect(
    await scalarInt(
      database.runtimeDatabase,
      "SELECT COUNT(*) FROM teams WHERE id = '${team.teamId}'",
    ),
    1,
  );
  expect(
    await scalarInt(
      database.runtimeDatabase,
      "SELECT COUNT(*) FROM team_members WHERE id IN ('${team.captainId}', '${team.memberId}')",
    ),
    2,
  );
  expect(
    await scalarInt(database.runtimeDatabase, '''SELECT COUNT(*)
FROM categories c
JOIN categories p ON p.id = c.parent_id
WHERE c.id = '${category.childId}'
  AND p.id = '${category.rootId}' '''),
    1,
    reason: 'missing category child on $location',
  );
  expect(
    await scalarInt(database.runtimeDatabase, '''SELECT COUNT(*)
FROM teams t
JOIN team_members captain ON captain.id = t.captain_member_id
WHERE t.id = '${team.teamId}'
  AND captain.id = '${team.captainId}' '''),
    1,
    reason: 'missing deferred captain edge on $location',
  );
}

Future<void> assertTypedRowState(
  RichRealServerDb database,
  TypedRowFixture row,
) async {
  final rows = await database.typedRows.selectAll().asList();
  final actual = rows.singleWhere((item) => item.id == row.id);
  expect(actual.name, row.name);
  expect(actual.note, row.note);
  expect(actual.countValue, row.countValue);
  expect(actual.enabledFlag, row.enabledFlag);
  expect(actual.rating, row.rating);
  expect(actual.data, row.data);
  if (row.createdAt == null) {
    expect(actual.createdAt, isNull);
  } else {
    expect(actual.createdAt, isNotNull);
    expect(
      DateTime.parse(
        actual.createdAt!,
      ).isAtSameMomentAs(DateTime.parse(row.createdAt!)),
      isTrue,
    );
  }
}

Future<void> assertBlobKeyState(
  RichRealServerDb database,
  BlobKeyFixture rowA, [
  BlobKeyFixture? rowB,
]) async {
  final expectedRows = [rowA, ?rowB];
  final rows = await database.files.selectAll().asList();
  expect(rows, hasLength(expectedRows.length));
  for (final row in expectedRows) {
    final actual = rows.singleWhere((item) => bytesEqual(item.id, row.fileId));
    expect(actual.name, 'File ${row.label}');
    expect(actual.data, row.data);
    expect(
      await scalarText(
        database.runtimeDatabase,
        "SELECT review FROM file_reviews WHERE lower(hex(id)) = '${hexBytes(row.reviewId)}'",
      ),
      'Review ${row.label}',
    );
    expect(
      await scalarText(
        database.runtimeDatabase,
        "SELECT lower(hex(file_id)) FROM file_reviews WHERE lower(hex(id)) = '${hexBytes(row.reviewId)}'",
      ),
      hexBytes(row.fileId),
    );
  }
}

Uint8List uuidBytes(String value) {
  final hex = value.replaceAll('-', '');
  return Uint8List.fromList([
    for (var i = 0; i < hex.length; i += 2)
      int.parse(hex.substring(i, i + 2), radix: 16),
  ]);
}

String hexBytes(Uint8List value) {
  const chars = '0123456789abcdef';
  return [
    for (final byte in value)
      '${chars[(byte >> 4) & 0x0f]}${chars[byte & 0x0f]}',
  ].join();
}

bool bytesEqual(Uint8List left, Uint8List right) {
  if (left.length != right.length) return false;
  for (var i = 0; i < left.length; i++) {
    if (left[i] != right[i]) return false;
  }
  return true;
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

final class BusinessDevice {
  const BusinessDevice({
    required this.database,
    required this.client,
    required this.http,
    required this.sourceId,
    required this.userId,
  });

  final SqliteNowDatabase database;
  final DefaultOversqliteClient client;
  final IoOversqliteHttpClient http;
  final String sourceId;
  final String userId;

  Future<AttachConnected> openAndAttach(AttachOutcome outcome) async {
    await client.open();
    return expectConnected(client.attach(userId), outcome);
  }

  Future<void> close() async {
    await client.close();
    http.close();
    await database.close();
  }
}

final class RichDevice {
  const RichDevice({
    required this.database,
    required this.client,
    required this.http,
    required this.sourceId,
    required this.userId,
  });

  final RichRealServerDb database;
  final DefaultOversqliteClient client;
  final IoOversqliteHttpClient http;
  final String sourceId;
  final String userId;

  Future<AttachConnected> openAndAttach(AttachOutcome outcome) async {
    await client.open();
    return expectConnected(client.attach(userId), outcome);
  }

  Future<void> close() async {
    await client.close();
    http.close();
    await database.close();
  }
}

final class CategoryGraphFixture {
  const CategoryGraphFixture({
    required this.rootId,
    required this.childId,
    required this.leafId,
    required this.label,
  });

  final String rootId;
  final String childId;
  final String leafId;
  final String label;
}

final class TeamGraphFixture {
  const TeamGraphFixture({
    required this.teamId,
    required this.captainId,
    required this.memberId,
    required this.label,
  });

  final String teamId;
  final String captainId;
  final String memberId;
  final String label;
}

final class BlobKeyFixture {
  const BlobKeyFixture({
    required this.fileId,
    required this.reviewId,
    required this.label,
    required this.data,
  });

  final Uint8List fileId;
  final Uint8List reviewId;
  final String label;
  final Uint8List data;
}

final class TypedRowFixture {
  const TypedRowFixture({
    required this.id,
    required this.name,
    required this.note,
    required this.countValue,
    required this.enabledFlag,
    required this.rating,
    required this.data,
    required this.createdAt,
  });

  final String id;
  final String name;
  final String? note;
  final int? countValue;
  final int enabledFlag;
  final double? rating;
  final Uint8List? data;
  final String? createdAt;
}
