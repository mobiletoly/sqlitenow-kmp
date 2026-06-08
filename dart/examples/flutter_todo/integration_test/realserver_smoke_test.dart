import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

import 'generated/rich_real_server_db.dart';

const _realserverEnabled = bool.fromEnvironment('OVERSQLITE_REALSERVER_TESTS');
const _baseUrl = String.fromEnvironment(
  'OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL',
  defaultValue: 'http://10.0.2.2:8080',
);

final _random = Random();

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets(
    'generated oversqlite database syncs against realserver on Flutter',
    skip: !_realserverEnabled,
    (tester) async {
      await _requireRealServer();
      await _resetRealServerState();
      final userId = _randomId('flutter-rich-user');
      final tempDir = await Directory.systemTemp.createTemp(
        'sqlitenow-flutter-realserver-',
      );
      addTearDown(() => tempDir.delete(recursive: true));

      final writerDb = RichRealServerDb(path: '${tempDir.path}/writer-rich.db');
      final readerDb = RichRealServerDb(path: '${tempDir.path}/reader-rich.db');
      addTearDown(writerDb.close);
      addTearDown(readerDb.close);
      await writerDb.open();
      await readerDb.open();

      final writerSource = await _bootstrapSource(writerDb);
      final readerSource = await _bootstrapSource(readerDb);
      final writerHttp = await _authenticatedHttp(userId, writerSource);
      final readerHttp = await _authenticatedHttp(userId, readerSource);
      addTearDown(writerHttp.close);
      addTearDown(readerHttp.close);

      final writer = _newClient(writerDb, writerHttp);
      final reader = _newClient(readerDb, readerHttp);
      addTearDown(writer.close);
      addTearDown(reader.close);

      await writer.open();
      await _expectConnected(writer.attach(userId), AttachOutcome.startedEmpty);
      await reader.open();
      await _expectConnected(
        reader.attach(userId),
        AttachOutcome.usedRemoteState,
      );

      final rowId = _uuid();
      final typedRowId = _uuid();
      final payload = Uint8List.fromList([0x10, 0x20, 0x30, 0x40]);
      await writerDb.connection.execute(
        'INSERT INTO users(id, name, email) VALUES(?, ?, ?)',
        parameters: [rowId, 'Flutter Rich User', 'flutter-rich@example.com'],
      );
      await writerDb.connection.execute(
        '''INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)''',
        parameters: [
          typedRowId,
          'Flutter Typed Row',
          'device',
          7,
          1,
          2.5,
          payload,
          '2026-03-24T18:42:11Z',
        ],
      );

      expect((await writer.pushPending()).outcome, PushOutcome.committed);
      expect(
        (await reader.pullToStable()).outcome,
        RemoteSyncOutcome.appliedIncremental,
      );

      final users = await readerDb.users.selectAll().asList();
      expect(
        users.singleWhere((row) => row.id == rowId).name,
        'Flutter Rich User',
      );
      final typed = (await readerDb.typedRows.selectAll().asList()).singleWhere(
        (row) => row.id == typedRowId,
      );
      expect(typed.name, 'Flutter Typed Row');
      expect(typed.note, 'device');
      expect(typed.countValue, 7);
      expect(typed.enabledFlag, 1);
      expect(typed.rating, 2.5);
      expect(typed.data, payload);
      expect(typed.createdAt, isNotNull);
      expect(
        DateTime.parse(
          typed.createdAt!,
        ).isAtSameMomentAs(DateTime.parse('2026-03-24T18:42:11Z')),
        isTrue,
      );
    },
    timeout: const Timeout(Duration(minutes: 2)),
  );
}

DefaultOversqliteClient _newClient(
  RichRealServerDb database,
  OversqliteHttpClient http,
) {
  return database.newOversqliteClient(
    schema: 'business',
    httpClient: http,
    uploadLimit: 8,
    downloadLimit: 8,
  );
}

Future<String> _bootstrapSource(RichRealServerDb database) async {
  final client = DefaultOversqliteClient(
    database: database.runtimeDatabase,
    config: database.buildOversqliteConfig(
      schema: 'business',
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

Future<void> _expectConnected(
  Future<AttachResult> future,
  AttachOutcome outcome,
) async {
  final result = await future;
  expect(result, isA<AttachConnected>());
  expect((result as AttachConnected).outcome, outcome);
}

Future<IoOversqliteHttpClient> _authenticatedHttp(
  String userId,
  String sourceId,
) async {
  final token = await _issueDummySigninToken(userId, sourceId);
  return IoOversqliteHttpClient(
    baseUri: Uri.parse(_baseUrl),
    defaultHeaders: {HttpHeaders.authorizationHeader: 'Bearer $token'},
  );
}

Future<String> _issueDummySigninToken(String userId, String sourceId) async {
  final response = await _sendJson(
    'POST',
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

Future<void> _requireRealServer() async {
  final health = await _sendJson('GET', 'syncx/health');
  if (health.statusCode != HttpStatus.ok) {
    throw StateError(
      'realserver unavailable at $_baseUrl: HTTP ${health.statusCode} ${health.body}',
    );
  }
  final status = await _sendJson('GET', 'syncx/status');
  if (status.statusCode != HttpStatus.ok) {
    throw StateError(
      'realserver status failed at $_baseUrl: HTTP ${status.statusCode} ${status.body}',
    );
  }
  final body = jsonDecode(status.body) as Map<String, Object?>;
  if (body['app_name'] != 'nethttp-server-example') {
    throw StateError(
      "realserver requires app_name='nethttp-server-example', got '${body['app_name']}'",
    );
  }
}

Future<void> _resetRealServerState() async {
  final response = await _sendJson('POST', 'test/reset', body: {});
  if (response.statusCode != HttpStatus.ok) {
    throw StateError(
      'server reset failed: HTTP ${response.statusCode} ${response.body}',
    );
  }
}

Future<_HttpResponseBody> _sendJson(
  String method,
  String path, {
  Object? body,
}) async {
  final http = HttpClient();
  try {
    final request = await http.openUrl(method, _resolve(path));
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

Uri _resolve(String path) {
  final normalizedBase = _baseUrl.endsWith('/') ? _baseUrl : '$_baseUrl/';
  final normalizedPath = path.startsWith('/') ? path.substring(1) : path;
  return Uri.parse(normalizedBase).resolve(normalizedPath);
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

final class _HttpResponseBody {
  const _HttpResponseBody({required this.statusCode, required this.body});

  final int statusCode;
  final String body;
}
