import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:sqlitenow_flutter_samplesync/src/sample_auth.dart';
import 'package:sqlitenow_flutter_samplesync/src/sample_sync_transport.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

void main() {
  test(
    'auth API accepts an empty password and validates server identity',
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      Map<String, Object?>? signInBody;
      final serving = server.forEach((request) async {
        request.response.headers.contentType = ContentType.json;
        if (request.uri.path == '/syncx/status') {
          request.response.write(jsonEncode({'app_name': 'samplesync-server'}));
        } else if (request.uri.path == '/dummy-signin') {
          signInBody =
              jsonDecode(await utf8.decoder.bind(request).join())
                  as Map<String, Object?>;
          request.response.write(
            jsonEncode({'token': 'token-1', 'expires_in': 180, 'user': 'u10'}),
          );
        } else {
          request.response.statusCode = HttpStatus.notFound;
        }
        await request.response.close();
      });
      addTearDown(() async {
        await server.close(force: true);
        await serving;
      });

      final api = IoSampleAuthApi(
        baseUri: _serverUri(server),
        now: () => DateTime.utc(2026, 7, 28),
      );
      await api.ensureSampleSyncServer();
      final token = await api.issueToken(
        user: 'u10',
        sourceId: 'source-1',
        password: '',
      );

      expect(token.value, 'token-1');
      expect(signInBody, {'user': 'u10', 'password': '', 'device': 'source-1'});
    },
  );

  test('auth refresh is single-flight', () async {
    final api = _CompletingAuthApi();
    final session = SampleAuthSession(
      api: api,
      user: 'u10',
      sourceId: 'source-1',
      password: '',
      initialToken: SampleAuthToken(
        value: 'expired',
        expiresAt: DateTime.utc(2026, 7, 28),
      ),
      now: () => DateTime.utc(2026, 7, 28, 0, 1),
    );
    addTearDown(session.close);

    final first = session.bearerToken();
    final second = session.bearerToken();
    expect(api.issueCalls, 1);
    api.complete(
      SampleAuthToken(value: 'fresh', expiresAt: DateTime.utc(2026, 7, 28, 1)),
    );

    expect(await first, 'fresh');
    expect(await second, 'fresh');
  });

  test(
    'transport refreshes once after 401 and retries with the new token',
    () async {
      final authorizations = <String?>[];
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final serving = server.forEach((request) async {
        authorizations.add(
          request.headers.value(HttpHeaders.authorizationHeader),
        );
        if (authorizations.length == 1) {
          request.response.statusCode = HttpStatus.unauthorized;
          request.response.write('expired');
        } else {
          request.response.headers.contentType = ContentType.json;
          request.response.write('{}');
        }
        await request.response.close();
      });
      addTearDown(() async {
        await server.close(force: true);
        await serving;
      });

      final api = _ImmediateAuthApi();
      final session = SampleAuthSession(
        api: api,
        user: 'u10',
        sourceId: 'source-1',
        password: '',
        initialToken: SampleAuthToken(
          value: 'old',
          expiresAt: DateTime.now().toUtc().add(const Duration(hours: 1)),
        ),
      );
      addTearDown(session.close);
      final transport = SampleSyncTransport(
        baseUri: _serverUri(server),
        authSession: session,
      );
      addTearDown(transport.close);

      final response = await transport.get(
        'sync/test',
        sourceId: 'source-1',
        operation: 'test',
        bounds: const OversqliteHttpRequestBounds(
          successBodyBytes: 1024,
          errorBodyBytes: 1024,
        ),
      );

      expect(response.statusCode, HttpStatus.ok);
      expect(api.issueCalls, 1);
      expect(authorizations, ['Bearer old', 'Bearer new']);
    },
  );
}

Uri _serverUri(HttpServer server) {
  return Uri.parse('http://${server.address.address}:${server.port}/');
}

final class _CompletingAuthApi implements SampleAuthApi {
  final _completer = Completer<SampleAuthToken>();
  var issueCalls = 0;

  @override
  Future<void> ensureSampleSyncServer() async {}

  @override
  Future<SampleAuthToken> issueToken({
    required String user,
    required String sourceId,
    required String password,
  }) {
    issueCalls++;
    return _completer.future;
  }

  void complete(SampleAuthToken token) => _completer.complete(token);
}

final class _ImmediateAuthApi implements SampleAuthApi {
  var issueCalls = 0;

  @override
  Future<void> ensureSampleSyncServer() async {}

  @override
  Future<SampleAuthToken> issueToken({
    required String user,
    required String sourceId,
    required String password,
  }) async {
    issueCalls++;
    return SampleAuthToken(
      value: 'new',
      expiresAt: DateTime.now().toUtc().add(const Duration(hours: 1)),
    );
  }
}
