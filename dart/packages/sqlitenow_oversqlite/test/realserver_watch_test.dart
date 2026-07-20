import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

import 'realserver_support.dart';

void main() {
  final realserverEnabled = flagEnabled('OVERSQLITE_REALSERVER_TESTS');
  group(
    'realserver watch',
    skip: realserverEnabled
        ? null
        : 'Set OVERSQLITE_REALSERVER_TESTS=true to run live realserver tests.',
    () {
      late RealServerConfig config;

      setUpAll(() async {
        config = await requireRealServerConfig();
      });

      test(
        'watch-enabled capability probe reports bundle_change_watch',
        () async {
          final db = await openBusinessDatabase();
          addTearDown(db.close);
          final sourceId = await bootstrapManagedSourceId(db);
          final userId = randomRealserverId('dart-watch-cap-user');
          final http = await authenticatedHttp(
            config.baseUrl,
            userId,
            sourceId,
          );
          addTearDown(http.close);

          final response = await http.get(
            'sync/capabilities',
            sourceId: sourceId,
            operation: 'capabilities request',
            bounds: const OversqliteHttpRequestBounds(
              successBodyBytes: 4 * 1024 * 1024,
              errorBodyBytes: 64 * 1024,
            ),
          );
          final capabilities = CapabilitiesResponse.fromJson(
            jsonDecode(response.body) as Map<String, Object?>,
          );

          expect(
            capabilities.bundleChangeWatchSupported,
            isTrue,
            reason:
                'realserver at ${config.baseUrl} must advertise features.bundle_change_watch=true',
          );
        },
      );

      test('watch-triggered two-client convergence uses SSE wakeup', () async {
        await resetRealServerState(config.baseUrl);
        final userId = randomRealserverId('dart-watch-user');
        final writerDb = await openBusinessDatabase();
        final observerDb = await openBusinessDatabase();
        addTearDown(writerDb.close);
        addTearDown(observerDb.close);

        final writerSource = await bootstrapManagedSourceId(writerDb);
        final observerSource = await bootstrapManagedSourceId(observerDb);
        final writerHttp = await authenticatedHttp(
          config.baseUrl,
          userId,
          writerSource,
        );
        final observerHttp = await authenticatedHttp(
          config.baseUrl,
          userId,
          observerSource,
        );
        addTearDown(writerHttp.close);
        addTearDown(observerHttp.close);
        final writer = newRealServerClient(writerDb, writerHttp);
        final observer = _newWatchClient(observerDb, observerHttp);
        addTearDown(writer.close);
        addTearDown(observer.close);

        await writer.open();
        await expectConnected(
          writer.attach(userId),
          AttachOutcome.startedEmpty,
        );
        await observer.open();
        await expectConnected(
          observer.attach(userId),
          AttachOutcome.usedRemoteState,
        );

        final worker = observer.startAutomaticDownloads();
        addTearDown(worker.stop);
        await _eventually(
          () async => await _watchSubscriberCount(config.baseUrl, userId) == 1,
        );

        final rowUserId = realserverUuid();
        final rowPostId = realserverUuid();
        await insertBusinessUserAndPost(
          writerDb,
          rowUserId,
          rowPostId,
          'watch',
        );
        expect((await writer.pushPending()).outcome, PushOutcome.committed);

        await _eventually(() async {
          return await scalarInt(
                observerDb,
                "SELECT COUNT(*) FROM users WHERE id = '$rowUserId'",
              ) ==
              1;
        });

        expect(
          await scalarText(
            observerDb,
            "SELECT name FROM users WHERE id = '$rowUserId'",
          ),
          'User watch',
        );
        expect(
          await scalarText(
            observerDb,
            "SELECT title FROM posts WHERE id = '$rowPostId'",
          ),
          'Title watch',
        );
        expect(
          (await observer.syncStatus()).pending.hasPendingSyncData,
          isFalse,
        );
      });

      test('watch cancellation stops idle worker promptly', () async {
        await resetRealServerState(config.baseUrl);
        final userId = randomRealserverId('dart-watch-cancel-user');
        final db = await openBusinessDatabase();
        addTearDown(db.close);
        final sourceId = await bootstrapManagedSourceId(db);
        final http = await authenticatedHttp(config.baseUrl, userId, sourceId);
        addTearDown(http.close);
        final client = _newWatchClient(db, http);
        addTearDown(client.close);
        await client.open();
        await expectConnected(
          client.attach(userId),
          AttachOutcome.startedEmpty,
        );

        final worker = client.startAutomaticDownloads();
        await _eventually(
          () async => await _watchSubscriberCount(config.baseUrl, userId) == 1,
        );

        await worker.stop().timeout(const Duration(seconds: 1));

        await _eventually(
          () async => await _watchSubscriberCount(config.baseUrl, userId) == 0,
        );
      });
    },
  );
}

DefaultOversqliteClient _newWatchClient(
  SqliteNowDatabase database,
  OversqliteHttpClient http,
) {
  return DefaultOversqliteClient(
    database: database,
    config: OversqliteConfig(
      schema: 'business',
      syncTables: businessSyncTables,
      uploadLimit: 8,
      downloadLimit: 8,
      automaticDownloadInterval: Duration(seconds: 60),
      bundleChangeWatchMode: BundleChangeWatchMode.auto,
      bundleChangeWatchReconnectMin: Duration(milliseconds: 25),
      bundleChangeWatchReconnectMax: Duration(milliseconds: 50),
    ),
    httpClient: http,
  );
}

Future<int> _watchSubscriberCount(String baseUrl, String userId) async {
  final response = await sendJson(
    'GET',
    baseUrl,
    'test/watch-subscribers?user_id=${Uri.encodeQueryComponent(userId)}',
  );
  if (response.statusCode != HttpStatus.ok) {
    throw StateError(
      'watch subscriber count failed: HTTP ${response.statusCode} ${response.body}',
    );
  }
  final decoded = jsonDecode(response.body) as Map<String, Object?>;
  return decoded['subscriber_count']! as int;
}

Future<void> _eventually(
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
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  fail(
    'condition was not met within $timeout${lastError == null ? '' : ': $lastError'}',
  );
}
