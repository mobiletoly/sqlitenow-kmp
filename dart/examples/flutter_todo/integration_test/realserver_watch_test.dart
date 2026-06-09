import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

import 'realserver_test_support.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('Flutter Android realserver bundle-change watch', () {
    testWidgets(
      'capability probe reports bundle-change watch support',
      skip: !realserverEnabled,
      (tester) async {
        final config = await requireRealServerConfig();
        final tempDir = await createRealserverTempDir();
        final userId = randomRealserverId('flutter-watch-cap-user');
        final device = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'watch-cap.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        addTearDown(device.close);

        await device.client.open();
        final capabilities = await device.client.fetchCapabilities();
        expect(
          capabilities.bundleChangeWatchSupported,
          isTrue,
          reason:
              'realserver at ${config.baseUrl} must advertise features.bundle_change_watch=true',
        );
      },
      timeout: const Timeout(Duration(minutes: 1)),
    );

    testWidgets(
      'SSE wake-up pulls remote bundles and cancellation stops the worker',
      skip: !realserverEnabled,
      (tester) async {
        final config = await requireRealServerConfig();
        await resetRealServerState(config.baseUrl);
        final tempDir = await createRealserverTempDir();
        final userId = randomRealserverId('flutter-watch-user');
        final writer = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'watch-writer.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        final observer = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'watch-observer.db',
          baseUrl: config.baseUrl,
          userId: userId,
          automaticDownloadInterval: const Duration(seconds: 60),
          bundleChangeWatchMode: BundleChangeWatchMode.auto,
        );
        addTearDown(writer.close);
        addTearDown(observer.close);

        await writer.openAndAttach(AttachOutcome.startedEmpty);
        await observer.openAndAttach(AttachOutcome.usedRemoteState);

        final worker = observer.client.startAutomaticDownloads();
        addTearDown(worker.stop);
        await eventually(
          () async => await watchSubscriberCount(config.baseUrl, userId) == 1,
        );

        final rowUser = realserverUuid();
        final rowPost = realserverUuid();
        await insertBusinessUserAndPost(
          writer.database,
          rowUser,
          rowPost,
          'watch',
        );
        expect(
          (await writer.client.pushPending()).outcome,
          PushOutcome.committed,
        );

        await eventually(() async {
          return await scalarInt(
                observer.database,
                "SELECT COUNT(*) FROM users WHERE id = '$rowUser'",
              ) ==
              1;
        });
        expect(
          await scalarText(
            observer.database,
            "SELECT title FROM posts WHERE id = '$rowPost'",
          ),
          'Title watch',
        );
        expect(
          (await observer.client.syncStatus()).pending.hasPendingSyncData,
          isFalse,
        );

        await worker.stop().timeout(const Duration(seconds: 1));
        await eventually(
          () async => await watchSubscriberCount(config.baseUrl, userId) == 0,
        );
      },
      timeout: const Timeout(Duration(minutes: 2)),
    );
  });
}
