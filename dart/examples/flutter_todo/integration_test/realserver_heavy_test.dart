import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

import 'realserver_test_support.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  final skipHeavy = !realserverEnabled || !heavyRealserverEnabled;

  group('Flutter Android realserver heavy stress', () {
    testWidgets(
      'dirty set larger than one chunk pushes and converges',
      skip: skipHeavy,
      (tester) async {
        final config = await requireRealServerConfig();
        await resetRealServerState(config.baseUrl);
        final tempDir = await createRealserverTempDir();
        final userId = randomRealserverId('flutter-heavy-chunk-user');
        final writer = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'chunk-writer.db',
          baseUrl: config.baseUrl,
          userId: userId,
          uploadLimit: 2,
          downloadLimit: 2,
        );
        final reader = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'chunk-reader.db',
          baseUrl: config.baseUrl,
          userId: userId,
          uploadLimit: 2,
          downloadLimit: 2,
        );
        addTearDown(writer.close);
        addTearDown(reader.close);

        await writer.openAndAttach(AttachOutcome.startedEmpty);
        await reader.openAndAttach(AttachOutcome.usedRemoteState);
        await insertBusinessUserAndPostBatch(writer.database, 'chunked', 5);
        expect(
          await scalarInt(
            writer.database,
            'SELECT COUNT(*) FROM _sync_dirty_rows',
          ),
          10,
        );

        expect(
          (await writer.client.pushPending()).outcome,
          PushOutcome.committed,
        );
        expect(
          (await reader.client.pullToStable()).outcome,
          RemoteSyncOutcome.appliedIncremental,
        );
        expect(
          await scalarInt(reader.database, 'SELECT COUNT(*) FROM users'),
          5,
        );
        expect(
          await scalarInt(reader.database, 'SELECT COUNT(*) FROM posts'),
          5,
        );
        expect(
          await scalarText(
            reader.database,
            "SELECT title FROM posts WHERE title = 'Title chunked-4'",
          ),
          'Title chunked-4',
        );
        await expectCleanSyncTables(writer.database);
        await expectCleanSyncTables(reader.database);
      },
      timeout: const Timeout(Duration(minutes: 2)),
    );

    testWidgets(
      'stale follower pruned under load rebuilds from snapshot',
      skip: skipHeavy,
      (tester) async {
        final config = await requireRealServerConfig();
        await resetRealServerState(config.baseUrl);
        final tempDir = await createRealserverTempDir();
        final userId = randomRealserverId('flutter-heavy-prune-user');
        final leader = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'prune-leader.db',
          baseUrl: config.baseUrl,
          userId: userId,
          uploadLimit: 2,
          downloadLimit: 2,
        );
        final follower = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'prune-follower.db',
          baseUrl: config.baseUrl,
          userId: userId,
          uploadLimit: 2,
          downloadLimit: 2,
        );
        addTearDown(leader.close);
        addTearDown(follower.close);

        await leader.openAndAttach(AttachOutcome.startedEmpty);
        await follower.openAndAttach(AttachOutcome.usedRemoteState);

        String lastUser = '';
        for (var round = 1; round <= 6; round++) {
          lastUser = realserverUuid();
          await insertBusinessUserAndPost(
            leader.database,
            lastUser,
            realserverUuid(),
            'prune-$round',
          );
          expect(
            (await leader.client.pushPending()).outcome,
            PushOutcome.committed,
          );
          if (round == 2) {
            expect(
              (await follower.client.pullToStable()).outcome,
              RemoteSyncOutcome.appliedIncremental,
            );
          }
        }

        final leaderSeq = (await leader.client.syncStatus()).lastBundleSeqSeen;
        final followerSource = await currentSourceId(follower.database);
        await setRetainedBundleFloor(config.baseUrl, userId, leaderSeq);
        final report = await follower.client.pullToStable();

        expect(report.outcome, RemoteSyncOutcome.appliedSnapshot);
        expect(
          (await follower.client.syncStatus()).lastBundleSeqSeen,
          leaderSeq,
        );
        expect(await currentSourceId(follower.database), followerSource);
        expect(
          await scalarInt(follower.database, 'SELECT COUNT(*) FROM users'),
          6,
        );
        expect(
          await scalarText(
            follower.database,
            "SELECT name FROM users WHERE id = '$lastUser'",
          ),
          'User prune-6',
        );
        await expectCleanSyncTables(follower.database);
      },
      timeout: const Timeout(Duration(minutes: 3)),
    );

    testWidgets(
      'shared database tolerates concurrent reads while syncing',
      skip: skipHeavy,
      (tester) async {
        final config = await requireRealServerConfig();
        await resetRealServerState(config.baseUrl);
        final tempDir = await createRealserverTempDir();
        final userId = randomRealserverId('flutter-heavy-shared-user');
        final seed = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'shared-seed.db',
          baseUrl: config.baseUrl,
          userId: userId,
          uploadLimit: 2,
          downloadLimit: 1,
        );
        final active = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'shared-active.db',
          baseUrl: config.baseUrl,
          userId: userId,
          uploadLimit: 2,
          downloadLimit: 1,
        );
        addTearDown(seed.close);
        addTearDown(active.close);

        await seed.openAndAttach(AttachOutcome.startedEmpty);
        await active.openAndAttach(AttachOutcome.usedRemoteState);
        for (var index = 1; index <= 5; index++) {
          await insertBusinessUserAndPost(
            seed.database,
            realserverUuid(),
            realserverUuid(),
            'shared-seed-$index',
          );
          expect(
            (await seed.client.pushPending()).outcome,
            PushOutcome.committed,
          );
        }
        await active.client.pullToStable();

        final readerErrors = <Object>[];
        Future<void> reader(int readerIndex) async {
          for (var iteration = 0; iteration < 30; iteration++) {
            try {
              expect(
                await scalarInt(active.database, 'SELECT COUNT(*) FROM users'),
                greaterThanOrEqualTo(5),
              );
              await scalarText(
                active.database,
                "SELECT name FROM users ORDER BY id LIMIT 1 OFFSET ${readerIndex % 3}",
              );
            } catch (error) {
              readerErrors.add(error);
            }
          }
        }

        final remoteWriter = () async {
          for (var round = 1; round <= 6; round++) {
            await insertBusinessUserAndPost(
              seed.database,
              realserverUuid(),
              realserverUuid(),
              'shared-remote-$round',
            );
            expect(
              (await seed.client.pushPending()).outcome,
              PushOutcome.committed,
            );
          }
        }();
        final syncer = () async {
          while ((await active.client.syncStatus()).lastBundleSeqSeen < 11) {
            await active.client.pullToStable();
          }
        }();

        await Future.wait([remoteWriter, syncer, reader(0), reader(1)]);

        expect(readerErrors, isEmpty);
        expect(
          await scalarInt(active.database, 'SELECT COUNT(*) FROM users'),
          11,
        );
        expect(
          await scalarInt(active.database, 'SELECT COUNT(*) FROM posts'),
          11,
        );
        expect((await active.client.syncStatus()).lastBundleSeqSeen, 11);
        await expectCleanSyncTables(active.database);
      },
      timeout: const Timeout(Duration(minutes: 5)),
    );
  });
}
