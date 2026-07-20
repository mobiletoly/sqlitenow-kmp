import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

import 'realserver_support.dart';

void main() {
  final realserverEnabled = flagEnabled('OVERSQLITE_REALSERVER_TESTS');
  final heavyEnabled = flagEnabled('OVERSQLITE_REALSERVER_HEAVY');
  final skipReason = !realserverEnabled
      ? 'Set OVERSQLITE_REALSERVER_TESTS=true to run live realserver tests.'
      : !heavyEnabled
      ? 'Set OVERSQLITE_REALSERVER_HEAVY=true to run heavy live realserver tests.'
      : null;

  group(
    'realserver heavy stress',
    skip: skipReason,
    () {
      late RealServerConfig config;

      setUpAll(() async {
        config = await requireRealServerConfig();
      });

      test('small chunks and interleaved same-user writers converge', () async {
        await resetRealServerState(config.baseUrl);
        final userId = randomRealserverId('dart-heavy-interleaved-user');
        final dbA = await openBusinessDatabase();
        final dbB = await openBusinessDatabase();
        final observerDb = await openBusinessDatabase();
        addTearDown(dbA.close);
        addTearDown(dbB.close);
        addTearDown(observerDb.close);

        final sourceA = await bootstrapManagedSourceId(dbA);
        final sourceB = await bootstrapManagedSourceId(dbB);
        final observerSource = await bootstrapManagedSourceId(observerDb);
        final httpA = await authenticatedHttp(config.baseUrl, userId, sourceA);
        final httpB = await authenticatedHttp(config.baseUrl, userId, sourceB);
        final observerHttp = await authenticatedHttp(
          config.baseUrl,
          userId,
          observerSource,
        );
        addTearDown(httpA.close);
        addTearDown(httpB.close);
        addTearDown(observerHttp.close);

        final clientA = newRealServerClient(
          dbA,
          httpA,
          uploadLimit: 1,
          downloadLimit: 1,
        );
        final clientB = newRealServerClient(
          dbB,
          httpB,
          uploadLimit: 1,
          downloadLimit: 1,
        );
        final observer = newRealServerClient(
          observerDb,
          observerHttp,
          uploadLimit: 1,
          downloadLimit: 1,
        );
        addTearDown(clientA.close);
        addTearDown(clientB.close);
        addTearDown(observer.close);

        await clientA.open();
        await expectConnected(
          clientA.attach(userId),
          AttachOutcome.startedEmpty,
        );
        await clientB.open();
        await expectConnected(
          clientB.attach(userId),
          AttachOutcome.usedRemoteState,
        );
        await observer.open();
        await expectConnected(
          observer.attach(userId),
          AttachOutcome.usedRemoteState,
        );

        final representativeA = realserverUuid();
        final representativeB = realserverUuid();
        for (var round = 1; round <= 6; round++) {
          final userA = round == 1 ? representativeA : realserverUuid();
          await insertBusinessUserAndPost(
            dbA,
            userA,
            realserverUuid(),
            'heavy-a-$round',
          );
          expect((await clientA.pushPending()).outcome, PushOutcome.committed);
          expect(
            (await clientB.pullToStable()).outcome,
            RemoteSyncOutcome.appliedIncremental,
          );

          final userB = round == 1 ? representativeB : realserverUuid();
          await insertBusinessUserAndPost(
            dbB,
            userB,
            realserverUuid(),
            'heavy-b-$round',
          );
          expect((await clientB.pushPending()).outcome, PushOutcome.committed);

          expect(
            (await clientA.pullToStable()).outcome,
            isIn([
              RemoteSyncOutcome.appliedIncremental,
              RemoteSyncOutcome.alreadyAtTarget,
            ]),
          );
          expect(
            (await observer.pullToStable()).outcome,
            isIn([
              RemoteSyncOutcome.appliedIncremental,
              RemoteSyncOutcome.alreadyAtTarget,
            ]),
          );
        }

        for (final db in [dbA, dbB, observerDb]) {
          expect(await scalarInt(db, 'SELECT COUNT(*) FROM users'), 12);
          expect(await scalarInt(db, 'SELECT COUNT(*) FROM posts'), 12);
          expect(
            await scalarText(
              db,
              "SELECT name FROM users WHERE id = '$representativeA'",
            ),
            'User heavy-a-1',
          );
          expect(
            await scalarText(
              db,
              "SELECT name FROM users WHERE id = '$representativeB'",
            ),
            'User heavy-b-1',
          );
          await expectCleanSyncTables(db);
        }
        expect((await clientA.syncStatus()).lastBundleSeqSeen, 12);
        expect((await clientB.syncStatus()).lastBundleSeqSeen, 12);
        expect((await observer.syncStatus()).lastBundleSeqSeen, 12);
      });

      test('stale follower pruned under load rebuilds from snapshot', () async {
        await resetRealServerState(config.baseUrl);
        final userId = randomRealserverId('dart-heavy-prune-user');
        final leaderDb = await openBusinessDatabase();
        final followerDb = await openBusinessDatabase();
        addTearDown(leaderDb.close);
        addTearDown(followerDb.close);

        final leaderSource = await bootstrapManagedSourceId(leaderDb);
        final followerSource = await bootstrapManagedSourceId(followerDb);
        final leaderHttp = await authenticatedHttp(
          config.baseUrl,
          userId,
          leaderSource,
        );
        final followerHttp = await authenticatedHttp(
          config.baseUrl,
          userId,
          followerSource,
        );
        addTearDown(leaderHttp.close);
        addTearDown(followerHttp.close);

        final leader = newRealServerClient(
          leaderDb,
          leaderHttp,
          uploadLimit: 2,
          downloadLimit: 2,
        );
        final follower = newRealServerClient(
          followerDb,
          followerHttp,
          uploadLimit: 2,
          downloadLimit: 2,
        );
        addTearDown(leader.close);
        addTearDown(follower.close);

        await leader.open();
        await expectConnected(
          leader.attach(userId),
          AttachOutcome.startedEmpty,
        );
        await follower.open();
        await expectConnected(
          follower.attach(userId),
          AttachOutcome.usedRemoteState,
        );

        String lastLeaderUser = '';
        for (var round = 1; round <= 10; round++) {
          lastLeaderUser = realserverUuid();
          await insertBusinessUserAndPost(
            leaderDb,
            lastLeaderUser,
            realserverUuid(),
            'prune-heavy-$round',
          );
          expect((await leader.pushPending()).outcome, PushOutcome.committed);
          if (round == 3) {
            expect(
              (await follower.pullToStable()).outcome,
              RemoteSyncOutcome.appliedIncremental,
            );
            expect((await follower.syncStatus()).lastBundleSeqSeen, 3);
          }
        }

        final leaderSeq = (await leader.syncStatus()).lastBundleSeqSeen;
        final followerSourceBefore = await currentSourceId(followerDb);
        expect(leaderSeq, 10);
        await setRetainedBundleFloor(config.baseUrl, userId, leaderSeq);

        final report = await follower.pullToStable();
        expect(report.outcome, RemoteSyncOutcome.appliedSnapshot);
        expect((await follower.syncStatus()).lastBundleSeqSeen, leaderSeq);
        expect(await currentSourceId(followerDb), followerSourceBefore);
        expect(await scalarInt(followerDb, 'SELECT COUNT(*) FROM users'), 10);
        expect(await scalarInt(followerDb, 'SELECT COUNT(*) FROM posts'), 10);
        expect(
          await scalarText(
            followerDb,
            "SELECT name FROM users WHERE id = '$lastLeaderUser'",
          ),
          'User prune-heavy-10',
        );
        await expectCleanSyncTables(followerDb);
      });

      test('repeated client-wins conflicts converge after prior bundles', () async {
        await resetRealServerState(config.baseUrl);
        final userId = randomRealserverId('dart-heavy-conflict-user');
        final dbA = await openBusinessDatabase();
        final dbB = await openBusinessDatabase();
        final observerDb = await openBusinessDatabase();
        addTearDown(dbA.close);
        addTearDown(dbB.close);
        addTearDown(observerDb.close);

        final sourceA = await bootstrapManagedSourceId(dbA);
        final sourceB = await bootstrapManagedSourceId(dbB);
        final observerSource = await bootstrapManagedSourceId(observerDb);
        final httpA = await authenticatedHttp(config.baseUrl, userId, sourceA);
        final httpB = await authenticatedHttp(config.baseUrl, userId, sourceB);
        final observerHttp = await authenticatedHttp(
          config.baseUrl,
          userId,
          observerSource,
        );
        addTearDown(httpA.close);
        addTearDown(httpB.close);
        addTearDown(observerHttp.close);

        final clientA = newRealServerClient(
          dbA,
          httpA,
          uploadLimit: 1,
          downloadLimit: 1,
        );
        final clientB = newRealServerClient(
          dbB,
          httpB,
          uploadLimit: 1,
          downloadLimit: 1,
          resolver: const ClientWinsResolver(),
        );
        final observer = newRealServerClient(
          observerDb,
          observerHttp,
          uploadLimit: 1,
          downloadLimit: 1,
        );
        addTearDown(clientA.close);
        addTearDown(clientB.close);
        addTearDown(observer.close);

        await clientA.open();
        await expectConnected(
          clientA.attach(userId),
          AttachOutcome.startedEmpty,
        );
        await clientB.open();
        await expectConnected(
          clientB.attach(userId),
          AttachOutcome.usedRemoteState,
        );
        await observer.open();
        await expectConnected(
          observer.attach(userId),
          AttachOutcome.usedRemoteState,
        );

        final hotUser = realserverUuid();
        await insertBusinessUserAndPost(
          dbA,
          hotUser,
          realserverUuid(),
          'conflict-heavy-base',
        );
        expect((await clientA.pushPending()).outcome, PushOutcome.committed);
        await clientB.pullToStable();
        await observer.pullToStable();

        for (var bundle = 1; bundle <= 3; bundle++) {
          await insertBusinessUserAndPost(
            dbA,
            realserverUuid(),
            realserverUuid(),
            'conflict-prior-$bundle',
          );
          expect((await clientA.pushPending()).outcome, PushOutcome.committed);
          await clientB.pullToStable();
          await observer.pullToStable();
        }

        for (var round = 1; round <= 3; round++) {
          await dbA.connection.execute(
            "UPDATE users SET name = 'Server Round $round' WHERE id = '$hotUser'",
          );
          expect((await clientA.pushPending()).outcome, PushOutcome.committed);
          await dbB.connection.execute(
            "UPDATE users SET name = 'Client Round $round' WHERE id = '$hotUser'",
          );
          expect((await clientB.pushPending()).outcome, PushOutcome.committed);
          await clientA.pullToStable();
          await clientB.pullToStable();
          await observer.pullToStable();

          for (final db in [dbA, dbB, observerDb]) {
            expect(
              await scalarText(
                db,
                "SELECT name FROM users WHERE id = '$hotUser'",
              ),
              'Client Round $round',
            );
          }
        }

        expect((await clientA.syncStatus()).lastBundleSeqSeen, 10);
        expect((await clientB.syncStatus()).lastBundleSeqSeen, 10);
        expect((await observer.syncStatus()).lastBundleSeqSeen, 10);
        for (final db in [dbA, dbB, observerDb]) {
          await expectCleanSyncTables(db);
        }
      });

      test(
        'source recovery after several bundles accepts replacement source',
        () async {
          await resetRealServerState(config.baseUrl);
          final userId = randomRealserverId('dart-heavy-source-user');
          final seedDb = await openBusinessDatabase();
          final recoverDb = await openBusinessDatabase();
          final verifyDb = await openBusinessDatabase();
          addTearDown(seedDb.close);
          addTearDown(recoverDb.close);
          addTearDown(verifyDb.close);

          final seedSource = await bootstrapManagedSourceId(seedDb);
          final recoverSource = await bootstrapManagedSourceId(recoverDb);
          final verifySource = await bootstrapManagedSourceId(verifyDb);
          final replacementSource = randomRealserverId(
            'dart-heavy-replacement',
          );
          final seedHttp = await authenticatedHttp(
            config.baseUrl,
            userId,
            seedSource,
          );
          final recoverHttp = await authenticatedHttp(
            config.baseUrl,
            userId,
            recoverSource,
          );
          final verifyHttp = await authenticatedHttp(
            config.baseUrl,
            userId,
            verifySource,
          );
          addTearDown(seedHttp.close);
          addTearDown(recoverHttp.close);
          addTearDown(verifyHttp.close);

          final seed = newRealServerClient(
            seedDb,
            seedHttp,
            uploadLimit: 1,
            downloadLimit: 1,
          );
          final recover = newRealServerClient(
            recoverDb,
            recoverHttp,
            uploadLimit: 1,
            downloadLimit: 1,
          );
          final verify = newRealServerClient(
            verifyDb,
            verifyHttp,
            uploadLimit: 1,
            downloadLimit: 1,
          );
          addTearDown(seed.close);
          addTearDown(recover.close);
          addTearDown(verify.close);

          await seed.open();
          await expectConnected(
            seed.attach(userId),
            AttachOutcome.startedEmpty,
          );
          for (var round = 1; round <= 5; round++) {
            await insertBusinessUserAndPost(
              seedDb,
              realserverUuid(),
              realserverUuid(),
              'source-heavy-seed-$round',
            );
            expect((await seed.pushPending()).outcome, PushOutcome.committed);
          }

          await recover.open();
          await expectConnected(
            recover.attach(userId),
            AttachOutcome.usedRemoteState,
          );
          await markSourceRecoveryRequired(recoverDb, replacementSource);
          expect(
            (await recover.rebuild()).outcome,
            RemoteSyncOutcome.appliedSnapshot,
          );
          expect(await currentSourceId(recoverDb), replacementSource);
          expect(
            await scalarText(
              recoverDb,
              "SELECT replaced_by_source_id FROM _sync_source_state WHERE source_id = '$recoverSource'",
            ),
            replacementSource,
          );

          final oldSourceHttp = await authenticatedHttp(
            config.baseUrl,
            userId,
            recoverSource,
          );
          addTearDown(oldSourceHttp.close);
          final oldSourceResponse = await oldSourceHttp.postJson(
            'sync/push-sessions',
            sourceId: recoverSource,
            operation: 'old source push probe',
            bounds: const OversqliteHttpRequestBounds(
              errorBodyBytes: 64 * 1024,
            ),
            body: {
              'source_bundle_id': 1,
              'planned_row_count': 1,
              'canonical_request_hash': '0' * 64,
            },
          );
          expect(oldSourceResponse.statusCode, HttpStatus.conflict);
          final oldSourceBody =
              jsonDecode(oldSourceResponse.body) as Map<String, Object?>;
          expect(oldSourceBody['error'], 'source_retired');
          expect(oldSourceBody['replaced_by_source_id'], replacementSource);

          final replacementHttp = await authenticatedHttp(
            config.baseUrl,
            userId,
            replacementSource,
          );
          addTearDown(replacementHttp.close);
          final replacementClient = newRealServerClient(
            recoverDb,
            replacementHttp,
            uploadLimit: 1,
            downloadLimit: 1,
          );
          addTearDown(replacementClient.close);
          await replacementClient.open();
          await expectConnected(
            replacementClient.attach(userId),
            AttachOutcome.resumedAttachedState,
          );

          final followupUser = realserverUuid();
          await insertBusinessUserAndPost(
            recoverDb,
            followupUser,
            realserverUuid(),
            'source-heavy-followup',
          );
          expect(
            (await replacementClient.pushPending()).outcome,
            PushOutcome.committed,
          );

          await verify.open();
          await expectConnected(
            verify.attach(userId),
            AttachOutcome.usedRemoteState,
          );
          await verify.pullToStable();
          expect(await scalarInt(verifyDb, 'SELECT COUNT(*) FROM users'), 6);
          expect(await scalarInt(verifyDb, 'SELECT COUNT(*) FROM posts'), 6);
          expect(
            await scalarText(
              verifyDb,
              "SELECT name FROM users WHERE id = '$followupUser'",
            ),
            'User source-heavy-followup',
          );
          await expectCleanSyncTables(recoverDb);
          await expectCleanSyncTables(verifyDb);
        },
      );

      test('shared database tolerates reads while sync catches up', () async {
        await resetRealServerState(config.baseUrl);
        final userId = randomRealserverId('dart-heavy-shared-user');
        final seedDb = await openBusinessDatabase();
        final activeDb = await openBusinessDatabase();
        addTearDown(seedDb.close);
        addTearDown(activeDb.close);

        final seedSource = await bootstrapManagedSourceId(seedDb);
        final activeSource = await bootstrapManagedSourceId(activeDb);
        final seedHttp = await authenticatedHttp(
          config.baseUrl,
          userId,
          seedSource,
        );
        final activeHttp = await authenticatedHttp(
          config.baseUrl,
          userId,
          activeSource,
        );
        addTearDown(seedHttp.close);
        addTearDown(activeHttp.close);

        final seed = newRealServerClient(
          seedDb,
          seedHttp,
          uploadLimit: 2,
          downloadLimit: 1,
        );
        final active = newRealServerClient(
          activeDb,
          activeHttp,
          uploadLimit: 2,
          downloadLimit: 1,
        );
        addTearDown(seed.close);
        addTearDown(active.close);

        await seed.open();
        await expectConnected(seed.attach(userId), AttachOutcome.startedEmpty);
        await active.open();
        await expectConnected(
          active.attach(userId),
          AttachOutcome.usedRemoteState,
        );

        for (var index = 1; index <= 8; index++) {
          await insertBusinessUserAndPost(
            seedDb,
            realserverUuid(),
            realserverUuid(),
            'shared-seed-$index',
          );
          expect((await seed.pushPending()).outcome, PushOutcome.committed);
        }
        await active.pullToStable();

        final readerErrors = <Object>[];
        Future<void> reader(int readerIndex) async {
          for (var iteration = 0; iteration < 80; iteration++) {
            try {
              final userCount = await scalarInt(
                activeDb,
                'SELECT COUNT(*) FROM users',
              );
              final postCount = await scalarInt(
                activeDb,
                'SELECT COUNT(*) FROM posts',
              );
              expect(userCount, greaterThanOrEqualTo(8));
              expect(postCount, greaterThanOrEqualTo(8));
              await scalarText(
                activeDb,
                "SELECT name FROM users ORDER BY id LIMIT 1 OFFSET ${readerIndex % 3}",
              );
            } catch (error) {
              readerErrors.add(error);
            }
          }
        }

        final remoteWriter = () async {
          for (var round = 1; round <= 12; round++) {
            await insertBusinessUserAndPost(
              seedDb,
              realserverUuid(),
              realserverUuid(),
              'shared-remote-$round',
            );
            expect((await seed.pushPending()).outcome, PushOutcome.committed);
          }
        }();
        final syncer = () async {
          while ((await active.syncStatus()).lastBundleSeqSeen < 20) {
            await active.pullToStable();
          }
        }();
        await Future.wait([
          remoteWriter,
          syncer,
          reader(0),
          reader(1),
          reader(2),
        ]);

        expect(readerErrors, isEmpty);
        expect(await scalarInt(activeDb, 'SELECT COUNT(*) FROM users'), 20);
        expect(await scalarInt(activeDb, 'SELECT COUNT(*) FROM posts'), 20);
        expect((await active.syncStatus()).lastBundleSeqSeen, 20);
        await expectCleanSyncTables(activeDb);
        await seed.pullToStable();
        await expectCleanSyncTables(seedDb);
      });
    },
    timeout: const Timeout(Duration(minutes: 5)),
  );
}
