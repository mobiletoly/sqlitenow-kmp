import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

import 'realserver_support.dart';

void main() {
  final realserverEnabled = flagEnabled('OVERSQLITE_REALSERVER_TESTS');
  group(
    'realserver conformance',
    skip: realserverEnabled
        ? null
        : 'Set OVERSQLITE_REALSERVER_TESTS=true to run live realserver tests.',
    () {
      late RealServerConfig config;

      setUpAll(() async {
        config = await requireRealServerConfig();
      });

      test('open connect push pull and fresh attach converge', () async {
        await resetRealServerState(config.baseUrl);
        final userId = randomRealserverId('dart-realserver-user');
        final dbA = await openBusinessDatabase();
        final dbB = await openBusinessDatabase();
        final dbC = await openBusinessDatabase();
        addTearDown(dbA.close);
        addTearDown(dbB.close);
        addTearDown(dbC.close);

        final sourceA = await bootstrapManagedSourceId(dbA);
        final sourceB = await bootstrapManagedSourceId(dbB);
        final sourceC = await bootstrapManagedSourceId(dbC);
        final httpA = await authenticatedHttp(config.baseUrl, userId, sourceA);
        final httpB = await authenticatedHttp(config.baseUrl, userId, sourceB);
        final httpC = await authenticatedHttp(config.baseUrl, userId, sourceC);
        addTearDown(httpA.close);
        addTearDown(httpB.close);
        addTearDown(httpC.close);

        final clientA = newRealServerClient(dbA, httpA);
        final clientB = newRealServerClient(dbB, httpB);
        final clientC = newRealServerClient(dbC, httpC);
        addTearDown(clientA.close);
        addTearDown(clientB.close);
        addTearDown(clientC.close);

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

        final rowUserId = realserverUuid();
        final rowPostId = realserverUuid();
        await insertBusinessUserAndPost(dbA, rowUserId, rowPostId, 'smoke');

        expect((await clientA.pushPending()).outcome, PushOutcome.committed);
        expect(
          (await clientB.pullToStable()).outcome,
          RemoteSyncOutcome.appliedIncremental,
        );

        await clientC.open();
        await expectConnected(
          clientC.attach(userId),
          AttachOutcome.usedRemoteState,
        );

        expect((await clientA.syncStatus()).lastBundleSeqSeen, 1);
        expect((await clientB.syncStatus()).lastBundleSeqSeen, 1);
        expect((await clientC.syncStatus()).lastBundleSeqSeen, 1);
        expect(
          await scalarText(
            dbB,
            "SELECT name FROM users WHERE id = '$rowUserId'",
          ),
          'User smoke',
        );
        expect(
          await scalarText(
            dbC,
            "SELECT content FROM posts WHERE id = '$rowPostId'",
          ),
          'Payload smoke',
        );
      });

      test('retry later pending sync status and blocked detach work', () async {
        await resetRealServerState(config.baseUrl);
        final userId = randomRealserverId('dart-lease-user');
        final dbA = await openBusinessDatabase();
        final dbB = await openBusinessDatabase();
        addTearDown(dbA.close);
        addTearDown(dbB.close);

        final sourceA = await bootstrapManagedSourceId(dbA);
        final sourceB = await bootstrapManagedSourceId(dbB);
        final httpA = await authenticatedHttp(config.baseUrl, userId, sourceA);
        final httpB = await authenticatedHttp(config.baseUrl, userId, sourceB);
        addTearDown(httpA.close);
        addTearDown(httpB.close);
        final clientA = newRealServerClient(dbA, httpA);
        final clientB = newRealServerClient(dbB, httpB);
        addTearDown(clientA.close);
        addTearDown(clientB.close);

        await insertBusinessUserAndPost(
          dbA,
          realserverUuid(),
          realserverUuid(),
          'local-seed',
        );
        await clientA.open();
        await expectConnected(
          clientA.attach(userId),
          AttachOutcome.seededFromLocal,
        );

        final pending = (await clientA.syncStatus()).pending;
        expect(pending.hasPendingSyncData, isTrue);
        expect(pending.pendingRowCount, greaterThan(0));
        expect(pending.blocksDetach, isTrue);
        expect(await clientA.detach(), DetachOutcome.blockedUnsyncedData);

        await clientB.open();
        final retry = await clientB.attach(userId);
        expect(retry, isA<AttachRetryLater>());

        expect((await clientA.pushPending()).outcome, PushOutcome.committed);
        await expectConnected(
          clientB.attach(userId),
          AttachOutcome.usedRemoteState,
        );
        expect(
          await scalarText(dbB, 'SELECT name FROM users LIMIT 1'),
          'User local-seed',
        );
      });

      test('local seed then remote authoritative restore works', () async {
        await resetRealServerState(config.baseUrl);
        final seedUserId = randomRealserverId('dart-seed-user');
        final restoredUserId = randomRealserverId('dart-restore-user');
        final installDb = await openBusinessDatabase();
        final remoteSeedDb = await openBusinessDatabase();
        final verifyDb = await openBusinessDatabase();
        addTearDown(installDb.close);
        addTearDown(remoteSeedDb.close);
        addTearDown(verifyDb.close);

        final installSeedSource = await bootstrapManagedSourceId(installDb);
        final remoteSeedSource = await bootstrapManagedSourceId(remoteSeedDb);
        final verifySource = await bootstrapManagedSourceId(verifyDb);

        final installSeedHttp = await authenticatedHttp(
          config.baseUrl,
          seedUserId,
          installSeedSource,
        );
        addTearDown(installSeedHttp.close);
        final installSeedClient = newRealServerClient(
          installDb,
          installSeedHttp,
        );
        addTearDown(installSeedClient.close);

        final localOnlyUserId = realserverUuid();
        await insertBusinessUserAndPost(
          installDb,
          localOnlyUserId,
          realserverUuid(),
          'install-local-seed',
        );
        await installSeedClient.open();
        await expectConnected(
          installSeedClient.attach(seedUserId),
          AttachOutcome.seededFromLocal,
        );
        expect(
          (await installSeedClient.pushPending()).outcome,
          PushOutcome.committed,
        );
        expect(await installSeedClient.detach(), DetachOutcome.detached);

        final installRestoreSource = await currentSourceId(installDb);
        expect(installRestoreSource, isNot(installSeedSource));

        final remoteSeedHttp = await authenticatedHttp(
          config.baseUrl,
          restoredUserId,
          remoteSeedSource,
        );
        addTearDown(remoteSeedHttp.close);
        final remoteSeedClient = newRealServerClient(
          remoteSeedDb,
          remoteSeedHttp,
        );
        addTearDown(remoteSeedClient.close);
        final remoteUserId = realserverUuid();
        await remoteSeedClient.open();
        await expectConnected(
          remoteSeedClient.attach(restoredUserId),
          AttachOutcome.startedEmpty,
        );
        await insertBusinessUserAndPost(
          remoteSeedDb,
          remoteUserId,
          realserverUuid(),
          'remote-authoritative-seed',
        );
        expect(
          (await remoteSeedClient.pushPending()).outcome,
          PushOutcome.committed,
        );

        final restoreHttp = await authenticatedHttp(
          config.baseUrl,
          restoredUserId,
          installRestoreSource,
        );
        addTearDown(restoreHttp.close);
        final restoreClient = newRealServerClient(installDb, restoreHttp);
        addTearDown(restoreClient.close);
        await restoreClient.open();
        await expectConnected(
          restoreClient.attach(restoredUserId),
          AttachOutcome.usedRemoteState,
        );
        expect(
          await scalarInt(
            installDb,
            "SELECT COUNT(*) FROM users WHERE id = '$localOnlyUserId'",
          ),
          0,
        );
        expect(
          await scalarText(
            installDb,
            "SELECT name FROM users WHERE id = '$remoteUserId'",
          ),
          'User remote-authoritative-seed',
        );

        final verifyHttp = await authenticatedHttp(
          config.baseUrl,
          restoredUserId,
          verifySource,
        );
        addTearDown(verifyHttp.close);
        final verifyClient = newRealServerClient(verifyDb, verifyHttp);
        addTearDown(verifyClient.close);
        await verifyClient.open();
        await expectConnected(
          verifyClient.attach(restoredUserId),
          AttachOutcome.usedRemoteState,
        );
        expect(
          await scalarText(
            verifyDb,
            "SELECT name FROM users WHERE id = '$remoteUserId'",
          ),
          'User remote-authoritative-seed',
        );
      });

      test('history pruned pull rebuilds from snapshot', () async {
        await resetRealServerState(config.baseUrl);
        final userId = randomRealserverId('dart-prune-user');
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
        final leader = newRealServerClient(leaderDb, leaderHttp);
        final follower = newRealServerClient(followerDb, followerHttp);
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

        for (var round = 1; round <= 4; round++) {
          await insertBusinessUserAndPost(
            leaderDb,
            realserverUuid(),
            realserverUuid(),
            'prune-$round',
          );
          expect((await leader.pushPending()).outcome, PushOutcome.committed);
        }
        final leaderSeq = (await leader.syncStatus()).lastBundleSeqSeen;
        expect(leaderSeq, 4);

        await setRetainedBundleFloor(config.baseUrl, userId, leaderSeq);

        final beforeSource = await currentSourceId(followerDb);
        final report = await follower.pullToStable();

        expect(report.outcome, RemoteSyncOutcome.appliedSnapshot);
        expect((await follower.syncStatus()).lastBundleSeqSeen, leaderSeq);
        expect(await currentSourceId(followerDb), beforeSource);
        expect(await scalarInt(followerDb, 'SELECT COUNT(*) FROM users'), 4);
        expect(
          await scalarInt(followerDb, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
          0,
        );
        expect(
          await scalarInt(
            followerDb,
            'SELECT COUNT(*) FROM _sync_snapshot_stage',
          ),
          0,
        );
      });

      test('client-wins conflict converges through real server', () async {
        await resetRealServerState(config.baseUrl);
        final userId = randomRealserverId('dart-conflict-user');
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

        final clientA = newRealServerClient(dbA, httpA);
        final clientB = newRealServerClient(
          dbB,
          httpB,
          resolver: const ClientWinsResolver(),
        );
        final observer = newRealServerClient(observerDb, observerHttp);
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

        final rowId = realserverUuid();
        await insertBusinessUserAndPost(
          dbA,
          rowId,
          realserverUuid(),
          'conflict-base',
        );
        expect((await clientA.pushPending()).outcome, PushOutcome.committed);
        await clientB.pullToStable();
        await observer.pullToStable();

        await dbA.connection.execute(
          "UPDATE users SET name = 'Server Winner' WHERE id = '$rowId'",
        );
        await dbB.connection.execute(
          "UPDATE users SET name = 'Client Winner' WHERE id = '$rowId'",
        );

        expect((await clientA.pushPending()).outcome, PushOutcome.committed);
        expect((await clientB.pushPending()).outcome, PushOutcome.committed);
        await observer.pullToStable();

        expect(
          await scalarText(dbB, "SELECT name FROM users WHERE id = '$rowId'"),
          'Client Winner',
        );
        expect(
          await scalarText(
            observerDb,
            "SELECT name FROM users WHERE id = '$rowId'",
          ),
          'Client Winner',
        );
        expect(
          await scalarInt(dbB, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
          0,
        );
      });

      test('source recovery rotates source and retires old source', () async {
        await resetRealServerState(config.baseUrl);
        final userId = randomRealserverId('dart-retired-user');
        final seedDb = await openBusinessDatabase();
        final recoverDb = await openBusinessDatabase();
        final verifyDb = await openBusinessDatabase();
        addTearDown(seedDb.close);
        addTearDown(recoverDb.close);
        addTearDown(verifyDb.close);

        final seedSource = await bootstrapManagedSourceId(seedDb);
        final recoverSource = await bootstrapManagedSourceId(recoverDb);
        final verifySource = await bootstrapManagedSourceId(verifyDb);
        final rotatedSource = randomRealserverId('dart-rotated-source');

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

        final seed = newRealServerClient(seedDb, seedHttp);
        final recover = newRealServerClient(recoverDb, recoverHttp);
        final verify = newRealServerClient(verifyDb, verifyHttp);
        addTearDown(seed.close);
        addTearDown(recover.close);
        addTearDown(verify.close);

        await seed.open();
        await expectConnected(seed.attach(userId), AttachOutcome.startedEmpty);
        await insertBusinessUserAndPost(
          seedDb,
          realserverUuid(),
          realserverUuid(),
          'rotated-seed',
        );
        expect((await seed.pushPending()).outcome, PushOutcome.committed);

        await recover.open();
        await expectConnected(
          recover.attach(userId),
          AttachOutcome.usedRemoteState,
        );
        await markSourceRecoveryRequired(recoverDb, rotatedSource);
        expect(
          (await recover.rebuild()).outcome,
          RemoteSyncOutcome.appliedSnapshot,
        );
        expect(await currentSourceId(recoverDb), rotatedSource);
        expect(
          await scalarText(
            recoverDb,
            "SELECT replaced_by_source_id FROM _sync_source_state WHERE source_id = '$recoverSource'",
          ),
          rotatedSource,
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
          body: {
            'source_bundle_id': 1,
            'planned_row_count': 1,
            'canonical_request_hash':
                '0000000000000000000000000000000000000000000000000000000000000000',
          },
        );
        expect(oldSourceResponse.statusCode, HttpStatus.conflict);
        final oldSourceBody =
            jsonDecode(oldSourceResponse.body) as Map<String, Object?>;
        expect(oldSourceBody['error'], 'source_retired');
        expect(oldSourceBody['source_id'], recoverSource);
        expect(oldSourceBody['replaced_by_source_id'], rotatedSource);

        final rotatedHttp = await authenticatedHttp(
          config.baseUrl,
          userId,
          rotatedSource,
        );
        addTearDown(rotatedHttp.close);
        final rotatedClient = newRealServerClient(recoverDb, rotatedHttp);
        addTearDown(rotatedClient.close);
        await rotatedClient.open();
        await expectConnected(
          rotatedClient.attach(userId),
          AttachOutcome.resumedAttachedState,
        );

        final followupUserId = realserverUuid();
        await insertBusinessUserAndPost(
          recoverDb,
          followupUserId,
          realserverUuid(),
          'rotated-followup',
        );
        expect(
          (await rotatedClient.pushPending()).outcome,
          PushOutcome.committed,
        );

        await verify.open();
        await expectConnected(
          verify.attach(userId),
          AttachOutcome.usedRemoteState,
        );
        await verify.pullToStable();
        expect(
          await scalarText(
            verifyDb,
            "SELECT name FROM users WHERE id = '$followupUserId'",
          ),
          'User rotated-followup',
        );
      });
    },
    timeout: const Timeout(Duration(minutes: 2)),
  );
}
