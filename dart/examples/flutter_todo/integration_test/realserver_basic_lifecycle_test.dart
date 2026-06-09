import 'dart:convert';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

import 'realserver_test_support.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('Flutter Android realserver basic lifecycle', () {
    testWidgets(
      'two clients, fresh attach, and three-client convergence work',
      skip: !realserverEnabled,
      (tester) async {
        final config = await requireRealServerConfig();
        await resetRealServerState(config.baseUrl);
        final tempDir = await createRealserverTempDir();
        final userId = randomRealserverId('flutter-basic-user');
        final writer = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'basic-writer.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        final reader = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'basic-reader.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        final observer = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'basic-observer.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        addTearDown(writer.close);
        addTearDown(reader.close);
        addTearDown(observer.close);

        await writer.openAndAttach(AttachOutcome.startedEmpty);
        await reader.openAndAttach(AttachOutcome.usedRemoteState);

        final writerUser = realserverUuid();
        final writerPost = realserverUuid();
        await insertBusinessUserAndPost(
          writer.database,
          writerUser,
          writerPost,
          'writer',
        );
        expect(
          (await writer.client.pushPending()).outcome,
          PushOutcome.committed,
        );
        expect(
          (await reader.client.pullToStable()).outcome,
          RemoteSyncOutcome.appliedIncremental,
        );

        await observer.openAndAttach(AttachOutcome.usedRemoteState);
        expect(
          await scalarText(
            observer.database,
            "SELECT content FROM posts WHERE id = '$writerPost'",
          ),
          'Payload writer',
        );

        final readerUser = realserverUuid();
        final readerPost = realserverUuid();
        await insertBusinessUserAndPost(
          reader.database,
          readerUser,
          readerPost,
          'reader',
        );
        expect(
          (await reader.client.pushPending()).outcome,
          PushOutcome.committed,
        );
        await writer.client.pullToStable();
        await observer.client.pullToStable();

        for (final device in [writer, reader, observer]) {
          expect(
            await scalarInt(device.database, 'SELECT COUNT(*) FROM users'),
            2,
          );
          expect(
            await scalarInt(device.database, 'SELECT COUNT(*) FROM posts'),
            2,
          );
          expect((await device.client.syncStatus()).lastBundleSeqSeen, 2);
          await expectCleanSyncTables(device.database);
        }
      },
      timeout: const Timeout(Duration(minutes: 2)),
    );

    testWidgets(
      'retry-later attach and blocked detach work',
      skip: !realserverEnabled,
      (tester) async {
        final config = await requireRealServerConfig();
        await resetRealServerState(config.baseUrl);
        final tempDir = await createRealserverTempDir();
        final userId = randomRealserverId('flutter-lease-user');
        final seeder = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'lease-seeder.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        final waiter = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'lease-waiter.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        addTearDown(seeder.close);
        addTearDown(waiter.close);

        await insertBusinessUserAndPost(
          seeder.database,
          realserverUuid(),
          realserverUuid(),
          'local-seed',
        );
        await seeder.openAndAttach(AttachOutcome.seededFromLocal);

        final pending = (await seeder.client.syncStatus()).pending;
        expect(pending.hasPendingSyncData, isTrue);
        expect(pending.pendingRowCount, greaterThan(0));
        expect(pending.blocksDetach, isTrue);
        expect(await seeder.client.detach(), DetachOutcome.blockedUnsyncedData);

        await waiter.client.open();
        final retry = await waiter.client.attach(userId);
        expect(retry, isA<AttachRetryLater>());

        expect(
          (await seeder.client.pushPending()).outcome,
          PushOutcome.committed,
        );
        await expectConnected(
          waiter.client.attach(userId),
          AttachOutcome.usedRemoteState,
        );
        expect(
          await scalarText(waiter.database, 'SELECT name FROM users LIMIT 1'),
          'User local-seed',
        );
        await expectCleanSyncTables(seeder.database);
        await expectCleanSyncTables(waiter.database);
      },
      timeout: const Timeout(Duration(minutes: 2)),
    );

    testWidgets(
      'syncThenDetach flushes remote state and reattach uses fresh source',
      skip: !realserverEnabled,
      (tester) async {
        final config = await requireRealServerConfig();
        await resetRealServerState(config.baseUrl);
        final tempDir = await createRealserverTempDir();
        final userId = randomRealserverId('flutter-detach-user');
        final install = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'detach-install.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        final verify = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'detach-verify.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        addTearDown(install.close);
        addTearDown(verify.close);

        await install.openAndAttach(AttachOutcome.startedEmpty);
        final firstUser = realserverUuid();
        await insertBusinessUserAndPost(
          install.database,
          firstUser,
          realserverUuid(),
          'first',
        );

        final result = await install.client.syncThenDetach();
        expect(result.isSuccess, isTrue);
        expect(result.detach, DetachOutcome.detached);
        expect(
          await scalarInt(install.database, 'SELECT COUNT(*) FROM users'),
          0,
        );
        final rotatedSource = await currentSourceId(install.database);
        expect(rotatedSource, isNot(install.sourceId));

        await verify.openAndAttach(AttachOutcome.usedRemoteState);
        expect(
          await scalarText(
            verify.database,
            "SELECT name FROM users WHERE id = '$firstUser'",
          ),
          'User first',
        );

        final rotatedHttp = await authenticatedHttp(
          config.baseUrl,
          userId,
          rotatedSource,
        );
        addTearDown(rotatedHttp.close);
        final reattached = newBusinessClient(install.database, rotatedHttp);
        addTearDown(reattached.close);
        await reattached.open();
        await expectConnected(
          reattached.attach(userId),
          AttachOutcome.usedRemoteState,
        );
        expect(
          await scalarInt(
            install.database,
            "SELECT COUNT(*) FROM users WHERE id = '$firstUser'",
          ),
          1,
        );
        expect(
          await scalarInt(
            install.database,
            "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$rotatedSource'",
          ),
          1,
        );
      },
      timeout: const Timeout(Duration(minutes: 2)),
    );

    testWidgets(
      'same install alternates users without leaking remote data',
      skip: !realserverEnabled,
      (tester) async {
        final config = await requireRealServerConfig();
        await resetRealServerState(config.baseUrl);
        final tempDir = await createRealserverTempDir();
        final userA = randomRealserverId('flutter-user-a');
        final userB = randomRealserverId('flutter-user-b');
        final db = await openBusinessDatabase(
          '${tempDir.path}/same-install.db',
        );
        addTearDown(db.close);

        final sourceA = await bootstrapBusinessSourceId(db);
        final httpA = await authenticatedHttp(config.baseUrl, userA, sourceA);
        addTearDown(httpA.close);
        final clientA = newBusinessClient(db, httpA);
        addTearDown(clientA.close);
        await clientA.open();
        await expectConnected(
          clientA.attach(userA),
          AttachOutcome.startedEmpty,
        );
        final rowA = realserverUuid();
        await insertBusinessUserAndPost(db, rowA, realserverUuid(), 'user-a');
        expect((await clientA.pushPending()).outcome, PushOutcome.committed);
        expect(await clientA.detach(), DetachOutcome.detached);

        final sourceB = await currentSourceId(db);
        final httpB = await authenticatedHttp(config.baseUrl, userB, sourceB);
        addTearDown(httpB.close);
        final clientB = newBusinessClient(db, httpB);
        addTearDown(clientB.close);
        await clientB.open();
        await expectConnected(
          clientB.attach(userB),
          AttachOutcome.startedEmpty,
        );
        expect(
          await scalarInt(db, "SELECT COUNT(*) FROM users WHERE id = '$rowA'"),
          0,
        );
        final rowB = realserverUuid();
        await insertBusinessUserAndPost(db, rowB, realserverUuid(), 'user-b');
        expect((await clientB.pushPending()).outcome, PushOutcome.committed);
        expect(await clientB.detach(), DetachOutcome.detached);

        final sourceVerifyA = await currentSourceId(db);
        final verifyHttpA = await authenticatedHttp(
          config.baseUrl,
          userA,
          sourceVerifyA,
        );
        addTearDown(verifyHttpA.close);
        final verifyClientA = newBusinessClient(db, verifyHttpA);
        addTearDown(verifyClientA.close);
        await verifyClientA.open();
        await expectConnected(
          verifyClientA.attach(userA),
          AttachOutcome.usedRemoteState,
        );
        expect(
          await scalarText(db, "SELECT name FROM users WHERE id = '$rowA'"),
          'User user-a',
        );
        expect(
          await scalarInt(db, "SELECT COUNT(*) FROM users WHERE id = '$rowB'"),
          0,
        );
      },
      timeout: const Timeout(Duration(minutes: 2)),
    );

    testWidgets(
      'source recovery rotates source and rejects the retired source',
      skip: !realserverEnabled,
      (tester) async {
        final config = await requireRealServerConfig();
        await resetRealServerState(config.baseUrl);
        final tempDir = await createRealserverTempDir();
        final userId = randomRealserverId('flutter-recovery-user');
        final seed = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'recovery-seed.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        final recover = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'recovery-active.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        final verify = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'recovery-verify.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        addTearDown(seed.close);
        addTearDown(recover.close);
        addTearDown(verify.close);

        await seed.openAndAttach(AttachOutcome.startedEmpty);
        await insertBusinessUserAndPost(
          seed.database,
          realserverUuid(),
          realserverUuid(),
          'recovery-seed',
        );
        expect(
          (await seed.client.pushPending()).outcome,
          PushOutcome.committed,
        );

        await recover.openAndAttach(AttachOutcome.usedRemoteState);
        final replacementSource = randomRealserverId('flutter-replacement');
        await markSourceRecoveryRequired(recover.database, replacementSource);
        expect(
          (await recover.client.rebuild()).outcome,
          RemoteSyncOutcome.appliedSnapshot,
        );
        expect(await currentSourceId(recover.database), replacementSource);

        final oldSourceHttp = await authenticatedHttp(
          config.baseUrl,
          userId,
          recover.sourceId,
        );
        addTearDown(oldSourceHttp.close);
        final oldSourceResponse = await oldSourceHttp.postJson(
          'sync/push-sessions',
          sourceId: recover.sourceId,
          body: {'source_bundle_id': 1, 'planned_row_count': 1},
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
        final replacement = newBusinessClient(
          recover.database,
          replacementHttp,
        );
        addTearDown(replacement.close);
        await replacement.open();
        await expectConnected(
          replacement.attach(userId),
          AttachOutcome.resumedAttachedState,
        );
        final followup = realserverUuid();
        await insertBusinessUserAndPost(
          recover.database,
          followup,
          realserverUuid(),
          'recovery-followup',
        );
        expect(
          (await replacement.pushPending()).outcome,
          PushOutcome.committed,
        );

        await verify.openAndAttach(AttachOutcome.usedRemoteState);
        await verify.client.pullToStable();
        expect(
          await scalarText(
            verify.database,
            "SELECT name FROM users WHERE id = '$followup'",
          ),
          'User recovery-followup',
        );
      },
      timeout: const Timeout(Duration(minutes: 3)),
    );
  });
}
