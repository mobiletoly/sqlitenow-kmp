import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

import 'realserver_test_support.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('Flutter Android realserver rich generated schema', () {
    testWidgets(
      'generated schema preserves topology, typed rows, blob keys, pull, and rebuild',
      skip: !realserverEnabled,
      (tester) async {
        final config = await requireRealServerConfig();
        await resetRealServerState(config.baseUrl);
        final tempDir = await createRealserverTempDir();
        final userId = randomRealserverId('flutter-rich-schema-user');
        final seed = await openRichDevice(
          tempDir: tempDir,
          fileName: 'rich-seed.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        final active = await openRichDevice(
          tempDir: tempDir,
          fileName: 'rich-active.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        final hydrate = await openRichDevice(
          tempDir: tempDir,
          fileName: 'rich-hydrate.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        addTearDown(seed.close);
        addTearDown(active.close);
        addTearDown(hydrate.close);

        await seed.openAndAttach(AttachOutcome.startedEmpty);
        await active.openAndAttach(AttachOutcome.usedRemoteState);
        await hydrate.openAndAttach(AttachOutcome.usedRemoteState);

        final category = await insertCategoryGraph(seed.database, 'topology');
        final team = await insertTeamGraph(seed.database, 'topology');
        final blob = BlobKeyFixture(
          fileId: uuidBytes(realserverUuid()),
          reviewId: uuidBytes(realserverUuid()),
          label: 'blob-key',
          data: Uint8List.fromList([0x00, 0x11, 0x22, 0x33, 0xfe, 0xff]),
        );
        final typedSeed = TypedRowFixture(
          id: realserverUuid(),
          name: 'Seed Typed Row',
          note: null,
          countValue: 42,
          enabledFlag: 1,
          rating: 1.25,
          data: Uint8List.fromList([0xde, 0xad, 0xbe, 0xef]),
          createdAt: '2026-03-24T18:42:11Z',
        );
        await insertBlobKeyPair(seed.database, blob);
        await insertTypedRow(seed.database, typedSeed);
        expect(
          (await seed.client.pushPending()).outcome,
          PushOutcome.committed,
        );

        final typedActive = TypedRowFixture(
          id: realserverUuid(),
          name: 'Active Typed Row',
          note: 'second-device',
          countValue: null,
          enabledFlag: 0,
          rating: 6.57111473696007,
          data: null,
          createdAt: null,
        );
        await insertTypedRow(active.database, typedActive);
        expect(
          (await active.client.pushPending()).outcome,
          PushOutcome.committed,
        );
        expect(
          (await active.client.pullToStable()).outcome,
          isIn([
            RemoteSyncOutcome.appliedIncremental,
            RemoteSyncOutcome.alreadyAtTarget,
          ]),
        );
        expect(
          (await hydrate.client.rebuild()).outcome,
          RemoteSyncOutcome.appliedSnapshot,
        );

        for (final device in [active, hydrate]) {
          await assertTopologyState(
            device.database,
            category,
            team,
            device.fileNameForDiagnostics,
          );
          await assertBlobKeyState(device.database, blob);
          await assertTypedRowState(device.database, typedSeed);
          await assertTypedRowState(device.database, typedActive);
          await expectForeignKeyIntegrity(device.database.runtimeDatabase);
          await expectCleanSyncTables(device.database.runtimeDatabase);
        }
      },
      timeout: const Timeout(Duration(minutes: 2)),
    );

    testWidgets(
      'cascade deletes for FK topology and blob keys converge',
      skip: !realserverEnabled,
      (tester) async {
        final config = await requireRealServerConfig();
        await resetRealServerState(config.baseUrl);
        final tempDir = await createRealserverTempDir();
        final userId = randomRealserverId('flutter-cascade-user');
        final writer = await openRichDevice(
          tempDir: tempDir,
          fileName: 'cascade-writer.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        final reader = await openRichDevice(
          tempDir: tempDir,
          fileName: 'cascade-reader.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        addTearDown(writer.close);
        addTearDown(reader.close);

        await writer.openAndAttach(AttachOutcome.startedEmpty);
        await reader.openAndAttach(AttachOutcome.usedRemoteState);

        final category = await insertCategoryGraph(writer.database, 'cascade');
        final blob = BlobKeyFixture(
          fileId: uuidBytes(realserverUuid()),
          reviewId: uuidBytes(realserverUuid()),
          label: 'cascade-blob',
          data: Uint8List.fromList([1, 2, 3, 255]),
        );
        await insertBlobKeyPair(writer.database, blob);
        expect(
          (await writer.client.pushPending()).outcome,
          PushOutcome.committed,
        );
        await reader.client.pullToStable();
        expect(
          await scalarInt(
            reader.database.runtimeDatabase,
            'SELECT COUNT(*) FROM categories',
          ),
          3,
        );
        await assertBlobKeyState(reader.database, blob);

        await writer.database.connection.execute(
          'DELETE FROM categories WHERE id = ?',
          parameters: [category.rootId],
        );
        await writer.database.connection.execute(
          'DELETE FROM files WHERE id = ?',
          parameters: [blob.fileId],
        );
        expect(
          (await writer.client.pushPending()).outcome,
          PushOutcome.committed,
        );
        await reader.client.pullToStable();

        expect(
          await scalarInt(
            reader.database.runtimeDatabase,
            'SELECT COUNT(*) FROM categories',
          ),
          0,
        );
        expect(
          await scalarInt(
            reader.database.runtimeDatabase,
            'SELECT COUNT(*) FROM files',
          ),
          0,
        );
        expect(
          await scalarInt(
            reader.database.runtimeDatabase,
            'SELECT COUNT(*) FROM file_reviews',
          ),
          0,
        );
        await expectForeignKeyIntegrity(reader.database.runtimeDatabase);
        await expectCleanSyncTables(reader.database.runtimeDatabase);
      },
      timeout: const Timeout(Duration(minutes: 2)),
    );
  });
}

extension on RichDevice {
  String get fileNameForDiagnostics => sourceId;
}
