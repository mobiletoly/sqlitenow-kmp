import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

import 'realserver_test_support.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets(
    'generated oversqlite database syncs against realserver on Flutter',
    skip: !realserverEnabled,
    (tester) async {
      final config = await requireRealServerConfig();
      await resetRealServerState(config.baseUrl);
      final tempDir = await createRealserverTempDir();
      final userId = randomRealserverId('flutter-rich-user');

      final writer = await openRichDevice(
        tempDir: tempDir,
        fileName: 'writer-rich.db',
        baseUrl: config.baseUrl,
        userId: userId,
      );
      final reader = await openRichDevice(
        tempDir: tempDir,
        fileName: 'reader-rich.db',
        baseUrl: config.baseUrl,
        userId: userId,
      );
      addTearDown(writer.close);
      addTearDown(reader.close);

      await writer.openAndAttach(AttachOutcome.startedEmpty);
      await reader.openAndAttach(AttachOutcome.usedRemoteState);

      final rowId = realserverUuid();
      final typedRow = TypedRowFixture(
        id: realserverUuid(),
        name: 'Flutter Typed Row',
        note: 'device',
        countValue: 7,
        enabledFlag: 1,
        rating: 2.5,
        data: Uint8List.fromList([0x10, 0x20, 0x30, 0x40]),
        createdAt: '2026-03-24T18:42:11Z',
      );
      await writer.database.connection.execute(
        'INSERT INTO users(id, name, email) VALUES(?, ?, ?)',
        parameters: [rowId, 'Flutter Rich User', 'flutter-rich@example.com'],
      );
      await insertTypedRow(writer.database, typedRow);

      expect(
        (await writer.client.pushPending()).outcome,
        PushOutcome.committed,
      );
      expect(
        (await reader.client.pullToStable()).outcome,
        RemoteSyncOutcome.appliedIncremental,
      );

      final users = await reader.database.users.selectAll().asList();
      expect(
        users.singleWhere((row) => row.id == rowId).name,
        'Flutter Rich User',
      );
      await assertTypedRowState(reader.database, typedRow);
      await expectCleanSyncTables(reader.database.runtimeDatabase);
    },
    timeout: const Timeout(Duration(minutes: 2)),
  );
}
