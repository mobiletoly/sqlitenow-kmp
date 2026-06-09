import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

import 'realserver_test_support.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('Flutter Android realserver conflict resolvers', () {
    for (final scenario in _conflictScenarios) {
      testWidgets(
        scenario.name,
        skip: !realserverEnabled,
        (tester) async {
          final config = await requireRealServerConfig();
          await resetRealServerState(config.baseUrl);
          final tempDir = await createRealserverTempDir();
          final userId = randomRealserverId('flutter-conflict-user');
          final server = await openBusinessDevice(
            tempDir: tempDir,
            fileName: '${scenario.filePrefix}-server.db',
            baseUrl: config.baseUrl,
            userId: userId,
          );
          final client = await openBusinessDevice(
            tempDir: tempDir,
            fileName: '${scenario.filePrefix}-client.db',
            baseUrl: config.baseUrl,
            userId: userId,
            resolver: scenario.resolver,
          );
          final observer = await openBusinessDevice(
            tempDir: tempDir,
            fileName: '${scenario.filePrefix}-observer.db',
            baseUrl: config.baseUrl,
            userId: userId,
          );
          addTearDown(server.close);
          addTearDown(client.close);
          addTearDown(observer.close);

          await server.openAndAttach(AttachOutcome.startedEmpty);
          await client.openAndAttach(AttachOutcome.usedRemoteState);
          await observer.openAndAttach(AttachOutcome.usedRemoteState);

          final rowId = realserverUuid();
          await insertBusinessUserAndPost(
            server.database,
            rowId,
            realserverUuid(),
            'conflict-base',
          );
          expect(
            (await server.client.pushPending()).outcome,
            PushOutcome.committed,
          );
          await client.client.pullToStable();
          await observer.client.pullToStable();

          await server.database.connection.execute(
            "UPDATE users SET name = 'Server Winner', email = 'server@example.com' WHERE id = '$rowId'",
          );
          await client.database.connection.execute(
            "UPDATE users SET name = 'Client Winner' WHERE id = '$rowId'",
          );

          expect(
            (await server.client.pushPending()).outcome,
            PushOutcome.committed,
          );
          expect(
            (await client.client.pushPending()).outcome,
            isIn([PushOutcome.committed, PushOutcome.noChange]),
          );
          await observer.client.pullToStable();

          for (final device in [client, observer]) {
            expect(
              await scalarText(
                device.database,
                "SELECT name FROM users WHERE id = '$rowId'",
              ),
              scenario.expectedName,
            );
            expect(
              await scalarText(
                device.database,
                "SELECT email FROM users WHERE id = '$rowId'",
              ),
              scenario.expectedEmail,
            );
            await expectCleanSyncTables(device.database);
          }
        },
        timeout: const Timeout(Duration(minutes: 2)),
      );
    }

    testWidgets(
      'client-wins conflict preserves sibling rows in same rejected bundle',
      skip: !realserverEnabled,
      (tester) async {
        final config = await requireRealServerConfig();
        await resetRealServerState(config.baseUrl);
        final tempDir = await createRealserverTempDir();
        final userId = randomRealserverId('flutter-conflict-sibling-user');
        final server = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'sibling-server.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        final client = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'sibling-client.db',
          baseUrl: config.baseUrl,
          userId: userId,
          resolver: const ClientWinsResolver(),
        );
        final observer = await openBusinessDevice(
          tempDir: tempDir,
          fileName: 'sibling-observer.db',
          baseUrl: config.baseUrl,
          userId: userId,
        );
        addTearDown(server.close);
        addTearDown(client.close);
        addTearDown(observer.close);

        await server.openAndAttach(AttachOutcome.startedEmpty);
        await client.openAndAttach(AttachOutcome.usedRemoteState);
        await observer.openAndAttach(AttachOutcome.usedRemoteState);

        final conflictedRow = realserverUuid();
        final siblingRow = realserverUuid();
        await insertBusinessUserAndPost(
          server.database,
          conflictedRow,
          realserverUuid(),
          'conflict-base',
        );
        await insertBusinessUserAndPost(
          server.database,
          siblingRow,
          realserverUuid(),
          'sibling-base',
        );
        expect(
          (await server.client.pushPending()).outcome,
          PushOutcome.committed,
        );
        await client.client.pullToStable();
        await observer.client.pullToStable();

        await server.database.connection.execute(
          "UPDATE users SET name = 'Server Conflict' WHERE id = '$conflictedRow'",
        );
        await client.database.connection.execute(
          "UPDATE users SET name = 'Client Conflict' WHERE id = '$conflictedRow'",
        );
        await client.database.connection.execute(
          "UPDATE users SET name = 'Client Sibling' WHERE id = '$siblingRow'",
        );

        expect(
          (await server.client.pushPending()).outcome,
          PushOutcome.committed,
        );
        expect(
          (await client.client.pushPending()).outcome,
          isIn([PushOutcome.committed, PushOutcome.noChange]),
        );
        await observer.client.pullToStable();

        for (final device in [client, observer]) {
          expect(
            await scalarText(
              device.database,
              "SELECT name FROM users WHERE id = '$conflictedRow'",
            ),
            'Client Conflict',
          );
          expect(
            await scalarText(
              device.database,
              "SELECT name FROM users WHERE id = '$siblingRow'",
            ),
            'Client Sibling',
          );
          await expectCleanSyncTables(device.database);
        }
      },
      timeout: const Timeout(Duration(minutes: 2)),
    );
  });
}

final _conflictScenarios = [
  _ConflictScenario(
    name: 'server-wins resolver converges to remote state',
    filePrefix: 'server-wins',
    resolver: const ServerWinsResolver(),
    expectedName: 'Server Winner',
    expectedEmail: 'server@example.com',
  ),
  _ConflictScenario(
    name: 'client-wins resolver converges to latest local intent',
    filePrefix: 'client-wins',
    resolver: const ClientWinsResolver(),
    expectedName: 'Client Winner',
    expectedEmail: 'conflict-base@example.com',
  ),
  _ConflictScenario(
    name: 'merged resolver preserves server sibling fields',
    filePrefix: 'merged',
    resolver: const _MergedNameResolver(),
    expectedName: 'Client Winner',
    expectedEmail: 'server@example.com',
  ),
];

final class _ConflictScenario {
  const _ConflictScenario({
    required this.name,
    required this.filePrefix,
    required this.resolver,
    required this.expectedName,
    required this.expectedEmail,
  });

  final String name;
  final String filePrefix;
  final Resolver resolver;
  final String expectedName;
  final String expectedEmail;
}

final class _MergedNameResolver implements Resolver {
  const _MergedNameResolver();

  @override
  MergeResult resolve(ConflictContext conflict) {
    final merged = Map<String, Object?>.from(conflict.serverRow!);
    merged['name'] = conflict.localPayload!['name'];
    return KeepMerged(merged);
  }
}
