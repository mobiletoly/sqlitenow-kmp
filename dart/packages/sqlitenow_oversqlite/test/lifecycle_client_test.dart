import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

void main() {
  test('construction is side-effect free and open is required', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final client = _newClient(database);
    addTearDown(client.close);

    expect(await _tableExists(database, '_sync_attachment_state'), isFalse);
    await expectLater(
      () => client.sourceInfo(),
      throwsA(isA<OpenRequiredException>()),
    );
    await expectLater(
      () => client.syncStatus(),
      throwsA(isA<OpenRequiredException>()),
    );

    await client.open();

    expect(await _tableExists(database, '_sync_attachment_state'), isTrue);
    await expectLater(
      () => client.syncStatus(),
      throwsA(isA<ConnectRequiredException>()),
    );
  });

  test(
    'open generates and persists source identity across client restart',
    () async {
      final database = await _openUsersDatabase();
      addTearDown(database.close);
      final client = _newClient(database);
      await client.open();
      final sourceId = (await client.sourceInfo()).currentSourceId;
      await client.close();

      final restarted = _newClient(database);
      addTearDown(restarted.close);
      await restarted.open();

      expect((await restarted.sourceInfo()).currentSourceId, sourceId);
    },
  );

  test('pause and resume toggles are local client state', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final client = _newClient(database);
    addTearDown(client.close);

    expect(client.uploadsPaused, isFalse);
    expect(client.downloadsPaused, isFalse);
    await client.pauseUploads();
    await client.pauseDownloads();
    expect(client.uploadsPaused, isTrue);
    expect(client.downloadsPaused, isTrue);
    await client.resumeUploads();
    await client.resumeDownloads();
    expect(client.uploadsPaused, isFalse);
    expect(client.downloadsPaused, isFalse);
  });

  test('operations are serialized per client instance', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final client = _newClient(database);
    addTearDown(client.close);

    final firstOpen = client.open();
    await expectLater(
      () => client.detach(),
      throwsA(isA<SyncOperationInProgressException>()),
    );
    await firstOpen;
  });

  test('progress stream exposes active and idle states', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final client = _newClient(database);
    addTearDown(client.close);
    final progress = expectLater(
      client.progressStream,
      emitsInOrder([isA<OversqliteActive>(), isA<OversqliteIdle>()]),
    );

    await client.open();

    await progress;
    expect(client.progress, isA<OversqliteIdle>());
  });

  test(
    'attach validates local preconditions and fails closed before HTTP phase',
    () async {
      final database = await _openUsersDatabase();
      addTearDown(database.close);
      final client = _newClient(database);
      addTearDown(client.close);

      await expectLater(
        () => client.attach('user-1'),
        throwsA(isA<OpenRequiredException>()),
      );

      await client.open();
      await expectLater(
        () => client.attach('  '),
        throwsA(isA<ArgumentError>()),
      );
      await expectLater(
        () => client.attach('user-1'),
        throwsA(isA<RemoteAttachDeferredException>()),
      );
    },
  );

  test('attach resumes durable attached state without HTTP', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final client = _newClient(database);
    await client.open();
    final sourceId = (await client.sourceInfo()).currentSourceId;
    await _persistAttachedState(
      database,
      sourceId: sourceId,
      userId: 'user-1',
      pendingInitializationId: 'init-1',
      lastBundleSeqSeen: 9,
    );
    await client.close();

    final restarted = _newClient(database);
    addTearDown(restarted.close);
    await restarted.open();
    final result = await restarted.attach('user-1');

    expect(result, isA<AttachConnected>());
    final connected = result as AttachConnected;
    expect(connected.outcome, AttachOutcome.resumedAttachedState);
    expect(connected.status.authority, AuthorityStatus.pendingLocalSeed);
    expect(connected.status.lastBundleSeqSeen, 9);
  });

  test('sourceInfo reports durable source recovery state', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final client = _newClient(database);
    addTearDown(client.close);
    await client.open();
    await database.connection.execute(
      "UPDATE _sync_operation_state SET kind = 'source_recovery', reason = 'history_pruned' WHERE singleton_key = 1",
    );

    final source = await client.sourceInfo();

    expect(source.rebuildRequired, isTrue);
    expect(source.sourceRecoveryRequired, isTrue);
    expect(source.sourceRecoveryReason, 'history_pruned');
  });

  test('detach blocks attached dirty state', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final client = _newClient(database);
    addTearDown(client.close);
    await client.open();
    final sourceId = (await client.sourceInfo()).currentSourceId;
    await _persistAttachedState(database, sourceId: sourceId, userId: 'user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('dirty-1', 'Dirty')",
    );

    final outcome = await client.detach();

    expect(outcome, DetachOutcome.blockedUnsyncedData);
    expect(await _scalarInt(database, 'SELECT COUNT(*) FROM users'), 1);
  });

  test(
    'detach cleans local state and rotates anonymous source when clean',
    () async {
      final database = await _openUsersDatabase();
      addTearDown(database.close);
      final client = _newClient(database);
      addTearDown(client.close);
      await client.open();
      final sourceId = (await client.sourceInfo()).currentSourceId;
      await _persistAttachedState(
        database,
        sourceId: sourceId,
        userId: 'user-1',
      );
      await database.connection.execute(
        'UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1',
      );
      await database.connection.execute(
        "INSERT INTO users(id, name) VALUES('remote-1', 'Remote')",
      );
      await database.connection.execute(
        'UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1',
      );
      final invalidated = expectLater(
        database.invalidationTracker.watchTables({'users'}),
        emits(anything),
      );

      final outcome = await client.detach();

      expect(outcome, DetachOutcome.detached);
      expect(await _scalarInt(database, 'SELECT COUNT(*) FROM users'), 0);
      expect(
        await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
        0,
      );
      final attachment = await _attachmentRow(database);
      expect(attachment['bindingState'], 'anonymous');
      expect(attachment['currentSourceId'], isNot(sourceId));
      expect(
        await _scalarInt(
          database,
          "SELECT COUNT(*) FROM _sync_source_state WHERE source_id = '$sourceId' AND replaced_by_source_id <> ''",
        ),
        1,
      );
      await invalidated;
    },
  );

  test('detach cancels pending remote replace without network', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final client = _newClient(database);
    addTearDown(client.close);
    await client.open();
    final sourceId = (await client.sourceInfo()).currentSourceId;
    await database.connection.execute(
      "UPDATE _sync_operation_state SET kind = 'remote_replace', target_user_id = 'user-1' WHERE singleton_key = 1",
    );

    final outcome = await client.detach();

    expect(outcome, DetachOutcome.detached);
    expect((await _operationRow(database))['kind'], 'none');
    expect((await _attachmentRow(database))['currentSourceId'], sourceId);
  });
}

DefaultOversqliteClient _newClient(SqliteNowDatabase database) {
  return DefaultOversqliteClient(
    database: database,
    config: const OversqliteConfig(
      schema: 'main',
      syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
    ),
  );
}

Future<SqliteNowDatabase> _openUsersDatabase() async {
  final database = SqliteNowDatabase.inMemory();
  await database.open(
    preInit: (connection) {
      return connection.execute(
        'CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)',
      );
    },
  );
  return database;
}

Future<void> _persistAttachedState(
  SqliteNowDatabase database, {
  required String sourceId,
  required String userId,
  String pendingInitializationId = '',
  int lastBundleSeqSeen = 0,
}) {
  return database.connection.execute(
    '''UPDATE _sync_attachment_state
SET current_source_id = ?,
    binding_state = 'attached',
    attached_user_id = ?,
    schema_name = 'main',
    last_bundle_seq_seen = ?,
    rebuild_required = 0,
    pending_initialization_id = ?
WHERE singleton_key = 1''',
    parameters: [sourceId, userId, lastBundleSeqSeen, pendingInitializationId],
  );
}

Future<bool> _tableExists(SqliteNowDatabase database, String tableName) async {
  final rows = await database.connection.select(
    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
    (row) => row.readInt(0),
    parameters: [tableName],
  );
  return rows.single > 0;
}

Future<int> _scalarInt(SqliteNowDatabase database, String sql) async {
  final rows = await database.connection.select(sql, (row) => row.readInt(0));
  return rows.single;
}

Future<Map<String, Object?>> _attachmentRow(SqliteNowDatabase database) async {
  final rows = await database.connection.select(
    '''SELECT current_source_id, binding_state, attached_user_id, last_bundle_seq_seen
FROM _sync_attachment_state
WHERE singleton_key = 1''',
    (row) => {
      'currentSourceId': row.readString(0),
      'bindingState': row.readString(1),
      'attachedUserId': row.readString(2),
      'lastBundleSeqSeen': row.readInt(3),
    },
  );
  return rows.single;
}

Future<Map<String, Object?>> _operationRow(SqliteNowDatabase database) async {
  final rows = await database.connection.select(
    'SELECT kind, target_user_id FROM _sync_operation_state WHERE singleton_key = 1',
    (row) => {'kind': row.readString(0), 'targetUserId': row.readString(1)},
  );
  return rows.single;
}
