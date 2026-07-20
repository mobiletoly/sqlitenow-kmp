import 'dart:convert';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';
import 'support/recording_sqlite_driver.dart';

void main() {
  test('pullToStable applies incremental bundles and tombstones', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer();
    server.addRemoteBundle([
      _bundleRow('INSERT', {'id': 'user-1'}, {'id': 'user-1', 'name': 'Ada'}),
    ]);
    server.addRemoteBundle([
      _bundleRow('DELETE', {'id': 'user-1'}, null, rowVersion: 2),
    ]);
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');

    final report = await client.pullToStable();

    expect(report.outcome, RemoteSyncOutcome.appliedIncremental);
    expect(report.status.lastBundleSeqSeen, 2);
    expect(await _scalarInt(database, 'SELECT COUNT(*) FROM users'), 0);
    expect(
      await _scalarInt(database, 'SELECT deleted FROM _sync_row_state'),
      1,
    );
  });

  test('pull rejects dirty rows before applying remote bundles', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer();
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('local', 'Local')",
    );

    await expectLater(
      client.pullToStable(),
      throwsA(isA<DirtyStateRejectedException>()),
    );

    expect(await _scalarInt(database, 'SELECT COUNT(*) FROM users'), 1);
    expect(
      await _scalarInt(
        database,
        'SELECT last_bundle_seq_seen FROM _sync_attachment_state',
      ),
      0,
    );
  });

  test('failed bundle apply rolls back checkpoint and apply mode', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer();
    server.addRemoteBundle([
      _bundleRow('INSERT', {'id': 'broken'}, {'id': 'broken'}),
    ]);
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');

    await expectLater(client.pullToStable(), throwsA(isA<Exception>()));

    expect(await _scalarInt(database, 'SELECT COUNT(*) FROM users'), 0);
    expect(
      await _scalarInt(
        database,
        'SELECT last_bundle_seq_seen FROM _sync_attachment_state',
      ),
      0,
    );
    expect(
      await _scalarInt(database, 'SELECT apply_mode FROM _sync_apply_state'),
      0,
    );
  });

  test('history-pruned pull rebuilds from snapshot', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer(historyPrunedOnPull: true)
      ..snapshotRows = [
        _snapshotRow({'id': 'user-1'}, {'id': 'user-1', 'name': 'Snapshot'}),
      ]
      ..stableBundleSeq = 7;
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');

    final report = await client.pullToStable();

    expect(report.outcome, RemoteSyncOutcome.appliedSnapshot);
    expect(report.restore?.bundleSeq, 7);
    expect(
      await _scalarText(database, "SELECT name FROM users WHERE id = 'user-1'"),
      'Snapshot',
    );
  });

  test('checkpoint-ahead pull rebuilds from snapshot', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer(checkpointAheadOnPull: true)
      ..snapshotRows = [
        _snapshotRow({'id': 'user-1'}, {'id': 'user-1', 'name': 'Snapshot'}),
      ]
      ..stableBundleSeq = 7;
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      'UPDATE _sync_attachment_state SET last_bundle_seq_seen = 99 WHERE singleton_key = 1',
    );

    final report = await client.pullToStable();

    expect(report.outcome, RemoteSyncOutcome.appliedSnapshot);
    expect(report.restore?.bundleSeq, 7);
    expect(
      await _scalarInt(
        database,
        'SELECT rebuild_required FROM _sync_attachment_state',
      ),
      0,
    );
    expect(
      await _scalarText(database, "SELECT name FROM users WHERE id = 'user-1'"),
      'Snapshot',
    );
  });

  test(
    'terminal snapshot row mismatch is rejected before apply begins',
    () async {
      final database = await _openUsersDatabase();
      addTearDown(database.close);
      final server = _SyncServer(
        declaredSnapshotRowCount: 2,
        snapshotRows: [
          _snapshotRow({'id': 'remote'}, {'id': 'remote', 'name': 'Remote'}),
        ],
      )..stableBundleSeq = 3;
      final client = _newClient(database, server);
      addTearDown(client.close);
      await client.open();
      await client.attach('user-1');
      await executeApplyModeSql(database, [
        "INSERT INTO users(id, name) VALUES('local', 'Local')",
      ]);
      await database.connection.execute('''
CREATE TRIGGER fail_snapshot_delete
BEFORE DELETE ON users
BEGIN
  SELECT RAISE(FAIL, 'snapshot apply entered');
END
''');

      await expectLater(
        client.rebuild(),
        throwsA(
          isA<OversqliteProtocolException>().having(
            (error) => error.message,
            'message',
            contains('staged snapshot row count 1 does not match'),
          ),
        ),
      );

      expect(await _scalarInt(database, 'SELECT COUNT(*) FROM users'), 1);
      expect(
        await _scalarText(
          database,
          "SELECT name FROM users WHERE id = 'local'",
        ),
        'Local',
      );
    },
  );

  test(
    'durable byte mismatch restarts with a fresh snapshot after reopen',
    () async {
      final database = await _openUsersDatabase();
      addTearDown(database.close);
      final server = _SyncServer(
        connectResolution: 'remote_authoritative',
        snapshotRows: [
          _snapshotRow(
            {'id': 'rejected'},
            {'id': 'rejected', 'name': 'Rejected'},
          ),
        ],
      )..stableBundleSeq = 3;
      server.declaredSnapshotByteCount =
          snapshotCompactWireByteCount(server.snapshotRows) + 1;
      var client = _newClient(database, server);
      addTearDown(() => client.close());
      await client.open();
      await executeApplyModeSql(database, [
        "INSERT INTO users(id, name) VALUES('authoritative-old', 'Old')",
      ]);

      await expectLater(
        client.attach('user-1'),
        throwsA(
          isA<OversqliteProtocolException>().having(
            (error) => error.message,
            'message',
            contains('snapshot final byte total'),
          ),
        ),
      );

      expect(server.snapshotSessionCreateCount, 1);
      expect(
        await _scalarText(
          database,
          "SELECT name FROM users WHERE id = 'authoritative-old'",
        ),
        'Old',
      );
      expect(
        await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_snapshot_stage'),
        1,
      );
      expect(
        await _scalarText(
          database,
          'SELECT staged_snapshot_id FROM _sync_operation_state WHERE singleton_key = 1',
        ),
        'snapshot-1',
      );

      await client.close();
      server
        ..snapshotId = 'snapshot-2'
        ..stableBundleSeq = 4
        ..declaredSnapshotByteCount = null
        ..snapshotRows = [
          _snapshotRow(
            {'id': 'accepted'},
            {'id': 'accepted', 'name': 'Accepted'},
          ),
        ];
      client = _newClient(database, server);
      await client.open();

      final result = await client.attach('user-1');

      expect(result, isA<AttachConnected>());
      expect(server.snapshotSessionCreateCount, 2);
      expect(await _scalarInt(database, 'SELECT COUNT(*) FROM users'), 1);
      expect(
        await _scalarText(
          database,
          "SELECT name FROM users WHERE id = 'accepted'",
        ),
        'Accepted',
      );
      expect(
        await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_snapshot_stage'),
        0,
      );
    },
  );

  test(
    'complete retired remote-replace stage is reused after apply failure',
    () async {
      final database = await _openUsersDatabase();
      addTearDown(database.close);
      final server = _SyncServer(
        connectResolution: 'remote_authoritative',
        snapshotRows: [
          _snapshotRow(
            {'id': 'remote-reused'},
            {'id': 'remote-reused', 'name': 'Reused'},
          ),
        ],
      )..stableBundleSeq = 5;
      var client = _newClient(database, server);
      addTearDown(() => client.close());
      await client.open();
      await executeApplyModeSql(database, [
        "INSERT INTO users(id, name) VALUES('old', 'Old')",
      ]);
      await database.connection.execute('''CREATE TRIGGER fail_snapshot_insert
BEFORE INSERT ON users
WHEN NEW.id = 'remote-reused'
BEGIN
  SELECT RAISE(ABORT, 'injected snapshot apply failure');
END''');

      await expectLater(
        client.attach('user-1'),
        throwsA(isA<SnapshotRowApplyException>()),
      );

      expect(server.snapshotSessionCreateCount, 1);
      expect(
        await _scalarInt(
          database,
          'SELECT snapshot_stage_complete FROM _sync_operation_state',
        ),
        1,
      );
      expect(
        await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_snapshot_stage'),
        1,
      );
      expect(
        await _scalarText(database, "SELECT name FROM users WHERE id = 'old'"),
        'Old',
      );

      await client.close();
      await database.connection.execute('DROP TRIGGER fail_snapshot_insert');
      server.snapshotRows = const [];
      client = _newClient(database, server);
      await client.open();

      final result = await client.attach('user-1');

      expect(result, isA<AttachConnected>());
      expect(server.snapshotSessionCreateCount, 1);
      expect(
        await _scalarText(
          database,
          "SELECT name FROM users WHERE id = 'remote-reused'",
        ),
        'Reused',
      );
      expect(await _scalarInt(database, 'SELECT COUNT(*) FROM users'), 1);
      expect(
        await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_snapshot_stage'),
        0,
      );
    },
  );

  test(
    'production snapshot chunk capacity retry succeeds end to end',
    () async {
      final database = await _openUsersDatabase();
      addTearDown(database.close);
      final config = OversqliteConfig(
        schema: 'main',
        syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
        snapshotCapacityRetryPolicy: OversqliteSnapshotCapacityRetryPolicy(
          maxWait: Duration(seconds: 1),
          fallbackDelay: Duration(milliseconds: 1),
          positiveJitterRatio: 0,
        ),
      );
      final server = _SyncServer(
        connectResolution: 'remote_authoritative',
        config: config,
        snapshotChunkCapacityFailures: 1,
        snapshotRows: [
          _snapshotRow({'id': 'remote'}, {'id': 'remote', 'name': 'Retried'}),
        ],
      )..stableBundleSeq = 4;
      final client = _newClient(database, server, config: config);
      addTearDown(client.close);
      await client.open();

      final result = await client.attach('user-1');

      expect(result, isA<AttachConnected>());
      expect(server.snapshotChunkRequestCount, 2);
      expect(server.snapshotChunkCapacityCloseCount, 1);
      expect(server.capabilitiesRequestCount, 1);
      expect(
        await _scalarText(
          database,
          "SELECT name FROM users WHERE id = 'remote'",
        ),
        'Retried',
      );
    },
  );

  test('normal sync automatically resumes durable checkpoint recovery', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer()
      ..snapshotRows = [
        _snapshotRow({'id': 'user-1'}, {'id': 'user-1', 'name': 'Resumed'}),
      ]
      ..stableBundleSeq = 9;
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "UPDATE _sync_attachment_state SET last_bundle_seq_seen = 99, rebuild_required = 1 WHERE singleton_key = 1",
    );
    await database.connection.execute(
      "UPDATE _sync_operation_state SET reason = 'checkpoint_ahead' WHERE singleton_key = 1",
    );

    final report = await client.sync();

    expect(report.remoteOutcome, RemoteSyncOutcome.appliedSnapshot);
    expect(report.status.lastBundleSeqSeen, 9);
    expect(
      await _scalarText(database, "SELECT name FROM users WHERE id = 'user-1'"),
      'Resumed',
    );
  });

  test(
    'checkpoint recovery automatically preserves a remotely committed pruned replay',
    () async {
      final database = await _openUsersDatabase();
      addTearDown(database.close);
      final server = _SyncServer(committedReplayPruned: true);
      final client = _newClient(database, server);
      addTearDown(client.close);
      await client.open();
      await client.attach('user-1');
      final sourceId = await _scalarText(
        database,
        'SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1',
      );
      await database.connection.execute(
        "INSERT INTO users(id, name) VALUES('user-1', 'Ada')",
      );
      await database.connection.execute(
        'UPDATE _sync_attachment_state SET last_bundle_seq_seen = 99, rebuild_required = 1 WHERE singleton_key = 1',
      );
      await database.connection.execute(
        "UPDATE _sync_operation_state SET kind = 'none', reason = 'checkpoint_ahead' WHERE singleton_key = 1",
      );

      final report = await client.sync();

      expect(report.remoteOutcome, RemoteSyncOutcome.appliedSnapshot);
      expect(
        await _scalarText(
          database,
          'SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1',
        ),
        sourceId,
      );
      expect(
        await _scalarInt(
          database,
          'SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1',
        ),
        0,
      );
      expect(
        await _scalarText(
          database,
          'SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1',
        ),
        'none',
      );
      expect(
        await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
        0,
      );
      expect(
        await _scalarInt(
          database,
          "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$sourceId'",
        ),
        2,
      );
      expect(
        await _scalarText(
          database,
          "SELECT name FROM users WHERE id = 'user-1'",
        ),
        'Ada',
      );
    },
  );

  test(
    'checkpoint recovery authentication failure remains typed and durable',
    () async {
      final database = await _openUsersDatabase();
      addTearDown(database.close);
      final server = _SyncServer(pushCreateUnauthorized: true);
      final client = _newClient(database, server);
      addTearDown(client.close);
      await client.open();
      await client.attach('user-1');
      await database.connection.execute(
        "INSERT INTO users(id, name) VALUES('user-1', 'Ada')",
      );
      await database.connection.execute(
        'UPDATE _sync_attachment_state SET last_bundle_seq_seen = 99, rebuild_required = 1 WHERE singleton_key = 1',
      );
      await database.connection.execute(
        "UPDATE _sync_operation_state SET kind = 'none', reason = 'checkpoint_ahead' WHERE singleton_key = 1",
      );

      await expectLater(
        client.sync(),
        throwsA(
          isA<OversqliteHttpException>().having(
            (error) => error.statusCode,
            'statusCode',
            401,
          ),
        ),
      );

      expect(server.createRequests, hasLength(1));
      expect(
        await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
        0,
      );
      expect(
        await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
        1,
      );
      expect(
        await _scalarText(
          database,
          'SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1',
        ),
        'prepared',
      );
      expect(
        await _scalarInt(
          database,
          'SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1',
        ),
        1,
      );
      expect(
        await _scalarInt(
          database,
          'SELECT last_bundle_seq_seen FROM _sync_attachment_state WHERE singleton_key = 1',
        ),
        99,
      );
    },
  );

  test('remote-authoritative attach hydrates snapshot', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer(connectResolution: 'remote_authoritative')
      ..snapshotRows = [
        _snapshotRow({'id': 'user-1'}, {'id': 'user-1', 'name': 'Remote'}),
      ]
      ..stableBundleSeq = 3;
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();

    final result = await client.attach('user-1');

    expect(result, isA<AttachConnected>());
    expect((result as AttachConnected).outcome, AttachOutcome.usedRemoteState);
    expect(
      await _scalarText(database, "SELECT name FROM users WHERE id = 'user-1'"),
      'Remote',
    );
    expect(
      await _scalarInt(
        database,
        'SELECT last_bundle_seq_seen FROM _sync_attachment_state',
      ),
      3,
    );
  });

  test('sync pushes local rows then pulls remote rows', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer();
    server.addRemoteBundle([
      _bundleRow(
        'INSERT',
        {'id': 'remote'},
        {'id': 'remote', 'name': 'Remote'},
      ),
    ]);
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('local', 'Local')",
    );

    final report = await client.sync();

    expect(report.pushOutcome, PushOutcome.committed);
    expect(report.remoteOutcome, RemoteSyncOutcome.appliedIncremental);
    expect(
      await _scalarText(database, "SELECT name FROM users WHERE id = 'local'"),
      'Local',
    );
    expect(
      await _scalarText(database, "SELECT name FROM users WHERE id = 'remote'"),
      'Remote',
    );
  });

  test('syncThenDetach drains and clears local managed state', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer();
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('local', 'Local')",
    );

    final result = await client.syncThenDetach();

    expect(result.isSuccess, isTrue);
    expect(result.syncRounds, 1);
    expect(await _scalarInt(database, 'SELECT COUNT(*) FROM users'), 0);
    expect(
      await _scalarText(
        database,
        'SELECT binding_state FROM _sync_attachment_state',
      ),
      'anonymous',
    );
  });

  test('syncThenDetach resumes checkpoint recovery before ordinary push', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer()
      ..snapshotRows = [
        _snapshotRow({'id': 'user-1'}, {'id': 'user-1', 'name': 'Recovered'}),
      ]
      ..stableBundleSeq = 7;
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      'UPDATE _sync_attachment_state SET last_bundle_seq_seen = 99, rebuild_required = 1 WHERE singleton_key = 1',
    );
    await database.connection.execute(
      "UPDATE _sync_operation_state SET kind = 'none', reason = 'checkpoint_ahead' WHERE singleton_key = 1",
    );

    final result = await client.syncThenDetach();

    expect(result.isSuccess, isTrue);
    expect(result.syncRounds, 1);
    expect(server.createRequests, isEmpty);
    expect(await _scalarInt(database, 'SELECT COUNT(*) FROM users'), 0);
    expect(
      await _scalarText(
        database,
        'SELECT binding_state FROM _sync_attachment_state',
      ),
      'anonymous',
    );
  });

  test(
    'source-recovery replay is bounded and source mismatches roll back',
    () async {
      await _proveBoundedSourceRecoveryReplay();
      await _proveSourceBundleMismatchRollback();
    },
  );

  test('malformed source retirement does not persist recovery state', () async {
    const sentinel = 'replacement-SECRET-token';
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer(
      sourceRetiredResponse: const {
        'error': 'source_retired',
        'message': 'source retired',
        'source_id': 'wrong-source',
        'replaced_by_source_id': sentinel,
      },
    );
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    final originalSourceId = await _scalarText(
      database,
      'SELECT current_source_id FROM _sync_attachment_state',
    );
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('local', 'Local')",
    );

    Object? caught;
    try {
      await client.pushPending();
    } catch (error) {
      caught = error;
    }

    expect(caught, isA<OversqliteProtocolException>());
    expect(caught.toString(), 'source retired response is malformed');
    expect(caught.toString(), isNot(contains(sentinel)));
    expect(
      await _scalarText(
        database,
        'SELECT kind FROM _sync_operation_state WHERE singleton_key = 1',
      ),
      'none',
    );
    expect(
      await _scalarText(
        database,
        'SELECT replacement_source_id FROM _sync_operation_state WHERE singleton_key = 1',
      ),
      isEmpty,
    );
    expect(
      await _scalarText(
        database,
        'SELECT current_source_id FROM _sync_attachment_state',
      ),
      originalSourceId,
    );
    expect(
      await _scalarText(database, "SELECT name FROM users WHERE id = 'local'"),
      'Local',
    );
  });

  test(
    'snapshot rebuild applies self-referential rows under deferred FKs',
    () async {
      final database = await _openNodeDatabase();
      addTearDown(database.close);
      final server = _SyncServer(
        config: _nodeConfig,
        snapshotRows: [
          _snapshotRow(
            {'id': 'child'},
            {'id': 'child', 'parent_id': 'parent', 'name': 'Child'},
            table: 'nodes',
          ),
          _snapshotRow(
            {'id': 'parent'},
            {'id': 'parent', 'parent_id': null, 'name': 'Parent'},
            table: 'nodes',
          ),
        ],
      )..stableBundleSeq = 4;
      final client = _newClient(database, server, config: _nodeConfig);
      addTearDown(client.close);
      await client.open();
      await client.attach('user-1');

      final report = await client.rebuild();

      expect(report.outcome, RemoteSyncOutcome.appliedSnapshot);
      expect(await _scalarInt(database, 'SELECT COUNT(*) FROM nodes'), 2);
      expect(
        await _scalarText(
          database,
          "SELECT parent_id FROM nodes WHERE id = 'child'",
        ),
        'parent',
      );
    },
  );

  test('pull applies cyclic foreign keys under apply mode', () async {
    final database = await _openAuthorProfileDatabase();
    addTearDown(database.close);
    final server = _SyncServer(config: _authorProfileConfig);
    server.addRemoteBundle([
      _bundleRow(
        'INSERT',
        {'id': 'author-1'},
        {'id': 'author-1', 'profile_id': 'profile-1', 'name': 'Author'},
        table: 'authors',
      ),
      _bundleRow(
        'INSERT',
        {'id': 'profile-1'},
        {'id': 'profile-1', 'author_id': 'author-1', 'bio': 'Cyclic'},
        table: 'profiles',
      ),
    ]);
    final client = _newClient(database, server, config: _authorProfileConfig);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');

    final report = await client.pullToStable();

    expect(report.outcome, RemoteSyncOutcome.appliedIncremental);
    expect(
      await _scalarText(
        database,
        "SELECT profile_id FROM authors WHERE id = 'author-1'",
      ),
      'profile-1',
    );
    expect(
      await _scalarText(
        database,
        "SELECT author_id FROM profiles WHERE id = 'profile-1'",
      ),
      'author-1',
    );
  });
}

Future<void> _proveBoundedSourceRecoveryReplay() async {
  final recordingDriver = RecordingSqliteDriver();
  final database = await _openUsersDatabase(driver: recordingDriver);
  final server = _SyncServer(sourceRetiredOnPushCreate: true);
  final client = _newClient(database, server);
  try {
    await client.open();
    await client.attach('user-1');
    for (var ordinal = 0; ordinal < 64; ordinal++) {
      await database.connection.execute(
        'INSERT INTO users(id, name) VALUES(?, ?)',
        parameters: ['local-$ordinal', 'Local $ordinal'],
      );
    }

    await expectLater(
      client.pushPending(),
      throwsA(isA<SourceRecoveryRequiredException>()),
    );
    server.sourceRetiredOnPushCreate = false;
    recordingDriver.reset();

    await client.rebuild();

    bool isReplayQuery(String sql) =>
        sql.contains('FROM _sync_outbox_rows') &&
        sql.contains('source_bundle_id = ? AND row_ordinal > ?') &&
        sql.contains('LIMIT 1');
    final replayCalls = recordingDriver
        .selectCallsWhere(isReplayQuery, prepared: true)
        .toList();
    expect(recordingDriver.prepareCountWhere(isReplayQuery), 1);
    expect(replayCalls, hasLength(65));
    expect(replayCalls.every((call) => call.returnedRowCount <= 1), isTrue);
    expect(
      replayCalls.map((call) => call.parameters[1]),
      List.generate(65, (index) => index - 1),
    );
    expect(await _scalarInt(database, 'SELECT COUNT(*) FROM users'), 64);
    expect(
      await _scalarText(
        database,
        "SELECT name FROM users WHERE id = 'local-63'",
      ),
      'Local 63',
    );
    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
      64,
    );
    expect(
      await _scalarInt(
        database,
        'SELECT COUNT(DISTINCT source_bundle_id) FROM _sync_outbox_rows',
      ),
      1,
    );
    expect(
      await _scalarInt(
        database,
        'SELECT source_bundle_id FROM _sync_outbox_rows LIMIT 1',
      ),
      1,
    );
    expect(
      await _scalarText(
        database,
        'SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1',
      ),
      'prepared',
    );
    expect(
      await _scalarText(
        database,
        'SELECT source_id FROM _sync_outbox_bundle WHERE singleton_key = 1',
      ),
      'server-replacement-source',
    );
    expect(
      await _scalarText(
        database,
        'SELECT current_source_id FROM _sync_attachment_state',
      ),
      'server-replacement-source',
    );
    expect(
      await _scalarInt(
        database,
        'SELECT rebuild_required FROM _sync_attachment_state',
      ),
      0,
    );
    expect(
      await _scalarText(database, 'SELECT kind FROM _sync_operation_state'),
      'none',
    );
    expect(
      await _scalarInt(
        database,
        "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = 'server-replacement-source'",
      ),
      2,
    );

    await client.pushPending();

    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
      0,
    );
    expect(server.uploadedRows, hasLength(64));
    expect(server.uploadedRows.first['payload'], {
      'id': 'local-0',
      'name': 'Local 0',
    });
    expect(server.uploadedRows.last['payload'], {
      'id': 'local-63',
      'name': 'Local 63',
    });
  } finally {
    await client.close();
    await database.close();
  }
}

Future<void> _proveSourceBundleMismatchRollback() async {
  final database = await _openUsersDatabase();
  final server = _SyncServer(sourceRetiredOnPushCreate: true);
  final client = _newClient(database, server);
  try {
    await client.open();
    await client.attach('user-1');
    final originalSourceId = await _scalarText(
      database,
      'SELECT current_source_id FROM _sync_attachment_state',
    );
    for (var ordinal = 0; ordinal < 2; ordinal++) {
      await database.connection.execute(
        'INSERT INTO users(id, name) VALUES(?, ?)',
        parameters: ['local-$ordinal', 'Local $ordinal'],
      );
    }
    await expectLater(
      client.pushPending(),
      throwsA(isA<SourceRecoveryRequiredException>()),
    );
    await database.connection.execute(
      'UPDATE _sync_outbox_rows SET source_bundle_id = 99 WHERE row_ordinal = 0',
    );
    server
      ..sourceRetiredOnPushCreate = false
      ..stableBundleSeq = 7
      ..snapshotRows = [
        _snapshotRow(
          {'id': 'remote'},
          {'id': 'remote', 'name': 'Must roll back'},
        ),
      ];

    Object? caught;
    try {
      await client.rebuild();
    } catch (error) {
      caught = error;
    }

    expect(caught, isA<StateError>());
    expect(
      caught.toString(),
      contains('prepared outbox row count changed before replay'),
    );
    expect(caught.toString(), isNot(contains('local-0')));
    expect(caught.toString(), isNot(contains('Must roll back')));
    expect(await _scalarInt(database, 'SELECT COUNT(*) FROM users'), 2);
    expect(
      await _scalarInt(
        database,
        "SELECT COUNT(*) FROM users WHERE id = 'remote'",
      ),
      0,
    );
    expect(
      await _scalarInt(
        database,
        'SELECT last_bundle_seq_seen FROM _sync_attachment_state',
      ),
      0,
    );
    expect(
      await _scalarText(
        database,
        'SELECT current_source_id FROM _sync_attachment_state',
      ),
      originalSourceId,
    );
    expect(
      await _scalarInt(
        database,
        'SELECT rebuild_required FROM _sync_attachment_state',
      ),
      1,
    );
    expect(
      await _scalarText(
        database,
        'SELECT binding_state FROM _sync_attachment_state',
      ),
      'attached',
    );
    expect(
      await _scalarText(database, 'SELECT kind FROM _sync_operation_state'),
      'source_recovery',
    );
    expect(
      await _scalarText(
        database,
        'SELECT replacement_source_id FROM _sync_operation_state',
      ),
      'server-replacement-source',
    );
    expect(
      await _scalarInt(
        database,
        'SELECT snapshot_stage_complete FROM _sync_operation_state',
      ),
      0,
    );
    expect(
      await _scalarText(
        database,
        'SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1',
      ),
      'prepared',
    );
    expect(
      await _scalarText(
        database,
        'SELECT source_id FROM _sync_outbox_bundle WHERE singleton_key = 1',
      ),
      originalSourceId,
    );
    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
      2,
    );
    expect(
      await _scalarInt(
        database,
        'SELECT COUNT(*) FROM _sync_outbox_rows WHERE source_bundle_id = 99',
      ),
      1,
    );
    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_snapshot_stage'),
      1,
    );
    expect(
      await _scalarText(
        database,
        'SELECT replaced_by_source_id FROM _sync_source_state WHERE source_id = ?',
        parameters: [originalSourceId],
      ),
      isEmpty,
    );
    expect(
      await _scalarInt(
        database,
        "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = 'server-replacement-source'",
      ),
      1,
    );
  } finally {
    await client.close();
    await database.close();
  }
}

DefaultOversqliteClient _newClient(
  SqliteNowDatabase database,
  OversqliteHttpClient http, {
  OversqliteConfig? config,
}) {
  return DefaultOversqliteClient(
    database: database,
    config: config ?? _usersConfig,
    httpClient: http,
  );
}

final _usersConfig = OversqliteConfig(
  schema: 'main',
  syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
);

final _nodeConfig = OversqliteConfig(
  schema: 'main',
  syncTables: [SyncTable(tableName: 'nodes', syncKeyColumnName: 'id')],
);

final _authorProfileConfig = OversqliteConfig(
  schema: 'main',
  syncTables: [
    SyncTable(tableName: 'authors', syncKeyColumnName: 'id'),
    SyncTable(tableName: 'profiles', syncKeyColumnName: 'id'),
  ],
);

Future<SqliteNowDatabase> _openUsersDatabase({
  SqliteNowDriver driver = const Sqlite3Driver(),
}) async {
  final database = SqliteNowDatabase.inMemory(driver: driver);
  await database.open(
    preInit: (connection) {
      return connection.execute(
        'CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)',
      );
    },
  );
  return database;
}

Future<SqliteNowDatabase> _openNodeDatabase() async {
  final database = SqliteNowDatabase.inMemory();
  await database.open(
    preInit: (connection) {
      return connection.execute('''CREATE TABLE nodes (
  id TEXT PRIMARY KEY NOT NULL,
  parent_id TEXT REFERENCES nodes(id) DEFERRABLE INITIALLY DEFERRED,
  name TEXT NOT NULL
)''');
    },
  );
  return database;
}

Future<SqliteNowDatabase> _openAuthorProfileDatabase() async {
  final database = SqliteNowDatabase.inMemory();
  await database.open(
    preInit: (connection) async {
      await connection.execute('''CREATE TABLE authors (
  id TEXT PRIMARY KEY NOT NULL,
  profile_id TEXT NOT NULL REFERENCES profiles(id),
  name TEXT NOT NULL
)''');
      await connection.execute('''CREATE TABLE profiles (
  id TEXT PRIMARY KEY NOT NULL,
  author_id TEXT NOT NULL REFERENCES authors(id),
  bio TEXT NOT NULL
)''');
    },
  );
  return database;
}

Future<int> _scalarInt(SqliteNowDatabase database, String sql) async {
  final rows = await database.connection.select(sql, (row) => row.readInt(0));
  return rows.single;
}

Future<String> _scalarText(
  SqliteNowDatabase database,
  String sql, {
  List<Object?> parameters = const [],
}) async {
  final rows = await database.connection.select(
    sql,
    (row) => row.readString(0),
    parameters: parameters,
  );
  return rows.single;
}

Map<String, Object?> _bundleRow(
  String op,
  Map<String, String> key,
  Map<String, Object?>? payload, {
  String table = 'users',
  int? rowVersion,
}) {
  return {
    'schema': 'main',
    'table': table,
    'key': key,
    'op': op,
    'row_version': rowVersion ?? 1,
    'payload': payload,
  };
}

Map<String, Object?> _snapshotRow(
  Map<String, String> key,
  Map<String, Object?> payload, {
  String table = 'users',
}) {
  return {
    'schema': 'main',
    'table': table,
    'key': key,
    'row_version': 1,
    'payload': payload,
  };
}

final class _SyncServer implements OversqliteHttpClient {
  _SyncServer({
    this.connectResolution = 'initialize_empty',
    this.historyPrunedOnPull = false,
    this.checkpointAheadOnPull = false,
    this.committedReplayPruned = false,
    this.pushCreateUnauthorized = false,
    this.sourceRetiredOnPushCreate = false,
    this.sourceRetiredResponse,
    this.declaredSnapshotRowCount,
    this.snapshotChunkCapacityFailures = 0,
    OversqliteConfig? config,
    List<Map<String, Object?>> snapshotRows = const [],
  }) : config = config ?? _usersConfig,
       snapshotRows = [...snapshotRows];

  final String connectResolution;
  final bool historyPrunedOnPull;
  final bool checkpointAheadOnPull;
  final bool committedReplayPruned;
  final bool pushCreateUnauthorized;
  bool sourceRetiredOnPushCreate;
  final Map<String, Object?>? sourceRetiredResponse;
  final int? declaredSnapshotRowCount;
  int? declaredSnapshotByteCount;
  String snapshotId = 'snapshot-1';
  int snapshotChunkCapacityFailures;
  final OversqliteConfig config;
  final bundles = <Map<String, Object?>>[];
  final uploadedRows = <Map<String, Object?>>[];
  final createRequests = <Map<String, Object?>>[];
  List<Map<String, Object?>> snapshotRows;
  var stableBundleSeq = 0;
  var _sourceId = '';
  var _canonicalRequestHash = '';
  var _nextBundleSeq = 1;
  var capabilitiesRequestCount = 0;
  var snapshotChunkRequestCount = 0;
  var snapshotChunkCapacityCloseCount = 0;
  var snapshotSessionCreateCount = 0;

  void addRemoteBundle(List<Map<String, Object?>> rows) {
    final bundleSeq = _nextBundleSeq++;
    stableBundleSeq = bundleSeq;
    bundles.add({
      'bundle_seq': bundleSeq,
      'source_id': 'remote-source',
      'source_bundle_id': bundleSeq,
      'rows': [
        for (var i = 0; i < rows.length; i++)
          {...rows[i], 'row_version': rows[i]['row_version'] ?? i + 1},
      ],
    });
  }

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    _sourceId = sourceId;
    if (path == 'sync/capabilities') {
      capabilitiesRequestCount++;
      return _json(phase4CapabilitiesResponse());
    }
    if (path.startsWith('sync/pull')) {
      if (historyPrunedOnPull) {
        return _json({
          'error': 'history_pruned',
          'message': 'history pruned',
        }, statusCode: 409);
      }
      if (checkpointAheadOnPull) {
        return _json({
          'error': 'checkpoint_ahead',
          'message': 'checkpoint ahead',
        }, statusCode: 409);
      }
      final uri = Uri.parse('http://local/$path');
      final after = int.parse(uri.queryParameters['after_bundle_seq'] ?? '0');
      final visible = bundles
          .where((bundle) => (bundle['bundle_seq']! as int) > after)
          .toList();
      return _json({
        'stable_bundle_seq': stableBundleSeq,
        'bundles': visible,
        'has_more': false,
      });
    }
    if (path.startsWith('sync/snapshot-sessions/')) {
      snapshotChunkRequestCount++;
      if (snapshotChunkCapacityFailures > 0) {
        snapshotChunkCapacityFailures--;
        return _json(
          {'error': 'snapshot_chunk_capacity', 'message': 'busy'},
          statusCode: 429,
          close: () async => snapshotChunkCapacityCloseCount++,
        );
      }
      final uri = Uri.parse('http://local/$path');
      final requestedSnapshotId = uri.pathSegments[2];
      final after = int.parse(uri.queryParameters['after_row_ordinal'] ?? '0');
      final maxRows = int.parse(uri.queryParameters['max_rows'] ?? '1000');
      final rows = snapshotRows.skip(after).take(maxRows).toList();
      return _json({
        'snapshot_id': requestedSnapshotId,
        'snapshot_bundle_seq': stableBundleSeq,
        'rows': rows,
        'byte_count': snapshotCompactWireByteCount(rows),
        'next_row_ordinal': after + rows.length,
        'has_more': after + rows.length < snapshotRows.length,
      });
    }
    if (path.startsWith('sync/committed-bundles/')) {
      if (committedReplayPruned) {
        return _json({
          'error': 'history_pruned',
          'message': 'committed replay is below retained floor',
        }, statusCode: 409);
      }
      final bundleSeq = int.parse(
        path.substring('sync/committed-bundles/'.length).split('/').first,
      );
      final rows = [
        for (final row in uploadedRows)
          {
            'schema': row['schema'],
            'table': row['table'],
            'key': row['key'],
            'op': row['op'],
            'row_version': bundleSeq,
            'payload': row['payload'],
          },
      ];
      return _json({
        'bundle_seq': bundleSeq,
        'source_id': _sourceId,
        'source_bundle_id': 1,
        'row_count': rows.length,
        'bundle_hash': committedBundleHashForFixtureRows(rows),
        'canonical_request_hash': _canonicalRequestHash,
        'rows': rows,
        'next_row_ordinal': rows.isEmpty ? -1 : rows.length - 1,
        'has_more': false,
      });
    }
    return _json({'error': 'not_found', 'message': path}, statusCode: 404);
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    _sourceId = sourceId;
    if (path == 'sync/connect') {
      return _json({'resolution': connectResolution});
    }
    if (path == 'sync/snapshot-sessions') {
      snapshotSessionCreateCount++;
      return _json({
        'snapshot_id': snapshotId,
        'snapshot_bundle_seq': stableBundleSeq,
        'row_count': declaredSnapshotRowCount ?? snapshotRows.length,
        'byte_count':
            declaredSnapshotByteCount ??
            snapshotCompactWireByteCount(snapshotRows),
        'expires_at': '2099-01-01T00:00:00Z',
      });
    }
    if (path == 'sync/push-sessions') {
      final request = (body! as Map).cast<String, Object?>();
      createRequests.add(request);
      _canonicalRequestHash = request['canonical_request_hash']! as String;
      if (pushCreateUnauthorized) {
        return _json({
          'error': 'unauthorized',
          'message': 'checkpoint reconcile denied',
        }, statusCode: 401);
      }
      if (sourceRetiredResponse != null) {
        return _json(sourceRetiredResponse!, statusCode: 409);
      }
      if (sourceRetiredOnPushCreate) {
        return _json({
          'error': 'source_retired',
          'message': 'source retired',
          'source_id': sourceId,
          'replaced_by_source_id': 'server-replacement-source',
        }, statusCode: 409);
      }
      uploadedRows.clear();
      return _json({
        'status': 'staging',
        'push_id': 'push-1',
        'planned_row_count': request['planned_row_count'],
        'next_expected_row_ordinal': 0,
        'canonical_request_hash': _canonicalRequestHash,
      });
    }
    if (path == 'sync/push-sessions/push-1/chunks') {
      final request = (body! as Map).cast<String, Object?>();
      uploadedRows.addAll(
        (request['rows']! as List<Object?>).cast<Map<String, Object?>>(),
      );
      return _json({
        'push_id': 'push-1',
        'next_expected_row_ordinal': uploadedRows.length,
      });
    }
    if (path == 'sync/push-sessions/push-1/commit') {
      stableBundleSeq = stableBundleSeq == 0 ? 1 : stableBundleSeq + 1;
      final committedRows = [
        for (final row in uploadedRows)
          {
            'schema': row['schema'],
            'table': row['table'],
            'key': row['key'],
            'op': row['op'],
            'row_version': stableBundleSeq,
            'payload': row['payload'],
          },
      ];
      bundles.add({
        'bundle_seq': stableBundleSeq,
        'source_id': _sourceId,
        'source_bundle_id': 1,
        'rows': committedRows,
      });
      snapshotRows = [
        for (final row in uploadedRows)
          if (row['op'] != 'DELETE')
            {
              'schema': row['schema'],
              'table': row['table'],
              'key': row['key'],
              'row_version': stableBundleSeq,
              'payload': row['payload'],
            },
      ];
      return _json({
        'bundle_seq': stableBundleSeq,
        'source_id': _sourceId,
        'source_bundle_id': 1,
        'row_count': uploadedRows.length,
        'bundle_hash': committedBundleHashForFixtureRows(committedRows),
        'canonical_request_hash': _canonicalRequestHash,
      });
    }
    return _json({'error': 'not_found', 'message': path}, statusCode: 404);
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    return const OversqliteHttpResponse(statusCode: 204, body: '');
  }

  OversqliteHttpResponse _json(
    Map<String, Object?> body, {
    int statusCode = 200,
    Future<void> Function()? close,
  }) {
    return OversqliteHttpResponse(
      statusCode: statusCode,
      body: jsonEncode(body),
      close: close,
    );
  }
}
