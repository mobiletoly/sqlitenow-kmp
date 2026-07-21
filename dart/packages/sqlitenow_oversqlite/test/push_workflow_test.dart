import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';
import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';

void main() {
  test('Dart push fixtures decode against protocol models', () {
    final fixtures = _readPushFixtures();
    for (final fixture in fixtures) {
      final create = fixture['pushSessionCreateResponse'];
      if (create is Map) {
        expect(
          PushSessionCreateResponse.fromJson(
            create.cast<String, Object?>(),
          ).status,
          create['status'],
        );
      }
      final chunk = fixture['pushChunkResponse'];
      if (chunk is Map) {
        expect(
          PushSessionChunkResponse.fromJson(
            chunk.cast<String, Object?>(),
          ).nextExpectedRowOrdinal,
          1,
        );
      }
      final commit = fixture['pushCommitResponse'];
      if (commit is Map) {
        expect(
          PushSessionCommitResponse.fromJson(
            commit.cast<String, Object?>(),
          ).bundleSeq,
          1,
        );
      }
      final committedRows = fixture['committedBundleRowsResponse'];
      if (committedRows is Map) {
        expect(
          CommittedBundleRowsResponse.fromJson(
            committedRows.cast<String, Object?>(),
          ).rows.length,
          (committedRows['rows']! as List<Object?>).length,
        );
      }
      final conflict = fixture['pushConflictResponse'];
      if (conflict is Map) {
        final response = conflict.cast<String, Object?>();
        expect(response['error'], 'push_conflict');
        expect(
          PushConflictDetails.fromJson(
            (response['conflict']! as Map).cast<String, Object?>(),
          ).serverRowVersion,
          2,
        );
      }
      final expected = fixture['expectedFinalState'];
      if (expected is Map && expected['error'] != null) {
        expect(
          expected['error'],
          anyOf('source_sequence_mismatch', 'rebuild_required'),
        );
        expect(expected['outboxState'], anyOf('prepared', 'committed_remote'));
      }
    }
    expect(
      () => BundleRow.fromJson({
        'schema': 'main',
        'table': 'users',
        'key': {'_sync_scope_id': 'server-only'},
        'op': 'INSERT',
        'row_version': 1,
        'payload': {'id': 'user-1', 'name': 'Ada'},
      }),
      throwsA(isA<OversqliteProtocolException>()),
    );
  });

  test('pushPending uploads dirty rows and replays committed bundle', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _PushServer();
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('user-1', 'Ada')",
    );

    final report = await client.pushPending();

    expect(report.outcome, PushOutcome.committed);
    expect(report.updatedTables, {'users'});
    expect(server.createRequests.single['planned_row_count'], 1);
    expect(server.uploadedRows.single['op'], 'INSERT');
    expect(server.uploadedRows.single['payload'], {
      'id': 'user-1',
      'name': 'Ada',
    });
    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
      0,
    );
    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
      0,
    );
    expect(
      await _scalarInt(database, 'SELECT row_version FROM _sync_row_state'),
      1,
    );
    expect(
      await _scalarInt(
        database,
        'SELECT last_bundle_seq_seen FROM _sync_attachment_state',
      ),
      1,
    );
  });

  test('first local seed push clears pending initialization id', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('user-1', 'Ada')",
    );
    final server = _PushServer(connectResolution: 'initialize_local');
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');

    await client.pushPending();

    expect(server.createRequests.single['initialization_id'], 'init-connect');
    expect(
      await _scalarText(
        database,
        'SELECT pending_initialization_id FROM _sync_attachment_state',
      ),
      '',
    );
  });

  test(
    'server-wins conflict accepts authoritative row and clears dirty state',
    () async {
      final database = await _openUsersDatabase();
      addTearDown(database.close);
      final server = _PushServer(
        conflict: {
          'schema': 'main',
          'table': 'users',
          'key': {'id': 'user-1'},
          'op': 'UPDATE',
          'base_row_version': 0,
          'server_row_version': 2,
          'server_row_deleted': false,
          'server_row': {'id': 'user-1', 'name': 'Server'},
        },
      );
      final client = _newClient(database, server);
      addTearDown(client.close);
      await client.open();
      await client.attach('user-1');
      await database.connection.execute(
        "INSERT INTO users(id, name) VALUES('user-1', 'Local')",
      );

      final report = await client.pushPending();

      expect(report.outcome, PushOutcome.noChange);
      expect(
        await _scalarText(
          database,
          "SELECT name FROM users WHERE id = 'user-1'",
        ),
        'Server',
      );
      expect(
        await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
        0,
      );
      expect(
        await _scalarInt(database, 'SELECT row_version FROM _sync_row_state'),
        2,
      );
    },
  );

  test('client-wins conflict retries local intent', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _PushServer(conflict: _updateConflict(serverName: 'Server'));
    final client = _newClient(
      database,
      server,
      resolver: const ClientWinsResolver(),
    );
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('user-1', 'Local')",
    );

    final report = await client.pushPending();

    expect(report.outcome, PushOutcome.committed);
    expect(server.createRequests, hasLength(2));
    expect(server.uploadedRows.single['payload'], {
      'id': 'user-1',
      'name': 'Local',
    });
    expect(
      await _scalarText(database, "SELECT name FROM users WHERE id = 'user-1'"),
      'Local',
    );
    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
      0,
    );
  });

  test('keep-merged conflict retries merged payload', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _PushServer(conflict: _updateConflict(serverName: 'Server'));
    final client = _newClient(
      database,
      server,
      resolver: const _MergedResolver(),
    );
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('user-1', 'Local')",
    );

    final report = await client.pushPending();

    expect(report.outcome, PushOutcome.committed);
    expect(server.createRequests, hasLength(2));
    expect(server.uploadedRows.single['payload'], {
      'id': 'user-1',
      'name': 'Merged',
    });
    expect(
      await _scalarText(database, "SELECT name FROM users WHERE id = 'user-1'"),
      'Merged',
    );
  });

  test('invalid keep-merged result restores replayable dirty state', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _PushServer(conflict: _updateConflict(serverName: 'Server'));
    final client = _newClient(
      database,
      server,
      resolver: const _InvalidMergedResolver(),
    );
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('user-1', 'Local')",
    );

    await expectLater(
      client.pushPending(),
      throwsA(isA<InvalidConflictResolutionException>()),
    );

    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
      0,
    );
    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
      1,
    );
  });

  test('conflict retry exhaustion leaves dirty state replayable', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _PushServer(
      conflicts: [
        _updateConflict(serverName: 'Server 1', serverRowVersion: 8),
        _updateConflict(serverName: 'Server 2', serverRowVersion: 9),
        _updateConflict(serverName: 'Server 3', serverRowVersion: 10),
      ],
    );
    final client = _newClient(
      database,
      server,
      resolver: const ClientWinsResolver(),
    );
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('user-1', 'Replay Me')",
    );

    final error = await _captureException(client.pushPending());

    expect(error, isA<PushConflictRetryExhaustedException>());
    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
      0,
    );
    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
      1,
    );
    server.conflicts.clear();

    await client.pushPending();

    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
      0,
    );
    expect(
      await _scalarText(database, "SELECT name FROM users WHERE id = 'user-1'"),
      'Replay Me',
    );
  });

  test('pre-commit retry reuses frozen outbox', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _PushServer(failFirstCommit: true);
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('user-1', 'Ada')",
    );

    await expectLater(
      client.pushPending(),
      throwsA(isA<OversqliteHttpException>()),
    );

    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
      0,
    );
    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
      1,
    );

    await client.pushPending();

    expect(server.createRequests, hasLength(2));
    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
      0,
    );
  });

  test(
    'post-commit replay resumes committed remote outbox after restart',
    () async {
      final database = await _openUsersDatabase();
      addTearDown(database.close);
      final server = _PushServer(failFirstCommittedFetch: true);
      var client = _newClient(database, server);
      addTearDown(client.close);
      await client.open();
      await client.attach('user-1');
      await database.connection.execute(
        "INSERT INTO users(id, name) VALUES('user-1', 'Ada')",
      );

      await expectLater(
        client.pushPending(),
        throwsA(isA<OversqliteHttpException>()),
      );

      expect(
        await _scalarText(
          database,
          'SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1',
        ),
        'committed_remote',
      );
      await client.close();
      client = _newClient(database, server);
      await client.open();
      await client.attach('user-1');

      await client.pushPending();

      expect(server.createRequests, hasLength(1));
      expect(
        await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
        0,
      );
    },
  );

  test('source recovery response persists a durable push gate', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _PushServer(sourceRetiredOnCreate: true);
    var client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('user-1', 'Blocked')",
    );

    final error = await _captureException(client.pushPending());

    expect(error, isA<SourceRecoveryRequiredException>());
    expect(
      await _scalarText(database, 'SELECT kind FROM _sync_operation_state'),
      'source_recovery',
    );
    expect(
      await _scalarText(database, 'SELECT reason FROM _sync_operation_state'),
      'source_retired',
    );
    expect(
      await _scalarInt(
        database,
        'SELECT rebuild_required FROM _sync_attachment_state',
      ),
      1,
    );

    await client.close();
    server.sourceRetiredOnCreate = false;
    client = _newClient(database, server);
    await client.open();
    await client.attach('user-1');

    await expectLater(
      client.pushPending(),
      throwsA(isA<SourceRecoveryRequiredException>()),
    );
    expect(server.createRequests, hasLength(1));
  });

  test('committed replay pruning marks rebuild required', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _PushServer(pruneFirstCommittedFetch: true);
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('user-1', 'Ada')",
    );

    await expectLater(
      client.pushPending(),
      throwsA(isA<RebuildRequiredException>()),
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
        'SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1',
      ),
      'committed_remote',
    );
  });

  test(
    'already committed mismatch contract preserves source sequence error',
    () async {
      final contract = _readPushCase(
        'already-committed-request-hash-mismatch-preserves-source-sequence-error',
      );
      final database = await _openUsersDatabase();
      addTearDown(database.close);
      final server = _PushServer(
        alreadyCommitted: true,
        committedRowsResponse:
            contract['committedBundleRowsResponse']! as Map<String, Object?>,
        createResponse:
            contract['pushSessionCreateResponse']! as Map<String, Object?>,
      );
      final client = _newClient(database, server);
      addTearDown(client.close);
      await client.open();
      await client.attach('user-1');
      await database.connection.execute(
        ((contract['localWrite']! as Map)['sql']! as String),
      );

      await expectLater(
        client.pushPending(),
        throwsA(isA<SourceSequenceMismatchException>()),
      );

      await _expectRecoveryState(database, contract);
    },
  );

  test('committed remote mismatch contract marks rebuild required', () async {
    final contract = _readPushCase(
      'committed-remote-request-hash-mismatch-requires-rebuild',
    );
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _PushServer(
      failFirstCommittedFetch: true,
      commitResponse: contract['pushCommitResponse']! as Map<String, Object?>,
      committedRowsResponse:
          contract['committedBundleRowsResponse']! as Map<String, Object?>,
    );
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      ((contract['localWrite']! as Map)['sql']! as String),
    );

    await expectLater(
      client.pushPending(),
      throwsA(isA<OversqliteHttpException>()),
    );
    await expectLater(
      client.pushPending(),
      throwsA(isA<RebuildRequiredException>()),
    );

    await _expectRecoveryState(database, contract);
  });

  test('committed replay applies cyclic foreign keys under apply mode', () async {
    final database = await _openAuthorProfileDatabase();
    addTearDown(database.close);
    final server = _PushServer(
      registeredTableSpecs: phase4RegisteredTableSpecsForConfig(
        _authorProfileConfig,
      ),
    );
    final client = _newClient(database, server, config: _authorProfileConfig);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.transaction(() async {
      await database.connection.execute('PRAGMA defer_foreign_keys = ON');
      await database.connection.execute(
        "INSERT INTO authors(id, profile_id, name) VALUES('author-1', 'profile-1', 'Author')",
      );
      await database.connection.execute(
        "INSERT INTO profiles(id, author_id, bio) VALUES('profile-1', 'author-1', 'Cyclic')",
      );
    }, mode: TransactionMode.immediate);

    final report = await client.pushPending();

    expect(report.outcome, PushOutcome.committed);
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
    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
      0,
    );
  });

  test('committed push does not skip local bundle checkpoint', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _PushServer(
      commitResponse: {
        'bundle_seq': 2,
        'source_id': 'ignored',
        'source_bundle_id': 1,
        'row_count': 1,
        'bundle_hash': 'test-hash',
      },
      committedRowsResponse: {
        'bundle_seq': 2,
        'source_id': 'ignored',
        'source_bundle_id': 1,
        'row_count': 1,
        'bundle_hash': 'test-hash',
        'rows': [
          {
            'schema': 'main',
            'table': 'users',
            'key': {'id': 'user-1'},
            'op': 'INSERT',
            'row_version': 1,
            'payload': {'id': 'user-1', 'name': 'Ada'},
          },
        ],
        'next_row_ordinal': 0,
        'has_more': false,
      },
    );
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('user-1', 'Ada')",
    );

    await client.pushPending();

    expect(
      await _scalarInt(
        database,
        'SELECT last_bundle_seq_seen FROM _sync_attachment_state',
      ),
      0,
    );
    expect(
      await _scalarInt(
        database,
        'SELECT next_source_bundle_id FROM _sync_source_state',
      ),
      2,
    );
    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
      0,
    );
  });

  test('payload codec uses canonical UUID and BLOB encodings', () async {
    final database = await _openDocumentsDatabase();
    addTearDown(database.close);
    final idBytes = Uint8List.fromList([
      0x12,
      0x34,
      0x56,
      0x78,
      0x9a,
      0xbc,
      0x4d,
      0xef,
      0x80,
      0x12,
      0x34,
      0x56,
      0x78,
      0x9a,
      0xbc,
      0xde,
    ]);
    final dataBytes = Uint8List.fromList([1, 2, 3, 255]);
    final server = _PushServer(
      registeredTableSpecs: phase4RegisteredTableSpecsForConfig(
        _documentsConfig,
      ),
      transformCommittedPayload: (payload) => {...payload, 'enabled': true},
    );
    final client = _newClient(database, server, config: _documentsConfig);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      'INSERT INTO documents(id, title, data, ratio, enabled, created_at) VALUES(?, ?, ?, ?, ?, ?)',
      parameters: [idBytes, 'Doc', dataBytes, 1.0, 1, '2026-05-06T12:34:56Z'],
    );

    await client.pushPending();

    const wireUuid = '12345678-9abc-4def-8012-3456789abcde';
    expect(server.uploadedRows.single['key'], {'id': wireUuid});
    expect(server.uploadedRows.single['payload'], {
      'id': wireUuid,
      'title': 'Doc',
      'data': base64Encode(dataBytes),
      'ratio': '1',
      'enabled': '1',
      'created_at': '2026-05-06T12:34:56Z',
    });
    expect(
      await _scalarText(database, 'SELECT key_json FROM _sync_row_state'),
      '{"id":"123456789abc4def80123456789abcde"}',
    );
    expect(await _scalarInt(database, 'SELECT enabled FROM documents'), 1);
  });

  test('committed bundle hash uses protocol number canonicalization', () async {
    final database = await _openDocumentsDatabase();
    addTearDown(database.close);
    final smallIdBytes = Uint8List.fromList([
      0x12,
      0x34,
      0x56,
      0x78,
      0x9a,
      0xbc,
      0x4d,
      0xef,
      0x80,
      0x12,
      0x34,
      0x56,
      0x78,
      0x9a,
      0xbc,
      0xde,
    ]);
    final largeIdBytes = Uint8List.fromList([
      0xab,
      0xcd,
      0xef,
      0x01,
      0x23,
      0x45,
      0x4a,
      0xbc,
      0x8d,
      0xef,
      0x01,
      0x23,
      0x45,
      0x67,
      0x89,
      0xab,
    ]);
    const smallWireUuid = '12345678-9abc-4def-8012-3456789abcde';
    const largeWireUuid = 'abcdef01-2345-4abc-8def-0123456789ab';
    final smallData = Uint8List.fromList([1, 2, 3]);
    final largeData = Uint8List.fromList([4, 5, 6]);
    final committedRows = [
      {
        'schema': 'main',
        'table': 'documents',
        'key': {'id': smallWireUuid},
        'op': 'INSERT',
        'row_version': 1,
        'payload': {
          'id': smallWireUuid,
          'title': 'Small',
          'data': base64Encode(smallData),
          'ratio': '1e-7',
          'enabled': true,
          'created_at': '2026-05-06T12:34:56Z',
        },
      },
      {
        'schema': 'main',
        'table': 'documents',
        'key': {'id': largeWireUuid},
        'op': 'INSERT',
        'row_version': 2,
        'payload': {
          'id': largeWireUuid,
          'title': 'Large',
          'data': base64Encode(largeData),
          'ratio': '1e+21',
          'enabled': true,
          'created_at': '2026-05-07T12:34:56Z',
        },
      },
    ];
    final committedHash = sha256
        .convert(
          utf8.encode(
            '[{"key":{"id":"$smallWireUuid"},"op":"INSERT","payload":{"created_at":"2026-05-06T12:34:56Z","data":"AQID","enabled":true,"id":"$smallWireUuid","ratio":"1e-7","title":"Small"},"row_ordinal":"0","row_version":"1","schema":"main","table":"documents"},{"key":{"id":"$largeWireUuid"},"op":"INSERT","payload":{"created_at":"2026-05-07T12:34:56Z","data":"BAUG","enabled":true,"id":"$largeWireUuid","ratio":"1e+21","title":"Large"},"row_ordinal":"1","row_version":"2","schema":"main","table":"documents"}]',
          ),
        )
        .bytes
        .map((byte) => byte.toRadixString(16).padLeft(2, '0'))
        .join();
    final server = _PushServer(
      registeredTableSpecs: phase4RegisteredTableSpecsForConfig(
        _documentsConfig,
      ),
      preserveConfiguredBundleHash: true,
      commitResponse: {
        'bundle_seq': 1,
        'source_bundle_id': 1,
        'row_count': committedRows.length,
        'bundle_hash': committedHash,
      },
      committedRowsResponse: {
        'bundle_seq': 1,
        'source_bundle_id': 1,
        'row_count': committedRows.length,
        'bundle_hash': committedHash,
        'rows': committedRows,
        'next_row_ordinal': committedRows.length - 1,
        'has_more': false,
      },
    );
    final client = _newClient(database, server, config: _documentsConfig);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      'INSERT INTO documents(id, title, data, ratio, enabled, created_at) VALUES(?, ?, ?, ?, ?, ?)',
      parameters: [
        smallIdBytes,
        'Small',
        smallData,
        1e-7,
        1,
        '2026-05-06T12:34:56Z',
      ],
    );
    await database.connection.execute(
      'INSERT INTO documents(id, title, data, ratio, enabled, created_at) VALUES(?, ?, ?, ?, ?, ?)',
      parameters: [
        largeIdBytes,
        'Large',
        largeData,
        1e21,
        1,
        '2026-05-07T12:34:56Z',
      ],
    );

    final report = await client.pushPending();

    expect(report.outcome, PushOutcome.committed);
    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
      0,
    );
    expect(
      await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_row_state'),
      2,
    );
  });

  test('payload codec treats BLOB foreign keys as UUID references', () async {
    final database = await _openFileReviewDatabase();
    addTearDown(database.close);
    final fileId = Uint8List.fromList([
      0x11,
      0x22,
      0x33,
      0x44,
      0x55,
      0x66,
      0x47,
      0x88,
      0x99,
      0xaa,
      0xbb,
      0xcc,
      0xdd,
      0xee,
      0xff,
      0x00,
    ]);
    final reviewId = Uint8List.fromList([
      0xaa,
      0xbb,
      0xcc,
      0xdd,
      0xee,
      0xff,
      0x40,
      0x11,
      0x80,
      0x22,
      0x33,
      0x44,
      0x55,
      0x66,
      0x77,
      0x88,
    ]);
    final content = Uint8List.fromList([4, 5, 6, 7]);
    final server = _PushServer(
      registeredTableSpecs: phase4RegisteredTableSpecsForConfig(
        _fileReviewConfig,
      ),
    );
    final client = _newClient(database, server, config: _fileReviewConfig);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      'INSERT INTO files(id, owner_id, content) VALUES(?, ?, ?)',
      parameters: [fileId, 'user-1', content],
    );
    await database.connection.execute(
      'INSERT INTO file_reviews(id, file_id, review) VALUES(?, ?, ?)',
      parameters: [reviewId, fileId, 'looks good'],
    );

    await client.pushPending();

    final fileRow = server.uploadedRows.singleWhere(
      (row) => row['table'] == 'files',
    );
    final reviewRow = server.uploadedRows.singleWhere(
      (row) => row['table'] == 'file_reviews',
    );
    const fileWireId = '11223344-5566-4788-99aa-bbccddeeff00';
    const reviewWireId = 'aabbccdd-eeff-4011-8022-334455667788';
    expect(fileRow['payload'], {
      'id': fileWireId,
      'owner_id': 'user-1',
      'content': base64Encode(content),
    });
    expect(reviewRow['payload'], {
      'id': reviewWireId,
      'file_id': fileWireId,
      'review': 'looks good',
    });
  });
}

List<Map<String, Object?>> _readPushFixtures() {
  final file = _repoRoot().uri
      .resolve('oversqlite-contracts/push/basic-insert.json')
      .toFilePath();
  final raw = jsonDecode(File(file).readAsStringSync()) as Map<String, Object?>;
  return (raw['cases']! as List<Object?>).cast<Map<String, Object?>>();
}

Map<String, Object?> _readPushCase(String name) {
  return _readPushFixtures().singleWhere((item) => item['name'] == name);
}

Future<void> _expectRecoveryState(
  SqliteNowDatabase database,
  Map<String, Object?> contract,
) async {
  final expected = (contract['expectedFinalState']! as Map)
      .cast<String, Object?>();
  final expectedError = expected['error'];
  expect(expectedError, anyOf('source_sequence_mismatch', 'rebuild_required'));
  expect(
    await _scalarInt(
      database,
      'SELECT rebuild_required FROM _sync_attachment_state',
    ),
    expected['rebuildRequired'] == true ? 1 : 0,
  );
  expect(
    await _scalarText(
      database,
      'SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1',
    ),
    expected['outboxState'],
  );
  expect(
    await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
    expected['outboxRowCount'],
  );
  expect(
    await _scalarInt(
      database,
      'SELECT next_source_bundle_id FROM _sync_source_state',
    ),
    expected['nextSourceBundleId'],
  );
}

Directory _repoRoot() {
  var current = Directory.current;
  while (true) {
    if (File.fromUri(current.uri.resolve('settings.gradle.kts')).existsSync()) {
      return current;
    }
    final parent = current.parent;
    if (parent.path == current.path) {
      throw StateError(
        'Could not locate repository root from ${Directory.current.path}',
      );
    }
    current = parent;
  }
}

DefaultOversqliteClient _newClient(
  SqliteNowDatabase database,
  OversqliteHttpClient http, {
  OversqliteConfig? config,
  Resolver resolver = const ServerWinsResolver(),
}) {
  return DefaultOversqliteClient(
    database: database,
    config: config ?? _usersConfig,
    httpClient: http,
    resolver: resolver,
  );
}

final _usersConfig = OversqliteConfig(
  schema: 'main',
  syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
);

final _documentsConfig = OversqliteConfig(
  schema: 'main',
  syncTables: [SyncTable(tableName: 'documents', syncKeyColumnName: 'id')],
);

final _fileReviewConfig = OversqliteConfig(
  schema: 'main',
  syncTables: [
    SyncTable(tableName: 'files', syncKeyColumnName: 'id'),
    SyncTable(tableName: 'file_reviews', syncKeyColumnName: 'id'),
  ],
);

final _authorProfileConfig = OversqliteConfig(
  schema: 'main',
  syncTables: [
    SyncTable(tableName: 'authors', syncKeyColumnName: 'id'),
    SyncTable(tableName: 'profiles', syncKeyColumnName: 'id'),
  ],
);

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

Future<SqliteNowDatabase> _openDocumentsDatabase() async {
  final database = SqliteNowDatabase.inMemory();
  await database.open(
    preInit: (connection) {
      return connection.execute('''CREATE TABLE documents (
  id BLOB PRIMARY KEY NOT NULL,
  title TEXT NOT NULL,
  data BLOB NOT NULL,
  ratio REAL NOT NULL,
  enabled INTEGER NOT NULL,
  created_at TEXT NOT NULL
)''');
    },
  );
  return database;
}

Future<SqliteNowDatabase> _openFileReviewDatabase() async {
  final database = SqliteNowDatabase.inMemory();
  await database.open(
    preInit: (connection) async {
      await connection.execute('''CREATE TABLE files (
  id BLOB PRIMARY KEY NOT NULL,
  owner_id TEXT NOT NULL,
  content BLOB NOT NULL
)''');
      await connection.execute('''CREATE TABLE file_reviews (
  id BLOB PRIMARY KEY NOT NULL,
  file_id BLOB NOT NULL REFERENCES files(id) ON DELETE CASCADE,
  review TEXT NOT NULL
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

Future<String> _scalarText(SqliteNowDatabase database, String sql) async {
  final rows = await database.connection.select(
    sql,
    (row) => row.readString(0),
  );
  return rows.single;
}

final class _PushServer implements OversqliteHttpClient {
  _PushServer({
    this.connectResolution = 'initialize_empty',
    List<Map<String, Object?>>? registeredTableSpecs,
    Map<String, Object?>? conflict,
    List<Map<String, Object?>> conflicts = const [],
    this.failFirstCommit = false,
    this.failFirstCommittedFetch = false,
    this.pruneFirstCommittedFetch = false,
    this.sourceRetiredOnCreate = false,
    this.transformCommittedPayload,
    this.alreadyCommitted = false,
    this.createResponse,
    this.commitResponse,
    this.committedRowsResponse,
    this.preserveConfiguredBundleHash = false,
  }) : registeredTableSpecs =
           registeredTableSpecs ?? phase4RegisteredTableSpecs(['users']),
       conflicts = [...conflicts, ?conflict];

  final String connectResolution;
  final List<Map<String, Object?>> registeredTableSpecs;
  final List<Map<String, Object?>> conflicts;
  final bool failFirstCommit;
  final bool failFirstCommittedFetch;
  final bool pruneFirstCommittedFetch;
  bool sourceRetiredOnCreate;
  final Map<String, Object?> Function(Map<String, Object?> payload)?
  transformCommittedPayload;
  final bool alreadyCommitted;
  final Map<String, Object?>? createResponse;
  final Map<String, Object?>? commitResponse;
  final Map<String, Object?>? committedRowsResponse;
  final bool preserveConfiguredBundleHash;
  final List<Map<String, Object?>> createRequests = [];
  final List<Map<String, Object?>> uploadedRows = [];
  var _commitAttempts = 0;
  var _committedFetchAttempts = 0;
  var _sourceId = '';
  var _canonicalRequestHash = '';

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    _sourceId = sourceId;
    if (path == 'sync/capabilities') {
      return _json(
        phase4CapabilitiesResponse(registeredTableSpecs: registeredTableSpecs),
      );
    }
    if (path.startsWith('sync/committed-bundles/')) {
      _committedFetchAttempts++;
      if (failFirstCommittedFetch && _committedFetchAttempts == 1) {
        return _json({
          'error': 'temporary',
          'message': 'retry',
        }, statusCode: 500);
      }
      if (pruneFirstCommittedFetch && _committedFetchAttempts == 1) {
        return _json({
          'error': 'history_pruned',
          'message': 'committed bundle replay pruned',
        }, statusCode: 409);
      }
      if (committedRowsResponse != null) {
        return _json({
          ..._withCommittedRowsHash(committedRowsResponse!),
          'source_id': _sourceId,
        });
      }
      final rows = _defaultCommittedRows();
      return _json({
        'bundle_seq': 1,
        'source_id': _sourceId,
        'source_bundle_id': 1,
        'row_count': uploadedRows.length,
        'bundle_hash': _committedBundleHashForRows(rows),
        'canonical_request_hash': _canonicalRequestHash,
        'rows': rows,
        'next_row_ordinal': uploadedRows.isEmpty ? -1 : uploadedRows.length - 1,
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
      return _json({
        'resolution': connectResolution,
        if (connectResolution == 'initialize_local')
          'initialization_id': 'init-connect',
      });
    }
    if (path == 'sync/push-sessions') {
      final request = (body! as Map).cast<String, Object?>();
      createRequests.add(request);
      _canonicalRequestHash = request['canonical_request_hash']! as String;
      if (sourceRetiredOnCreate) {
        return _json({
          'error': 'source_retired',
          'message': 'source retired',
          'source_id': sourceId,
          'replaced_by_source_id': 'server-replacement-source',
        }, statusCode: 409);
      }
      if (alreadyCommitted) {
        final rows = committedRowsResponse == null
            ? _defaultCommittedRows()
            : _rowsFromCommittedRowsResponse(committedRowsResponse!);
        final response =
            createResponse ??
            {
              'status': 'already_committed',
              'bundle_seq': 1,
              'source_id': sourceId,
              'source_bundle_id': request['source_bundle_id'],
              'row_count': rows.length,
            };
        return _json(
          _withCommittedRowsHash({
            ...response,
            'canonical_request_hash':
                response['canonical_request_hash'] ?? _canonicalRequestHash,
          }, rows: rows),
          sourceIdOverride: sourceId,
        );
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
      _commitAttempts++;
      if (failFirstCommit && _commitAttempts == 1) {
        return _json({
          'error': 'temporary',
          'message': 'retry',
        }, statusCode: 500);
      }
      if (conflicts.isNotEmpty) {
        final conflictBody = conflicts.removeAt(0);
        return _json({
          'error': 'push_conflict',
          'message': 'conflict',
          'conflict': conflictBody,
        }, statusCode: 409);
      }
      final rows = committedRowsResponse == null
          ? _defaultCommittedRows()
          : _rowsFromCommittedRowsResponse(committedRowsResponse!);
      return _json(
        _withCommittedRowsHash(
          commitResponse ??
              {
                'bundle_seq': 1,
                'source_id': _sourceId,
                'source_bundle_id': 1,
                'row_count': rows.length,
              },
          rows: rows,
        ),
        sourceIdOverride: _sourceId,
      );
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
    String? sourceIdOverride,
  }) {
    final responseBody = sourceIdOverride == null
        ? body
        : {...body, 'source_id': sourceIdOverride};
    return OversqliteHttpResponse(
      statusCode: statusCode,
      body: jsonEncode(responseBody),
    );
  }

  Object? _committedPayload(Object? payload) {
    if (payload is! Map) {
      return payload;
    }
    final typed = payload.cast<String, Object?>();
    return transformCommittedPayload == null
        ? typed
        : transformCommittedPayload!(typed);
  }

  List<Map<String, Object?>> _defaultCommittedRows() {
    return [
      for (var i = 0; i < uploadedRows.length; i++)
        {
          'schema': uploadedRows[i]['schema'],
          'table': uploadedRows[i]['table'],
          'key': uploadedRows[i]['key'],
          'op': uploadedRows[i]['op'],
          'row_version': i + 1,
          'payload': _committedPayload(uploadedRows[i]['payload']),
        },
    ];
  }

  Map<String, Object?> _withCommittedRowsHash(
    Map<String, Object?> response, {
    List<Map<String, Object?>>? rows,
  }) {
    final committedRows = rows ?? _rowsFromCommittedRowsResponse(response);
    return {
      ...response,
      'row_count': response['row_count'] ?? committedRows.length,
      'bundle_hash':
          preserveConfiguredBundleHash && response['bundle_hash'] != null
          ? response['bundle_hash']
          : _committedBundleHashForRows(committedRows),
      'canonical_request_hash':
          response['canonical_request_hash'] ?? _canonicalRequestHash,
    };
  }

  List<Map<String, Object?>> _rowsFromCommittedRowsResponse(
    Map<String, Object?> response,
  ) {
    final rows = response['rows'];
    if (rows is! List) {
      return _defaultCommittedRows();
    }
    return rows
        .cast<Map<String, Object?>>()
        .map((row) => row.cast<String, Object?>())
        .toList();
  }
}

String _committedBundleHashForRows(List<Map<String, Object?>> rows) {
  return sha256
      .convert(
        utf8.encode(
          _canonicalizeFixtureJson([
            for (var i = 0; i < rows.length; i++)
              {
                'row_ordinal': i.toString(),
                'schema': rows[i]['schema'],
                'table': rows[i]['table'],
                'key': rows[i]['key'],
                'op': rows[i]['op'],
                'row_version': rows[i]['row_version'].toString(),
                'payload': rows[i]['payload'],
              },
          ]),
        ),
      )
      .bytes
      .map((byte) => byte.toRadixString(16).padLeft(2, '0'))
      .join();
}

String _canonicalizeFixtureJson(Object? value) {
  if (value == null) {
    return 'null';
  }
  if (value is String || value is bool) {
    return jsonEncode(value);
  }
  if (value is num) {
    return _canonicalizeFixtureJsonNumber(value.toString());
  }
  if (value is List) {
    return '[${value.map(_canonicalizeFixtureJson).join(',')}]';
  }
  if (value is Map) {
    final entries = value.entries.toList()
      ..sort(
        (left, right) => left.key.toString().compareTo(right.key.toString()),
      );
    return '{${entries.map((entry) => '${jsonEncode(entry.key.toString())}:${_canonicalizeFixtureJson(entry.value)}').join(',')}}';
  }
  return jsonEncode(value);
}

final _fixtureJsonNumberPattern = RegExp(
  r'^-?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?$',
);

String _canonicalizeFixtureJsonNumber(String value) {
  if (!_fixtureJsonNumberPattern.hasMatch(value)) {
    throw ArgumentError('invalid JSON number: $value');
  }
  final number = double.parse(value);
  if (!number.isFinite) {
    throw ArgumentError('JSON number must be finite: $value');
  }
  if (number == 0) {
    return '0';
  }

  final absNumber = number.abs();
  final rendered = number.toString().toLowerCase();
  if (absNumber >= 1e-6 && absNumber < 1e21) {
    return _normalizeFixturePlainNumber(_fixtureScientificToPlain(rendered));
  }
  return _normalizeFixtureScientificNumber(rendered);
}

String _fixtureScientificToPlain(String raw) {
  if (!raw.contains('e')) {
    return raw;
  }
  final sign = raw.startsWith('-') ? '-' : '';
  final unsigned = sign.isEmpty ? raw : raw.substring(1);
  final parts = unsigned.split('e');
  if (parts.length != 2) {
    throw ArgumentError('invalid scientific notation: $raw');
  }

  final mantissa = parts[0];
  final exponent = int.parse(parts[1]);
  final dotIndex = mantissa.indexOf('.');
  final digits = mantissa.replaceAll('.', '');
  final fractionalDigits = dotIndex >= 0 ? mantissa.length - dotIndex - 1 : 0;
  final decimalIndex = digits.length + exponent - fractionalDigits;

  if (decimalIndex <= 0) {
    return '${sign}0.${_zeroes(-decimalIndex)}$digits';
  }
  if (decimalIndex >= digits.length) {
    return '$sign$digits${_zeroes(decimalIndex - digits.length)}';
  }
  return '$sign${digits.substring(0, decimalIndex)}.'
      '${digits.substring(decimalIndex)}';
}

String _normalizeFixturePlainNumber(String raw) {
  final sign = raw.startsWith('-') ? '-' : '';
  final unsigned = sign.isEmpty ? raw : raw.substring(1);
  final dotIndex = unsigned.indexOf('.');
  final integerRaw = dotIndex >= 0 ? unsigned.substring(0, dotIndex) : unsigned;
  final fractionRaw = dotIndex >= 0 ? unsigned.substring(dotIndex + 1) : '';
  final integerPart = integerRaw.replaceFirst(RegExp(r'^0+'), '');
  final normalizedInteger = integerPart.isEmpty ? '0' : integerPart;
  final fractionalPart = fractionRaw.replaceFirst(RegExp(r'0+$'), '');
  if (normalizedInteger == '0' && fractionalPart.isEmpty) {
    return '0';
  }
  if (fractionalPart.isEmpty) {
    return '$sign$normalizedInteger';
  }
  return '$sign$normalizedInteger.$fractionalPart';
}

String _normalizeFixtureScientificNumber(String raw) {
  if (!raw.contains('e')) {
    return _fixturePlainToScientific(_normalizeFixturePlainNumber(raw));
  }
  final sign = raw.startsWith('-') ? '-' : '';
  final unsigned = sign.isEmpty ? raw : raw.substring(1);
  final parts = unsigned.split('e');
  if (parts.length != 2) {
    throw ArgumentError('invalid scientific notation: $raw');
  }

  final mantissa = _normalizeFixturePlainNumber(parts[0]);
  final exponent = int.parse(parts[1]);
  return '$sign$mantissa'
      'e${exponent >= 0 ? '+' : ''}$exponent';
}

String _fixturePlainToScientific(String raw) {
  final sign = raw.startsWith('-') ? '-' : '';
  final unsigned = sign.isEmpty ? raw : raw.substring(1);
  final dotIndex = unsigned.indexOf('.');
  final integerPart = dotIndex >= 0
      ? unsigned.substring(0, dotIndex)
      : unsigned;
  final fractionalPart = dotIndex >= 0 ? unsigned.substring(dotIndex + 1) : '';

  final firstIntegerNonZero = _firstNonZeroIndex(integerPart);
  if (firstIntegerNonZero >= 0) {
    final digits = (integerPart + fractionalPart).replaceFirst(
      RegExp(r'^0+'),
      '',
    );
    final exponent = integerPart.length - 1;
    return '$sign${_fixtureScientificMantissa(digits)}'
        'e+$exponent';
  }

  final firstFractionNonZero = _firstNonZeroIndex(fractionalPart);
  if (firstFractionNonZero < 0) {
    throw ArgumentError('plainToScientific requires a non-zero number: $raw');
  }
  final digits = fractionalPart.substring(firstFractionNonZero);
  final exponent = -(firstFractionNonZero + 1);
  return '$sign${_fixtureScientificMantissa(digits)}'
      'e$exponent';
}

int _firstNonZeroIndex(String value) {
  for (var i = 0; i < value.length; i++) {
    if (value.codeUnitAt(i) != 0x30) {
      return i;
    }
  }
  return -1;
}

String _fixtureScientificMantissa(String digits) {
  final head = digits.substring(0, 1);
  final tail = digits.substring(1);
  return tail.isEmpty ? head : '$head.$tail';
}

String _zeroes(int count) => List.filled(count, '0').join();

Map<String, Object?> _updateConflict({
  required String serverName,
  int serverRowVersion = 2,
}) {
  return {
    'schema': 'main',
    'table': 'users',
    'key': {'id': 'user-1'},
    'op': 'UPDATE',
    'base_row_version': 0,
    'server_row_version': serverRowVersion,
    'server_row_deleted': false,
    'server_row': {'id': 'user-1', 'name': serverName},
  };
}

Future<Object?> _captureException(Future<dynamic> future) async {
  try {
    await future;
    return null;
  } catch (error) {
    return error;
  }
}

final class _MergedResolver implements Resolver {
  const _MergedResolver();

  @override
  MergeResult resolve(ConflictContext conflict) {
    return const KeepMerged({'id': 'user-1', 'name': 'Merged'});
  }
}

final class _InvalidMergedResolver implements Resolver {
  const _InvalidMergedResolver();

  @override
  MergeResult resolve(ConflictContext conflict) {
    return const KeepMerged({'id': 'user-1'});
  }
}
