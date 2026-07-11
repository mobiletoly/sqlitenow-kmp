import 'dart:typed_data';

import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

import 'generated/rich_real_server_db.dart';
import 'realserver_support.dart';

final _richSyncTables = [
  for (final table in RichRealServerDb.syncTables)
    if (table.tableName == 'typed_rows')
      const SyncTable(
        tableName: 'typed_rows',
        syncKeyColumnName: 'id',
        numericColumns: {
          'count_value': NumericColumnKind.exactInt64,
          'enabled_flag': NumericColumnKind.exactInt64,
          'rating': NumericColumnKind.approximate,
        },
      )
    else
      table,
];

void main() {
  final realserverEnabled = flagEnabled('OVERSQLITE_REALSERVER_TESTS');
  group(
    'realserver rich generated schema',
    skip: realserverEnabled
        ? null
        : 'Set OVERSQLITE_REALSERVER_TESTS=true to run live realserver tests.',
    () {
      late RealServerConfig config;

      setUpAll(() async {
        config = await requireRealServerConfig();
      });

      test(
        'generated config preserves topology, typed rows, and blob payloads',
        () async {
          await resetRealServerState(config.baseUrl);
          final userId = randomRealserverId('dart-rich-user');
          final seedDb = RichRealServerDb.inMemory();
          final pullDb = RichRealServerDb.inMemory();
          final hydrateDb = RichRealServerDb.inMemory();
          addTearDown(seedDb.close);
          addTearDown(pullDb.close);
          addTearDown(hydrateDb.close);
          await seedDb.open();
          await pullDb.open();
          await hydrateDb.open();

          final seedSource = await _bootstrapSource(seedDb);
          final pullSource = await _bootstrapSource(pullDb);
          final hydrateSource = await _bootstrapSource(hydrateDb);
          final seedHttp = await authenticatedHttp(
            config.baseUrl,
            userId,
            seedSource,
          );
          final pullHttp = await authenticatedHttp(
            config.baseUrl,
            userId,
            pullSource,
          );
          final hydrateHttp = await authenticatedHttp(
            config.baseUrl,
            userId,
            hydrateSource,
          );
          addTearDown(seedHttp.close);
          addTearDown(pullHttp.close);
          addTearDown(hydrateHttp.close);

          final seed = _newGeneratedClient(seedDb, seedHttp);
          final pull = _newGeneratedClient(pullDb, pullHttp);
          final hydrate = _newGeneratedClient(hydrateDb, hydrateHttp);
          addTearDown(seed.close);
          addTearDown(pull.close);
          addTearDown(hydrate.close);

          await seed.open();
          await expectConnected(
            seed.attach(userId),
            AttachOutcome.startedEmpty,
          );
          await pull.open();
          await expectConnected(
            pull.attach(userId),
            AttachOutcome.usedRemoteState,
          );
          await hydrate.open();
          await expectConnected(
            hydrate.attach(userId),
            AttachOutcome.usedRemoteState,
          );

          final blob = _BlobKeyFixture(
            fileId: _uuidBytes(realserverUuid()),
            reviewId: _uuidBytes(realserverUuid()),
            label: 'rich-topology',
            data: Uint8List.fromList([
              0x00,
              0x11,
              0x22,
              0x33,
              0x44,
              0x55,
              0x66,
              0x77,
            ]),
          );
          final typedSeed = _TypedRowFixture(
            id: realserverUuid(),
            name: 'Seed Typed Row',
            note: null,
            countValue: 42,
            enabledFlag: 1,
            rating: 1.25,
            data: Uint8List.fromList([0xde, 0xad, 0xbe, 0xef]),
            createdAt: '2026-03-24T18:42:11Z',
          );
          final typedActive = _TypedRowFixture(
            id: realserverUuid(),
            name: 'Active Typed Row',
            note: 'second-device',
            countValue: null,
            enabledFlag: 0,
            rating: 6.57111473696007,
            data: null,
            createdAt: null,
          );

          await _insertCategoryGraph(seedDb, 'rich-topology');
          await _insertTeamGraph(seedDb, 'rich-topology');
          await _insertBlobKeyPair(seedDb, blob);
          await _insertTypedRow(seedDb, typedSeed);
          expect((await seed.pushPending()).outcome, PushOutcome.committed);

          await _insertTypedRow(pullDb, typedActive);
          expect((await pull.pushPending()).outcome, PushOutcome.committed);
          expect(
            (await pull.pullToStable()).outcome,
            isIn([
              RemoteSyncOutcome.appliedIncremental,
              RemoteSyncOutcome.alreadyAtTarget,
            ]),
          );
          expect(
            (await hydrate.rebuild()).outcome,
            RemoteSyncOutcome.appliedSnapshot,
          );

          await _assertTopologyState(pullDb, 'pull');
          await _assertTopologyState(hydrateDb, 'hydrate');
          await _assertBlobKeyState(pullDb, blob);
          await _assertBlobKeyState(hydrateDb, blob);
          await _assertTypedRowState(pullDb, typedSeed);
          await _assertTypedRowState(pullDb, typedActive);
          await _assertTypedRowState(hydrateDb, typedSeed);
          await _assertTypedRowState(hydrateDb, typedActive);
          await expectCleanSyncTables(pullDb.runtimeDatabase);
          await expectCleanSyncTables(hydrateDb.runtimeDatabase);
        },
      );

      test(
        'generated blob primary key tables converge through pull and rebuild',
        () async {
          await resetRealServerState(config.baseUrl);
          final userId = randomRealserverId('dart-blob-key-user');
          final seedDb = RichRealServerDb.inMemory();
          final pullDb = RichRealServerDb.inMemory();
          final hydrateDb = RichRealServerDb.inMemory();
          addTearDown(seedDb.close);
          addTearDown(pullDb.close);
          addTearDown(hydrateDb.close);
          await seedDb.open();
          await pullDb.open();
          await hydrateDb.open();

          final seedSource = await _bootstrapSource(seedDb);
          final pullSource = await _bootstrapSource(pullDb);
          final hydrateSource = await _bootstrapSource(hydrateDb);
          final seedHttp = await authenticatedHttp(
            config.baseUrl,
            userId,
            seedSource,
          );
          final pullHttp = await authenticatedHttp(
            config.baseUrl,
            userId,
            pullSource,
          );
          final hydrateHttp = await authenticatedHttp(
            config.baseUrl,
            userId,
            hydrateSource,
          );
          addTearDown(seedHttp.close);
          addTearDown(pullHttp.close);
          addTearDown(hydrateHttp.close);

          final seed = _newGeneratedClient(seedDb, seedHttp);
          final pull = _newGeneratedClient(pullDb, pullHttp);
          final hydrate = _newGeneratedClient(hydrateDb, hydrateHttp);
          addTearDown(seed.close);
          addTearDown(pull.close);
          addTearDown(hydrate.close);

          await seed.open();
          await expectConnected(
            seed.attach(userId),
            AttachOutcome.startedEmpty,
          );
          await pull.open();
          await expectConnected(
            pull.attach(userId),
            AttachOutcome.usedRemoteState,
          );
          await hydrate.open();
          await expectConnected(
            hydrate.attach(userId),
            AttachOutcome.usedRemoteState,
          );

          final blobA = _BlobKeyFixture(
            fileId: _uuidBytes('12345678-9abc-4def-8012-3456789abcde'),
            reviewId: _uuidBytes('22345678-9abc-4def-8012-3456789abcde'),
            label: 'blob-contract-a',
            data: Uint8List.fromList([1, 2, 3, 255]),
          );
          final blobB = _BlobKeyFixture(
            fileId: _uuidBytes('32345678-9abc-4def-8012-3456789abcde'),
            reviewId: _uuidBytes('42345678-9abc-4def-8012-3456789abcde'),
            label: 'blob-contract-b',
            data: Uint8List.fromList([0xaa, 0xbb, 0xcc, 0xdd]),
          );
          await _insertBlobKeyPair(seedDb, blobA);
          await _insertBlobKeyPair(seedDb, blobB);

          expect((await seed.pushPending()).outcome, PushOutcome.committed);
          expect(
            (await pull.pullToStable()).outcome,
            RemoteSyncOutcome.appliedIncremental,
          );
          expect(
            (await hydrate.rebuild()).outcome,
            RemoteSyncOutcome.appliedSnapshot,
          );

          await _assertBlobKeyState(pullDb, blobA, blobB);
          await _assertBlobKeyState(hydrateDb, blobA, blobB);
          await expectCleanSyncTables(pullDb.runtimeDatabase);
          await expectCleanSyncTables(hydrateDb.runtimeDatabase);
        },
      );
    },
    timeout: const Timeout(Duration(minutes: 2)),
  );
}

DefaultOversqliteClient _newGeneratedClient(
  RichRealServerDb database,
  OversqliteHttpClient http, {
  int uploadLimit = 8,
  int downloadLimit = 8,
}) {
  return database.newOversqliteClient(
    schema: 'business',
    httpClient: http,
    syncTables: _richSyncTables,
    uploadLimit: uploadLimit,
    downloadLimit: downloadLimit,
  );
}

Future<String> _bootstrapSource(RichRealServerDb database) async {
  final client = DefaultOversqliteClient(
    database: database.runtimeDatabase,
    config: database.buildOversqliteConfig(
      schema: 'business',
      syncTables: _richSyncTables,
      uploadLimit: 8,
      downloadLimit: 8,
    ),
    httpClient: null,
  );
  await client.open();
  final source = (await client.sourceInfo()).currentSourceId;
  await client.close();
  return source;
}

Future<void> _insertCategoryGraph(
  RichRealServerDb database,
  String label,
) async {
  final rootId = realserverUuid();
  final childId = realserverUuid();
  final leafId = realserverUuid();
  await database.connection.execute(
    'INSERT INTO categories(id, name, parent_id) VALUES(?, ?, NULL)',
    parameters: [rootId, 'Category root $label'],
  );
  await database.connection.execute(
    'INSERT INTO categories(id, name, parent_id) VALUES(?, ?, ?)',
    parameters: [childId, 'Category child $label', rootId],
  );
  await database.connection.execute(
    'INSERT INTO categories(id, name, parent_id) VALUES(?, ?, ?)',
    parameters: [leafId, 'Category leaf $label', childId],
  );
}

Future<void> _insertTeamGraph(RichRealServerDb database, String label) async {
  final teamId = realserverUuid();
  final captainId = realserverUuid();
  final memberId = realserverUuid();
  await database.transaction(() async {
    await database.connection.execute(
      'INSERT INTO teams(id, name, captain_member_id) VALUES(?, ?, ?)',
      parameters: [teamId, 'Team $label', captainId],
    );
    await database.connection.execute(
      'INSERT INTO team_members(id, name, team_id) VALUES(?, ?, ?)',
      parameters: [captainId, 'Captain $label', teamId],
    );
    await database.connection.execute(
      'INSERT INTO team_members(id, name, team_id) VALUES(?, ?, ?)',
      parameters: [memberId, 'Member $label', teamId],
    );
  });
}

Future<void> _insertTypedRow(
  RichRealServerDb database,
  _TypedRowFixture row,
) async {
  await database.connection.execute(
    '''INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)''',
    parameters: [
      row.id,
      row.name,
      row.note,
      row.countValue,
      row.enabledFlag,
      row.rating,
      row.data,
      row.createdAt,
    ],
  );
}

Future<void> _insertBlobKeyPair(
  RichRealServerDb database,
  _BlobKeyFixture row,
) async {
  await database.connection.execute(
    'INSERT INTO files(id, name, data) VALUES(?, ?, ?)',
    parameters: [row.fileId, 'File ${row.label}', row.data],
  );
  await database.connection.execute(
    'INSERT INTO file_reviews(id, review, file_id) VALUES(?, ?, ?)',
    parameters: [row.reviewId, 'Review ${row.label}', row.fileId],
  );
}

Future<void> _assertTopologyState(
  RichRealServerDb database,
  String location,
) async {
  expect(
    await scalarInt(
      database.runtimeDatabase,
      'SELECT COUNT(*) FROM categories',
    ),
    3,
  );
  expect(
    await scalarInt(database.runtimeDatabase, 'SELECT COUNT(*) FROM teams'),
    1,
  );
  expect(
    await scalarInt(
      database.runtimeDatabase,
      'SELECT COUNT(*) FROM team_members',
    ),
    2,
  );
  expect(
    await scalarInt(database.runtimeDatabase, '''SELECT COUNT(*)
FROM categories c
JOIN categories p ON p.id = c.parent_id
WHERE c.name = 'Category child rich-topology'
  AND p.name = 'Category root rich-topology' '''),
    1,
    reason: 'missing category child on $location',
  );
  expect(
    await scalarInt(database.runtimeDatabase, '''SELECT COUNT(*)
FROM teams t
JOIN team_members captain ON captain.id = t.captain_member_id
WHERE t.name = 'Team rich-topology'
  AND captain.name = 'Captain rich-topology' '''),
    1,
    reason: 'missing deferred captain edge on $location',
  );
}

Future<void> _assertTypedRowState(
  RichRealServerDb database,
  _TypedRowFixture row,
) async {
  final rows = await database.typedRows.selectAll().asList();
  final actual = rows.singleWhere((item) => item.id == row.id);
  expect(actual.name, row.name);
  expect(actual.note, row.note);
  expect(actual.countValue, row.countValue);
  expect(actual.enabledFlag, row.enabledFlag);
  expect(actual.rating, row.rating);
  expect(actual.data, row.data);
  if (row.createdAt == null) {
    expect(actual.createdAt, isNull);
  } else {
    expect(actual.createdAt, isNotNull);
    expect(
      DateTime.parse(
        actual.createdAt!,
      ).isAtSameMomentAs(DateTime.parse(row.createdAt!)),
      isTrue,
    );
  }
}

Future<void> _assertBlobKeyState(
  RichRealServerDb database,
  _BlobKeyFixture rowA, [
  _BlobKeyFixture? rowB,
]) async {
  final expectedRows = [rowA, ?rowB];
  final rows = await database.files.selectAll().asList();
  expect(rows, hasLength(expectedRows.length));
  for (final row in expectedRows) {
    final actual = rows.singleWhere((item) => _bytesEqual(item.id, row.fileId));
    expect(actual.name, 'File ${row.label}');
    expect(actual.data, row.data);
    expect(
      await scalarText(
        database.runtimeDatabase,
        "SELECT review FROM file_reviews WHERE lower(hex(id)) = '${_hex(row.reviewId)}'",
      ),
      'Review ${row.label}',
    );
    expect(
      await scalarText(
        database.runtimeDatabase,
        "SELECT lower(hex(file_id)) FROM file_reviews WHERE lower(hex(id)) = '${_hex(row.reviewId)}'",
      ),
      _hex(row.fileId),
    );
  }
}

Uint8List _uuidBytes(String value) {
  final hex = value.replaceAll('-', '');
  return Uint8List.fromList([
    for (var i = 0; i < hex.length; i += 2)
      int.parse(hex.substring(i, i + 2), radix: 16),
  ]);
}

String _hex(Uint8List value) {
  const chars = '0123456789abcdef';
  return [
    for (final byte in value)
      '${chars[(byte >> 4) & 0x0f]}${chars[byte & 0x0f]}',
  ].join();
}

bool _bytesEqual(Uint8List left, Uint8List right) {
  if (left.length != right.length) return false;
  for (var i = 0; i < left.length; i++) {
    if (left[i] != right[i]) return false;
  }
  return true;
}

final class _BlobKeyFixture {
  const _BlobKeyFixture({
    required this.fileId,
    required this.reviewId,
    required this.label,
    required this.data,
  });

  final Uint8List fileId;
  final Uint8List reviewId;
  final String label;
  final Uint8List data;
}

final class _TypedRowFixture {
  const _TypedRowFixture({
    required this.id,
    required this.name,
    required this.note,
    required this.countValue,
    required this.enabledFlag,
    required this.rating,
    required this.data,
    required this.createdAt,
  });

  final String id;
  final String name;
  final String? note;
  final int? countValue;
  final int enabledFlag;
  final double? rating;
  final Uint8List? data;
  final String? createdAt;
}
