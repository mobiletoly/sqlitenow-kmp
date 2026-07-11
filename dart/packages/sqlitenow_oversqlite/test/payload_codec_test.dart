import 'dart:convert';
import 'dart:typed_data';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/src/config.dart';
import 'package:sqlitenow_oversqlite/src/local_row_store.dart';
import 'package:sqlitenow_oversqlite/src/payload_codec.dart';
import 'package:sqlitenow_oversqlite/src/payload_source.dart';
import 'package:sqlitenow_oversqlite/src/table_info.dart';
import 'package:test/test.dart';

void main() {
  test('typed exact values use ordinary SQLite INTEGER and TEXT', () async {
    final database = SqliteNowDatabase.inMemory();
    await database.open(
      preInit: (connection) => connection.execute('''CREATE TABLE exact_values (
  id TEXT PRIMARY KEY,
  min_value INTEGER NOT NULL,
  max_value INTEGER NOT NULL,
  amount TEXT NOT NULL
)'''),
    );
    addTearDown(database.close);
    final store = OversqliteLocalRowStore(database.connection, const [
      SyncTable(
        tableName: 'exact_values',
        syncKeyColumnName: 'id',
        numericColumns: {
          'min_value': NumericColumnKind.exactInt64,
          'max_value': NumericColumnKind.exactInt64,
          'amount': NumericColumnKind.exactDecimal,
        },
      ),
    ]);
    await store.upsertPayload(
      'exact_values',
      const {
        'id': 'n-1',
        'min_value': '-9223372036854775808',
        'max_value': '9223372036854775807',
        'amount': '1234567890.123456789',
      },
      PayloadSource.authoritativeWire,
      requireCompletePayload: true,
    );
    final stored = await database.connection.select(
      '''SELECT CAST(min_value AS TEXT), CAST(max_value AS TEXT), amount,
       typeof(min_value), typeof(max_value), typeof(amount)
FROM exact_values''',
      (row) => [for (var i = 0; i < 6; i++) row.readString(i)],
    );
    expect(stored.single, [
      '-9223372036854775808',
      '9223372036854775807',
      '1234567890.123456789',
      'integer',
      'integer',
      'text',
    ]);
    await expectLater(
      store.upsertPayload(
        'exact_values',
        const {
          'id': 'n-2',
          'min_value': '1.5',
          'max_value': '1',
          'amount': '1',
        },
        PayloadSource.authoritativeWire,
        requireCompletePayload: true,
      ),
      throwsArgumentError,
    );
    final count = await database.connection.select(
      "SELECT COUNT(*) FROM exact_values WHERE id = 'n-2'",
      (row) => row.readInt(0),
    );
    expect(count.single, 0);
  });

  test('typed exact numeric values stay strings and enforce int64 range', () {
    const exactInt = OversqliteColumnInfo(
      name: 'value',
      kind: OversqliteColumnKind.exactInt64,
      primaryKey: false,
    );
    const exactDecimal = OversqliteColumnInfo(
      name: 'amount',
      kind: OversqliteColumnKind.exactDecimal,
      primaryKey: false,
    );
    expect(
      bindOversqlitePayloadValue(exactInt, '-9223372036854775808'),
      '-9223372036854775808',
    );
    expect(
      wireOversqlitePayloadValue(exactInt, '9223372036854775807'),
      '9223372036854775807',
    );
    expect(
      () => bindOversqlitePayloadValue(exactInt, '9223372036854775808'),
      throwsArgumentError,
    );
    expect(
      bindOversqlitePayloadValue(exactDecimal, '1234567890.123456789'),
      '1234567890.123456789',
    );
    expect(
      () => bindOversqlitePayloadValue(exactDecimal, -0.0),
      throwsArgumentError,
    );
    for (final value in ['-0', '-0.0', '-0e10', '-0.000E-9']) {
      expect(
        () => bindOversqlitePayloadValue(exactDecimal, value),
        throwsArgumentError,
        reason: value,
      );
    }
  });

  test('canonical protocol JSON sorts keys and normalizes finite numbers', () {
    expect(
      canonicalizeOversqliteProtocolJson({
        'z': 1.20,
        'a': [true, 1e-7, 1e21],
      }),
      '{"a":[true,1e-7,1e+21],"z":1.2}',
    );
  });

  test('canonical protocol JSON rejects non-finite numbers', () {
    expect(
      () => canonicalizeOversqliteProtocolJson(double.nan),
      throwsA(isA<ArgumentError>()),
    );
    expect(
      () => canonicalizeOversqliteProtocolJson(double.infinity),
      throwsA(isA<ArgumentError>()),
    );
  });

  test('C1 typed exact numeric strings and UTF-16 property order', () {
    expect(
      canonicalizeOversqliteProtocolJson('9007199254740993'),
      '"9007199254740993"',
    );
    expect(
      canonicalizeOversqliteProtocolJson('1234567890.123456789'),
      '"1234567890.123456789"',
    );
    expect(canonicalizeOversqliteProtocolJson('1e700'), '"1e700"');
    expect(
      canonicalizeOversqliteProtocolJson({
        '\ue000': 'bmp',
        '𐀀': 'supplementary',
      }),
      '{"𐀀":"supplementary","":"bmp"}',
    );
  });

  test('UUID BLOB values round-trip through local and wire encodings', () {
    final bytes = Uint8List.fromList([
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
    final hex = hexBytes(bytes);
    const wireUuid = '12345678-9abc-4def-8012-3456789abcde';

    expect(hexBytes(decodeOversqliteLocalUuid(hex)), hex);
    expect(hexBytes(decodeOversqliteLocalUuid(wireUuid)), hex);
    expect(hexBytes(decodeOversqliteLocalUuid(base64Encode(bytes))), hex);
    expect(hexBytes(decodeOversqliteWireUuid(wireUuid)), hex);
    expect(canonicalOversqliteUuid(hex), wireUuid);
    expect(
      () => decodeOversqliteWireUuid(wireUuid.toUpperCase()),
      throwsA(isA<ArgumentError>()),
    );
  });

  test(
    'BLOB payload equivalence supports local hex and authoritative base64',
    () {
      final bytes = Uint8List.fromList([1, 2, 3, 255]);
      const idColumn = OversqliteColumnInfo(
        name: 'id',
        kind: OversqliteColumnKind.text,
        primaryKey: true,
      );
      const table = OversqliteTableInfo(
        name: 'files',
        primaryKey: idColumn,
        columns: [
          idColumn,
          OversqliteColumnInfo(
            name: 'data',
            kind: OversqliteColumnKind.blob,
            primaryKey: false,
          ),
        ],
      );

      expect(
        equivalentOversqlitePayloadObjects(
          table,
          {'id': 'file-1', 'data': hexBytes(bytes)},
          {'id': 'file-1', 'data': base64Encode(bytes)},
          PayloadSource.localState,
          PayloadSource.authoritativeWire,
        ),
        isTrue,
      );
    },
  );

  test('UUID primary keys convert between local and wire key shapes', () {
    const idColumn = OversqliteColumnInfo(
      name: 'id',
      kind: OversqliteColumnKind.uuidBlob,
      primaryKey: true,
    );
    const table = OversqliteTableInfo(
      name: 'documents',
      primaryKey: idColumn,
      columns: [idColumn],
    );
    const localPk = '123456789abc4def80123456789abcde';
    const wireUuid = '12345678-9abc-4def-8012-3456789abcde';

    expect(wireKeyFromOversqliteLocalKey(table, localPk), {'id': wireUuid});

    final localKey = localKeyFromOversqliteWireKey(table, {'id': wireUuid});
    expect(localKey.localPk, localPk);
    expect(localKey.keyJson, '{"id":"$localPk"}');
  });
}
