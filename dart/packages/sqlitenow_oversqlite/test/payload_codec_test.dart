import 'dart:convert';
import 'dart:typed_data';

import 'package:sqlitenow_oversqlite/src/payload_codec.dart';
import 'package:sqlitenow_oversqlite/src/payload_source.dart';
import 'package:sqlitenow_oversqlite/src/table_info.dart';
import 'package:test/test.dart';

void main() {
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
