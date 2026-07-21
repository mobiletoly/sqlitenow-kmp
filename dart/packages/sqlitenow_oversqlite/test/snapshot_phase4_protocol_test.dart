import 'dart:convert';

import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:sqlitenow_oversqlite/src/protocol_models.dart';
import 'package:sqlitenow_oversqlite/src/remote_api.dart';
import 'package:sqlitenow_oversqlite/src/watch_protocol.dart';
import 'package:sqlitenow_oversqlite/src/snapshot_wire_decoder.dart';
import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';

void main() {
  test('snapshot configuration validates every runtime-only bound', () {
    final config = OversqliteConfig(schema: 'main', syncTables: []);

    expect(config.downloadLimit, 1000);
    expect(config.snapshotChunkRows, 1000);
    expect(config.snapshotChunkBytes, 4 * 1024 * 1024);
    expect(config.snapshotApplyBatchRows, 256);
    expect(config.snapshotApplyBatchBytes, 4 * 1024 * 1024);
    final invalidConfigs =
        <({String field, Object value, Object Function() create})>[
          (
            field: 'snapshotChunkRows',
            value: 0,
            create: () => OversqliteConfig(
              schema: 'main',
              syncTables: const [],
              snapshotChunkRows: 0,
            ),
          ),
          (
            field: 'snapshotChunkBytes',
            value: -1,
            create: () => OversqliteConfig(
              schema: 'main',
              syncTables: const [],
              snapshotChunkBytes: -1,
            ),
          ),
          (
            field: 'snapshotApplyBatchRows',
            value: 0,
            create: () => OversqliteConfig(
              schema: 'main',
              syncTables: const [],
              snapshotApplyBatchRows: 0,
            ),
          ),
          (
            field: 'snapshotApplyBatchBytes',
            value: -1,
            create: () => OversqliteConfig(
              schema: 'main',
              syncTables: const [],
              snapshotApplyBatchBytes: -1,
            ),
          ),
        ];
    for (final scenario in invalidConfigs) {
      expect(
        scenario.create,
        throwsA(
          isA<ArgumentError>()
              .having((error) => error.name, 'name', scenario.field)
              .having((error) => error.invalidValue, 'value', scenario.value),
        ),
        reason: scenario.field,
      );
    }

    final invalidPolicies =
        <({String field, Object value, Object Function() create})>[
          (
            field: 'maxWait',
            value: Duration.zero,
            create: () =>
                OversqliteSnapshotCapacityRetryPolicy(maxWait: Duration.zero),
          ),
          (
            field: 'fallbackDelay',
            value: const Duration(microseconds: -1),
            create: () => OversqliteSnapshotCapacityRetryPolicy(
              fallbackDelay: const Duration(microseconds: -1),
            ),
          ),
          for (final value in [double.nan, double.infinity, -0.1, 1.1])
            (
              field: 'positiveJitterRatio',
              value: value,
              create: () => OversqliteSnapshotCapacityRetryPolicy(
                positiveJitterRatio: value,
              ),
            ),
        ];
    for (final scenario in invalidPolicies) {
      expect(
        scenario.create,
        throwsA(
          isA<ArgumentError>()
              .having((error) => error.name, 'name', scenario.field)
              .having(
                (error) => error.invalidValue,
                'value',
                same(scenario.value),
              ),
        ),
        reason: '${scenario.field}=${scenario.value}',
      );
    }
  });

  test('only protocol v1 is accepted with an exact typed mismatch', () {
    final accepted = _capabilities();
    expect(requireOversqliteProtocol(accepted), same(accepted));

    for (final actual in ['v0', '', 'v2', ' v1', 'V1']) {
      expect(
        () => requireOversqliteProtocol(_capabilities(protocolVersion: actual)),
        throwsA(
          isA<ProtocolVersionMismatchException>()
              .having((error) => error.expected, 'expected', 'v1')
              .having((error) => error.actual, 'actual', actual),
        ),
      );
    }
  });

  test('snapshot capability fields are required and negotiated by minimum', () {
    const required = {
      'default_rows_per_snapshot_chunk',
      'max_rows_per_snapshot_chunk',
      'default_bytes_per_snapshot_chunk',
      'max_bytes_per_snapshot_chunk',
      'max_bytes_per_snapshot_row',
      'max_concurrent_snapshot_builds',
      'max_concurrent_snapshot_chunk_requests',
    };
    for (final field in required) {
      final json = phase4CapabilitiesResponse(
        registeredTableSpecs: phase4RegisteredTableSpecs(['users']),
      );
      final limits = (json['bundle_limits']! as Map).cast<String, Object?>();
      limits.remove(field);
      expect(
        () => CapabilitiesResponse.fromJson(json),
        throwsA(isA<SnapshotCapabilitiesException>()),
        reason: field,
      );
    }
    for (final field in [
      'protocol_version',
      'schema_version',
      'features',
      'bundle_limits',
    ]) {
      final json = phase4CapabilitiesResponse(
        registeredTableSpecs: phase4RegisteredTableSpecs(['users']),
      )..remove(field);
      expect(
        () => CapabilitiesResponse.fromJson(json),
        throwsA(isA<SnapshotCapabilitiesException>()),
        reason: field,
      );
    }

    for (final omitted in <Set<String>>[
      {'snapshot_materialization_batch_rows'},
      {'snapshot_materialization_batch_bytes'},
      {
        'snapshot_materialization_batch_rows',
        'snapshot_materialization_batch_bytes',
      },
    ]) {
      final json = phase4CapabilitiesResponse(
        registeredTableSpecs: phase4RegisteredTableSpecs(['users']),
      );
      final limits = (json['bundle_limits']! as Map).cast<String, Object?>();
      for (final field in omitted) {
        limits.remove(field);
      }
      final parsed = CapabilitiesResponse.fromJson(json).bundleLimits;
      expect(
        parsed.snapshotMaterializationBatchRows,
        omitted.contains('snapshot_materialization_batch_rows') ? 0 : 1000,
      );
      expect(
        parsed.snapshotMaterializationBatchBytes,
        omitted.contains('snapshot_materialization_batch_bytes') ? 0 : 4194304,
      );
    }

    for (final value in [0, -1]) {
      final capabilities = _capabilities(
        limits: {
          'snapshot_materialization_batch_rows': value,
          'snapshot_materialization_batch_bytes': value,
        },
      );
      expect(capabilities.bundleLimits.snapshotMaterializationBatchRows, value);
      expect(
        capabilities.bundleLimits.snapshotMaterializationBatchBytes,
        value,
      );
      expect(
        negotiateSnapshotLimits(
          capabilities,
          OversqliteConfig(schema: 'main', syncTables: []),
        ).maxRows,
        1000,
      );
    }
    for (final field in [
      'snapshot_materialization_batch_rows',
      'snapshot_materialization_batch_bytes',
    ]) {
      for (final value in <Object>[
        '1',
        1.5,
        _jsonNumber('9223372036854775808'),
      ]) {
        final json = phase4CapabilitiesResponse(
          registeredTableSpecs: phase4RegisteredTableSpecs(['users']),
        );
        json['bundle_limits'] = {
          ...(json['bundle_limits']! as Map).cast<String, Object?>(),
          field: value,
        };
        expect(
          () => CapabilitiesResponse.fromJson(json),
          throwsA(isA<SnapshotCapabilitiesException>()),
          reason: '$field=$value',
        );
      }
    }

    final negotiated = negotiateSnapshotLimits(
      _capabilities(
        limits: {
          'default_rows_per_snapshot_chunk': 300,
          'max_rows_per_snapshot_chunk': 300,
          'default_bytes_per_snapshot_chunk': 2048,
          'max_bytes_per_snapshot_chunk': 2048,
          'max_bytes_per_snapshot_row': 1024,
        },
      ),
      OversqliteConfig(
        schema: 'main',
        syncTables: [],
        downloadLimit: 7,
        snapshotChunkRows: 400,
        snapshotChunkBytes: 4096,
        snapshotApplyBatchBytes: 4096,
      ),
    );
    expect(negotiated.maxRows, 300);
    expect(negotiated.maxBytes, 2048);
    expect(negotiated.maxRowBytes, 1024);
  });

  test('invalid snapshot capability relationships fail closed', () {
    for (final overrides in <Map<String, Object?>>[
      {'default_rows_per_snapshot_chunk': 0},
      {
        'default_rows_per_snapshot_chunk': 1001,
        'max_rows_per_snapshot_chunk': 1000,
      },
      {'default_bytes_per_snapshot_chunk': 0},
      {
        'default_bytes_per_snapshot_chunk': 4097,
        'max_bytes_per_snapshot_chunk': 4096,
      },
      {
        'max_bytes_per_snapshot_chunk': 1024,
        'max_bytes_per_snapshot_row': 1025,
      },
      {'max_concurrent_snapshot_builds': 0},
      {'max_concurrent_snapshot_chunk_requests': 0},
    ]) {
      expect(
        () => negotiateSnapshotLimits(
          _capabilities(limits: overrides),
          OversqliteConfig(schema: 'main', syncTables: []),
        ),
        throwsA(isA<SnapshotCapabilitiesException>()),
        reason: overrides.toString(),
      );
    }

    expect(
      () => negotiateSnapshotLimits(
        _capabilities(limits: {'max_bytes_per_snapshot_row': 1025}),
        OversqliteConfig(
          schema: 'main',
          syncTables: [],
          snapshotChunkBytes: 1024,
        ),
      ),
      throwsA(isA<SnapshotCapabilitiesException>()),
    );
    expect(
      () => negotiateSnapshotLimits(
        _capabilities(limits: {'max_bytes_per_snapshot_row': 1025}),
        OversqliteConfig(
          schema: 'main',
          syncTables: [],
          snapshotApplyBatchBytes: 1024,
        ),
      ),
      throwsA(isA<SnapshotCapabilitiesException>()),
    );
  });

  test('capability and session integers enforce signed-int64 boundaries', () {
    const minInt64 = -0x8000000000000000;
    const maxInt64 = 0x7fffffffffffffff;

    for (final value in [minInt64, maxInt64]) {
      final json = phase4CapabilitiesResponse(
        registeredTableSpecs: phase4RegisteredTableSpecs(['users']),
      );
      json['schema_version'] = value;
      expect(CapabilitiesResponse.fromJson(json).schemaVersion, value);

      final limits = (json['bundle_limits']! as Map).cast<String, Object?>();
      limits['max_rows_per_bundle'] = value;
      expect(
        CapabilitiesResponse.fromJson(json).bundleLimits.maxRowsPerBundle,
        value,
      );
    }

    for (final value in [
      _jsonNumber('-9223372036854775809'),
      _jsonNumber('9223372036854775808'),
    ]) {
      for (final field in ['schema_version', 'max_rows_per_bundle']) {
        final json = phase4CapabilitiesResponse(
          registeredTableSpecs: phase4RegisteredTableSpecs(['users']),
        );
        if (field == 'schema_version') {
          json[field] = value;
        } else {
          json['bundle_limits'] = {
            ...(json['bundle_limits']! as Map).cast<String, Object?>(),
            field: value,
          };
        }
        expect(
          () => CapabilitiesResponse.fromJson(json),
          throwsA(isA<SnapshotCapabilitiesException>()),
          reason: '$field=$value',
        );
      }
    }
    for (final value in <Object?>[null, '1', 1.5]) {
      final json = phase4CapabilitiesResponse(
        registeredTableSpecs: phase4RegisteredTableSpecs(['users']),
      );
      json['schema_version'] = value;
      expect(
        () => CapabilitiesResponse.fromJson(json),
        throwsA(isA<SnapshotCapabilitiesException>()),
        reason: 'schema_version=$value',
      );
    }

    final maxSession = SnapshotSession.fromJson({
      'snapshot_id': 'snapshot-max',
      'snapshot_bundle_seq': maxInt64,
      'row_count': maxInt64,
      'byte_count': maxInt64,
      'expires_at': '2030-01-01T00:00:00Z',
    });
    expect(maxSession.snapshotBundleSeq, maxInt64);
    expect(maxSession.rowCount, maxInt64);
    expect(maxSession.byteCount, maxInt64);

    for (final field in ['snapshot_bundle_seq', 'row_count', 'byte_count']) {
      final minimum = <String, Object?>{
        'snapshot_id': 'snapshot-min',
        'snapshot_bundle_seq': 1,
        'row_count': 1,
        'byte_count': 1,
        'expires_at': '2030-01-01T00:00:00Z',
        field: minInt64,
      };
      expect(
        () => SnapshotSession.fromJson(minimum),
        throwsA(isA<OversqliteProtocolException>()),
        reason: '$field signed minimum reaches semantic validation',
      );
      for (final value in [
        _jsonNumber('-9223372036854775809'),
        _jsonNumber('9223372036854775808'),
      ]) {
        final outside = {...minimum, field: value};
        expect(
          () => SnapshotSession.fromJson(outside),
          throwsA(isA<OversqliteProtocolException>()),
          reason: '$field=$value',
        );
      }
    }
    for (final field in ['snapshot_bundle_seq', 'row_count', 'byte_count']) {
      for (final value in <Object?>[null, '1', 1.5]) {
        final malformed = <String, Object?>{
          'snapshot_id': 'snapshot-malformed',
          'snapshot_bundle_seq': 1,
          'row_count': 1,
          'byte_count': 1,
          'expires_at': '2030-01-01T00:00:00Z',
          field: value,
        };
        expect(
          () => SnapshotSession.fromJson(malformed),
          throwsA(isA<OversqliteProtocolException>()),
          reason: '$field=$value',
        );
      }
      final missing = <String, Object?>{
        'snapshot_id': 'snapshot-missing',
        'snapshot_bundle_seq': 1,
        'row_count': 1,
        'byte_count': 1,
        'expires_at': '2030-01-01T00:00:00Z',
      }..remove(field);
      expect(
        () => SnapshotSession.fromJson(missing),
        throwsA(isA<OversqliteProtocolException>()),
        reason: '$field missing',
      );
    }
  });

  test('strict chunk decoder retains exact row-object UTF-8 bytes', () {
    const row =
        '{\n  "schema":"main",\n  "table":"users",\n  "key":{"id":"1"},\n  "row_version":1,\n  "payload":{"id":"1","name":"Ada"}\n}';
    final byteCount = utf8.encode(row).length;
    final raw =
        '{"snapshot_id":"snapshot-1","snapshot_bundle_seq":1,'
        '"rows":[$row],"next_row_ordinal":1,"has_more":false,'
        '"byte_count":$byteCount}';

    final decoded = decodeSnapshotChunkWire(raw);

    expect(decoded.rowWireByteCount, byteCount);
    expect(decoded.chunk.byteCount, byteCount);
    expect(decoded.chunk.rows.single.key, {'id': '1'});
  });

  test('row wire byte accounting is exact for Unicode and escapes', () {
    const unicodeRow =
        '{"schema":"main","table":"users","key":{"id":"😀"},'
        '"row_version":1,"payload":{"name":"Māia 😀"}}';
    const escapedRow =
        '{"schema":"main","table":"users","key":{"id":"2"},'
        '"row_version":2,"payload":{"name":"M\\u0101ia\\nline"}}';
    final byteCount =
        utf8.encode(unicodeRow).length + utf8.encode(escapedRow).length;
    final raw =
        '{"snapshot_id":"snapshot-1","snapshot_bundle_seq":1,'
        '"rows":[$unicodeRow,$escapedRow],"next_row_ordinal":2,'
        '"has_more":false,"byte_count":$byteCount}';

    final decoded = decodeSnapshotChunkWire(raw);

    expect(decoded.rowWireByteCount, byteCount);
    expect(decoded.chunk.rows.first.key, {'id': '😀'});
    expect(decoded.chunk.rows.last.payload, {'name': 'Māia\nline'});
  });

  test(
    'row wire bytes exclude array separators and surrounding whitespace',
    () {
      const first =
          '{"schema":"main","table":"users","key":{"id":"1"},'
          '"row_version":1,"payload":{"id":"1"}}';
      const second =
          '{"schema":"main","table":"users","key":{"id":"2"},'
          '"row_version":2,"payload":{"id":"2"}}';
      final byteCount = utf8.encode(first).length + utf8.encode(second).length;
      final raw =
          '{"snapshot_id":"snapshot-1","snapshot_bundle_seq":1,'
          '"rows":[\n  $first,\n\t$second\n],"next_row_ordinal":2,'
          '"has_more":false,"byte_count":$byteCount}';

      final decoded = decodeSnapshotChunkWire(raw);

      expect(decoded.rowWireByteCount, byteCount);
      expect(decoded.chunk.rows, hasLength(2));
    },
  );

  test(
    'strict JSON validation rejects duplicate, escaped, and unknown names',
    () {
      final valid = _emptyChunkJson();
      final invalid = <String>[
        valid.replaceFirst('"byte_count":0', '"byte_count":0,"byte_count":0'),
        valid.replaceFirst(
          '"byte_count":0',
          '"byte_count":0,"byte_c\\u006funt":0',
        ),
        valid.replaceFirst('"byte_count":0', '"byte_count":0,"extra":0'),
        valid.replaceFirst('"byte_count":0', ''),
        valid.replaceFirst('"rows":[]', '"rows":[{"schema":"main"}]'),
      ];
      for (final raw in invalid) {
        expect(
          () => decodeSnapshotChunkWire(raw),
          throwsA(
            anyOf(
              isA<SnapshotSemanticException>(),
              isA<SnapshotResponseDecodeException>(),
            ),
          ),
        );
      }
    },
  );

  test('valid JSON rows with invalid values use the fixed row sentinel', () {
    final invalidRows = <String>[
      '{"schema":"","table":"users","key":{"id":"1"},'
          '"row_version":1,"payload":{"id":"1"}}',
      '{"schema":"main","table":"","key":{"id":"1"},'
          '"row_version":1,"payload":{"id":"1"}}',
      '{"schema":"main","table":"users","key":{},'
          '"row_version":1,"payload":{"id":"1"}}',
      '{"schema":"main","table":"users","key":{"id":"1"},'
          '"row_version":0,"payload":{"id":"1"}}',
      '{"schema":"main","table":"users","key":{"id":"1"},'
          '"row_version":1,"payload":null}',
      '{"schema":"main","table":"users","key":{"id":"1"},'
          '"row_version":1}',
    ];
    for (final row in invalidRows) {
      final rowBytes = utf8.encode(row).length;
      final raw =
          '{"snapshot_id":"snapshot-1","snapshot_bundle_seq":1,'
          '"rows":[$row],"next_row_ordinal":1,"has_more":false,'
          '"byte_count":$rowBytes}';

      expect(
        () {
          final decoded = decodeSnapshotChunkWire(raw);
          validateSnapshotRow(decoded.chunk.rows.single);
        },
        throwsA(
          isA<SnapshotSemanticException>().having(
            (error) => error.failure,
            'failure',
            SnapshotSemanticFailure.invalidRow,
          ),
        ),
        reason: row,
      );
    }
  });

  test('duplicate-member preflight rejects duplicates nested in arrays', () {
    expect(
      () => requireUniqueSnapshotJsonObjectMembers(
        '{"outer":[{"nested":{"value":1,"value":2}}]}',
        operation: 'nested duplicate proof',
      ),
      throwsA(
        isA<SnapshotSemanticException>().having(
          (error) => error.failure,
          'failure',
          SnapshotSemanticFailure.duplicateObjectMember,
        ),
      ),
    );
  });

  test('source IDs are exact visible ASCII tokens and are never trimmed', () {
    expect(requireValidOversqliteSourceId('source-1'), 'source-1');
    for (final value in ['', ' source', 'source ', 'source\n', 'sourcé']) {
      expect(
        () => requireValidOversqliteSourceId(value),
        throwsA(isA<InvalidOversqliteSourceIdException>()),
        reason: jsonEncode(value),
      );
    }
  });

  test('snapshot expiry accepts only canonical RFC 3339 timestamps', () {
    const valid = [
      '2024-02-29T23:59:59Z',
      '2024-02-29T23:59:59.1Z',
      '2024-02-29T23:59:59.123456789Z',
      '2024-02-29T23:59:59+00:00',
      '2024-02-29T23:59:59-23:59',
    ];
    for (final expiresAt in valid) {
      final session = SnapshotSession.fromJson({
        'snapshot_id': 'snapshot-valid',
        'snapshot_bundle_seq': 0,
        'row_count': 0,
        'byte_count': 0,
        'expires_at': expiresAt,
      });
      expect(session.expiresAt, expiresAt, reason: expiresAt);
    }

    const sessionSentinel = 'snapshot-SECRET-session';
    const invalid = <Object?>[
      null,
      7,
      '',
      '   ',
      '2023-02-29T00:00:00Z',
      '2024-13-01T00:00:00Z',
      '2024-01-01T24:00:00Z',
      '2024-01-01T00:60:00Z',
      '2024-01-01T00:00:60Z',
      '2024-01-01 00:00:00Z',
      '2024-01-01t00:00:00z',
      '2024-01-01T00:00:00',
      '2024-01-01T00:00:00+24:00',
      '2024-01-01T00:00:00.1234567890Z',
      '024-01-01T00:00:00Z',
      '2024-01-01T00:00:00Z-trailing-SECRET',
    ];
    for (final expiresAt in invalid) {
      Object? caught;
      try {
        SnapshotSession.fromJson({
          'snapshot_id': sessionSentinel,
          'snapshot_bundle_seq': 1,
          'row_count': 1,
          'byte_count': 1,
          'expires_at': expiresAt,
        });
      } catch (error) {
        caught = error;
      }

      expect(caught, isA<SnapshotSemanticException>(), reason: '$expiresAt');
      final error = caught! as SnapshotSemanticException;
      expect(
        error.failure,
        SnapshotSemanticFailure.invalidSession,
        reason: '$expiresAt',
      );
      expect(error.toString(), isNot(contains(sessionSentinel)));
      if (expiresAt is String && expiresAt.isNotEmpty) {
        expect(error.toString(), isNot(contains(expiresAt)));
      }
    }

    expect(
      () => SnapshotSession.fromJson({
        'snapshot_id': sessionSentinel,
        'snapshot_bundle_seq': 1,
        'row_count': 1,
        'byte_count': 1,
      }),
      throwsA(
        isA<SnapshotSemanticException>().having(
          (error) => error.failure,
          'failure',
          SnapshotSemanticFailure.invalidSession,
        ),
      ),
    );
  });

  test('derived chunk response limits use checked int64 arithmetic', () {
    expect(checkedSnapshotChunkBodyLimit(32, 2), 32 + 2 + 64 * 1024);
    expect(
      () => checkedSnapshotChunkBodyLimit(maxOversqliteInt64, 1),
      throwsA(isA<OversqliteProtocolException>()),
    );
  });
}

CapabilitiesResponse _capabilities({
  String protocolVersion = 'v1',
  Map<String, Object?> limits = const {},
}) {
  final json = phase4CapabilitiesResponse(
    protocolVersion: protocolVersion,
    registeredTableSpecs: phase4RegisteredTableSpecs(['users']),
  );
  json['bundle_limits'] = {
    ...(json['bundle_limits']! as Map).cast<String, Object?>(),
    ...limits,
  };
  return CapabilitiesResponse.fromJson(json);
}

String _emptyChunkJson() {
  return '{"snapshot_id":"snapshot-1","snapshot_bundle_seq":0,'
      '"rows":[],"next_row_ordinal":0,"has_more":false,"byte_count":0}';
}

Object _jsonNumber(String token) {
  return (jsonDecode('{"value":$token}')! as Map)['value']!;
}
