import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_oversqlite/src/payload_codec.dart';
import 'package:sqlitenow_oversqlite/src/table_info.dart';
import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';

void main() {
  test('Dart consumes authoritative JCS typed numeric vectors', () {
    final file = repoRoot().uri.resolve(
      'oversqlite-contracts/canonical-json/jcs-typed-numerics.json',
    );
    final fixture = (jsonDecode(File.fromUri(file).readAsStringSync()) as Map)
        .cast<String, Object?>();

    for (final raw in fixture['json_cases']! as List) {
      final item = (raw as Map).cast<String, Object?>();
      expect(
        canonicalizeOversqliteProtocolJson(
          jsonDecode(item['input']! as String),
        ),
        item['canonical'],
        reason: item['name']! as String,
      );
    }
    for (final raw in fixture['invalid_cases']! as List) {
      final item = (raw as Map).cast<String, Object?>();
      expect(
        () => canonicalizeOversqliteProtocolJson(
          jsonDecode(item['input']! as String),
        ),
        throwsA(anything),
        reason: item['name']! as String,
      );
    }

    final hashes = (fixture['hash_cases']! as Map).cast<String, Object?>();
    for (final name in ['canonical_request', 'committed_bundle']) {
      final item = (hashes[name]! as Map).cast<String, Object?>();
      expect(sha256Hex(item['canonical']! as String), item['sha256']);
    }

    for (final raw in fixture['sqlite_cases']! as List) {
      final item = (raw as Map).cast<String, Object?>();
      final kind = switch (item['numeric_kind']) {
        'exact_int64' => OversqliteColumnKind.exactInt64,
        'exact_decimal' => OversqliteColumnKind.exactDecimal,
        'approximate' => OversqliteColumnKind.real,
        final value => throw StateError('unknown numeric kind $value'),
      };
      if (item['name'] == 'exact_decimal_wrong_affinity') {
        continue; // Affinity validation is exercised by table-info runtime tests.
      }
      const name = 'value';
      final column = OversqliteColumnInfo(
        name: name,
        kind: kind,
        primaryKey: false,
      );
      if (item['outcome'] == 'reject_before_mutation') {
        expect(
          () => bindOversqlitePayloadValue(column, item['wire']),
          throwsA(anything),
          reason: item['name']! as String,
        );
      } else {
        bindOversqlitePayloadValue(column, item['wire']);
      }
    }
    final reset = (fixture['reset']! as Map).cast<String, Object?>();
    expect(reset['legacy_state'], 'recreate_database');
    expect(reset['mixed_versions'], 'unsupported');
  });
}
