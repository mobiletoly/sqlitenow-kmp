import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart';
import 'package:test/test.dart';

void main() {
  test('Dart reads the uniform numeric-string contract fixture', () {
    final fixture = _object(
      jsonDecode(
        File.fromUri(
          _repoRoot().uri.resolve(
            'oversqlite-contracts/canonical-json/'
            'jcs-uniform-numeric-strings.json',
          ),
        ).readAsStringSync(),
      ),
    );

    expect(fixture.keys.toSet(), {
      'contract',
      'contract_id',
      'fixture_schema_version',
      'required_protocol_version',
      'integer_cases',
      'decimal_cases',
      'float_cases',
      'boolean_cases',
      'replay_cases',
      'invalid_cases',
      'canonical_examples',
      'protocol',
    });
    expect(fixture['contract_id'], 'jcs_uniform_numeric_strings_v1');
    expect(fixture['fixture_schema_version'], 1);
    expect(fixture['required_protocol_version'], 'v1');

    _expectValueCases(fixture, 'integer_cases', {
      'int64_min',
      'int64_max',
      'above_javascript_safe_integer',
      'int2_min',
      'int2_max',
      'int4_min',
      'int4_max',
    });
    _expectValueCases(fixture, 'decimal_cases', {
      'decimal_precision_and_scale',
      'decimal_exponent',
      'decimal_postgresql_authoritative_scale',
    });
    _expectValueCases(fixture, 'float_cases', {
      'binary64_min_subnormal',
      'binary64_max_finite',
      'binary64_negative_max_finite',
      'float4_rounding',
      'floating_negative_zero_normalizes',
      'floating_integer_spelling',
    });
    _expectBooleanCases(fixture);
    _expectReplayCases(fixture);
    _expectInvalidCases(fixture);
    _expectCanonicalExamples(fixture);
    _expectProtocol(fixture);
  });
}

void _expectValueCases(
  Map<String, Object?> fixture,
  String section,
  Set<String> requiredNames,
) {
  final cases = _objects(fixture[section]);
  expect(_names(cases), requiredNames, reason: section);
  for (final item in cases) {
    expect(item.keys.toSet(), {
      'name',
      'local_sqlite',
      'uploaded_wire',
      'postgres_destination',
      'committed_server',
    }, reason: item['name'] as String);
    expect(_object(item['local_sqlite']).keys.toSet(), {
      'storage_class',
      'value_text',
    });
    expect(item['uploaded_wire'], isA<String>());
    expect(item['committed_server'], isA<String>());
  }
}

void _expectBooleanCases(Map<String, Object?> fixture) {
  final cases = _objects(fixture['boolean_cases']);
  expect(_names(cases), {'sqlite_boolean_false', 'sqlite_boolean_true'});
  expect(cases.map((item) => item['uploaded_wire']).toSet(), {'0', '1'});
  expect(cases.map((item) => item['committed_server']).toSet(), {false, true});
  for (final item in cases) {
    expect(_object(item['local_sqlite'])['storage_class'], 'integer');
    expect(item['postgres_destination'], 'boolean');
  }
}

void _expectReplayCases(Map<String, Object?> fixture) {
  final cases = _objects(fixture['replay_cases']);
  expect(_names(cases), {
    'committed_replay_unchanged_local_intent',
    'committed_replay_later_local_edit',
  });
  expect(cases.map((item) => item['decision']).toSet(), {
    'apply_committed_and_clear_dirty',
    'preserve_later_local_edit_and_requeue',
  });
  for (final item in cases) {
    expect(item.keys.toSet(), {
      'name',
      'frozen_local',
      'uploaded_wire',
      'committed_server',
      'live_local',
      'decision',
    });
  }
}

void _expectInvalidCases(Map<String, Object?> fixture) {
  final cases = _objects(fixture['invalid_cases']);
  expect(_names(cases), {
    'integer_leading_plus',
    'integer_leading_zero',
    'integer_negative_zero',
    'integer_fraction',
    'integer_exponent',
    'integer_out_of_range_high',
    'integer_out_of_range_low',
    'integer_legacy_json_number',
    'decimal_leading_plus',
    'decimal_leading_zero',
    'decimal_negative_zero',
    'decimal_malformed_fraction',
    'decimal_malformed_exponent',
    'decimal_nan',
    'decimal_legacy_json_number',
    'float_noncanonical_fraction',
    'float_noncanonical_exponent',
    'float_negative_zero',
    'float_malformed',
    'float_nan',
    'float_positive_infinity',
    'float_negative_infinity',
    'float_legacy_json_number',
    'boolean_string_true',
    'boolean_integer_two',
  });
  const categoriesByFamily = {
    'integer': {
      'leading_plus',
      'leading_zero',
      'negative_zero',
      'fraction',
      'exponent',
      'out_of_range',
      'legacy_json_number',
    },
    'decimal': {
      'leading_plus',
      'leading_zero',
      'negative_zero',
      'malformed',
      'non_finite',
      'legacy_json_number',
    },
    'float': {'noncanonical', 'malformed', 'non_finite', 'legacy_json_number'},
    'boolean': {'invalid_boolean_bridge'},
  };
  for (final item in cases) {
    expect(item.keys.toSet(), {
      'name',
      'family',
      'input',
      'input_json_type',
      'category',
      'outcome',
    });
    expect(
      categoriesByFamily[item['family']]!,
      contains(item['category']),
      reason: item['name'] as String,
    );
    expect(item['outcome'], 'reject_before_mutation');
    if (item['input_json_type'] == 'number') {
      expect(item['input'], isA<num>());
      expect(item['category'], 'legacy_json_number');
    } else {
      expect(item['input_json_type'], 'string');
      expect(item['input'], isA<String>());
    }
  }
}

void _expectCanonicalExamples(Map<String, Object?> fixture) {
  final examples = _object(fixture['canonical_examples']);
  expect(examples.keys.toSet(), {
    'push_request',
    'committed_bundle',
    'pull_response',
    'snapshot_response',
    'conflict_response',
  });
  for (final entry in examples.entries) {
    final item = _object(entry.value);
    expect(item.keys.toSet(), {'canonical_bytes', 'utf8_base64', 'sha256'});
    final bytes = utf8.encode(item['canonical_bytes']! as String);
    expect(base64.encode(bytes), item['utf8_base64'], reason: entry.key);
    expect(sha256.convert(bytes).toString(), item['sha256'], reason: entry.key);
    jsonDecode(utf8.decode(bytes));
  }
}

void _expectProtocol(Map<String, Object?> fixture) {
  final protocol = _object(fixture['protocol']);
  expect(_object(protocol['capabilities'])['protocol_version'], 'v1');
  final rejections = _objects(protocol['updated_client_rejections']);
  expect(_names(rejections), {
    'reject_v0',
    'reject_empty_version',
    'reject_unknown_version',
  });
  expect(rejections.map((item) => item['actual']).toSet(), {
    'v0',
    '',
    'v-next',
  });
  for (final item in rejections) {
    expect(item['category'], 'protocol_version_mismatch');
    expect(item['timing'], 'before_connect_or_outbox_freeze');
  }

  final reset = _object(protocol['full_reset']);
  expect(reset['client_database'], 'recreate');
  expect(reset['server_database'], 'recreate_including_business_data');
  expect(reset['preserve_frozen_outbox'], isFalse);
  final incompatible = _object(protocol['incompatible_development_build']);
  expect(incompatible['same_v1_may_be_incompatible'], isTrue);
  expect(incompatible['mixed_versions'], 'unsupported');
}

Map<String, Object?> _object(Object? value) =>
    (value! as Map).cast<String, Object?>();

List<Map<String, Object?>> _objects(Object? value) =>
    (value! as List).map(_object).toList();

Set<String> _names(List<Map<String, Object?>> items) =>
    items.map((item) => item['name']! as String).toSet();

Directory _repoRoot() {
  var current = Directory.current;
  while (true) {
    if (File.fromUri(current.uri.resolve('settings.gradle.kts')).existsSync()) {
      return current;
    }
    final parent = current.parent;
    if (parent.path == current.path) {
      throw StateError('Could not locate repository root');
    }
    current = parent;
  }
}
