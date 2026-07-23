import 'dart:convert';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';

import '../integration_test/rich_numeric_scenarios.dart';

void main() {
  test('Flutter rich numeric scenarios match the canonical fixture', () async {
    final manifestFile = await _findRichSchemaManifest();
    final manifest =
        jsonDecode(await manifestFile.readAsString()) as Map<String, Object?>;

    expect([
      for (final scenario in richNumericScenarios) scenario.toJson(),
    ], manifest['numericScenarios']);
  });
}

Future<File> _findRichSchemaManifest() async {
  var directory = Directory.current.absolute;
  while (true) {
    final candidate = File(
      '${directory.path}/oversqlite-contracts/rich-schema/business-rich-v0.json',
    );
    if (await candidate.exists()) {
      return candidate;
    }
    final parent = directory.parent;
    if (parent.path == directory.path) {
      throw StateError('could not locate business-rich-v0.json');
    }
    directory = parent;
  }
}
