import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';
import 'support/runtime_state_fixture_support.dart';

void main() {
  test('Dart shared runtime-state schema fixture matches runtime', () async {
    final database = await openUsersDatabase();
    addTearDown(database.close);
    final client = newRuntimeStateClient(database, PushFixtureServer());
    addTearDown(client.close);

    await client.open();

    final actual = await dumpRuntimeSchema(database);
    final expected = readRuntimeStateFixture(
      'oversqlite-contracts/runtime-state/schema/v0.json',
    );
    expect(actual, expected);
  });
}
