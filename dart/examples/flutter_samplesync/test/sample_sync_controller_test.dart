import 'package:flutter_test/flutter_test.dart';
import 'package:sqlitenow_flutter_samplesync/src/sample_models.dart';
import 'package:sqlitenow_flutter_samplesync/src/sample_repository.dart';
import 'package:sqlitenow_flutter_samplesync/src/sample_sync_controller.dart';
import 'package:sqlitenow_flutter_samplesync/src/session_preferences.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

void main() {
  test(
    'controller opens durable source and persists signed-out mode only',
    () async {
      final preferences = MemorySampleSessionPreferences();
      final controller = SampleSyncController(
        repository: SampleSyncRepository.inMemory(),
        preferences: preferences,
      );
      addTearDown(controller.close);

      await controller.initialize();
      expect(controller.sourceId, isNotEmpty);
      expect(controller.state, SampleControllerState.signedOut);

      await controller.setMode(SampleSyncMode.polling);
      controller.skipSignIn();
      await controller.addRandomPerson();

      expect(await preferences.readMode(), 'polling');
      expect(await preferences.readUsername(), isNull);
      expect(await controller.listPeople(), hasLength(1));
    },
  );

  test('updated-at resolver keeps only a strictly newer valid local row', () {
    const resolver = UpdatedAtWinsResolver();
    final newerLocal = _conflict(
      local: '2026-07-28T12:00:01Z',
      server: '2026-07-28T12:00:00Z',
    );
    final equal = _conflict(
      local: '2026-07-28T12:00:00Z',
      server: '2026-07-28T12:00:00Z',
    );
    final malformed = _conflict(local: 'not-a-time', server: null);

    expect(resolver.resolve(newerLocal), isA<KeepLocal>());
    expect(resolver.resolve(equal), isA<AcceptServer>());
    expect(resolver.resolve(malformed), isA<AcceptServer>());
  });
}

ConflictContext _conflict({String? local, String? server}) {
  return ConflictContext(
    schema: 'business',
    table: 'person',
    key: const {'id': '1'},
    localOp: 'UPDATE',
    localPayload: {'updated_at': local},
    baseRowVersion: 1,
    serverRowVersion: 2,
    serverRowDeleted: false,
    serverRow: {'updated_at': server},
  );
}
