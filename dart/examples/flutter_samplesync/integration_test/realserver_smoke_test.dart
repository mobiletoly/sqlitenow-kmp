import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:sqlitenow_flutter_samplesync/src/sample_models.dart';
import 'package:sqlitenow_flutter_samplesync/src/sample_repository.dart';
import 'package:sqlitenow_flutter_samplesync/src/sample_sync_controller.dart';
import 'package:sqlitenow_flutter_samplesync/src/session_preferences.dart';

const _baseUrl = String.fromEnvironment(
  'SAMPLESYNC_BASE_URL',
  defaultValue: 'http://10.0.2.2:8080',
);
const _runTokenRefresh = bool.fromEnvironment('SAMPLESYNC_RUN_TOKEN_REFRESH');

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets('empty-password session syncs, watches, polls, and restores', (
    tester,
  ) async {
    final temp = await Directory.systemTemp.createTemp(
      'sqlitenow-flutter-samplesync-live-',
    );
    addTearDown(() => temp.delete(recursive: true));
    final preferencesA = MemorySampleSessionPreferences();
    final preferencesB = MemorySampleSessionPreferences();

    final clientA = SampleSyncController(
      repository: SampleSyncRepository.file('${temp.path}/a.db'),
      preferences: preferencesA,
      baseUrl: _baseUrl,
    );
    await clientA.initialize();
    await clientA.signIn('u10', '');
    expect(clientA.signedIn, isTrue, reason: clientA.errorMessage);
    await clientA.addRandomPerson();
    await clientA.manualSync();
    expect(clientA.reportMessage, contains('Pending rows: 0'));

    final clientB = SampleSyncController(
      repository: SampleSyncRepository.file('${temp.path}/b.db'),
      preferences: preferencesB,
      baseUrl: _baseUrl,
    );
    await clientB.initialize();
    await clientB.signIn('u10', '');
    expect(clientB.signedIn, isTrue, reason: clientB.errorMessage);

    final beforeWatch = (await clientB.listPeople()).length;
    await clientA.addRandomPerson();
    await clientA.manualSync();
    await _waitUntil(
      () async => (await clientB.listPeople()).length > beforeWatch,
      description: 'Watch delivery to the second Flutter client',
    );

    await clientB.setMode(SampleSyncMode.polling);
    final beforePolling = (await clientB.listPeople()).length;
    await clientA.addRandomPerson();
    await clientA.manualSync();
    await _waitUntil(
      () async => (await clientB.listPeople()).length > beforePolling,
      timeout: const Duration(seconds: 25),
      description: 'Polling delivery to the second Flutter client',
    );

    await clientA.close();
    final restoredA = SampleSyncController(
      repository: SampleSyncRepository.file('${temp.path}/a.db'),
      preferences: preferencesA,
      baseUrl: _baseUrl,
    );
    await restoredA.initialize();
    expect(restoredA.signedIn, isTrue, reason: restoredA.errorMessage);

    if (_runTokenRefresh) {
      await Future<void>.delayed(const Duration(seconds: 190));
      await restoredA.manualSync();
      expect(restoredA.errorMessage, isNull);
    }

    await restoredA.signOut();
    expect(restoredA.signedIn, isFalse, reason: restoredA.errorMessage);
    await clientB.signOut();
    expect(clientB.signedIn, isFalse, reason: clientB.errorMessage);
    await restoredA.close();
    await clientB.close();
  });
}

Future<void> _waitUntil(
  Future<bool> Function() condition, {
  required String description,
  Duration timeout = const Duration(seconds: 15),
}) async {
  final deadline = DateTime.now().add(timeout);
  while (!await condition()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('Timed out waiting for $description.');
    }
    await Future<void>.delayed(const Duration(milliseconds: 200));
  }
}
