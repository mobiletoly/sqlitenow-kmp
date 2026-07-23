import 'dart:io';
import 'dart:math';

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:sqlitenow_flutter_samplesync/src/app.dart';
import 'package:sqlitenow_flutter_samplesync/src/sample_repository.dart';
import 'package:sqlitenow_flutter_samplesync/src/sample_sync_controller.dart';
import 'package:sqlitenow_flutter_samplesync/src/session_preferences.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets('device database persists all local SampleSync actions', (
    tester,
  ) async {
    final temp = await Directory.systemTemp.createTemp(
      'sqlitenow-flutter-samplesync-',
    );
    addTearDown(() => temp.delete(recursive: true));
    final databasePath = '${temp.path}/samplesync.db';
    final preferences = MemorySampleSessionPreferences();

    final first = SampleSyncController(
      repository: SampleSyncRepository.file(databasePath, random: Random(11)),
      preferences: preferences,
    );
    await tester.pumpWidget(SampleSyncApp(controller: first));
    await _pumpUntilFound(
      tester,
      find.byKey(const ValueKey('samplesync-sign-in-dialog')),
    );
    await tester.tap(find.byKey(const ValueKey('samplesync-skip-sign-in')));
    await _pumpUntilFound(tester, find.text('Local-only mode'));
    await tester.tap(find.byKey(const ValueKey('samplesync-add-person')));
    await _pumpUntilFound(tester, find.text('Rnd'));

    await tester.tap(find.text('Rnd'));
    await tester.pump(const Duration(milliseconds: 100));
    await tester.tap(find.text('Addr'));
    await tester.pump(const Duration(milliseconds: 100));
    await tester.tap(find.text('Cmnt'));
    await _pumpUntilFound(tester, find.textContaining('• '));

    final person = (await first.listPeople()).single;
    expect(await first.listAddresses(person.id), hasLength(1));
    await tester.pumpWidget(const SizedBox.shrink());
    await first.close();

    final reopened = SampleSyncController(
      repository: SampleSyncRepository.file(databasePath),
      preferences: preferences,
    );
    await tester.pumpWidget(SampleSyncApp(controller: reopened));
    await _pumpUntilFound(
      tester,
      find.byKey(const ValueKey('samplesync-sign-in-dialog')),
    );
    await tester.tap(find.byKey(const ValueKey('samplesync-skip-sign-in')));
    await _pumpUntilFound(tester, find.text('Rnd'));

    expect(await reopened.listPeople(), hasLength(1));
    expect(await reopened.listAddresses(person.id), hasLength(1));
    await tester.ensureVisible(find.text('Del'));
    await tester.pump(const Duration(milliseconds: 100));
    await tester.tap(find.text('Del'));
    await _pumpUntilFound(
      tester,
      find.byKey(const ValueKey('samplesync-empty')),
    );
    expect(await reopened.listPeople(), isEmpty);

    await tester.pumpWidget(const SizedBox.shrink());
    await reopened.close();
  });
}

Future<void> _pumpUntilFound(
  WidgetTester tester,
  Finder finder, {
  Duration timeout = const Duration(seconds: 10),
}) async {
  final deadline = tester.binding.clock.fromNowBy(timeout);
  while (finder.evaluate().isEmpty) {
    if (tester.binding.clock.now().isAfter(deadline)) {
      fail('Timed out waiting for $finder.');
    }
    await tester.pump(const Duration(milliseconds: 50));
  }
}
