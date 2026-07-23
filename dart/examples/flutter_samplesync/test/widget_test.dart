import 'dart:math';

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sqlitenow_flutter_samplesync/src/app.dart';
import 'package:sqlitenow_flutter_samplesync/src/sample_repository.dart';
import 'package:sqlitenow_flutter_samplesync/src/sample_sync_controller.dart';
import 'package:sqlitenow_flutter_samplesync/src/session_preferences.dart';

void main() {
  testWidgets('full-height local UI supports parity actions after Skip', (
    tester,
  ) async {
    final controller = SampleSyncController(
      repository: SampleSyncRepository.inMemory(random: Random(3)),
      preferences: MemorySampleSessionPreferences(),
    );
    addTearDown(controller.close);

    await tester.pumpWidget(SampleSyncApp(controller: controller));
    await _pumpUntilFound(
      tester,
      find.byKey(const ValueKey('samplesync-sign-in-dialog')),
    );

    expect(
      find.byKey(const ValueKey('samplesync-sign-in-dialog')),
      findsOneWidget,
    );
    expect(
      tester.getSize(find.byKey(const ValueKey('samplesync-scaffold'))).height,
      600,
    );

    await tester.tap(find.byKey(const ValueKey('samplesync-skip-sign-in')));
    await _pumpUntilFound(tester, find.text('Local-only mode'));
    expect(find.text('Local-only mode'), findsOneWidget);

    await tester.tap(find.byKey(const ValueKey('samplesync-add-person')));
    await _pumpUntilFound(tester, find.text('Rnd'));
    expect(find.byKey(const ValueKey('samplesync-empty')), findsNothing);
    expect(find.text('Rnd'), findsOneWidget);
    expect(find.text('Addr'), findsOneWidget);
    expect(find.text('Cmnt'), findsOneWidget);
    expect(find.text('Del'), findsOneWidget);

    await tester.tap(find.text('Cmnt'));
    await _pumpUntilFound(tester, find.textContaining('• '));
    expect(find.textContaining('• '), findsOneWidget);

    await tester.pumpWidget(const SizedBox.shrink());
    await tester.pump();
    await controller.close();
  });
}

Future<void> _pumpUntilFound(WidgetTester tester, Finder finder) async {
  final deadline = tester.binding.clock.fromNowBy(const Duration(seconds: 10));
  while (finder.evaluate().isEmpty) {
    if (tester.binding.clock.now().isAfter(deadline)) {
      fail('Timed out waiting for $finder.');
    }
    await tester.pump(const Duration(milliseconds: 50));
  }
}
