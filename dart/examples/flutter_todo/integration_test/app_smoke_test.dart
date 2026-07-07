import 'dart:io';

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:sqlitenow_flutter_todo/src/app.dart';
import 'package:sqlitenow_flutter_todo/src/todo_repository.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets('opens sqlite, adds a todo, and observes completion', (
    tester,
  ) async {
    final tempDir = await Directory.systemTemp.createTemp(
      'sqlitenow-flutter-integration-',
    );
    addTearDown(() => tempDir.delete(recursive: true));
    final databaseFile = File('${tempDir.path}/todo.db');

    await tester.pumpWidget(
      TodoApp(repository: TodoRepository.file(databaseFile.path)),
    );
    await _pumpUntilFound(
      tester,
      find.byKey(const ValueKey('todo-title-field')),
      description: 'todo input field',
    );

    await tester.enterText(
      find.byKey(const ValueKey('todo-title-field')),
      'Device smoke todo',
    );
    await tester.tap(find.byKey(const ValueKey('todo-add-button')));
    await _pumpUntilFound(
      tester,
      find.text('Device smoke todo'),
      description: 'created todo row',
    );

    expect(find.text('Device smoke todo'), findsOneWidget);

    await tester.tap(find.byType(Checkbox).first);
    await _pumpUntil(tester, () {
      final checkboxFinder = find.byType(Checkbox);
      if (checkboxFinder.evaluate().isEmpty) return false;
      return tester.widget<Checkbox>(checkboxFinder.first).value == true;
    }, description: 'todo checkbox to become checked');

    final checkbox = tester.widget<Checkbox>(find.byType(Checkbox).first);
    expect(checkbox.value, isTrue);
  });
}

Future<void> _pumpUntilFound(
  WidgetTester tester,
  Finder finder, {
  required String description,
}) {
  return _pumpUntil(
    tester,
    () => finder.evaluate().isNotEmpty,
    description: description,
  );
}

Future<void> _pumpUntil(
  WidgetTester tester,
  bool Function() condition, {
  required String description,
  Duration timeout = const Duration(seconds: 10),
  Duration step = const Duration(milliseconds: 50),
}) async {
  final end = tester.binding.clock.fromNowBy(timeout);
  while (!condition()) {
    if (tester.binding.clock.now().isAfter(end)) {
      fail('Timed out waiting for $description.');
    }
    await tester.pump(step);
  }
}
