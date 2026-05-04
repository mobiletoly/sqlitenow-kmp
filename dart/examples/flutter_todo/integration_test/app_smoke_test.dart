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
    await tester.pumpAndSettle();

    await tester.enterText(
      find.byKey(const ValueKey('todo-title-field')),
      'Device smoke todo',
    );
    await tester.tap(find.byKey(const ValueKey('todo-add-button')));
    await tester.pumpAndSettle();

    expect(find.text('Device smoke todo'), findsOneWidget);

    await tester.tap(find.byType(Checkbox).first);
    await tester.pumpAndSettle();

    final checkbox = tester.widget<Checkbox>(find.byType(Checkbox).first);
    expect(checkbox.value, isTrue);
  });
}
