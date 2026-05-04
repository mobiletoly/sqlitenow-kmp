import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sqlitenow_flutter_todo/src/app.dart';
import 'package:sqlitenow_flutter_todo/src/todo_repository.dart';

void main() {
  testWidgets('todo screen adds and completes a todo', (tester) async {
    await tester.pumpWidget(TodoApp(repository: TodoRepository.inMemory()));
    await tester.pumpAndSettle();

    expect(find.text('No todos'), findsOneWidget);

    await tester.enterText(
      find.byKey(const ValueKey('todo-title-field')),
      'Ship Flutter example',
    );
    await tester.tap(find.byKey(const ValueKey('todo-add-button')));
    await tester.pumpAndSettle();

    expect(find.text('Ship Flutter example'), findsOneWidget);
    expect(find.text('normal'), findsOneWidget);

    await tester.tap(find.byType(Checkbox).first);
    await tester.pumpAndSettle();

    final checkbox = tester.widget<Checkbox>(find.byType(Checkbox).first);
    expect(checkbox.value, isTrue);
  });
}
