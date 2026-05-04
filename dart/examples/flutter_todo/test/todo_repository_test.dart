import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:sqlitenow_flutter_todo/src/todo_models.dart';
import 'package:sqlitenow_flutter_todo/src/todo_repository.dart';

void main() {
  test('repository validates generated database workflow', () async {
    final repository = TodoRepository.inMemory();
    addTearDown(repository.close);

    await repository.open();
    expect(await repository.listTodos(), isEmpty);

    final emissions = <List<TodoItem>>[];
    final subscription = repository.watchTodos().listen(emissions.add);
    addTearDown(subscription.cancel);
    await Future<void>.delayed(Duration.zero);

    await repository.addTodo('First', TodoPriority.high);
    await repository.addTodo('Second', TodoPriority.low);
    await _pumpEvents();

    var todos = await repository.listTodos();
    expect(todos.map((todo) => todo.title), ['First', 'Second']);
    expect(todos.first.priority, TodoPriority.high);
    expect(todos.first.completed, isFalse);

    await repository.completeTodo(todos.first.id);
    await _pumpEvents();

    todos = await repository.listTodos();
    expect(todos.first.completed, isTrue);
    expect(emissions.last.map((todo) => todo.title), ['First', 'Second']);
  });

  test('repository reopens file-backed database', () async {
    final tempDir = await Directory.systemTemp.createTemp(
      'sqlitenow-flutter-repository-',
    );
    addTearDown(() => tempDir.delete(recursive: true));
    final path = '${tempDir.path}/todo.db';

    final first = TodoRepository.file(path);
    await first.open();
    await first.addTodo('Persisted', TodoPriority.normal);
    await first.close();

    final second = TodoRepository.file(path);
    addTearDown(second.close);
    await second.open();

    final todos = await second.listTodos();
    expect(todos.single.title, 'Persisted');
  });
}

Future<void> _pumpEvents() async {
  await Future<void>.delayed(const Duration(milliseconds: 20));
}
