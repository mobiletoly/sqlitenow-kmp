import 'package:flutter/material.dart';

import 'todo_repository.dart';
import 'todo_screen.dart';

class TodoApp extends StatelessWidget {
  const TodoApp({super.key, TodoRepository? repository})
    : _repository = repository;

  final TodoRepository? _repository;

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'SQLiteNow Todo',
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: const Color(0xFF276EF1)),
        splashFactory: NoSplash.splashFactory,
        useMaterial3: true,
      ),
      home: TodoScreen(repository: _repository ?? TodoRepository.persistent()),
    );
  }
}
