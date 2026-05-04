import 'dart:async';

import 'package:path_provider/path_provider.dart';

import 'db/generated/todo_database.dart';
import 'todo_models.dart';

class TodoRepository {
  TodoRepository.persistent()
    : _databaseFactory = (() async {
        final dir = await getApplicationDocumentsDirectory();
        return TodoDatabase(
          path: '${dir.path}/sqlitenow_flutter_todo.db',
          adapters: _adapters,
        );
      });

  TodoRepository.inMemory()
    : _databaseFactory = (() async {
        return TodoDatabase.inMemory(adapters: _adapters);
      });

  TodoRepository.file(String path)
    : _databaseFactory = (() async {
        return TodoDatabase(path: path, adapters: _adapters);
      });

  static final _adapters = TodoDatabaseAdapters(
    taskPriorityToSql: (value) => (value as String).toLowerCase(),
    sqlValueToTaskPriority: (value) => (value as String).toUpperCase(),
  );

  final Future<TodoDatabase> Function() _databaseFactory;

  TodoDatabase? _database;
  var _nextId = 1;

  Future<void> open() async {
    if (_database != null) return;
    final database = await _databaseFactory();
    await database.open();
    _database = database;
    final rows = await database.task.selectAll().asList();
    if (rows.isNotEmpty) {
      _nextId = rows.map((row) => row.id).reduce((a, b) => a > b ? a : b) + 1;
    }
  }

  Stream<List<TodoItem>> watchTodos() {
    final database = _requireDatabase();
    return database.task.selectAll().watch().map(_mapRows);
  }

  Future<List<TodoItem>> listTodos() async {
    return _mapRows(await _requireDatabase().task.selectAll().asList());
  }

  Future<void> addTodo(String title, TodoPriority priority) async {
    final trimmed = title.trim();
    if (trimmed.isEmpty) return;
    final database = _requireDatabase();
    final id = _nextId++;
    await database.task.insertOne(
      TaskInsertOneParams(
        id: id,
        title: trimmed,
        priority: priority.sqlValue.toUpperCase(),
        completed: 0,
      ),
    );
  }

  Future<void> completeTodo(int id) async {
    await _requireDatabase().transaction(() async {
      await _requireDatabase().task.completeById(
        TaskCompleteByIdParams(id: id),
      );
    });
  }

  Future<void> close() async {
    final database = _database;
    _database = null;
    await database?.close();
  }

  TodoDatabase _requireDatabase() {
    final database = _database;
    if (database == null) {
      throw StateError('TodoRepository.open() must be called first.');
    }
    return database;
  }

  List<TodoItem> _mapRows(List<TaskRow> rows) {
    return [
      for (final row in rows)
        TodoItem(
          id: row.id,
          title: row.title,
          priority: TodoPriority.fromSql(row.priority.toLowerCase()),
          completed: row.completed != 0,
        ),
    ];
  }
}
