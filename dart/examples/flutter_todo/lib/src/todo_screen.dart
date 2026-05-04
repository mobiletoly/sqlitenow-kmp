import 'dart:async';

import 'package:flutter/material.dart';

import 'todo_models.dart';
import 'todo_repository.dart';

class TodoScreen extends StatefulWidget {
  const TodoScreen({super.key, required this.repository});

  final TodoRepository repository;

  @override
  State<TodoScreen> createState() => _TodoScreenState();
}

class _TodoScreenState extends State<TodoScreen> {
  final _controller = TextEditingController();
  TodoPriority _priority = TodoPriority.normal;
  Future<void>? _openFuture;

  @override
  void initState() {
    super.initState();
    _openFuture = widget.repository.open();
  }

  @override
  void dispose() {
    _controller.dispose();
    unawaited(widget.repository.close());
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('SQLiteNow Todo')),
      body: FutureBuilder<void>(
        future: _openFuture,
        builder: (context, snapshot) {
          if (snapshot.hasError) {
            return Center(child: Text('Open failed: ${snapshot.error}'));
          }
          if (snapshot.connectionState != ConnectionState.done) {
            return const Center(child: CircularProgressIndicator());
          }
          return Column(
            children: [
              Padding(
                padding: const EdgeInsets.all(16),
                child: Row(
                  children: [
                    Expanded(
                      child: TextField(
                        key: const ValueKey('todo-title-field'),
                        controller: _controller,
                        decoration: const InputDecoration(
                          labelText: 'Title',
                          border: OutlineInputBorder(),
                        ),
                        onSubmitted: (_) => _addTodo(),
                      ),
                    ),
                    const SizedBox(width: 12),
                    DropdownButton<TodoPriority>(
                      key: const ValueKey('todo-priority-menu'),
                      value: _priority,
                      onChanged: (value) {
                        if (value != null) setState(() => _priority = value);
                      },
                      items: const [
                        DropdownMenuItem(
                          value: TodoPriority.low,
                          child: Text('Low'),
                        ),
                        DropdownMenuItem(
                          value: TodoPriority.normal,
                          child: Text('Normal'),
                        ),
                        DropdownMenuItem(
                          value: TodoPriority.high,
                          child: Text('High'),
                        ),
                      ],
                    ),
                    const SizedBox(width: 12),
                    FilledButton(
                      key: const ValueKey('todo-add-button'),
                      onPressed: _addTodo,
                      child: const Text('Add'),
                    ),
                  ],
                ),
              ),
              Expanded(
                child: StreamBuilder<List<TodoItem>>(
                  stream: widget.repository.watchTodos(),
                  builder: (context, snapshot) {
                    final todos = snapshot.data ?? const <TodoItem>[];
                    if (todos.isEmpty) {
                      return const Center(child: Text('No todos'));
                    }
                    return ListView.separated(
                      itemCount: todos.length,
                      separatorBuilder: (_, _) => const Divider(height: 1),
                      itemBuilder: (context, index) {
                        final todo = todos[index];
                        return CheckboxListTile(
                          key: ValueKey('todo-${todo.id}'),
                          value: todo.completed,
                          onChanged: todo.completed
                              ? null
                              : (_) => widget.repository.completeTodo(todo.id),
                          title: Text(todo.title),
                          subtitle: Text(todo.priority.name),
                        );
                      },
                    );
                  },
                ),
              ),
            ],
          );
        },
      ),
    );
  }

  void _addTodo() {
    final title = _controller.text;
    _controller.clear();
    unawaited(widget.repository.addTodo(title, _priority));
  }
}
