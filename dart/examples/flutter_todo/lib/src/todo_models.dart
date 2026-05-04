enum TodoPriority {
  low,
  normal,
  high;

  String get sqlValue => name;

  static TodoPriority fromSql(String value) {
    return TodoPriority.values.firstWhere(
      (priority) => priority.name == value,
      orElse: () => TodoPriority.normal,
    );
  }
}

final class TodoItem {
  const TodoItem({
    required this.id,
    required this.title,
    required this.priority,
    required this.completed,
  });

  final int id;
  final String title;
  final TodoPriority priority;
  final bool completed;
}
