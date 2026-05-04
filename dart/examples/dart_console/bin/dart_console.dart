import 'package:sqlitenow_console_example/src/db/generated/app_database.dart';

Future<void> main() async {
  final database = AppDatabase.inMemory();
  try {
    await database.open();
    await database.task.insertOne(
      const TaskInsertOneParams(
        id: 1,
        title: 'Try SQLiteNow Dart',
        completed: 0,
      ),
    );
    final tasks = await database.task.selectAll().asList();
    for (final task in tasks) {
      print('${task.id}: ${task.title}');
    }
  } finally {
    await database.close();
  }
}
