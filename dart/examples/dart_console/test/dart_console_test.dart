import 'package:sqlitenow_console_example/src/db/generated/app_database.dart';
import 'package:test/test.dart';

void main() {
  test('generated console database runs basic workflow', () async {
    final database = AppDatabase.inMemory();
    addTearDown(database.close);

    await database.open();
    await database.task.insertOne(
      const TaskInsertOneParams(id: 1, title: 'First task', completed: 0),
    );
    await database.task.insertOne(
      const TaskInsertOneParams(id: 2, title: 'Second task', completed: 0),
    );

    expect(await database.task.selectAll().asList(), hasLength(2));

    await database.task.completeById(const TaskCompleteByIdParams(id: 2));
    final completed = await database.task.selectAll().asList();

    expect(completed.first.completed, 0);
    expect(completed.last.completed, 1);
  });
}
