import 'dart:async';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:test/test.dart';

import '../generated/dart_db.dart';
import 'test_support.dart';

void main() {
  group('runtime coordination conformance', () {
    test(
      'generated writes inside nested transaction roll back together',
      () async {
        final database = await openInMemoryDartDb();
        addTearDown(database.close);

        await expectLater(
          database.transaction(() async {
            await insertPerson(database, id: 1, name: 'Outer');
            await database.transaction(() async {
              await insertPerson(database, id: 2, name: 'Inner');
              throw StateError('boom');
            }, mode: TransactionMode.exclusive);
          }, mode: TransactionMode.immediate),
          throwsStateError,
        );

        expect(await database.person.selectAll().asList(), isEmpty);
      },
    );

    test(
      'generated watcher refresh waits behind a suspended transaction',
      () async {
        final database = await openInMemoryDartDb();
        addTearDown(database.close);

        final emissions = <List<PersonRow>>[];
        final subscription = database.person.selectAll().watch().listen(
          emissions.add,
        );
        addTearDown(subscription.cancel);
        await waitForEmissions(emissions, 1);
        expect(emissions.single, isEmpty);

        final transactionStarted = Completer<void>();
        final releaseTransaction = Completer<void>();
        final transaction = database.transaction(() async {
          await insertPerson(database, id: 1, name: 'Ada');
          transactionStarted.complete();
          await releaseTransaction.future;
        }, mode: TransactionMode.immediate);

        await transactionStarted.future;
        await Future<void>.delayed(const Duration(milliseconds: 100));
        expect(emissions, hasLength(1));

        releaseTransaction.complete();
        await transaction;
        await waitForEmissions(emissions, 2);
        expect(emissions.last.map((row) => row.name), ['Ada']);
      },
    );
  });
}
