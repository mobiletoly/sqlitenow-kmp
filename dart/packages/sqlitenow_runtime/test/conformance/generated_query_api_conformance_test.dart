import 'dart:async';
import 'dart:typed_data';

import 'package:test/test.dart';

import '../generated/dart_db.dart';
import 'test_support.dart';

void main() {
  group('generated query API conformance', () {
    test(
      'covers insert, update, delete, returning, and select runner shapes',
      () async {
        final database = await openInMemoryDartDb();
        addTearDown(database.close);

        final people = StreamIterator(database.person.selectAll().watch());
        addTearDown(people.cancel);
        expect(await nextEmission(people), isEmpty);

        await database.person.insertOne(
          PersonInsertOneParams(
            id: 1,
            name: 'Ada',
            status: 'ACTIVE',
            score: 98.5,
            avatar: Uint8List.fromList([1, 2, 3]),
          ),
        );
        expect((await nextEmission(people)).map((row) => row.name), ['Ada']);

        final returned = await database.person.insertReturningOne(
          const PersonInsertReturningParams(
            id: 2,
            name: 'Bob',
            status: 'PAUSED',
            score: null,
            avatar: null,
          ),
        );
        expect(returned.id, 2);
        expect(returned.status, 'PAUSED');
        expect((await nextEmission(people)).map((row) => row.name), [
          'Ada',
          'Bob',
        ]);

        await database.person.updateName(
          const PersonUpdateNameParams(id: 1, name: 'Ada Lovelace'),
        );
        expect((await nextEmission(people)).first.name, 'Ada Lovelace');

        final byId = database.person.selectById(
          const PersonSelectByIdParams(id: 1),
        );
        expect((await byId.asOne()).name, 'Ada Lovelace');
        expect((await byId.asOneOrNull())?.name, 'Ada Lovelace');
        expect(
          await database.person
              .selectById(const PersonSelectByIdParams(id: 999))
              .asOneOrNull(),
          isNull,
        );
        await expectLater(
          database.person
              .selectById(const PersonSelectByIdParams(id: 999))
              .asOne(),
          throwsStateError,
        );

        await database.person.deleteById(const PersonDeleteByIdParams(id: 2));
        expect((await nextEmission(people)).map((row) => row.name), [
          'Ada Lovelace',
        ]);
        expect((await database.person.selectAll().asList()).single.id, 1);
      },
    );
  });
}
