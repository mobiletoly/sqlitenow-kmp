import 'dart:async';
import 'dart:typed_data';

import 'package:test/test.dart';

import '../generated/dart_db.dart';
import 'test_support.dart';

void main() {
  group('dynamic mapping and invalidation conformance', () {
    test(
      'hydrates per-row, empty collection, collection, and nested-view mappings',
      () async {
        final database = await openInMemoryDartDb();
        addTearDown(database.close);

        await insertPerson(database, id: 1, name: 'Ada');
        await insertPerson(database, id: 2, name: 'Cy');

        final noProfile = await database.person
            .selectWithProfile(const PersonSelectWithProfileParams(id: 2))
            .asOne();
        expect(noProfile.profile, isNull);

        await insertProfile(
          database,
          id: 100,
          personId: 1,
          displayName: 'Countess',
          payload: Uint8List.fromList([4, 5, 6]),
        );
        final withProfile = await database.person
            .selectWithProfile(const PersonSelectWithProfileParams(id: 1))
            .asOne();
        expect(withProfile.profile?.displayName, 'Countess');
        expect(withProfile.profile?.payload, Uint8List.fromList([4, 5, 6]));

        await insertNote(database, id: 10, personId: 1, body: 'First note');
        await insertNote(database, id: 11, personId: 1, body: 'Second note');
        await insertAttachment(database, id: 1000, noteId: 10, label: 'image');
        await insertAttachment(database, id: 1001, noteId: 10, label: 'pdf');

        final withNotes = await database.person.selectWithNotes().asList();
        expect(withNotes.first.notes.map((row) => row.body), [
          'First note',
          'Second note',
        ]);
        expect(
          withNotes.where((row) => row.name == 'Cy').single.notes,
          isEmpty,
        );

        final detailed = await database.person
            .selectWithDetailedNotes()
            .asList();
        expect(detailed.first.notes.map((row) => row.id), [10, 11]);
        expect(detailed.first.notes.map((row) => row.body), [
          'First note',
          'Second note',
        ]);
      },
    );

    test(
      'refreshes generated watchers for views, cascades, and external reports',
      () async {
        final database = await openInMemoryDartDb();
        addTearDown(database.close);

        final activePeople = StreamIterator(
          database.person.selectActive().watch(),
        );
        final notes = StreamIterator(database.note.selectAll().watch());
        final allPeopleEmissions = <List<PersonRow>>[];
        final allPeopleSubscription = database.person
            .selectAll()
            .watch()
            .listen(allPeopleEmissions.add);
        addTearDown(activePeople.cancel);
        addTearDown(notes.cancel);
        addTearDown(allPeopleSubscription.cancel);

        expect(await nextEmission(activePeople), isEmpty);
        expect(await nextEmission(notes), isEmpty);
        await waitForEmissions(allPeopleEmissions, 1);
        expect(allPeopleEmissions.single, isEmpty);

        await insertPerson(database, id: 1, name: 'Ada', status: 'ACTIVE');
        expect((await nextEmission(activePeople)).map((row) => row.name), [
          'Ada',
        ]);
        await waitForEmissions(allPeopleEmissions, 2);
        expect(allPeopleEmissions.last.map((row) => row.name), ['Ada']);

        await insertNote(database, id: 10, personId: 1, body: 'First note');
        expect((await nextEmission(notes)).single.body, 'First note');

        database.reportExternalTableChanges({});
        await Future<void>.delayed(const Duration(milliseconds: 100));
        expect(allPeopleEmissions, hasLength(2));

        await database.connection.execute(
          "INSERT INTO person(id, name, status) VALUES (2, 'Raw', 'active')",
        );
        database.reportExternalTableChanges({'PERSON'});
        await waitForEmissions(allPeopleEmissions, 3);
        expect(allPeopleEmissions.last.map((row) => row.name), ['Ada', 'Raw']);
        expect((await nextEmission(activePeople)).map((row) => row.name), [
          'Ada',
          'Raw',
        ]);

        await database.person.deleteById(const PersonDeleteByIdParams(id: 1));
        expect(await nextEmission(notes), isEmpty);
      },
    );
  });
}
