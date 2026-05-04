import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:test/test.dart';

import 'generated/dart_db.dart';

void main() {
  test('generated Dart vertical slice runs end to end', () async {
    final database = DartDb.inMemory(adapters: _adapters);
    addTearDown(database.close);

    await database.open();
    expect(database.isOpen, isTrue);

    final allPeople = StreamIterator(database.person.selectAll().watch());
    final activePeople = StreamIterator(database.person.selectActive().watch());
    final notes = StreamIterator(database.note.selectAll().watch());
    addTearDown(allPeople.cancel);
    addTearDown(activePeople.cancel);
    addTearDown(notes.cancel);
    expect(await _next(allPeople), isEmpty);
    expect(await _next(activePeople), isEmpty);
    expect(await _next(notes), isEmpty);

    await database.person.insertOne(
      PersonInsertOneParams(
        id: 1,
        name: 'Ada',
        status: 'ACTIVE',
        score: 98.5,
        avatar: Uint8List.fromList([1, 2, 3]),
      ),
    );
    expect((await _next(allPeople)).single.status, 'ACTIVE');
    expect((await _next(activePeople)).single.name, 'Ada');

    final returned = await database.person.insertReturningOne(
      const PersonInsertReturningParams(
        id: 2,
        name: 'Bob',
        status: 'PAUSED',
        score: null,
        avatar: null,
      ),
    );
    expect(returned.status, 'PAUSED');
    expect((await _next(allPeople)).map((row) => row.name), ['Ada', 'Bob']);
    expect((await _next(activePeople)).map((row) => row.name), ['Ada']);

    final selected = await database.person
        .selectById(const PersonSelectByIdParams(id: 1))
        .asOne();
    expect(selected.name, 'Ada');
    expect(selected.status, 'ACTIVE');
    expect(selected.avatar, Uint8List.fromList([1, 2, 3]));

    await database.person.updateName(
      const PersonUpdateNameParams(name: 'Ada Lovelace', id: 1),
    );
    expect((await _next(allPeople)).first.name, 'Ada Lovelace');
    expect((await _next(activePeople)).single.name, 'Ada Lovelace');

    await database.person.insertOne(
      const PersonInsertOneParams(
        id: 3,
        name: 'Cy',
        status: 'ACTIVE',
        score: null,
        avatar: null,
      ),
    );
    expect((await _next(allPeople)).map((row) => row.name), [
      'Ada Lovelace',
      'Bob',
      'Cy',
    ]);
    expect((await _next(activePeople)).map((row) => row.name), [
      'Ada Lovelace',
      'Cy',
    ]);

    expect(
      (await database.person
              .selectByNames(
                const PersonSelectByNamesParams(names: ['Ada Lovelace', 'Cy']),
              )
              .asList())
          .map((row) => row.id),
      [1, 3],
    );
    expect(
      (await database.person
              .selectByStatuses(
                const PersonSelectByStatusesParams(statuses: ['ACTIVE']),
              )
              .asList())
          .map((row) => row.name),
      ['Ada Lovelace', 'Cy'],
    );
    expect(
      (await database.person
              .selectRepeatedId(const PersonSelectRepeatedIdParams(id: 3))
              .asOne())
          .name,
      'Cy',
    );

    await database.note.insertOne(
      const NoteInsertOneParams(id: 10, personId: 1, body: 'First note'),
    );
    await database.profile.insertOne(
      ProfileInsertOneParams(
        id: 100,
        personId: 1,
        displayName: 'Countess',
        createdAt: DateTime.utc(2026, 5, 5, 12),
        visitCount: 7,
        confidence: 0.75,
        metadataJson: '{"focus":"analysis"}',
        payload: Uint8List.fromList([9, 8, 7]),
      ),
    );
    expect((await _next(notes)).single.body, 'First note');
    await database.note.insertOne(
      const NoteInsertOneParams(id: 11, personId: 1, body: 'Second note'),
    );
    await database.attachment.insertOne(
      const AttachmentInsertOneParams(id: 1000, noteId: 10, label: 'image'),
    );
    await database.attachment.insertOne(
      const AttachmentInsertOneParams(id: 1001, noteId: 10, label: 'pdf'),
    );
    expect((await _next(notes)).map((row) => row.body), [
      'First note',
      'Second note',
    ]);
    expect(
      (await database.attachment.selectAll().asList()).map((row) => row.label),
      ['image', 'pdf'],
    );

    final profile = await database.profile.selectAll().asOne();
    expect(profile.displayName, 'Countess');
    expect(profile.createdAt, DateTime.utc(2026, 5, 5, 12));
    expect(profile.visitCount, 7);
    expect(profile.confidence, 0.75);
    expect(profile.metadataJson, '{"focus":"analysis"}');
    expect(profile.payload, Uint8List.fromList([9, 8, 7]));

    final withProfile = await database.person
        .selectWithProfile(const PersonSelectWithProfileParams(id: 1))
        .asOne();
    expect(withProfile.profile?.displayName, 'Countess');
    expect(withProfile.profile?.payload, Uint8List.fromList([9, 8, 7]));

    final withNotes = await database.person.selectWithNotes().asList();
    expect(withNotes.first.notes.map((row) => row.body), [
      'First note',
      'Second note',
    ]);
    expect(withNotes.where((row) => row.name == 'Cy').single.notes, isEmpty);

    final withDetailedNotes = await database.person
        .selectWithDetailedNotes()
        .asList();
    expect(withDetailedNotes.first.notes.map((row) => row.body), [
      'First note',
      'Second note',
    ]);
    expect(withDetailedNotes.first.notes.map((row) => row.id), [10, 11]);

    await database.person.deleteById(const PersonDeleteByIdParams(id: 2));
    expect((await _next(allPeople)).map((row) => row.name), [
      'Ada Lovelace',
      'Cy',
    ]);
    expect((await _next(activePeople)).map((row) => row.name), [
      'Ada Lovelace',
      'Cy',
    ]);
    expect((await _next(notes)).map((row) => row.body), [
      'First note',
      'Second note',
    ]);

    await database.person.deleteById(const PersonDeleteByIdParams(id: 1));
    expect((await _next(allPeople)).map((row) => row.name), ['Cy']);
    expect((await _next(activePeople)).map((row) => row.name), ['Cy']);
    expect(await _next(notes), isEmpty);

    final remaining = await database.person.selectAll().asOne();
    expect(remaining.id, 3);
    expect(await database.connection.readUserVersion(), 2);
  });

  test(
    'generated file-backed database reopens without rerunning fresh schema',
    () async {
      final tempDir = await Directory.systemTemp.createTemp(
        'sqlitenow-generated-file-',
      );
      addTearDown(() => tempDir.delete(recursive: true));
      final path = '${tempDir.path}/generated.db';

      final first = DartDb(path: path, adapters: _adapters);
      await first.open();
      await first.person.insertOne(
        const PersonInsertOneParams(
          id: 1,
          name: 'Persisted',
          status: 'ACTIVE',
          score: null,
          avatar: null,
        ),
      );
      await first.close();

      final second = DartDb(path: path, adapters: _adapters);
      addTearDown(second.close);
      await second.open();

      expect(await second.connection.readUserVersion(), 2);
      expect((await second.person.selectAll().asOne()).name, 'Persisted');
    },
  );
}

final _adapters = DartDbAdapters(
  personStatusToSql: (value) => (value as String).toLowerCase(),
  sqlValueToPersonStatus: (value) => (value as String).toUpperCase(),
  personProfileCreatedAtToSql: (value) => (value as DateTime).toIso8601String(),
  sqlValueToPersonProfileCreatedAt: (value) => DateTime.parse(value as String),
  personProfileVisitCountToSql: (value) => value,
  sqlValueToPersonProfileVisitCount: (value) => value,
  personProfileConfidenceToSql: (value) => value,
  sqlValueToPersonProfileConfidence: (value) => value,
  personProfileMetadataJsonToSql: (value) => value,
  sqlValueToPersonProfileMetadataJson: (value) => value,
  personProfilePayloadToSql: (value) => value,
  sqlValueToPersonProfilePayload: (value) => value,
);

Future<T> _next<T>(StreamIterator<T> iterator) async {
  final hasNext = await iterator.moveNext();
  expect(hasNext, isTrue);
  return iterator.current;
}
