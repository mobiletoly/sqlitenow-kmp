import 'dart:math';

import 'package:flutter_test/flutter_test.dart';
import 'package:sqlitenow_flutter_samplesync/src/sample_repository.dart';

void main() {
  test('local CRUD stays reactive and cascades child rows', () async {
    final repository = SampleSyncRepository.inMemory(
      random: Random(7),
      now: () => DateTime.utc(2026, 7, 28, 12),
    );
    addTearDown(repository.close);
    await repository.open();

    final populated = repository.watchPeople().firstWhere(
      (people) => people.isNotEmpty,
    );
    await repository.addRandomPerson();
    final people = await populated;
    expect(people, hasLength(1));
    final person = people.single;

    await repository.randomizePerson(person);
    final updated = (await repository.listPeople()).single;
    expect(
      '${updated.myFirstName} ${updated.myLastName}',
      isNot('${person.myFirstName} ${person.myLastName}'),
    );

    await repository.addRandomAddress(person.id);
    expect(await repository.listAddresses(person.id), hasLength(1));

    final commentAdded = repository
        .watchComments(person.id)
        .firstWhere((comments) => comments.isNotEmpty);
    await repository.addRandomComment(person.id);
    final comments = await commentAdded;
    expect(comments.single.tags, isEmpty);

    await repository.deletePerson(person.id);
    expect(await repository.listPeople(), isEmpty);
    expect(await repository.listAddresses(person.id), isEmpty);
    expect(await repository.watchComments(person.id).first, isEmpty);
  });

  test(
    'database adapters round-trip dates, timestamps, booleans, and type text',
    () {
      final adapters = SampleSyncRepository.sampleDatabaseAdapters;
      final date = DateTime.utc(1994, 3, 8);
      final timestamp = DateTime.utc(2026, 7, 28, 12, 34, 56, 789);

      expect(adapters.personBirthDateToSql(date), '1994-03-08');
      expect(
        adapters.sqlValueToPersonBirthDate('1994-03-08'),
        DateTime.utc(1994, 3, 8),
      );
      expect(adapters.personUpdatedAtToSql(timestamp), '2026-07-28T12:34:56Z');
      expect(
        adapters.sqlValueToPersonUpdatedAt('2026-07-28T12:34:56Z'),
        DateTime.utc(2026, 7, 28, 12, 34, 56),
      );
      expect(adapters.personIsActiveToSql(true), 1);
      expect(adapters.sqlValueToPersonIsActive(0), false);
      expect(adapters.personAddressAddressTypeToSql('HOME'), 'home');
    },
  );
}
