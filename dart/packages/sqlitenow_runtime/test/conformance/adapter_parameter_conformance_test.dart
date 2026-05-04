import 'dart:typed_data';

import 'package:test/test.dart';

import '../generated/dart_db.dart';
import 'test_support.dart';

void main() {
  group('adapter and parameter conformance', () {
    test(
      'round-trips adapter-backed generated parameters and result values',
      () async {
        final database = await openInMemoryDartDb();
        addTearDown(database.close);

        await insertPerson(database, id: 1, name: 'Ada', status: 'ACTIVE');
        await insertProfile(
          database,
          id: 100,
          personId: 1,
          displayName: 'Countess',
          createdAt: DateTime.utc(2026, 5, 5, 12, 30),
          visitCount: 7,
          confidence: 0.75,
          metadataJson: '{"focus":"analysis"}',
          payload: Uint8List.fromList([9, 8, 7]),
        );

        await insertPerson(
          database,
          id: 2,
          name: 'No Optional Profile',
          status: 'PAUSED',
        );
        await insertProfile(
          database,
          id: 101,
          personId: 2,
          displayName: 'Minimal',
          confidence: null,
          metadataJson: null,
        );

        final profiles = await database.profile.selectAll().asList();
        expect(profiles.map((row) => row.displayName), ['Countess', 'Minimal']);
        expect(profiles.first.createdAt, DateTime.utc(2026, 5, 5, 12, 30));
        expect(profiles.first.visitCount, 7);
        expect(profiles.first.confidence, 0.75);
        expect(profiles.first.metadataJson, '{"focus":"analysis"}');
        expect(profiles.first.payload, Uint8List.fromList([9, 8, 7]));
        expect(profiles.last.confidence, isNull);
        expect(profiles.last.metadataJson, isNull);
      },
    );

    test(
      'covers repeated, collection, adapter collection, and special-character params',
      () async {
        final database = await openInMemoryDartDb();
        addTearDown(database.close);

        await insertPerson(database, id: 1, name: 'Ada', status: 'ACTIVE');
        await insertPerson(database, id: 2, name: 'Bob', status: 'PAUSED');
        await insertPerson(
          database,
          id: 3,
          name: 'Special\'Chars "Quoted" / Slash',
          status: 'ACTIVE',
        );

        expect(
          (await database.person
                  .selectRepeatedId(const PersonSelectRepeatedIdParams(id: 3))
                  .asOne())
              .name,
          'Special\'Chars "Quoted" / Slash',
        );

        expect(
          (await database.person
                  .selectByNames(
                    const PersonSelectByNamesParams(
                      names: ['Ada', 'Special\'Chars "Quoted" / Slash'],
                    ),
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
          ['Ada', 'Special\'Chars "Quoted" / Slash'],
        );
      },
    );
  });
}
