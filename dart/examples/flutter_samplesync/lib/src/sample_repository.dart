import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

import 'package:path_provider/path_provider.dart';

import 'db/generated/now_sample_sync_database.dart';
import 'sample_models.dart';

typedef SampleDatabaseFactory = Future<NowSampleSyncDatabase> Function();

final class SampleSyncRepository {
  SampleSyncRepository.persistent({Random? random, DateTime Function()? now})
    : this._(
        () async {
          final directory = await getApplicationDocumentsDirectory();
          return NowSampleSyncDatabase(
            path: '${directory.path}/sqlitenow_flutter_samplesync.db',
            adapters: sampleDatabaseAdapters,
          );
        },
        random: random,
        now: now,
      );

  SampleSyncRepository.inMemory({Random? random, DateTime Function()? now})
    : this._(
        () async =>
            NowSampleSyncDatabase.inMemory(adapters: sampleDatabaseAdapters),
        random: random,
        now: now,
      );

  SampleSyncRepository.file(
    String path, {
    Random? random,
    DateTime Function()? now,
  }) : this._(
         () async => NowSampleSyncDatabase(
           path: path,
           adapters: sampleDatabaseAdapters,
         ),
         random: random,
         now: now,
       );

  SampleSyncRepository._(
    this._databaseFactory, {
    Random? random,
    DateTime Function()? now,
  }) : _random = random ?? Random.secure(),
       _now = now ?? DateTime.now;

  static final sampleDatabaseAdapters = NowSampleSyncDatabaseAdapters(
    commentCreatedAtToSql: _timestampToSql,
    sqlValueToCommentCreatedAt: _timestampFromSql,
    personBirthDateToSql: _dateToSql,
    sqlValueToPersonBirthDate: _dateFromSql,
    personCreatedAtToSql: _timestampToSql,
    sqlValueToPersonCreatedAt: _timestampFromSql,
    personUpdatedAtToSql: _timestampToSql,
    sqlValueToPersonUpdatedAt: _timestampFromSql,
    personIsActiveToSql: _boolToSql,
    sqlValueToPersonIsActive: _boolFromSql,
    personAddressAddressTypeToSql: (value) => (value as String).toLowerCase(),
    sqlValueToPersonAddressAddressType: (value) =>
        (value as String).toLowerCase(),
    personAddressIsPrimaryToSql: _boolToSql,
    sqlValueToPersonAddressIsPrimary: _boolFromSql,
    personAddressCreatedAtToSql: _timestampToSql,
    sqlValueToPersonAddressCreatedAt: _timestampFromSql,
  );

  final SampleDatabaseFactory _databaseFactory;
  final Random _random;
  final DateTime Function() _now;

  NowSampleSyncDatabase? _database;

  NowSampleSyncDatabase get database {
    return _database ??
        (throw StateError('SampleSyncRepository.open() must be called first.'));
  }

  Future<void> open() async {
    if (_database != null) return;
    final next = await _databaseFactory();
    await next.open();
    await next.connection.execute('PRAGMA foreign_keys = ON');
    _database = next;
  }

  Stream<List<PersonRow>> watchPeople() {
    return database.person
        .selectAll(const PersonSelectAllParams(limit: -1, offset: 0))
        .watch();
  }

  Future<List<PersonRow>> listPeople() {
    return database.person
        .selectAll(const PersonSelectAllParams(limit: -1, offset: 0))
        .asList();
  }

  Stream<List<SampleComment>> watchComments(Uint8List personId) {
    return database.comment
        .selectAll(CommentSelectAllParams(personId: personId))
        .watch()
        .map((rows) => [for (final row in rows) SampleComment.fromRow(row)]);
  }

  Future<List<PersonAddressRow>> listAddresses(Uint8List personId) {
    return database.personAddress
        .selectAll(PersonAddressSelectAllParams(personId: personId))
        .asList();
  }

  Future<void> addRandomPerson() async {
    final firstName = _firstNames[_random.nextInt(_firstNames.length)];
    final lastName = _lastNames[_random.nextInt(_lastNames.length)];
    final suffix = _random.nextInt(0x7fffffff);
    final domain = _domains[_random.nextInt(_domains.length)];
    await database.person.add(
      PersonAddParams(
        email:
            '${firstName.toLowerCase()}.${lastName.toLowerCase()}.$suffix@$domain',
        firstName: firstName,
        lastName: lastName,
        phone: '+1-555-${1000 + _random.nextInt(9000)}',
        birthDate: DateTime(
          1960 + _random.nextInt(45),
          1 + _random.nextInt(12),
          1 + _random.nextInt(28),
        ),
        ssn: 100000000 + _random.nextInt(899999999),
        score: _random.nextDouble() * 100,
        isActive: true,
        notes: '',
      ),
    );
  }

  Future<void> randomizePerson(PersonRow person) {
    return database.person.updateById(
      PersonUpdateByIdParams(
        firstName: _firstNames[_random.nextInt(_firstNames.length)],
        lastName: _lastNames[_random.nextInt(_lastNames.length)],
        email: person.email,
        phone: person.phone,
        birthDate: person.birthDate,
        ssn: person.ssn,
        score: person.score,
        isActive: person.isActive,
        notes: person.notes,
        id: person.id,
      ),
    );
  }

  Future<void> deletePerson(Uint8List personId) {
    return database.person.deleteById(PersonDeleteByIdParams(id: personId));
  }

  Future<void> addRandomAddress(Uint8List personId) {
    const streets = ['Main St', 'Oak Ave', 'Pine Rd', 'Maple Blvd'];
    const cities = ['Springfield', 'Riverdale', 'Fairview', 'Greenville'];
    const states = ['CA', 'NY', 'TX', 'WA'];
    final type = _random.nextBool() ? AddressType.home : AddressType.work;
    return database.personAddress.add(
      PersonAddressAddParams(
        personId: personId,
        addressType: type.sqlValue,
        street:
            '${10 + _random.nextInt(9989)} ${streets[_random.nextInt(streets.length)]}',
        city: cities[_random.nextInt(cities.length)],
        state: states[_random.nextInt(states.length)],
        postalCode: '${10000 + _random.nextInt(89999)}',
        country: 'US',
        isPrimary: _random.nextBool(),
      ),
    );
  }

  Future<void> addRandomComment(Uint8List personId) {
    const comments = [
      'Great person!',
      'Met at the event.',
      'Loves Flutter',
      'Follows up quickly',
    ];
    return database.comment.add(
      CommentAddParams(
        id: _uuidV4(_random),
        personId: personId,
        comment: comments[_random.nextInt(comments.length)],
        createdAt: _now().toUtc(),
        tags: jsonEncode(const <String>[]),
      ),
    );
  }

  Future<void> close() async {
    final current = _database;
    _database = null;
    await current?.close();
  }
}

Object? _dateToSql(Object? value) {
  if (value == null) return null;
  final date = value as DateTime;
  return '${date.year.toString().padLeft(4, '0')}-'
      '${date.month.toString().padLeft(2, '0')}-'
      '${date.day.toString().padLeft(2, '0')}';
}

Object? _dateFromSql(Object? value) {
  if (value == null) return null;
  return DateTime.parse('${value as String}T00:00:00.000Z');
}

Object? _timestampToSql(Object? value) {
  if (value == null) return null;
  final utc = (value as DateTime).toUtc();
  return '${utc.year.toString().padLeft(4, '0')}-'
      '${utc.month.toString().padLeft(2, '0')}-'
      '${utc.day.toString().padLeft(2, '0')}T'
      '${utc.hour.toString().padLeft(2, '0')}:'
      '${utc.minute.toString().padLeft(2, '0')}:'
      '${utc.second.toString().padLeft(2, '0')}Z';
}

Object? _timestampFromSql(Object? value) {
  if (value == null) return null;
  return DateTime.parse(value as String).toUtc();
}

Object? _boolToSql(Object? value) => (value as bool) ? 1 : 0;

Object? _boolFromSql(Object? value) {
  return switch (value) {
    0 => false,
    1 => true,
    _ => throw FormatException('Expected SQLite boolean 0 or 1, got $value'),
  };
}

String _uuidV4(Random random) {
  final bytes = Uint8List.fromList([
    for (var index = 0; index < 16; index++) random.nextInt(256),
  ]);
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  final hex = bytes
      .map((value) => value.toRadixString(16).padLeft(2, '0'))
      .join();
  return '${hex.substring(0, 8)}-'
      '${hex.substring(8, 12)}-'
      '${hex.substring(12, 16)}-'
      '${hex.substring(16, 20)}-'
      '${hex.substring(20)}';
}

const _firstNames = [
  'John',
  'Jane',
  'Alice',
  'Bob',
  'Charlie',
  'Diana',
  'Eve',
  'Frank',
  'Grace',
];

const _lastNames = [
  'Smith',
  'Johnson',
  'Williams',
  'Brown',
  'Jones',
  'Garcia',
  'Miller',
  'Davis',
];

const _domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'example.com'];
