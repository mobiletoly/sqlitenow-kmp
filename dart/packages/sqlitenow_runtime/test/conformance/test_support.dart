import 'dart:async';
import 'dart:typed_data';

import 'package:test/test.dart';

import '../generated/dart_db.dart';

final testAdapters = DartDbAdapters(
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

Future<DartDb> openInMemoryDartDb() async {
  final database = DartDb.inMemory(adapters: testAdapters);
  await database.open();
  return database;
}

Future<int> insertPerson(
  DartDb database, {
  required int id,
  required String name,
  String status = 'ACTIVE',
  double? score,
  Uint8List? avatar,
}) async {
  await database.person.insertOne(
    PersonInsertOneParams(
      id: id,
      name: name,
      status: status,
      score: score,
      avatar: avatar,
    ),
  );
  return id;
}

Future<void> insertProfile(
  DartDb database, {
  required int id,
  required int personId,
  required String displayName,
  DateTime? createdAt,
  int visitCount = 1,
  double? confidence,
  String? metadataJson,
  Uint8List? payload,
}) {
  return database.profile.insertOne(
    ProfileInsertOneParams(
      id: id,
      personId: personId,
      displayName: displayName,
      createdAt: createdAt ?? DateTime.utc(2026, 5, 5, 12),
      visitCount: visitCount,
      confidence: confidence,
      metadataJson: metadataJson,
      payload: payload ?? Uint8List.fromList([1, 2, 3]),
    ),
  );
}

Future<void> insertNote(
  DartDb database, {
  required int id,
  required int personId,
  required String body,
}) {
  return database.note.insertOne(
    NoteInsertOneParams(id: id, personId: personId, body: body),
  );
}

Future<void> insertAttachment(
  DartDb database, {
  required int id,
  required int noteId,
  required String label,
}) {
  return database.attachment.insertOne(
    AttachmentInsertOneParams(id: id, noteId: noteId, label: label),
  );
}

Future<T> nextEmission<T>(StreamIterator<T> iterator) async {
  final hasNext = await iterator.moveNext();
  expect(hasNext, isTrue);
  return iterator.current;
}

Future<T?> waitOrNull<T>(
  Future<T> future, [
  Duration timeout = const Duration(milliseconds: 100),
]) async {
  try {
    return await future.timeout(timeout);
  } on TimeoutException {
    return null;
  }
}

Future<void> waitForEmissions<T>(List<T> emissions, int count) async {
  final deadline = DateTime.now().add(const Duration(seconds: 2));
  while (emissions.length < count && DateTime.now().isBefore(deadline)) {
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  expect(emissions, hasLength(greaterThanOrEqualTo(count)));
}
