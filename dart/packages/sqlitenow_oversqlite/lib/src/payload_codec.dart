import 'dart:convert';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';

import 'payload_source.dart';
import 'protocol.dart';
import 'table_info.dart';

final _jsonNumberPattern = RegExp(
  r'^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$',
);

final _rfc3339TimestampPattern = RegExp(
  r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
  r'(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$',
);

String wireKeyJsonFromOversqliteLocalKey(
  OversqliteTableInfo table,
  String localPk,
) {
  return canonicalizeOversqliteProtocolJson({
    table.primaryKey.name
        .toLowerCase(): table.primaryKey.kind == OversqliteColumnKind.uuidBlob
        ? canonicalOversqliteUuid(localPk)
        : localPk,
  });
}

String localPkFromOversqliteKeyJson(OversqliteTableInfo table, String keyJson) {
  final key = (jsonDecode(keyJson) as Map).cast<String, Object?>();
  return key[table.primaryKey.name.toLowerCase()].toString();
}

SyncKey syncKeyFromOversqliteJson(String jsonText) {
  final parsed = (jsonDecode(jsonText) as Map).cast<String, Object?>();
  return {
    for (final entry in parsed.entries) entry.key: entry.value.toString(),
  };
}

bool oversqliteSyncKeysEqual(SyncKey left, SyncKey right) {
  if (left.length != right.length) {
    return false;
  }
  for (final entry in left.entries) {
    if (right[entry.key] != entry.value) {
      return false;
    }
  }
  return true;
}

({String keyJson, String localPk}) localKeyFromOversqliteWireKey(
  OversqliteTableInfo table,
  SyncKey key,
) {
  final keyColumn = table.primaryKey.name.toLowerCase();
  final wireValue = key[keyColumn] ?? key[table.primaryKey.name];
  if (wireValue == null) {
    throw OversqliteProtocolException(
      'bundle row key for ${table.name} is missing ${table.primaryKey.name}',
    );
  }
  final localPk = table.primaryKey.kind == OversqliteColumnKind.uuidBlob
      ? hexBytes(decodeOversqliteWireUuid(wireValue))
      : wireValue;
  return (
    keyJson: canonicalizeOversqliteProtocolJson({keyColumn: localPk}),
    localPk: localPk,
  );
}

SyncKey wireKeyFromOversqliteLocalKey(
  OversqliteTableInfo table,
  String localPk,
) {
  return {
    table.primaryKey.name
        .toLowerCase(): table.primaryKey.kind == OversqliteColumnKind.uuidBlob
        ? canonicalOversqliteUuid(localPk)
        : localPk,
  };
}

Object? localOversqliteJsonValue(OversqliteColumnInfo column, Object? value) {
  if (value == null) {
    return null;
  }
  if (value is Uint8List) {
    return hexBytes(value);
  }
  if (value is List<int>) {
    return hexBytes(Uint8List.fromList(value));
  }
  if (column.kind == OversqliteColumnKind.integer) {
    return value.toString();
  }
  if (column.kind == OversqliteColumnKind.real) {
    return value.toString();
  }
  return value;
}

Map<String, Object?> wireOversqlitePayloadForUpload(
  OversqliteTableInfo table,
  Map<String, Object?> payload,
) {
  final normalized = {
    for (final entry in payload.entries) entry.key.toLowerCase(): entry.value,
  };
  return {
    for (final column in table.columns)
      column.name.toLowerCase(): wireOversqlitePayloadValue(
        column,
        normalized[column.name.toLowerCase()],
      ),
  };
}

Object? wireOversqlitePayloadValue(OversqliteColumnInfo column, Object? value) {
  if (value == null) {
    return null;
  }
  return switch (column.kind) {
    OversqliteColumnKind.blob => base64Encode(
      decodeOversqliteLocalBlob(value.toString()),
    ),
    OversqliteColumnKind.uuidBlob => canonicalOversqliteUuid(value.toString()),
    OversqliteColumnKind.integer =>
      value is bool ? (value ? '1' : '0') : _requireInt64(value.toString()),
    OversqliteColumnKind.real => canonicalizeFiniteJsonNumber(value.toString()),
    OversqliteColumnKind.text =>
      value is String
          ? value
          : throw ArgumentError('TEXT requires a string for ${column.name}'),
  };
}

Object? bindOversqlitePayloadValue(
  OversqliteColumnInfo column,
  Object? value, [
  PayloadSource source = PayloadSource.authoritativeWire,
]) {
  if (value == null) {
    return null;
  }
  return switch (column.kind) {
    OversqliteColumnKind.blob =>
      source == PayloadSource.authoritativeWire
          ? base64Decode(value.toString())
          : decodeOversqliteLocalBlob(value.toString()),
    OversqliteColumnKind.uuidBlob =>
      source == PayloadSource.authoritativeWire
          ? decodeOversqliteWireUuid(value.toString())
          : decodeOversqliteLocalUuid(value.toString()),
    OversqliteColumnKind.integer =>
      value is bool ? (value ? 1 : 0) : _requireInt64(value.toString()),
    OversqliteColumnKind.real =>
      source == PayloadSource.authoritativeWire
          ? _canonicalFiniteDouble(value, column.name)
          : _finiteDouble(value.toString(), column.name),
    OversqliteColumnKind.text =>
      value is String
          ? value
          : throw ArgumentError('TEXT requires a string for ${column.name}'),
  };
}

Object bindOversqlitePrimaryKey(OversqliteTableInfo table, String localPk) {
  if (table.primaryKey.kind == OversqliteColumnKind.uuidBlob) {
    return decodeOversqliteLocalUuid(localPk);
  }
  return localPk;
}

bool equivalentOversqlitePayloadTexts(
  OversqliteTableInfo table,
  String? left,
  String? right,
  PayloadSource leftSource,
  PayloadSource rightSource,
) {
  if (left == null && right == null) {
    return true;
  }
  if (left == null || right == null) {
    return false;
  }
  final leftObject = jsonDecode(left);
  final rightObject = jsonDecode(right);
  if (leftObject is! Map || rightObject is! Map) {
    return false;
  }
  return equivalentOversqlitePayloadObjects(
    table,
    leftObject.cast<String, Object?>(),
    rightObject.cast<String, Object?>(),
    leftSource,
    rightSource,
  );
}

bool equivalentOversqliteLocalPayload(
  OversqliteTableInfo table,
  String? left,
  String? right,
) {
  return equivalentOversqlitePayloadTexts(
    table,
    left,
    right,
    PayloadSource.localState,
    PayloadSource.localState,
  );
}

bool equivalentOversqlitePayloadObjects(
  OversqliteTableInfo table,
  Map<String, Object?> left,
  Map<String, Object?> right,
  PayloadSource leftSource,
  PayloadSource rightSource, {
  bool allowRfc3339InstantEquivalence = false,
}) {
  if (left.length != table.columns.length ||
      right.length != table.columns.length) {
    return false;
  }
  for (final column in table.columns) {
    final leftLookup = lookupOversqlitePayloadValue(left, column);
    final rightLookup = lookupOversqlitePayloadValue(right, column);
    if (!leftLookup.found || !rightLookup.found) {
      return false;
    }
    final equivalent = equivalentOversqlitePayloadValue(
      column,
      leftLookup.value,
      rightLookup.value,
      leftSource,
      rightSource,
    );
    if (equivalent ||
        (allowRfc3339InstantEquivalence &&
            rfc3339InstantEquivalent(leftLookup.value, rightLookup.value))) {
      continue;
    }
    return false;
  }
  return true;
}

({bool found, Object? value}) lookupOversqlitePayloadValue(
  Map<String, Object?> payload,
  OversqliteColumnInfo column,
) {
  final lower = column.name.toLowerCase();
  if (payload.containsKey(lower)) {
    return (found: true, value: payload[lower]);
  }
  if (payload.containsKey(column.name)) {
    return (found: true, value: payload[column.name]);
  }
  return (found: false, value: null);
}

bool equivalentOversqlitePayloadValue(
  OversqliteColumnInfo column,
  Object? left,
  Object? right,
  PayloadSource leftSource,
  PayloadSource rightSource,
) {
  if (left == null || right == null) {
    return left == right;
  }
  try {
    return switch (column.kind) {
      OversqliteColumnKind.text => left.toString() == right.toString(),
      OversqliteColumnKind.real =>
        canonicalizeFiniteJsonNumber(left.toString()) ==
            canonicalizeFiniteJsonNumber(right.toString()),
      OversqliteColumnKind.integer =>
        _requireInt64(left.toString()) == _requireInt64(right.toString()),
      OversqliteColumnKind.blob => bytesEqual(
        decodeOversqliteBlobPayloadValue(left, leftSource),
        decodeOversqliteBlobPayloadValue(right, rightSource),
      ),
      OversqliteColumnKind.uuidBlob => bytesEqual(
        decodeOversqliteUuidPayloadValue(left, leftSource),
        decodeOversqliteUuidPayloadValue(right, rightSource),
      ),
    };
  } catch (_) {
    return false;
  }
}

String _requireInt64(String raw) {
  if (!RegExp(r'^-?(0|[1-9][0-9]*)$').hasMatch(raw) || raw == '-0') {
    throw ArgumentError('expected an exact signed 64-bit integer: $raw');
  }
  final negative = raw.startsWith('-');
  final magnitude = negative ? raw.substring(1) : raw;
  final limit = negative ? '9223372036854775808' : '9223372036854775807';
  if (magnitude.length > limit.length ||
      (magnitude.length == limit.length && magnitude.compareTo(limit) > 0)) {
    throw ArgumentError('signed 64-bit integer is out of range: $raw');
  }
  return raw;
}

double _finiteDouble(String raw, String columnName) {
  final value = double.parse(raw);
  if (!value.isFinite) {
    throw ArgumentError('REAL requires a finite value for $columnName');
  }
  return value;
}

double _canonicalFiniteDouble(Object value, String columnName) {
  if (value is! String) {
    throw ArgumentError(
      'authoritative REAL requires a canonical binary64 string for $columnName',
    );
  }
  final parsed = _finiteDouble(value, columnName);
  if (canonicalizeFiniteJsonNumber(parsed.toString()) != value) {
    throw ArgumentError(
      'authoritative REAL requires canonical binary64 text for $columnName',
    );
  }
  return parsed;
}

Uint8List decodeOversqliteBlobPayloadValue(Object value, PayloadSource source) {
  return switch (source) {
    PayloadSource.localState => decodeOversqliteLocalBlob(value.toString()),
    PayloadSource.authoritativeWire => base64Decode(value.toString()),
  };
}

Uint8List decodeOversqliteUuidPayloadValue(Object value, PayloadSource source) {
  return switch (source) {
    PayloadSource.localState => decodeOversqliteLocalUuid(value.toString()),
    PayloadSource.authoritativeWire => decodeOversqliteWireUuid(
      value.toString(),
    ),
  };
}

bool bytesEqual(Uint8List left, Uint8List right) {
  if (left.length != right.length) {
    return false;
  }
  for (var i = 0; i < left.length; i++) {
    if (left[i] != right[i]) {
      return false;
    }
  }
  return true;
}

bool rfc3339InstantEquivalent(Object? left, Object? right) {
  if (left is! String || right is! String) {
    return false;
  }
  if (!_rfc3339TimestampPattern.hasMatch(left.trim()) ||
      !_rfc3339TimestampPattern.hasMatch(right.trim())) {
    return false;
  }
  try {
    return DateTime.parse(
      left,
    ).toUtc().isAtSameMomentAs(DateTime.parse(right).toUtc());
  } on FormatException {
    return false;
  }
}

Uint8List decodeOversqliteLocalBlob(String value) {
  final clean = value.trim().replaceFirst(
    RegExp(r'^\\x', caseSensitive: false),
    '',
  );
  if (clean.length.isEven && RegExp(r'^[0-9a-fA-F]*$').hasMatch(clean)) {
    return decodeHex(clean);
  }
  return base64Decode(value);
}

Uint8List decodeOversqliteLocalUuid(String value) {
  final normalized = value.trim();
  if (RegExp(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$',
  ).hasMatch(normalized)) {
    return decodeHex(normalized.replaceAll('-', ''));
  }
  if (normalized.length == 32 &&
      RegExp(r'^[0-9a-fA-F]{32}$').hasMatch(normalized)) {
    return decodeHex(normalized);
  }
  final decoded = base64Decode(value);
  if (decoded.length != 16) {
    throw ArgumentError('UUID BLOB must decode to 16 bytes');
  }
  return Uint8List.fromList(decoded);
}

Uint8List decodeOversqliteWireUuid(String value) {
  final normalized = value.trim();
  if (normalized != value ||
      !RegExp(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
      ).hasMatch(normalized)) {
    throw ArgumentError('invalid canonical wire UUID encoding');
  }
  return decodeHex(normalized.replaceAll('-', ''));
}

String canonicalOversqliteUuid(String value) {
  final hex = hexBytes(decodeOversqliteLocalUuid(value));
  return '${hex.substring(0, 8)}-${hex.substring(8, 12)}-${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20)}';
}

Uint8List decodeHex(String value) {
  if (!value.length.isEven) {
    throw ArgumentError('hex value must have even length');
  }
  return Uint8List.fromList([
    for (var i = 0; i < value.length; i += 2)
      int.parse(value.substring(i, i + 2), radix: 16),
  ]);
}

String hexBytes(List<int> bytes) {
  return bytes
      .map((byte) => (byte & 0xff).toRadixString(16).padLeft(2, '0'))
      .join();
}

String canonicalizeFiniteJsonNumber(String value) {
  return canonicalizeOversqliteProtocolJsonNumber(value);
}

String trimDecimalNumber(String plain) {
  final parts = plain.split('.');
  final integerPart = parts[0].replaceFirst(RegExp(r'^0+'), '');
  final fractionPart = parts[1].replaceFirst(RegExp(r'0+$'), '');
  final normalizedInteger = integerPart.isEmpty ? '0' : integerPart;
  if (fractionPart.isEmpty) {
    return normalizedInteger;
  }
  return '$normalizedInteger.$fractionPart';
}

String zeroes(int count) => List.filled(count, '0').join();

String canonicalizeOversqliteProtocolJson(Object? value) {
  if (value == null) {
    return 'null';
  }
  if (value is String) {
    _validateDartString(value);
    return jsonEncode(value);
  }
  if (value is bool) {
    return jsonEncode(value);
  }
  if (value is num) {
    return canonicalizeOversqliteProtocolJsonNumber(value.toString());
  }
  if (value is List) {
    return '[${value.map(canonicalizeOversqliteProtocolJson).join(',')}]';
  }
  if (value is Map) {
    final entries = value.entries.toList()
      ..sort(
        (left, right) => left.key.toString().compareTo(right.key.toString()),
      );
    return '{${entries.map((entry) => '${canonicalizeOversqliteProtocolJson(entry.key.toString())}:${canonicalizeOversqliteProtocolJson(entry.value)}').join(',')}}';
  }
  return jsonEncode(value);
}

String canonicalizeOversqliteProtocolJsonNumber(String raw) {
  if (!_jsonNumberPattern.hasMatch(raw)) {
    throw ArgumentError('invalid JSON number: $raw');
  }
  final number = double.parse(raw);
  if (!number.isFinite) throw ArgumentError('JSON number must be finite: $raw');
  if (number == 0) return '0';
  final absNumber = number.abs();
  final rendered = number.toString().toLowerCase();
  if (absNumber >= 1e-6 && absNumber < 1e21) {
    return normalizeProtocolPlainNumber(protocolScientificToPlain(rendered));
  }
  return normalizeProtocolScientificNumber(rendered);
}

String protocolScientificToPlain(String raw) {
  if (!raw.contains('e')) {
    return raw;
  }
  final sign = raw.startsWith('-') ? '-' : '';
  final unsigned = sign.isEmpty ? raw : raw.substring(1);
  final parts = unsigned.split('e');
  if (parts.length != 2) {
    throw ArgumentError('invalid scientific notation: $raw');
  }

  final mantissa = parts[0];
  final exponent = int.parse(parts[1]);
  final dotIndex = mantissa.indexOf('.');
  final digits = mantissa.replaceAll('.', '');
  final fractionalDigits = dotIndex >= 0 ? mantissa.length - dotIndex - 1 : 0;
  final decimalIndex = digits.length + exponent - fractionalDigits;

  if (decimalIndex <= 0) {
    return '${sign}0.${zeroes(-decimalIndex)}$digits';
  }
  if (decimalIndex >= digits.length) {
    return '$sign$digits${zeroes(decimalIndex - digits.length)}';
  }
  return '$sign${digits.substring(0, decimalIndex)}.'
      '${digits.substring(decimalIndex)}';
}

String normalizeProtocolPlainNumber(String raw) {
  final sign = raw.startsWith('-') ? '-' : '';
  final unsigned = sign.isEmpty ? raw : raw.substring(1);
  final dotIndex = unsigned.indexOf('.');
  final integerRaw = dotIndex >= 0 ? unsigned.substring(0, dotIndex) : unsigned;
  final fractionRaw = dotIndex >= 0 ? unsigned.substring(dotIndex + 1) : '';
  final integerPart = integerRaw.replaceFirst(RegExp(r'^0+'), '');
  final normalizedInteger = integerPart.isEmpty ? '0' : integerPart;
  final fractionalPart = fractionRaw.replaceFirst(RegExp(r'0+$'), '');
  if (normalizedInteger == '0' && fractionalPart.isEmpty) {
    return '0';
  }
  if (fractionalPart.isEmpty) {
    return '$sign$normalizedInteger';
  }
  return '$sign$normalizedInteger.$fractionalPart';
}

String normalizeProtocolScientificNumber(String raw) {
  if (!raw.contains('e')) {
    return protocolPlainToScientific(normalizeProtocolPlainNumber(raw));
  }
  final sign = raw.startsWith('-') ? '-' : '';
  final unsigned = sign.isEmpty ? raw : raw.substring(1);
  final parts = unsigned.split('e');
  if (parts.length != 2) {
    throw ArgumentError('invalid scientific notation: $raw');
  }

  final mantissa = normalizeProtocolPlainNumber(parts[0]);
  final exponent = int.parse(parts[1]);
  return '$sign$mantissa'
      'e${exponent >= 0 ? '+' : ''}$exponent';
}

String protocolPlainToScientific(String raw) {
  final sign = raw.startsWith('-') ? '-' : '';
  final unsigned = sign.isEmpty ? raw : raw.substring(1);
  final dotIndex = unsigned.indexOf('.');
  final integerPart = dotIndex >= 0
      ? unsigned.substring(0, dotIndex)
      : unsigned;
  final fractionalPart = dotIndex >= 0 ? unsigned.substring(dotIndex + 1) : '';

  final firstIntegerNonZero = firstNonZeroIndex(integerPart);
  if (firstIntegerNonZero >= 0) {
    final digits = (integerPart + fractionalPart).replaceFirst(
      RegExp(r'^0+'),
      '',
    );
    final exponent = integerPart.length - 1;
    return '$sign${protocolScientificMantissa(digits)}'
        'e+$exponent';
  }

  final firstFractionNonZero = firstNonZeroIndex(fractionalPart);
  if (firstFractionNonZero < 0) {
    throw ArgumentError('plainToScientific requires a non-zero number: $raw');
  }
  final digits = fractionalPart.substring(firstFractionNonZero);
  final exponent = -(firstFractionNonZero + 1);
  return '$sign${protocolScientificMantissa(digits)}'
      'e$exponent';
}

int firstNonZeroIndex(String value) {
  for (var i = 0; i < value.length; i++) {
    if (value.codeUnitAt(i) != 0x30) {
      return i;
    }
  }
  return -1;
}

String protocolScientificMantissa(String digits) {
  final head = digits.substring(0, 1);
  final tail = digits.substring(1);
  return tail.isEmpty ? head : '$head.$tail';
}

String sha256Hex(String text) {
  return sha256.convert(utf8.encode(text)).bytes.map((byte) {
    return byte.toRadixString(16).padLeft(2, '0');
  }).join();
}

void _validateDartString(String value) {
  for (var index = 0; index < value.length; index++) {
    final code = value.codeUnitAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      if (index + 1 >= value.length) {
        throw ArgumentError('invalid unpaired high surrogate');
      }
      final low = value.codeUnitAt(++index);
      if (low < 0xdc00 || low > 0xdfff) {
        throw ArgumentError('invalid unpaired high surrogate');
      }
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      throw ArgumentError('invalid unpaired low surrogate');
    }
  }
}
