import 'protocol_models.dart';
import 'snapshot_utf8.dart';

final class SnapshotChunkWireDecodeResult {
  const SnapshotChunkWireDecodeResult({
    required this.chunk,
    required this.rowWireByteCount,
  });

  final SnapshotChunkResponse chunk;
  final int rowWireByteCount;
}

SnapshotChunkWireDecodeResult decodeSnapshotChunkWire(String raw) {
  try {
    return _SnapshotChunkJsonDecoder(raw).decode();
  } on _SnapshotWireSyntaxException {
    throw const SnapshotResponseDecodeException('snapshot chunk request');
  } on FormatException {
    throw const SnapshotResponseDecodeException('snapshot chunk request');
  }
}

void requireUniqueSnapshotJsonObjectMembers(
  String raw, {
  required String operation,
}) {
  try {
    final parser = _SnapshotJsonParser(raw);
    parser.skipJsonValue(0);
    parser.skipWhitespace();
    if (!parser.isAtEnd) parser.syntaxError();
  } on _SnapshotWireSyntaxException {
    throw SnapshotResponseDecodeException(operation);
  } on FormatException {
    throw SnapshotResponseDecodeException(operation);
  }
}

final class _SnapshotChunkJsonDecoder extends _SnapshotJsonParser {
  _SnapshotChunkJsonDecoder(super.raw);

  SnapshotChunkWireDecodeResult decode() {
    skipWhitespace();
    final result = _readChunk(0);
    skipWhitespace();
    if (!isAtEnd) syntaxError();
    return result;
  }

  SnapshotChunkWireDecodeResult _readChunk(int depth) {
    requireDepth(depth);
    expect('{');
    skipWhitespace();
    final seen = <String>{};
    String? snapshotId;
    int? snapshotBundleSeq;
    List<SnapshotRow>? rows;
    int? nextRowOrdinal;
    bool? hasMore;
    int? byteCount;
    var rowWireByteCount = 0;

    if (!consume('}')) {
      while (true) {
        final name = readMemberName();
        if (!seen.add(name)) duplicateMember();
        expectMemberValue();
        switch (name) {
          case 'snapshot_id':
            snapshotId = readStringValue(depth + 1);
          case 'snapshot_bundle_seq':
            snapshotBundleSeq = readIntValue(depth + 1);
          case 'rows':
            final decodedRows = _readRows(depth + 1);
            rows = decodedRows.rows;
            rowWireByteCount = decodedRows.rowWireByteCount;
          case 'next_row_ordinal':
            nextRowOrdinal = readIntValue(depth + 1);
          case 'has_more':
            hasMore = readBoolValue(depth + 1);
          case 'byte_count':
            byteCount = readIntValue(depth + 1);
          default:
            syntaxError();
        }
        skipWhitespace();
        if (consume('}')) break;
        expect(',');
      }
    }

    const members = {
      'snapshot_id',
      'snapshot_bundle_seq',
      'rows',
      'next_row_ordinal',
      'has_more',
      'byte_count',
    };
    if (seen.length != members.length || !seen.containsAll(members)) {
      if (seen.length == members.length - 1 && !seen.contains('byte_count')) {
        throw const SnapshotSemanticException(
          SnapshotSemanticFailure.invalidChunk,
        );
      }
      syntaxError();
    }
    return SnapshotChunkWireDecodeResult(
      chunk: SnapshotChunkResponse(
        snapshotId: snapshotId!,
        snapshotBundleSeq: snapshotBundleSeq!,
        rows: rows!,
        nextRowOrdinal: nextRowOrdinal!,
        hasMore: hasMore!,
        byteCount: byteCount!,
      ),
      rowWireByteCount: rowWireByteCount,
    );
  }

  ({List<SnapshotRow> rows, int rowWireByteCount}) _readRows(int depth) {
    requireDepth(depth);
    skipWhitespace();
    expect('[');
    skipWhitespace();
    final rows = <SnapshotRow>[];
    var rowWireByteCount = 0;
    if (consume(']')) {
      return (rows: rows, rowWireByteCount: rowWireByteCount);
    }
    while (true) {
      // Array whitespace and separators are envelope bytes, not part of the
      // complete SnapshotRow object whose exact wire bytes the server declares.
      skipWhitespace();
      final start = index;
      rows.add(_readRow(depth + 1));
      rowWireByteCount = checkedAddOversqliteInt64(
        rowWireByteCount,
        snapshotUtf8ByteLength(raw, start: start, end: index),
        'snapshot row wire byte count',
      );
      skipWhitespace();
      if (consume(']')) {
        return (rows: rows, rowWireByteCount: rowWireByteCount);
      }
      expect(',');
    }
  }

  SnapshotRow _readRow(int depth) {
    requireDepth(depth);
    skipWhitespace();
    expect('{');
    skipWhitespace();
    final seen = <String>{};
    String? schema;
    String? table;
    Map<String, String>? key;
    int? rowVersion;
    Map<String, Object?>? payload;
    if (!consume('}')) {
      while (true) {
        final name = readMemberName();
        if (!seen.add(name)) duplicateMember();
        expectMemberValue();
        switch (name) {
          case 'schema':
            schema = _readRowString(depth + 1);
          case 'table':
            table = _readRowString(depth + 1);
          case 'key':
            key = _readStringMap(depth + 1);
          case 'row_version':
            rowVersion = _readRowInt(depth + 1);
          case 'payload':
            payload = _readRowPayload(depth + 1);
          default:
            syntaxError();
        }
        skipWhitespace();
        if (consume('}')) break;
        expect(',');
      }
    }
    const members = {'schema', 'table', 'key', 'row_version', 'payload'};
    if (seen.length != members.length || !seen.containsAll(members)) {
      _invalidRow();
    }
    return SnapshotRow(
      schema: schema!,
      table: table!,
      key: key!,
      rowVersion: rowVersion!,
      payload: payload!,
    );
  }

  Map<String, String> _readStringMap(int depth) {
    requireDepth(depth);
    skipWhitespace();
    expect('{');
    skipWhitespace();
    final result = <String, String>{};
    if (consume('}')) return result;
    while (true) {
      final name = readMemberName();
      if (result.containsKey(name)) duplicateMember();
      expectMemberValue();
      result[name] = _readRowString(depth + 1);
      skipWhitespace();
      if (consume('}')) return result;
      expect(',');
    }
  }

  String _readRowString(int depth) {
    requireDepth(depth);
    skipWhitespace();
    if (peek() != '"') {
      skipJsonValue(depth);
      _invalidRow();
    }
    return readStringValue(depth);
  }

  int _readRowInt(int depth) {
    requireDepth(depth);
    skipWhitespace();
    final current = peek();
    if (current != '-' && !_isDigit(current)) {
      skipJsonValue(depth);
      _invalidRow();
    }
    final token = readNumberToken();
    if (token.contains(RegExp(r'[.eE]'))) _invalidRow();
    final value = int.tryParse(token);
    if (value == null ||
        value < -0x8000000000000000 ||
        value > maxOversqliteInt64) {
      _invalidRow();
    }
    return value;
  }

  Map<String, Object?> _readRowPayload(int depth) {
    requireDepth(depth);
    skipWhitespace();
    if (peek() != '{') {
      skipJsonValue(depth);
      _invalidRow();
    }
    return readJsonObject(depth);
  }

  Never _invalidRow() =>
      throw const SnapshotSemanticException(SnapshotSemanticFailure.invalidRow);
}

class _SnapshotJsonParser {
  _SnapshotJsonParser(this.raw);

  static const maxNestingDepth = 128;
  final String raw;
  int index = 0;

  bool get isAtEnd => index == raw.length;

  Object? readJsonValue(int depth) {
    requireDepth(depth);
    skipWhitespace();
    final current = peek();
    if (current == '{') return readJsonObject(depth);
    if (current == '[') return readJsonArray(depth);
    if (current == '"') return readString();
    if (current == 't') {
      expectLiteral('true');
      return true;
    }
    if (current == 'f') {
      expectLiteral('false');
      return false;
    }
    if (current == 'n') {
      expectLiteral('null');
      return null;
    }
    if (current == '-' || _isDigit(current)) {
      final token = readNumberToken();
      return token.contains(RegExp(r'[.eE]'))
          ? double.parse(token)
          : int.parse(token);
    }
    syntaxError();
  }

  void skipJsonValue(int depth) {
    requireDepth(depth);
    skipWhitespace();
    final current = peek();
    if (current == '{') {
      skipJsonObject(depth);
      return;
    }
    if (current == '[') {
      skipJsonArray(depth);
      return;
    }
    if (current == '"') {
      skipString();
      return;
    }
    if (current == 't') {
      expectLiteral('true');
      return;
    }
    if (current == 'f') {
      expectLiteral('false');
      return;
    }
    if (current == 'n') {
      expectLiteral('null');
      return;
    }
    if (current == '-' || _isDigit(current)) {
      skipNumberToken();
      return;
    }
    syntaxError();
  }

  void skipJsonObject(int depth) {
    requireDepth(depth);
    skipWhitespace();
    expect('{');
    skipWhitespace();
    final seen = <String>{};
    if (consume('}')) return;
    while (true) {
      final name = readMemberName();
      if (!seen.add(name)) duplicateMember();
      expectMemberValue();
      skipJsonValue(depth + 1);
      skipWhitespace();
      if (consume('}')) return;
      expect(',');
    }
  }

  void skipJsonArray(int depth) {
    requireDepth(depth);
    skipWhitespace();
    expect('[');
    skipWhitespace();
    if (consume(']')) return;
    while (true) {
      skipJsonValue(depth + 1);
      skipWhitespace();
      if (consume(']')) return;
      expect(',');
    }
  }

  Map<String, Object?> readJsonObject(int depth) {
    requireDepth(depth);
    skipWhitespace();
    expect('{');
    skipWhitespace();
    final result = <String, Object?>{};
    if (consume('}')) return result;
    while (true) {
      final name = readMemberName();
      if (result.containsKey(name)) duplicateMember();
      expectMemberValue();
      result[name] = readJsonValue(depth + 1);
      skipWhitespace();
      if (consume('}')) return result;
      expect(',');
    }
  }

  List<Object?> readJsonArray(int depth) {
    requireDepth(depth);
    skipWhitespace();
    expect('[');
    skipWhitespace();
    final result = <Object?>[];
    if (consume(']')) return result;
    while (true) {
      result.add(readJsonValue(depth + 1));
      skipWhitespace();
      if (consume(']')) return result;
      expect(',');
    }
  }

  String readStringValue(int depth) {
    requireDepth(depth);
    skipWhitespace();
    if (peek() != '"') syntaxError();
    return readString();
  }

  int readIntValue(int depth) {
    requireDepth(depth);
    skipWhitespace();
    final token = readNumberToken();
    if (token.contains(RegExp(r'[.eE]'))) syntaxError();
    final value = int.tryParse(token);
    if (value == null ||
        value < -0x8000000000000000 ||
        value > maxOversqliteInt64) {
      syntaxError();
    }
    return value;
  }

  bool readBoolValue(int depth) {
    requireDepth(depth);
    skipWhitespace();
    if (peek() == 't') {
      expectLiteral('true');
      return true;
    }
    if (peek() == 'f') {
      expectLiteral('false');
      return false;
    }
    syntaxError();
  }

  String readMemberName() {
    skipWhitespace();
    if (peek() != '"') syntaxError();
    return readString();
  }

  void expectMemberValue() {
    skipWhitespace();
    expect(':');
  }

  String readString() {
    expect('"');
    final output = StringBuffer();
    while (index < raw.length) {
      final current = raw[index++];
      if (current == '"') return output.toString();
      if (current == '\\') {
        readEscape(output);
      } else {
        if (current.codeUnitAt(0) < 0x20) syntaxError();
        output.write(current);
      }
    }
    syntaxError();
  }

  void skipString() {
    expect('"');
    while (index < raw.length) {
      final current = raw[index++];
      if (current == '"') return;
      if (current == '\\') {
        final escaped = peek();
        if (escaped == null) syntaxError();
        index++;
        switch (escaped) {
          case '"':
          case '\\':
          case '/':
          case 'b':
          case 'f':
          case 'n':
          case 'r':
          case 't':
            break;
          case 'u':
            readUnicodeEscape();
          default:
            syntaxError();
        }
      } else if (current.codeUnitAt(0) < 0x20) {
        syntaxError();
      }
    }
    syntaxError();
  }

  void readEscape(StringBuffer output) {
    final escaped = peek();
    if (escaped == null) syntaxError();
    index++;
    switch (escaped) {
      case '"':
      case '\\':
      case '/':
        output.write(escaped);
      case 'b':
        output.write('\b');
      case 'f':
        output.write('\f');
      case 'n':
        output.write('\n');
      case 'r':
        output.write('\r');
      case 't':
        output.write('\t');
      case 'u':
        output.writeCharCode(readUnicodeEscape());
      default:
        syntaxError();
    }
  }

  int readUnicodeEscape() {
    if (index + 4 > raw.length) syntaxError();
    var value = 0;
    for (var count = 0; count < 4; count++) {
      value = (value << 4) | hexValue(raw[index++]);
    }
    return value;
  }

  String readNumberToken() {
    final start = index;
    skipNumberToken();
    return raw.substring(start, index);
  }

  void skipNumberToken() {
    consume('-');
    if (peek() == '0') {
      index++;
      if (_isDigit(peek())) syntaxError();
    } else if (_isNonZeroDigit(peek())) {
      while (_isDigit(peek())) {
        index++;
      }
    } else {
      syntaxError();
    }
    if (consume('.')) {
      if (!_isDigit(peek())) syntaxError();
      while (_isDigit(peek())) {
        index++;
      }
    }
    if (peek() == 'e' || peek() == 'E') {
      index++;
      if (peek() == '+' || peek() == '-') index++;
      if (!_isDigit(peek())) syntaxError();
      while (_isDigit(peek())) {
        index++;
      }
    }
  }

  void expectLiteral(String expected) {
    if (!raw.startsWith(expected, index)) syntaxError();
    index += expected.length;
  }

  void requireDepth(int depth) {
    if (depth > maxNestingDepth) {
      throw const SnapshotSemanticException(
        SnapshotSemanticFailure.excessiveNesting,
      );
    }
  }

  void skipWhitespace() {
    while (peek() == ' ' ||
        peek() == '\t' ||
        peek() == '\n' ||
        peek() == '\r') {
      index++;
    }
  }

  bool consume(String expected) {
    if (peek() != expected) return false;
    index++;
    return true;
  }

  void expect(String expected) {
    if (!consume(expected)) syntaxError();
  }

  String? peek() => index < raw.length ? raw[index] : null;

  int hexValue(String value) {
    final unit = value.codeUnitAt(0);
    if (unit >= 0x30 && unit <= 0x39) return unit - 0x30;
    if (unit >= 0x61 && unit <= 0x66) return unit - 0x61 + 10;
    if (unit >= 0x41 && unit <= 0x46) return unit - 0x41 + 10;
    syntaxError();
  }

  Never duplicateMember() => throw const SnapshotSemanticException(
    SnapshotSemanticFailure.duplicateObjectMember,
  );

  Never syntaxError() => throw const _SnapshotWireSyntaxException();
}

bool _isDigit(String? value) {
  if (value == null) return false;
  final unit = value.codeUnitAt(0);
  return unit >= 0x30 && unit <= 0x39;
}

bool _isNonZeroDigit(String? value) {
  if (value == null) return false;
  final unit = value.codeUnitAt(0);
  return unit >= 0x31 && unit <= 0x39;
}

final class _SnapshotWireSyntaxException implements Exception {
  const _SnapshotWireSyntaxException();
}
