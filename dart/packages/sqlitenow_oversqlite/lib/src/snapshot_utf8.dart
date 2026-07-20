int snapshotUtf8ByteLength(String value, {int start = 0, int? end}) {
  final resolvedEnd = end ?? value.length;
  RangeError.checkValidRange(start, resolvedEnd, value.length);
  var length = 0;
  var index = start;
  while (index < resolvedEnd) {
    final unit = value.codeUnitAt(index++);
    if (unit <= 0x7f) {
      length++;
    } else if (unit <= 0x7ff) {
      length += 2;
    } else if (unit >= 0xd800 && unit <= 0xdbff && index < resolvedEnd) {
      final next = value.codeUnitAt(index);
      if (next >= 0xdc00 && next <= 0xdfff) {
        index++;
        length += 4;
      } else {
        length += 3;
      }
    } else {
      length += 3;
    }
  }
  return length;
}
