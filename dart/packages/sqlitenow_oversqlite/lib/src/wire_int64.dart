const minWireOversqliteInt64 = -0x8000000000000000;
const maxWireOversqliteInt64 = 0x7fffffffffffffff;

int readWireOversqliteInt64(
  Map<String, Object?> json,
  String key, {
  required bool required,
  required Object Function(String message) error,
}) {
  if (!json.containsKey(key)) {
    if (!required) return 0;
    throw error('required integer $key is missing');
  }
  final value = json[key];
  if (value is! int ||
      value < minWireOversqliteInt64 ||
      value > maxWireOversqliteInt64) {
    throw error('$key must be a signed int64 integer');
  }
  return value;
}
