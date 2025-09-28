INSERT INTO value_registry (key, value_type, updated_at, string_value, numeric_value)
VALUES (:key, :valueType, :updatedAt, :stringValue, :numericValue)
ON CONFLICT(key) DO UPDATE SET
  value_type    = excluded.value_type,
  updated_at    = excluded.updated_at,
  string_value  = excluded.string_value,
  numeric_value = excluded.numeric_value
WHERE excluded.updated_at >= value_registry.updated_at;
