-- @@{ queryResult=ValueRegistryRow }
SELECT id, key, value_type, updated_at, string_value, numeric_value
FROM value_registry
WHERE key = :key;
