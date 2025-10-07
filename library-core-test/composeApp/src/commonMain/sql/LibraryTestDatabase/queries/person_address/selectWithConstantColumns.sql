SELECT pa.id,
       pa.person_id,
       pa.is_primary,
       -- @@{ field=constant_int, propertyType=kotlin.Int }
       42 AS constant_int,
       -- @@{ field=constant_real, propertyType=kotlin.Double }
       3.14 AS constant_real,
       -- @@{ field=constant_timestamp, propertyType=kotlinx.datetime.LocalDateTime }
       strftime('%Y-%m-%dT%H:%M:%S', 'now') AS constant_timestamp,
       -- @@{ field=constant_null, propertyType=kotlin.String, notNull=false }
       NULL AS constant_null
FROM person_address pa
WHERE pa.country = 'CA'
  AND pa.person_id = :personId;
