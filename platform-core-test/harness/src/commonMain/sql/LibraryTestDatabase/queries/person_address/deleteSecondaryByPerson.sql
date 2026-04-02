DELETE FROM person_address
WHERE is_primary = 0
  AND country = 'CA'
  AND person_id = :personId;
