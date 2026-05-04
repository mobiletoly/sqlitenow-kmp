-- @@{ queryResult=ProfileRow }
SELECT
  id,
  person_id,
  nickname,
  created_at,
  visit_count,
  confidence,
  metadata_json,
  payload
FROM person_profile
ORDER BY id;
