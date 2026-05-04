INSERT INTO person_profile (
  id,
  person_id,
  nickname,
  created_at,
  visit_count,
  confidence,
  metadata_json,
  payload
) VALUES (
  :id,
  :personId,
  :displayName,
  :createdAt,
  :visitCount,
  :confidence,
  :metadataJson,
  :payload
);
