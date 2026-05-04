-- @@{ queryResult=PersonWithProfileRow }
SELECT
  p.id,
  p.name,
  p.status,
  p.score,
  p.avatar,
  pr.id AS profile__id,
  pr.person_id AS profile__person_id,
  pr.nickname AS profile__nickname,
  pr.created_at AS profile__created_at,
  pr.visit_count AS profile__visit_count,
  pr.confidence AS profile__confidence,
  pr.metadata_json AS profile__metadata_json,
  pr.payload AS profile__payload

/* @@{ dynamicField=profile,
       mappingType=perRow,
       propertyType=ProfileRow,
       sourceTable=pr,
       aliasPrefix=profile__ } */

FROM person p
LEFT JOIN person_profile pr ON pr.person_id = p.id
WHERE p.id = :id;
