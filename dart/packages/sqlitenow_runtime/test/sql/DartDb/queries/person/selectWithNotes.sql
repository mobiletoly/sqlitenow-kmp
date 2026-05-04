-- @@{ queryResult=PersonWithNotesRow, collectionKey=person_id }
SELECT
  p.id AS person_id,
  p.name,
  p.status,
  p.score,
  p.avatar,
  n.id AS note__id,
  n.person_id AS note__person_id,
  n.body AS note__body

/* @@{ dynamicField=notes,
       mappingType=collection,
       propertyType=List<NoteSelectAllResult>,
       sourceTable=n,
       collectionKey=note__id,
       aliasPrefix=note__,
       notNull=true } */

FROM person p
LEFT JOIN person_note n ON n.person_id = p.id
ORDER BY p.id, n.id;
