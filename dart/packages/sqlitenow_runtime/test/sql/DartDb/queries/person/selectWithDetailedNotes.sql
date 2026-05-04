-- @@{ queryResult=PersonWithDetailedNotesRow, collectionKey=person_id }
SELECT
  p.id AS person_id,
  p.name,
  p.status,
  p.score,
  p.avatar,
  ndv.note__id,
  ndv.note__person_id,
  ndv.note__body,
  ndv.attachment__id,
  ndv.attachment__note_id,
  ndv.attachment__label

/* @@{ dynamicField=notes,
       mappingType=collection,
       propertyType=List<NoteSelectAllResult>,
       sourceTable=ndv,
       collectionKey=note__id,
       aliasPrefix=note__,
       notNull=true } */

FROM person p
LEFT JOIN note_detailed_view ndv ON ndv.note__person_id = p.id
ORDER BY p.id, ndv.note__id, ndv.attachment__id;
