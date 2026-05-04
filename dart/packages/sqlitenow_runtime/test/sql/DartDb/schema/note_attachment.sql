CREATE TABLE note_attachment (
  id INTEGER PRIMARY KEY NOT NULL,
  note_id INTEGER NOT NULL,
  label TEXT NOT NULL,
  FOREIGN KEY (note_id) REFERENCES person_note(id) ON DELETE CASCADE
);

CREATE VIEW note_detailed_view AS
SELECT
  n.id AS note__id,
  n.person_id AS note__person_id,
  n.body AS note__body,
  a.id AS attachment__id,
  a.note_id AS attachment__note_id,
  a.label AS attachment__label

/* @@{ dynamicField=attachment,
       mappingType=perRow,
       propertyType=AttachmentRow,
       sourceTable=a,
       aliasPrefix=attachment__ } */

FROM person_note n
LEFT JOIN note_attachment a ON a.note_id = n.id;
