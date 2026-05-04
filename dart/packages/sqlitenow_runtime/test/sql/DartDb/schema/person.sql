-- @@{ cascadeNotify = { delete = [person_note] } }
CREATE TABLE person (
  id INTEGER PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,
  -- @@{ field=status, adapter=custom, propertyType=String }
  status TEXT NOT NULL,
  score REAL,
  avatar BLOB
);
