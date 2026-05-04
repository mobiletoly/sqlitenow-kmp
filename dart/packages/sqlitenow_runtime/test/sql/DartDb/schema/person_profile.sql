CREATE TABLE person_profile (
  id INTEGER PRIMARY KEY NOT NULL,
  person_id INTEGER NOT NULL,
  -- @@{ field=nickname, propertyName=displayName }
  nickname TEXT NOT NULL,
  -- @@{ field=created_at, adapter=custom, propertyType=DateTime }
  created_at TEXT NOT NULL,
  -- @@{ field=visit_count, adapter=custom, propertyType=int }
  visit_count INTEGER NOT NULL,
  -- @@{ field=confidence, adapter=custom, propertyType=double }
  confidence REAL,
  -- @@{ field=metadata_json, adapter=custom, propertyType=String }
  metadata_json TEXT,
  -- @@{ field=payload, adapter=custom, propertyType=Uint8List }
  payload BLOB NOT NULL,
  FOREIGN KEY (person_id) REFERENCES person(id) ON DELETE CASCADE
);
