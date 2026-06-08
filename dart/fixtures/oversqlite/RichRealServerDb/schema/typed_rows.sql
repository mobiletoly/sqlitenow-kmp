-- @@{enableSync=true}
CREATE TABLE typed_rows (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,
  note TEXT NULL,
  count_value INTEGER NULL,
  enabled_flag INTEGER NOT NULL,
  rating REAL NULL,
  data BLOB NULL,
  created_at TEXT NULL
);
