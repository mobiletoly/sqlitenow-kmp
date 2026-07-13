-- @@{enableSync=true}
CREATE TABLE typed_rows (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    note TEXT NULL,
  count_value INTEGER NULL,
  small_count INTEGER NULL,
  medium_count INTEGER NULL,
  exact_amount TEXT NULL,
  enabled_flag INTEGER NOT NULL,
  rating REAL NULL,
  float4_value REAL NULL,
    data BLOB NULL,
    created_at TEXT NULL
);
