-- @@{ enableSync=true }
CREATE TABLE typed_rows (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    note TEXT,
    count_value INTEGER,
    small_count INTEGER,
    medium_count INTEGER,
    exact_amount TEXT,
    -- @@{ field=enabled_flag, propertyType=Boolean }
    enabled_flag INTEGER NOT NULL CHECK (enabled_flag IN (0, 1)),
    rating REAL,
    float4_value REAL,
    data BLOB,
    created_at TEXT
);
