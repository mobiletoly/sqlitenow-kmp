-- @@{ enableSync=true }
CREATE TABLE files (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    data BLOB NOT NULL
);
