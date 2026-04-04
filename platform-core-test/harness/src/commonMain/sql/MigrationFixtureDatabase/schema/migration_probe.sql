CREATE TABLE migration_probe (
    id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    nickname TEXT,
    created_at TEXT NOT NULL DEFAULT 'migrated'
);
