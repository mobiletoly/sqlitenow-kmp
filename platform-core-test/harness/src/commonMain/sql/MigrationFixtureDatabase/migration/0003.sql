ALTER TABLE migration_probe ADD COLUMN created_at TEXT NOT NULL DEFAULT 'migrated';

CREATE TABLE migration_meta (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);
