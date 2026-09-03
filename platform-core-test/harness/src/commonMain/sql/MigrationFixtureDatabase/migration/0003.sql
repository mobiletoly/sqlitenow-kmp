ALTER TABLE migration_probe ADD COLUMN created_at TEXT NOT NULL DEFAULT 'migrated';

CREATE TABLE migration_meta (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);

CREATE TABLE migration_person_v3 (
    id INTEGER PRIMARY KEY NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL
);

INSERT INTO migration_person_v3 (id, first_name, last_name)
SELECT id, first_name, last_name
FROM migration_person;

DROP TABLE migration_person;
ALTER TABLE migration_person_v3 RENAME TO migration_person;
