-- @@{ enableSync=true }
CREATE TABLE comment
(
    id         TEXT PRIMARY KEY NOT NULL,
    person_id  BLOB            NOT NULL,
    comment    TEXT            NOT NULL,
    created_at TEXT            NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    tags       TEXT,

    FOREIGN KEY (person_id) REFERENCES person (id) ON DELETE CASCADE
)
WITHOUT ROWID;
