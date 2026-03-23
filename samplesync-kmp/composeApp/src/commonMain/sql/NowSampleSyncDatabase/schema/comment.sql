-- @@{ enableSync=true }
CREATE TABLE comment
(
    id         TEXT PRIMARY KEY NOT NULL,  -- MUST be uuid
    person_id  BLOB NOT NULL,
    comment    TEXT    NOT NULL,

    -- @@{ field=created_at, propertyType=kotlin.time.Instant }
    created_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),

    -- @@{ field=tags, propertyType=List<String> }
    tags       TEXT,

    FOREIGN KEY (person_id) REFERENCES person (id) ON DELETE CASCADE
)
WITHOUT ROWID;
