CREATE TABLE comment
(
    id         INTEGER PRIMARY KEY NOT NULL,
    person_id  INTEGER NOT NULL,
    comment    TEXT    NOT NULL,

    -- @@{ field=created_at, propertyType=kotlin.time.Instant }
    created_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),

    -- @@{ field=tags, propertyType=List<String> }
    tags       TEXT,

    FOREIGN KEY (person_id) REFERENCES person (id) ON DELETE CASCADE
);
