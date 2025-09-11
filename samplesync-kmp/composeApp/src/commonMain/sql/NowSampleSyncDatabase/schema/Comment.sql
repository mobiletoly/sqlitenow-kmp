-- @@{ enableSync=true }
CREATE TABLE comment
(
    id         TEXT PRIMARY KEY NOT NULL,
    person_id  TEXT NOT NULL,
    comment    TEXT    NOT NULL,

    -- @@{ field=created_at, propertyType=kotlinx.datetime.LocalDateTime }
    created_at TEXT    NOT NULL DEFAULT current_timestamp,

    -- @@{ field=tags, propertyType=List<String> }
    tags       TEXT,

    FOREIGN KEY (person_id) REFERENCES person (id) ON DELETE CASCADE
);
