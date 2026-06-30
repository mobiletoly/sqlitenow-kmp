CREATE TABLE category
(
    id         INTEGER PRIMARY KEY NOT NULL,
    name       TEXT                NOT NULL UNIQUE,
    description TEXT,
    -- @@{ field=created_at, propertyType=kotlin.time.Instant }
    created_at TEXT                NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX idx_category_name ON category (name);
