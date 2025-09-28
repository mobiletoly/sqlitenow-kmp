CREATE TABLE category
(
    id         INTEGER PRIMARY KEY NOT NULL,
    name       TEXT                NOT NULL UNIQUE,
    description TEXT,
    -- @@{ field=created_at, propertyType=kotlinx.datetime.LocalDateTime }
    created_at TEXT                NOT NULL DEFAULT current_timestamp
);

CREATE INDEX idx_category_name ON category (name);
