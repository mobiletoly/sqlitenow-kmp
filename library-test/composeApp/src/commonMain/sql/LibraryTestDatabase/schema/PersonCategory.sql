-- Junction table for many-to-many relationship between Person and Category
CREATE TABLE person_category
(
    id          INTEGER PRIMARY KEY NOT NULL,
    person_id   INTEGER NOT NULL,
    category_id INTEGER NOT NULL,
    -- @@{ field=assigned_at, propertyType=kotlinx.datetime.LocalDateTime }
    assigned_at TEXT                NOT NULL DEFAULT current_timestamp,
    -- @@{ field=is_primary, propertyType=kotlin.Boolean }
    is_primary  INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0,1)),

    FOREIGN KEY (person_id) REFERENCES person (id) ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES category (id) ON DELETE CASCADE,
    UNIQUE (person_id, category_id)
);

CREATE INDEX idx_person_category_person ON person_category (person_id);
CREATE INDEX idx_person_category_category ON person_category (category_id);
