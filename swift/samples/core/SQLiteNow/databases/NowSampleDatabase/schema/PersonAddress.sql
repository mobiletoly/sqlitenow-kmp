CREATE TABLE person_address
(
    id           INTEGER PRIMARY KEY NOT NULL,
    person_id    INTEGER             NOT NULL,
    address_type TEXT                NOT NULL,
    street       TEXT                NOT NULL,
    city         TEXT                NOT NULL,
    state        TEXT,
    postal_code  TEXT,
    country      TEXT                NOT NULL,
    -- @@{ field=is_primary, propertyType=Boolean }
    is_primary   INTEGER             NOT NULL DEFAULT 0,
    created_at   TEXT                NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),

    FOREIGN KEY (person_id) REFERENCES person (id) ON DELETE CASCADE
);

CREATE INDEX idx_person_address_person_id ON person_address (person_id);
CREATE INDEX idx_person_address_primary ON person_address (person_id, is_primary);
