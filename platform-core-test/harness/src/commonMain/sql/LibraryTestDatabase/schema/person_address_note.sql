CREATE TABLE person_address_note
(
    id               INTEGER PRIMARY KEY NOT NULL,
    address_id       INTEGER             NOT NULL,
    -- @@{ field=note, propertyType=kotlin.String }
    note             TEXT                NOT NULL,
    -- @@{ field=created_at, propertyType=kotlin.time.Instant }
    created_at       TEXT                NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),

    FOREIGN KEY (address_id) REFERENCES person_address (id) ON DELETE CASCADE
);
