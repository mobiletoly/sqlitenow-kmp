CREATE TABLE person_address_note
(
    id               INTEGER PRIMARY KEY NOT NULL,
    address_id       INTEGER             NOT NULL,
    -- @@{ field=note, propertyType=kotlin.String }
    note             TEXT                NOT NULL,
    -- @@{ field=created_at, propertyType=kotlinx.datetime.LocalDateTime }
    created_at       TEXT                NOT NULL DEFAULT current_timestamp,

    FOREIGN KEY (address_id) REFERENCES person_address (id) ON DELETE CASCADE
);
