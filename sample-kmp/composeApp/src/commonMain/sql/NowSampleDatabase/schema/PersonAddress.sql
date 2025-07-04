CREATE TABLE PersonAddress
(
    id           INTEGER PRIMARY KEY NOT NULL,
    person_id    INTEGER             NOT NULL,

    -- @@field=address_type @@propertyType=AddressType @@adapter
    address_type TEXT                NOT NULL,

    street       TEXT                NOT NULL,
    city         TEXT                NOT NULL,
    state        TEXT,

    postal_code  TEXT,

    country      TEXT                NOT NULL,

    -- @@field=is_primary @@propertyType=Boolean
    is_primary   INTEGER             NOT NULL DEFAULT 0,

    -- @@field=created_at @@propertyType=kotlinx.datetime.LocalDateTime @@adapter
    created_at   INT                 NOT NULL DEFAULT current_timestamp,

    FOREIGN KEY (person_id) REFERENCES Person (id) ON DELETE CASCADE
);

-- Create indexes for better performance
CREATE INDEX idx_address_person_id ON PersonAddress (person_id);
CREATE INDEX idx_address_primary ON PersonAddress (person_id, is_primary);
