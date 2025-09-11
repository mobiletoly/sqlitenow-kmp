-- @@{ enableSync=true }}
CREATE TABLE person_address
(
    id           TEXT PRIMARY KEY NOT NULL,
    person_id    TEXT              NOT NULL,

    -- @@{ field=address_type, propertyType=AddressType }
    address_type TEXT                NOT NULL,

    street       TEXT                NOT NULL,
    city         TEXT                NOT NULL,
    state        TEXT,

    postal_code  TEXT,

    country      TEXT                NOT NULL,

    -- @@{ field=is_primary, propertyType=Boolean }
    is_primary   INTEGER             NOT NULL DEFAULT 0,

    -- @@{ field=created_at, propertyType=kotlinx.datetime.LocalDateTime }
    created_at   TEXT                NOT NULL DEFAULT current_timestamp,

    FOREIGN KEY (person_id) REFERENCES person (id) ON DELETE CASCADE
);

-- Create indexes for better performance
CREATE INDEX idx_personaddress_personid ON person_address (person_id);
CREATE INDEX idx_personaddress_personid_isprimary ON person_address (person_id, is_primary);
