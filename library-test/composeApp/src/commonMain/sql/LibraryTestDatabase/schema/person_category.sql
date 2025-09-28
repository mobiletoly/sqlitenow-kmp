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

-- CREATE VIEW for complex collection mapping with hierarchical data
CREATE VIEW person_category_view AS
SELECT
    p.id AS person_id,
    p.first_name,
    p.last_name,
    p.email,
    p.phone,
    p.birth_date,
    p.created_at AS person_created_at,

    c.id AS category_id,
    c.name AS category_name,
    c.description AS category_description,
    c.created_at AS category_created_at,

    pc.assigned_at,
    pc.is_primary,

    pa.id AS address_id,
    pa.address_type,
    pa.street,
    pa.city,
    pa.state,
    pa.postal_code,
    pa.country,
    pa.is_primary AS address_is_primary,
    pa.created_at AS address_created_at

FROM person p
    LEFT JOIN person_category pc ON p.id = pc.person_id
    LEFT JOIN category c ON pc.category_id = c.id
    LEFT JOIN person_address pa ON p.id = pa.person_id;
