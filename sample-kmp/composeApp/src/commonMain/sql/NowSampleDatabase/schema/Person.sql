CREATE TABLE Person
(
    id         INTEGER PRIMARY KEY NOT NULL,

    -- @@field=first_name @@propertyName=myFirstName
    first_name TEXT                NOT NULL,

    -- @@field=last_name @@propertyName=myLastName
    last_name  TEXT                NOT NULL,

    email      TEXT                NOT NULL UNIQUE,
    phone      TEXT,

    -- @@field=birth_date @@adapter
    -- @@propertyType=kotlinx.datetime.LocalDate
    birth_date TEXT,

    -- @@field=created_at @@adapter
    -- @@propertyType=kotlinx.datetime.LocalDateTime
    created_at TEXT                NOT NULL DEFAULT current_timestamp,

    -- @@field=notes @@adapter
    -- @@propertyType=dev.goquick.sqlitenow.samplekmp.model.PersonNote
   notes      BLOB
);

CREATE INDEX idx_person_name ON Person (last_name, first_name);
CREATE INDEX idx_person_email ON Person (email);

-- @@name=PersonWithAddressEntity
CREATE VIEW PersonWithAddressView AS
SELECT p.id         AS person_id,
       p.first_name,
       p.last_name,
       p.email,
       p.phone,

       -- @@field=birth_date @@adapter
       -- @@propertyType=kotlinx.datetime.LocalDate
       p.birth_date,

       p.created_at AS person_created_at,
       a.id         AS address_id,
       a.address_type,
       a.street,
       a.city,
       a.state,
       a.postal_code,
       a.country,
       a.is_primary,
       a.created_at AS address_created_at
FROM Person p
         JOIN
     PersonAddress a ON p.id = a.person_id;
