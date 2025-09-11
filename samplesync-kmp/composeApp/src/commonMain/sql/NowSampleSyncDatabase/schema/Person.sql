-- @@{ enableSync=true }
CREATE TABLE person
(
    id         TEXT PRIMARY KEY NOT NULL,
    -- @@{  field=first_name, propertyName=myFirstName }
    first_name TEXT                NOT NULL,
    -- @@{ field=last_name, propertyName=myLastName }
    last_name  TEXT                NOT NULL,

    email      TEXT                NOT NULL UNIQUE,
    phone      TEXT,

    -- @@{ field=birth_date, propertyType=kotlinx.datetime.LocalDate }
    birth_date TEXT,
    -- @@{ field=created_at, propertyType=kotlinx.datetime.LocalDateTime }
    created_at TEXT                NOT NULL DEFAULT current_timestamp,

    ssn        INTEGER,
    -- @@{ field=score, propertyType=Double }
    score      REAL,
    -- @@{ field=is_active, propertyType=Boolean }
    is_active  INTEGER             NOT NULL DEFAULT 1,

    notes      TEXT
);

CREATE INDEX idx_person_name ON person (last_name, first_name);
CREATE INDEX idx_person_email ON person (email);
