-- @@{ enableSync=true }
CREATE TABLE person
(
    id         BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),
    -- @@{  field=first_name, propertyName=myFirstName }
    first_name TEXT                NOT NULL,
    -- @@{ field=last_name, propertyName=myLastName }
    last_name  TEXT                NOT NULL,

    email      TEXT                NOT NULL UNIQUE,
    phone      TEXT,

    -- @@{ field=birth_date, propertyType=kotlinx.datetime.LocalDate }
    birth_date TEXT,
    -- @@{ field=created_at, propertyType=kotlin.time.Instant }
    created_at TEXT                NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    -- @@{ field=updated_at, propertyType=kotlin.time.Instant }
    updated_at TEXT                NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),

    ssn        INTEGER,
    -- @@{ field=score, propertyType=Double }
    score      REAL,
    -- @@{ field=is_active, propertyType=Boolean }
    is_active  INTEGER             NOT NULL DEFAULT 1,

    notes      TEXT
)
WITHOUT ROWID;

CREATE INDEX idx_person_name ON person (last_name, first_name);
CREATE INDEX idx_person_email ON person (email);
