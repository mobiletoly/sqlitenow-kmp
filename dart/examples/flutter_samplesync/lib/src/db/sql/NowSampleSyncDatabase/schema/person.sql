-- @@{ enableSync=true }
CREATE TABLE person
(
    id         BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),
    -- @@{ field=first_name, propertyName=myFirstName }
    first_name TEXT                NOT NULL,
    -- @@{ field=last_name, propertyName=myLastName }
    last_name  TEXT                NOT NULL,

    email      TEXT                NOT NULL UNIQUE,
    phone      TEXT,

    -- @@{ field=birth_date, adapter=custom, propertyType=DateTime }
    birth_date TEXT,
    -- @@{ field=created_at, adapter=custom, propertyType=DateTime }
    created_at TEXT                NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    -- @@{ field=updated_at, adapter=custom, propertyType=DateTime }
    updated_at TEXT                NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),

    ssn        INTEGER,
    score      REAL,
    -- @@{ field=is_active, adapter=custom, propertyType=bool }
    is_active  INTEGER             NOT NULL DEFAULT 1,

    notes      TEXT
)
WITHOUT ROWID;

CREATE INDEX idx_person_name ON person (last_name, first_name);
CREATE INDEX idx_person_email ON person (email);
