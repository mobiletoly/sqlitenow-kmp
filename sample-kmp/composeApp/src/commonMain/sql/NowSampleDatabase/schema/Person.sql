CREATE TABLE person
(
    id         INTEGER PRIMARY KEY NOT NULL,
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
    -- @@{ field=notes, propertyType=dev.goquick.sqlitenow.samplekmp.model.PersonNote }
    notes      BLOB
);

CREATE INDEX idx_person_name ON person (last_name, first_name);
CREATE INDEX idx_person_email ON person (email);
