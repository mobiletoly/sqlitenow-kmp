CREATE TABLE person_address (
    id INTEGER PRIMARY KEY NOT NULL,
    person_id INTEGER NOT NULL,
    street TEXT NOT NULL,
    -- @@{ field=is_primary, propertyType=Boolean }
    is_primary INTEGER NOT NULL
);
