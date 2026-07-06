CREATE TABLE doc (
    id INTEGER PRIMARY KEY NOT NULL,
    -- @@{ field=status, adapter=custom, propertyType=String }
    status TEXT NOT NULL
);
