-- @@{enableSync=true}
CREATE TABLE files (
    id BLOB PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    data BLOB NOT NULL
);
