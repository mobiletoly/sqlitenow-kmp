-- @@{enableSync=true}
CREATE TABLE attachments (
    id TEXT PRIMARY KEY NOT NULL,
    person_id TEXT,
    filename TEXT NOT NULL,
    FOREIGN KEY (person_id) REFERENCES people(id) ON DELETE CASCADE
);
