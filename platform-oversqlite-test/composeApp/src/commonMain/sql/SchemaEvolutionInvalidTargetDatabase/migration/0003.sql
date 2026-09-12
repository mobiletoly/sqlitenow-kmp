CREATE TABLE projects (
    id TEXT PRIMARY KEY NOT NULL,
    title TEXT NOT NULL
);

ALTER TABLE people ADD COLUMN details TEXT;
ALTER TABLE people ADD COLUMN project_id TEXT;

CREATE TABLE attachments (
    id TEXT PRIMARY KEY NOT NULL,
    person_id TEXT,
    filename TEXT NOT NULL
);
