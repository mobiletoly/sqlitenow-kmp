CREATE TABLE projects (
    id TEXT PRIMARY KEY NOT NULL,
    title TEXT NOT NULL
);

ALTER TABLE people ADD COLUMN details TEXT;
ALTER TABLE people ADD COLUMN project_id TEXT REFERENCES projects(id) ON DELETE SET NULL;

CREATE TABLE attachments (
    id TEXT PRIMARY KEY NOT NULL,
    person_id TEXT REFERENCES people(id) ON DELETE CASCADE,
    filename TEXT NOT NULL
);

CREATE INDEX projects_title_lookup ON projects(title);

CREATE VIEW active_project_people AS
SELECT people.id, people.name, projects.title
FROM people JOIN projects ON projects.id = people.project_id;
