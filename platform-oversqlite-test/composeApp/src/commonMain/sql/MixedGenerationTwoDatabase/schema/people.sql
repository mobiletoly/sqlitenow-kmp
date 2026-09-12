-- @@{enableSync=true}
CREATE TABLE people (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    note TEXT DEFAULT 'server-default',
    project_id TEXT,
    FOREIGN KEY (project_id) REFERENCES projects(id) DEFERRABLE INITIALLY DEFERRED
);
