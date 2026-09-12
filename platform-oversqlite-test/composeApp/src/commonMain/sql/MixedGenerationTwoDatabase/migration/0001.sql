CREATE TABLE projects (
    id TEXT PRIMARY KEY NOT NULL,
    title TEXT NOT NULL
);

CREATE TABLE notes (
    id TEXT PRIMARY KEY NOT NULL,
    body TEXT NOT NULL
);

CREATE TABLE people (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    note TEXT DEFAULT 'server-default',
    project_id TEXT,
    FOREIGN KEY (project_id) REFERENCES projects(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE sentinel (
    id TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);
